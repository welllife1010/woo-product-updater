/**
 * =============================================================================
 * FILE: s3-helpers.js
 * =============================================================================
 * 
 * PURPOSE:
 * This file handles all interactions with AWS S3 for CSV file processing.
 * It reads CSV files from S3, splits them into batches, and enqueues jobs
 * for parallel processing by workers.
 * 
 * KEY RESPONSIBILITIES:
 * 1. Discover and list CSV files in S3 bucket
 * 2. Count total rows in each CSV (for progress tracking)
 * 3. Stream CSV data and split into batches
 * 4. Create batch jobs with proper metadata (including startIndex for race-condition fix)
 * 5. Enqueue jobs to Redis/BullMQ queue
 * 
 * RACE CONDITION FIX (2025):
 * Previously, each job would read `lastProcessedRow` from the checkpoint file
 * when it started processing. With multiple concurrent workers, this caused:
 *   - Worker A reads checkpoint: lastProcessedRow = 100
 *   - Worker B reads checkpoint: lastProcessedRow = 100 (same stale value!)
 *   - Both workers process the same rows = duplicate work
 * 
 * THE FIX:
 * Now each job carries its own `startIndex` that is assigned AT JOB CREATION TIME.
 * This means:
 *   - Job 1 is created with startIndex = 0
 *   - Job 2 is created with startIndex = 20
 *   - Job 3 is created with startIndex = 40
 *   - etc.
 * Workers use the startIndex from the job data, not from checkpoint file.
 * 
 * =============================================================================
 */

// =============================================================================
// IMPORTS
// =============================================================================

const fs = require("fs");

// BullMQ queue instance for job management
const { batchQueue } = require("./queue");

// AWS SDK v3 for S3 operations
const {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
} = require("@aws-sdk/client-s3");

// Node.js utilities for streaming
const { promisify } = require("util");
const { Readable, pipeline } = require("stream");

// Convert callback-based pipeline to Promise-based
const streamPipeline = promisify(pipeline);

// CSV parsing library
const csvParser = require("csv-parser");

// Custom logging utilities
const {
  logErrorToFile,
  logUpdatesToFile,
  logInfoToFile,
} = require("./logger");

// Redis client for tracking progress
const { appRedis } = require("./queue");

// Job management utilities
const { addBatchJob } = require("./job-manager");
const { handleError, createUniqueJobId } = require("./utils");

// Checkpoint management (for resume functionality)
const { saveCheckpoint } = require("./checkpoint");

// CSV mapping configuration
const {
  getReadyCsvFiles,
  getMappingForFile,
} = require("./csv-mapping-store");

// =============================================================================
// CONFIGURATION
// =============================================================================

/**
 * Execution mode determines which environment we're running in.
 * Affects which S3 bucket we use and other behavior.
 * Values: "production" | "development" | "test"
 */
const executionMode = process.env.EXECUTION_MODE || "production";

/**
 * CSV_HEADER_ROW: Which row contains the column headers (1-based index)
 * 
 * Some vendor CSVs have metadata rows before the actual headers:
 *   Row 1-9: Vendor info, dates, etc.
 *   Row 10: Actual column headers (Part Number, Manufacturer, etc.)
 *   Row 11+: Data rows
 * 
 * Set this via .env file: CSV_HEADER_ROW=10
 * Default: 10 (to handle vendor template with 9 metadata rows)
 */
const CSV_HEADER_ROW = Number(process.env.CSV_HEADER_ROW || "10");

/**
 * CSV_SKIP_LINES: How many lines to skip before the header row
 * 
 * csv-parser uses 0-based indexing for skipLines:
 *   - If headers are on row 10, we skip 9 lines (rows 1-9)
 *   - If headers are on row 1, we skip 0 lines
 */
const CSV_SKIP_LINES = CSV_HEADER_ROW > 0 ? CSV_HEADER_ROW - 1 : 0;

// =============================================================================
// AWS S3 CLIENT SETUP
// =============================================================================

/**
 * Initialize the S3 client with configuration from environment variables.
 * 
 * Required .env variables:
 *   AWS_REGION_NAME: AWS region (e.g., "us-west-1")
 *   AWS_ENDPOINT_URL: S3 endpoint (e.g., "https://s3.us-west-1.amazonaws.com")
 */
const s3Client = new S3Client({
  region: process.env.AWS_REGION_NAME,
  endpoint: process.env.AWS_ENDPOINT_URL,
  forcePathStyle: true,           // Required for custom endpoints
  requestTimeout: 300000,         // 5 minute timeout for large files
});

/**
 * File pattern to match CSV files based on execution mode.
 * Production: Match specific naming pattern
 * Other modes: Match any CSV file
 */
const pattern =
  executionMode === "production"
    ? /^product-.*\.csv$/i    // Production: files starting with "product-"
    : /\.csv$/i;              // Dev/Test: any CSV file

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

/**
 * Normalize CSV header keys to a consistent format.
 * 
 * Vendor CSVs often have inconsistent header names:
 *   "Part Number" vs "part_number" vs "PART NUMBER" vs " Part # "
 * 
 * This function normalizes all headers to lowercase_underscore format:
 *   "Part Number" → "part_number"
 *   "MANUFACTURER" → "manufacturer"
 *   " Category " → "category"
 * 
 * @param {string} rawKey - The original header name from the CSV
 * @returns {string} - Normalized header name
 */
function normalizeHeaderKey(rawKey) {
  return String(rawKey || "")
    .trim()                      // Remove leading/trailing whitespace
    .toLowerCase()               // Convert to lowercase
    .replace(/\s+/g, "_");       // Replace spaces with underscores
}

/**
 * Get the correct S3 bucket name based on execution mode.
 * 
 * @param {string} mode - "production", "development", or "test"
 * @returns {string} - The S3 bucket name to use
 */
function getS3BucketName(mode) {
  if (mode === "test") {
    return process.env.S3_BUCKET_NAME_TEST;
  }
  return process.env.S3_BUCKET_NAME;
}

// =============================================================================
// REDIS TRACKING INITIALIZATION
// =============================================================================

/**
 * Initialize Redis keys for tracking file processing progress.
 * 
 * Creates these Redis keys for each file:
 *   - total-rows:{fileKey}      → Total rows in the CSV
 *   - updated-products:{fileKey} → Count of successfully updated products
 *   - skipped-products:{fileKey} → Count of skipped products (no changes)
 *   - failed-products:{fileKey}  → Count of failed products (errors)
 * 
 * These counters are incremented by workers as they process each row,
 * enabling real-time progress tracking and accurate completion detection.
 * 
 * @param {string} fileKey - The S3 key or filename being processed
 * @param {number} totalRows - Total number of data rows in the CSV
 */
const initializeFileTracking = async (fileKey, totalRows) => {
  try {
    await appRedis.mSet({
      [`total-rows:${fileKey}`]: String(totalRows),
      [`updated-products:${fileKey}`]: "0",
      [`skipped-products:${fileKey}`]: "0",
      [`failed-products:${fileKey}`]: "0",
    });

    logInfoToFile(
      `✅ Initialized Redis tracking for ${fileKey} (${totalRows} total rows)`
    );
  } catch (error) {
    logErrorToFile(
      `❌ Redis mSet failed in initializeFileTracking: ${error.message}`
    );
  }
};

// =============================================================================
// S3 FILE OPERATIONS
// =============================================================================

/**
 * Get the latest folder in an S3 bucket (by name, assuming date-based naming).
 * 
 * Many vendors organize uploads by date:
 *   /2025-01-15/
 *   /2025-01-16/
 *   /2025-01-17/
 * 
 * This function finds the "latest" folder alphabetically,
 * which works for date-based naming (YYYY-MM-DD sorts correctly).
 * 
 * @param {string} bucketName - The S3 bucket to search
 * @returns {string|null} - The latest folder key, or null if none found
 */
const getLatestFolderKey = async (bucketName) => {
  try {
    const listParams = { Bucket: bucketName, Delimiter: "/" };
    const data = await s3Client.send(new ListObjectsV2Command(listParams));

    if (!data.CommonPrefixes || data.CommonPrefixes.length === 0) {
      logErrorToFile(`No folders found in S3 bucket: ${bucketName}`);
      return null;
    }

    // Sort folders alphabetically and take the last one (latest date)
    const folders = data.CommonPrefixes.map((prefix) => prefix.Prefix).sort();
    const latestFolder = folders[folders.length - 1];

    logInfoToFile(`Latest folder in ${bucketName}: ${latestFolder}`);
    return latestFolder;
  } catch (error) {
    logErrorToFile(`Error getting latest folder: ${error.message}`);
    return null;
  }
};

/**
 * Count the total number of data rows in a CSV file on S3.
 * 
 * This is important for:
 *   1. Progress tracking (X of Y rows processed)
 *   2. Completion detection (all rows processed?)
 *   3. Job creation (knowing when we've reached the end)
 * 
 * Note: We subtract 1 from the line count to account for the header row.
 * 
 * @param {string} bucketName - The S3 bucket
 * @param {string} key - The S3 object key (file path)
 * @returns {number|null} - Total data rows, or null on error
 */
const getTotalRowsFromS3 = async (bucketName, key) => {
  try {
    const params = { Bucket: bucketName, Key: key };
    const data = await s3Client.send(new GetObjectCommand(params));
    const bodyContent = await data.Body.transformToString();

    // Count newlines to get line count, subtract for header
    const lineCount = bodyContent.split("\n").length;
    
    // Account for header row and any trailing newlines
    // If CSV_HEADER_ROW is 10, we have 9 metadata rows + 1 header row = 10 rows to skip
    const dataRows = Math.max(0, lineCount - CSV_HEADER_ROW);

    logInfoToFile(`CSV ${key}: ${lineCount} total lines, ${dataRows} data rows`);
    return dataRows;
  } catch (error) {
    logErrorToFile(`Error counting rows in ${key}: ${error.message}`);
    return null;
  }
};

// =============================================================================
// JOB DUPLICATE DETECTION
// =============================================================================

/**
 * Check if jobs for this file are already in the queue.
 * 
 * Prevents duplicate job creation if:
 *   - Script is run multiple times
 *   - Previous run was interrupted
 *   - Jobs are still being processed
 * 
 * @param {string} fileKey - The S3 key or filename
 * @returns {boolean} - True if jobs already exist, false otherwise
 */
const checkExistingJobs = async (fileKey) => {
  try {
    const jobs = await batchQueue.getJobs(["waiting", "active", "delayed"]);
    const hasExisting = jobs.some((job) => job.data?.fileKey === fileKey);
    
    if (hasExisting) {
      logInfoToFile(`Jobs for ${fileKey} already in queue`);
    }
    
    return hasExisting;
  } catch (error) {
    logErrorToFile(`Error checking existing jobs: ${error.message}`);
    return false;
  }
};

/**
 * Check if a file has been fully processed (based on checkpoint data).
 * 
 * Reads the local checkpoint file to see if all rows have been completed.
 * 
 * @param {string} fileKey - The S3 key or filename
 * @returns {boolean} - True if fully processed, false otherwise
 */
const isFileFullyProcessed = (fileKey) => {
  const checkpointPath = "process_checkpoint.json";

  if (!fs.existsSync(checkpointPath)) {
    return false;
  }

  try {
    const checkpointData = JSON.parse(
      fs.readFileSync(checkpointPath, "utf-8") || "{}"
    );
    
    // Check if remainingRows is 0 (all rows processed)
    return checkpointData[fileKey]?.rowLevel?.remainingRows === 0;
  } catch (error) {
    logErrorToFile(`Error reading checkpoint: ${error.message}`);
    return false;
  }
};

// =============================================================================
// MAIN FUNCTION: readCSVAndEnqueueJobs
// =============================================================================

/**
 * Read a CSV file from S3 and create batch jobs for processing.
 * 
 * =============================================================================
 * RACE CONDITION FIX EXPLANATION
 * =============================================================================
 * 
 * PROBLEM (Before Fix):
 * ---------------------
 * When multiple workers processed jobs concurrently, they would all read
 * `lastProcessedRow` from the checkpoint file. But the checkpoint file
 * is only updated AFTER a batch completes. This caused:
 * 
 *   Time 0: Checkpoint says lastProcessedRow = 100
 *   Time 1: Worker A picks up job, reads checkpoint → starts at row 100
 *   Time 2: Worker B picks up job, reads checkpoint → starts at row 100 (SAME!)
 *   Time 3: Both workers process rows 100-120 → DUPLICATE WORK
 *   Time 4: Worker A finishes, saves checkpoint → 120
 *   Time 5: Worker B finishes, saves checkpoint → 120 (overwrites, no problem here)
 *   
 *   But if Worker B was processing rows 120-140 and saves 140,
 *   then Worker A (slower) saves 120, we LOSE progress!
 * 
 * SOLUTION (After Fix):
 * ---------------------
 * Each job now carries its own `startIndex` that is assigned when the job
 * is CREATED, not when it's processed. The flow is now:
 * 
 *   Job Creation (in this function):
 *     - Job 1: { startIndex: 0,   batch: rows 0-19 }
 *     - Job 2: { startIndex: 20,  batch: rows 20-39 }
 *     - Job 3: { startIndex: 40,  batch: rows 40-59 }
 *   
 *   Job Processing (in worker.js):
 *     - Worker reads startIndex FROM THE JOB DATA
 *     - Worker processes rows [startIndex, startIndex + batch.length)
 *     - Worker saves checkpoint using atomic "max" logic
 * 
 *   This means:
 *     - Every job knows exactly which rows it owns
 *     - Workers never fight over the same rows
 *     - Checkpoint saves use "only update if higher" logic
 * 
 * =============================================================================
 * 
 * @param {string} bucketName - The S3 bucket containing the CSV
 * @param {string} key - The S3 object key (file path)
 * @param {number} batchSize - Number of rows per batch job (default: 20)
 * @returns {Promise<void>}
 */
const readCSVAndEnqueueJobs = async (bucketName, key, batchSize) => {
  logInfoToFile(
    `🚀 readCSVAndEnqueueJobs() called: bucket=${bucketName}, key=${key}, batchSize=${batchSize}`
  );

  // =========================================================================
  // STEP 1: Load column mapping for this file (if configured)
  // =========================================================================
  /**
   * Column mappings allow us to handle vendor CSVs with non-standard headers.
   * 
   * Example: A vendor might use "MPN" instead of "part_number"
   * The mapping tells us: { partNumber: "MPN", manufacturer: "Brand", ... }
   * 
   * We look this up from csv-mappings.json which is configured per-file.
   */
  const mappingEntry = getMappingForFile(key);
  const mapping = mappingEntry?.mapping || null;

  if (!mapping) {
    logInfoToFile(
      `⚠️ No column mapping found for ${key}. ` +
      `Relying on normalized headers (part_number, category, manufacturer).`
    );
  } else {
    logInfoToFile(
      `✅ Using column mapping for ${key}: ` +
      `partNumber="${mapping.partNumber}", ` +
      `category="${mapping.category}", ` +
      `manufacturer="${mapping.manufacturer}"`
    );
  }

  // =========================================================================
  // STEP 2: Count total rows in the CSV
  // =========================================================================
  /**
   * We need to know the total row count for:
   *   - Progress tracking (show "50 of 10000 rows processed")
   *   - Completion detection (all rows done?)
   *   - Passing to jobs so they know the file size
   */
  const totalRows = await getTotalRowsFromS3(bucketName, key);

  if (totalRows === null || totalRows <= 0) {
    logErrorToFile(`❌ Skipping ${key}: Could not count rows or file is empty.`);
    return;
  }

  logInfoToFile(`📊 Total data rows in ${key}: ${totalRows}`);

  // =========================================================================
  // STEP 3: Check if we should skip this file
  // =========================================================================
  
  // 3a. Skip if jobs are already queued for this file
  try {
    const alreadyInQueue = await checkExistingJobs(key);
    if (alreadyInQueue) {
      logInfoToFile(`⚠️ Skipping ${key}: Jobs already in queue.`);
      return;
    }
  } catch (error) {
    logErrorToFile(`❌ Error checking queue: ${error.message}`);
    return;
  }

  // 3b. Skip if file was already fully processed
  try {
    const fileProcessed = isFileFullyProcessed(key);
    if (fileProcessed) {
      logInfoToFile(`✅ Skipping ${key}: Already fully processed.`);
      return;
    }
  } catch (error) {
    logErrorToFile(`❌ Error checking processing status: ${error.message}`);
    return;
  }

  // =========================================================================
  // STEP 4: Initialize Redis tracking for this file
  // =========================================================================
  /**
   * Set up Redis keys to track progress:
   *   - total-rows:{key} = totalRows
   *   - updated-products:{key} = 0
   *   - skipped-products:{key} = 0
   *   - failed-products:{key} = 0
   */
  await initializeFileTracking(key, totalRows);

  // =========================================================================
  // STEP 5: Determine where to resume (if this is a restart)
  // =========================================================================
  /**
   * If the script was interrupted and restarted, we don't want to
   * re-process rows that were already completed.
   * 
   * We check:
   *   1. Completed jobs in the queue (to find highest processed row)
   *   2. Checkpoint file (as a fallback)
   * 
   * NOTE: This resumeFromRow is only used to SKIP creating jobs for
   * rows we've already processed. Each job still carries its own startIndex.
   */
  const completedJobs = await batchQueue.getJobs(["completed"]);
  
  // Find the highest row number from completed jobs for this file
  const completedRowNumbers = completedJobs
    .filter((job) => job.data?.fileKey === key)
    .map((job) => {
      // Jobs have startIndex in their data (after our fix)
      // Or we extract from job ID for legacy jobs
      if (typeof job.data?.startIndex === "number") {
        return job.data.startIndex + (job.data.batch?.length || 0);
      }
      // Legacy: extract from job ID pattern "..._row-{number}"
      const match = job.id?.match(/row-(\d+)/);
      return match ? Number(match[1]) : 0;
    });

  // Resume from the highest completed row (or 0 if no completed jobs)
  let resumeFromRow = completedRowNumbers.length > 0 
    ? Math.max(...completedRowNumbers) 
    : 0;

  // Validate resumeFromRow
  if (isNaN(resumeFromRow) || resumeFromRow < 0) {
    logInfoToFile(`⚠️ Invalid resumeFromRow (${resumeFromRow}), resetting to 0.`);
    resumeFromRow = 0;
  }

  // Check if we've already processed everything
  if (resumeFromRow >= totalRows) {
    logInfoToFile(`✅ All rows in ${key} already processed. Nothing to enqueue.`);
    return;
  }

  logInfoToFile(
    `🚀 Processing ${key} | Resuming from row ${resumeFromRow} | Total: ${totalRows}`
  );

  // =========================================================================
  // STEP 6: Fetch CSV content from S3
  // =========================================================================
  const params = { Bucket: bucketName, Key: key };
  let bodyContent;
  
  try {
    const data = await s3Client.send(new GetObjectCommand(params));
    bodyContent = await data.Body.transformToString();
  } catch (error) {
    logErrorToFile(`❌ Failed to fetch ${key} from S3: ${error.message}`);
    return;
  }

  // =========================================================================
  // STEP 7: Stream and process the CSV
  // =========================================================================
  /**
   * We use streaming to handle large files efficiently:
   *   - Don't load entire file into memory
   *   - Process row-by-row
   *   - Create jobs as we go
   * 
   * IMPORTANT: absoluteRowIndex is the KEY to our race-condition fix.
   * Each job's startIndex is set based on absoluteRowIndex at creation time.
   */

  // Track which row we're currently on (0-based index into DATA rows)
  // This is the absolute position in the file, used to assign startIndex to jobs
  let absoluteRowIndex = 0;

  // Accumulator for building batches
  let batch = [];

  // Track the starting row of the current batch being built
  let currentBatchStartIndex = 0;

  // Get list of existing jobs to avoid duplicates
  const existingJobs = await batchQueue.getJobs([
    "waiting",
    "active",
    "delayed",
    "completed",
    "failed",
  ]);

  // Create a readable stream from the CSV content
  const dataStream = Readable.from(bodyContent);

  try {
    await streamPipeline(
      dataStream,
      // Skip metadata rows before the header (CSV_SKIP_LINES = CSV_HEADER_ROW - 1)
      csvParser({ skipLines: CSV_SKIP_LINES }),
      
      // Process each row as it arrives
      async function* (source) {
        for await (const chunk of source) {
          try {
            // =================================================================
            // STEP 7a: Check if we should skip this row (resuming from crash)
            // =================================================================
            if (absoluteRowIndex < resumeFromRow) {
              // This row was already processed before the crash
              absoluteRowIndex++;
              continue; // Skip to next row
            }

            // =================================================================
            // STEP 7b: Normalize the row's column names
            // =================================================================
            /**
             * Convert all column names to lowercase_underscore format:
             *   "Part Number" → "part_number"
             *   "MANUFACTURER" → "manufacturer"
             */
            const normalizedData = Object.keys(chunk).reduce(
              (acc, rawKey) => {
                const safeKey = normalizeHeaderKey(rawKey);
                acc[safeKey] = chunk[rawKey];
                return acc;
              },
              {} // Start with empty object
            );

            // =================================================================
            // STEP 7c: Apply column mapping (if configured)
            // =================================================================
            /**
             * If this file has a custom mapping, use it to standardize key fields.
             * 
             * Example mapping: { partNumber: "MPN", manufacturer: "Brand" }
             * Result: normalizedData.part_number = normalizedData.mpn
             */
            if (mapping) {
              // Get the normalized key names from the mapping
              const partKeySafe = normalizeHeaderKey(mapping.partNumber);
              const categoryKeySafe = normalizeHeaderKey(mapping.category);
              const manufacturerKeySafe = normalizeHeaderKey(mapping.manufacturer);

              // Copy values to standard field names
              normalizedData.part_number = normalizedData[partKeySafe];
              normalizedData.category = normalizedData[categoryKeySafe];
              normalizedData.manufacturer = normalizedData[manufacturerKeySafe];
            }

            // =================================================================
            // STEP 7d: Add row to current batch
            // =================================================================
            
            // If this is the first row of a new batch, record the start index
            if (batch.length === 0) {
              currentBatchStartIndex = absoluteRowIndex;
            }
            
            batch.push(normalizedData);

            // =================================================================
            // STEP 7e: If batch is full, create a job
            // =================================================================
            if (batch.length >= batchSize) {
              // Create job data with startIndex for race-condition fix
              const jobData = {
                // The actual row data to process
                batch: [...batch], // Clone to avoid mutation issues
                
                // Which file this batch belongs to
                fileKey: key,
                
                // Total rows in the file (for progress calculation)
                totalProductsInFile: totalRows,
                
                // ⭐ RACE CONDITION FIX: startIndex is assigned here at job creation
                // Workers will use this value instead of reading from checkpoint
                startIndex: currentBatchStartIndex,
                
                // How many rows are in this batch
                batchSize: batch.length,
              };

              // Generate unique job ID including the start index
              const jobId = createUniqueJobId(
                key,
                "s3-helper_readCSVAndEnqueueJobs",
                String(currentBatchStartIndex) // Use startIndex in job ID for uniqueness
              );

              // Check for duplicate job
              const isDuplicate = existingJobs.some((job) => job.id === jobId);
              if (isDuplicate) {
                logInfoToFile(`⚠️ Duplicate job ${jobId}, skipping.`);
              } else {
                // Enqueue the job
                try {
                  const job = await addBatchJob(jobData, jobId);
                  
                  if (!job) {
                    throw new Error(`addBatchJob returned null for ${jobId}`);
                  }
                  
                  logInfoToFile(
                    `✅ Job enqueued: ${job.id} | ` +
                    `Rows ${currentBatchStartIndex}-${currentBatchStartIndex + batch.length - 1} | ` +
                    `File: ${key}`
                  );
                } catch (error) {
                  logErrorToFile(
                    `❌ Failed to enqueue job ${jobId}: ${error.message}`
                  );
                }
              }

              // Clear the batch for the next group of rows
              batch = [];
            }

            // Increment the absolute row counter
            absoluteRowIndex++;

          } catch (error) {
            // Log error but continue processing other rows
            logErrorToFile(
              `Error processing row ${absoluteRowIndex} in ${key}: ${error.message}`
            );
            absoluteRowIndex++;
          }
        }

        // =====================================================================
        // STEP 7f: Handle remaining rows (final partial batch)
        // =====================================================================
        /**
         * If the total rows isn't evenly divisible by batchSize,
         * we'll have leftover rows that didn't trigger a job creation.
         * 
         * Example: 105 rows with batchSize=20
         *   - 5 full batches (rows 0-99)
         *   - 1 partial batch (rows 100-104) → handle here
         */
        if (batch.length > 0) {
          const jobData = {
            batch: [...batch],
            fileKey: key,
            totalProductsInFile: totalRows,
            startIndex: currentBatchStartIndex, // ⭐ Race condition fix
            batchSize: batch.length,
          };

          const jobId = createUniqueJobId(
            key,
            "s3-helper_readCSVAndEnqueueJobs_FINAL",
            String(currentBatchStartIndex)
          );

          const isDuplicate = existingJobs.some((job) => job.id === jobId);
          if (isDuplicate) {
            logInfoToFile(`⚠️ Duplicate final job ${jobId}, skipping.`);
          } else {
            try {
              const job = await addBatchJob(jobData, jobId);
              
              if (!job) {
                throw new Error(`addBatchJob returned null for final job ${jobId}`);
              }
              
              logInfoToFile(
                `✅ FINAL job enqueued: ${job.id} | ` +
                `Rows ${currentBatchStartIndex}-${currentBatchStartIndex + batch.length - 1} | ` +
                `File: ${key}`
              );
            } catch (error) {
              logErrorToFile(
                `❌ Failed to enqueue final job ${jobId}: ${error.message}`
              );
            }
          }
        }
      }
    );

    // =========================================================================
    // STEP 8: Log completion
    // =========================================================================
    logUpdatesToFile(
      `✅ Completed reading ${key}: ${absoluteRowIndex} rows processed into jobs`
    );

  } catch (error) {
    logErrorToFile(
      `❌ Error streaming CSV ${key}: ${error.message}`,
      error.stack
    );
  }
};

// =============================================================================
// PROCESS ALL READY CSV FILES
// =============================================================================

/**
 * Process all CSV files listed as "ready" in csv-mappings.json.
 * 
 * This is the main entry point called from index.js.
 * It reads the configuration file to find which CSVs should be processed,
 * then calls readCSVAndEnqueueJobs for each one.
 * 
 * @param {string} bucketName - The S3 bucket to read from
 * @param {number} batchSize - Number of rows per batch job
 */
const processReadyCsvFilesFromMappings = async (bucketName, batchSize) => {
  try {
    // Get list of files marked as "ready" for processing
    const readyFiles = getReadyCsvFiles();

    if (!readyFiles.length) {
      logInfoToFile(
        "No READY CSV files found in csv-mappings.json. Nothing to process."
      );
      return;
    }

    logInfoToFile(`Found ${readyFiles.length} READY CSV files to process.`);

    // Process each file
    for (const fileEntry of readyFiles) {
      const fileKey = fileEntry.fileKey;
      logInfoToFile(`🔄 Processing: ${fileKey}`);
      
      try {
        await readCSVAndEnqueueJobs(bucketName, fileKey, batchSize);
      } catch (error) {
        logErrorToFile(
          `❌ Error processing ${fileKey}: ${error.message}`,
          error.stack
        );
        // Continue with next file even if one fails
      }
    }

    logUpdatesToFile("✅ All READY CSV files have been processed.");
  } catch (error) {
    logErrorToFile(
      `❌ Error in processReadyCsvFilesFromMappings: ${error.message}`,
      error.stack
    );
  }
};

/**
 * Process all CSV files in the latest S3 folder.
 * 
 * Alternative to processReadyCsvFilesFromMappings - uses folder-based discovery
 * instead of configuration file.
 * 
 * @param {string} bucketName - The S3 bucket to read from
 * @param {number} batchSize - Number of rows per batch job
 */
const processCSVFilesInS3LatestFolder = async (bucketName, batchSize) => {
  try {
    // Find the latest folder (by date naming convention)
    const latestFolder = await getLatestFolderKey(bucketName);
    
    if (!latestFolder) {
      logErrorToFile(`No folders found in bucket: ${bucketName}`);
      return;
    }

    logInfoToFile(`📂 Processing files in latest folder: ${latestFolder}`);
    
    // List all files in the folder
    const listParams = { Bucket: bucketName, Prefix: latestFolder };
    const listData = await s3Client.send(new ListObjectsV2Command(listParams));

    if (!listData.Contents) {
      logErrorToFile(`No files found in folder: ${latestFolder}`);
      return;
    }

    // Filter for CSV files only
    const csvFiles = listData.Contents.filter((file) =>
      file.Key.toLowerCase().endsWith(".csv")
    );

    logInfoToFile(`Found ${csvFiles.length} CSV files in ${latestFolder}`);

    if (csvFiles.length === 0) {
      logErrorToFile(`No CSV files found in folder: ${latestFolder}`);
      return;
    }

    // Process each CSV file
    const processingTasks = csvFiles.map(async (file) => {
      try {
        logInfoToFile(`🔄 Processing: ${file.Key}`);
        await readCSVAndEnqueueJobs(bucketName, file.Key, batchSize);
      } catch (error) {
        logErrorToFile(
          `❌ Error processing ${file.Key}: ${error.message}`,
          error.stack
        );
      }
    });

    await Promise.all(processingTasks);
    logUpdatesToFile("✅ All CSV files in latest folder have been processed.");
  } catch (error) {
    logErrorToFile(
      `❌ Error in processCSVFilesInS3LatestFolder: ${error.message}`,
      error.stack
    );
  }
};

// =============================================================================
// EXPORTS
// =============================================================================

module.exports = {
  // Folder discovery
  getLatestFolderKey,
  
  // Main processing functions
  processCSVFilesInS3LatestFolder,
  processReadyCsvFilesFromMappings,
  readCSVAndEnqueueJobs,
  
  // Utility functions (exported for testing)
  normalizeHeaderKey,
  getTotalRowsFromS3,
  checkExistingJobs,
  isFileFullyProcessed,
  initializeFileTracking,
};
