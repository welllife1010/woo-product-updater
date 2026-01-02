/**
 * =============================================================================
 * FILE: worker.js
 * =============================================================================
 * 
 * PURPOSE:
 * This is the BullMQ worker that processes batch jobs from the queue.
 * Each job contains a batch of CSV rows to process against WooCommerce.
 * 
 * HOW IT WORKS:
 * 1. Worker picks up a job from the Redis queue
 * 2. Extracts batch data and metadata from job.data
 * 3. Calls processBatch() to handle WooCommerce updates
 * 4. Saves checkpoint progress (atomic to prevent race conditions)
 * 5. Marks job as complete and picks up the next one
 * 
 * AUTO-PICKUP FEATURE (2025):
 * When all files finish processing, the worker automatically:
 * 1. Marks completed files in csv-mappings.json
 * 2. Checks for new "ready" files
 * 3. Restarts woo-update-app to pick them up
 * 
 * CONCURRENCY:
 * Multiple workers can run simultaneously (controlled by CONCURRENCY env var).
 * Each worker processes one job at a time, but multiple workers = parallel jobs.
 * 
 * RACE CONDITION FIX (2025):
 * Previously, workers would read `lastProcessedRow` from the checkpoint file
 * to determine where they were in the file. This caused issues:
 * 
 *   Problem: Two workers could read the same checkpoint and process same rows
 *   
 *   OLD FLOW (buggy):
 *     Worker A reads checkpoint → lastProcessedRow = 100
 *     Worker B reads checkpoint → lastProcessedRow = 100  ← SAME VALUE!
 *     Both process rows starting at 100 → DUPLICATE WORK
 * 
 * THE FIX:
 * Now each job carries its own `startIndex` that was assigned when the job
 * was created in s3-helpers.js. Workers use THIS value, not the checkpoint.
 * 
 *   NEW FLOW (fixed):
 *     Job A has { startIndex: 100, batch: rows 100-119 }
 *     Job B has { startIndex: 120, batch: rows 120-139 }
 *     Worker A uses startIndex=100 from job data
 *     Worker B uses startIndex=120 from job data
 *     No overlap, no duplicate work!
 * 
 * =============================================================================
 */

// =============================================================================
// IMPORTS
// =============================================================================

// Load environment variables from .env file
require("dotenv").config();

// Node.js built-ins for auto-pickup feature
const { exec } = require("child_process");
const path = require("path");
const fs = require("fs");

// BullMQ Worker class for processing queue jobs
const { Worker } = require("bullmq");

// Custom logging utilities
const { logErrorToFile, logInfoToFile } = require("./src/utils/logger");

// Redis client for cleanup on shutdown + BullMQ connection details
const {
  appRedis,
  QUEUE_NAME,
  BULLMQ_PREFIX,
  bullmqConnection,
} = require("./src/services/queue");

// Main batch processing function
const { processBatch } = require("./src/batch/process-batch");

// Checkpoint management (with atomic save for race condition fix)
const {
  getLastProcessedRow,        // Sync version (for backward compatibility)
  saveCheckpointAtomic,       // Atomic save to prevent race conditions
} = require("./src/batch/checkpoint");

// =============================================================================
// CONFIGURATION
// =============================================================================

/**
 * CONCURRENCY: How many jobs this worker processes simultaneously.
 * 
 * Higher values = more parallel processing, but also:
 *   - More memory usage
 *   - More API calls to WooCommerce at once
 *   - More potential for rate limiting
 * 
 * Recommended: Start with 2, increase if WooCommerce can handle it.
 * Set via .env: CONCURRENCY=2
 */
const concurrency = parseInt(process.env.CONCURRENCY) || 2;

/**
 * BATCH_SIZE: Number of rows per batch job.
 * This should match what's used in s3-helpers.js when creating jobs.
 * 
 * Set via .env: BATCH_SIZE=20
 */
const batchSize = parseInt(process.env.BATCH_SIZE) || 10;

// Log configuration on startup
logInfoToFile(`🚀 Worker starting with concurrency=${concurrency}, batchSize=${batchSize}`);

// =============================================================================
// AUTO-PICKUP CONFIGURATION & HELPERS
// =============================================================================

/**
 * Cooldown tracking to prevent restart spam.
 * We don't want to restart woo-update-app multiple times in quick succession.
 */
let lastRestartTime = 0;
const RESTART_COOLDOWN_MS = 30000; // 30 seconds between restarts

/**
 * Track which files we've already marked as completed to avoid duplicate work.
 */
const markedCompleted = new Set();

/**
 * Debounce counter for completion checks.
 * We don't want to check after every single job.
 */
let completionCheckCounter = 0;
const COMPLETION_CHECK_INTERVAL = 10; // Check every N job completions

/**
 * Load csv-mappings.json safely.
 * @returns {Object} The mappings data with a files array
 */
function loadMappingsFile() {
  const mappingsPath = path.join(__dirname, "csv-mappings.json");
  
  if (!fs.existsSync(mappingsPath)) {
    return { files: [] };
  }
  
  try {
    const raw = fs.readFileSync(mappingsPath, "utf-8");
    const data = JSON.parse(raw);
    
    // Handle both array and object formats
    if (Array.isArray(data)) {
      return { files: data };
    }
    return data.files ? data : { files: [] };
  } catch (error) {
    logErrorToFile(`Error reading csv-mappings.json: ${error.message}`);
    return { files: [] };
  }
}

/**
 * Save csv-mappings.json safely.
 * @param {Object} data - The mappings data to save
 */
function saveMappingsFile(data) {
  const mappingsPath = path.join(__dirname, "csv-mappings.json");
  
  try {
    fs.writeFileSync(mappingsPath, JSON.stringify(data, null, 2));
  } catch (error) {
    logErrorToFile(`Error writing csv-mappings.json: ${error.message}`);
  }
}

/**
 * Mark a file as completed in csv-mappings.json.
 * @param {string} fileKey - The file key to mark as completed
 */
function markFileAsCompletedInMappings(fileKey) {
  if (markedCompleted.has(fileKey)) {
    return; // Already marked
  }
  
  const mappings = loadMappingsFile();
  const file = mappings.files.find(f => f.fileKey === fileKey);
  
  if (file && file.status !== "completed") {
    file.status = "completed";
    file.completedAt = new Date().toISOString();
    saveMappingsFile(mappings);
    markedCompleted.add(fileKey);
    logInfoToFile(`📋 Marked ${fileKey} as completed in csv-mappings.json`);
  }
}

/**
 * Check if there are any files with status "ready" waiting to be processed.
 * @returns {boolean} True if there are ready files
 */
function hasReadyFiles() {
  const mappings = loadMappingsFile();
  return mappings.files.some(f => f.status === "ready" && f.mapping);
}

/**
 * Get count of ready files for logging.
 * @returns {number} Number of files with status "ready"
 */
function getReadyFilesCount() {
  const mappings = loadMappingsFile();
  return mappings.files.filter(f => f.status === "ready" && f.mapping).length;
}

/**
 * Restart woo-update-app to pick up new ready files.
 * Includes cooldown to prevent restart spam.
 */
function triggerMainAppRestart() {
  const now = Date.now();
  
  // Check cooldown
  if (now - lastRestartTime < RESTART_COOLDOWN_MS) {
    const remaining = Math.ceil((RESTART_COOLDOWN_MS - (now - lastRestartTime)) / 1000);
    logInfoToFile(`⏳ Restart cooldown active, ${remaining}s remaining`);
    return;
  }
  
  const readyCount = getReadyFilesCount();
  
  if (readyCount === 0) {
    logInfoToFile("✅ No more ready files to process. System idle.");
    return;
  }
  
  lastRestartTime = now;
  
  logInfoToFile(`🔄 Found ${readyCount} ready file(s). Restarting woo-update-app to pick them up...`);
  
  exec("pm2 restart woo-update-app", { timeout: 10000 }, (error, stdout, stderr) => {
    if (error) {
      logErrorToFile(`Failed to restart woo-update-app: ${error.message}`);
    } else {
      logInfoToFile("✅ woo-update-app restarted successfully - will pick up next file");
    }
  });
}

/**
 * Check if a specific file is fully processed based on Redis counters.
 * @param {string} fileKey - The file key to check
 * @returns {Promise<{complete: boolean, stats: Object}>} Completion status and stats
 */
async function getFileCompletionStatus(fileKey) {
  try {
    const totalRows = parseInt((await appRedis.get(`total-rows:${fileKey}`)) || "0", 10);
    
    // If no total rows tracked, file isn't being processed
    if (totalRows === 0) {
      return { complete: false, stats: { totalRows: 0, processed: 0 } };
    }
    
    const updated = parseInt((await appRedis.get(`updated-products:${fileKey}`)) || "0", 10);
    const skipped = parseInt((await appRedis.get(`skipped-products:${fileKey}`)) || "0", 10);
    const failed = parseInt((await appRedis.get(`failed-products:${fileKey}`)) || "0", 10);
    
    const processedCount = updated + skipped + failed;
    
    return {
      complete: processedCount >= totalRows,
      stats: { totalRows, updated, skipped, failed, processed: processedCount }
    };
  } catch (error) {
    logErrorToFile(`Error checking file completion: ${error.message}`);
    return { complete: false, stats: {} };
  }
}

// =============================================================================
// COMPLETION LOG DEDUPE (avoid repeating the same "complete" logs when idle)
// =============================================================================

// Tracks which files we've already announced as complete (and with what stats)
// so periodic completion checks don't spam the logs.
const announcedCompletedFileSignatures = new Map(); // fileKey -> signature string

// Tracks whether we've already announced the "all tracked files completed" banner
// for the current tracked set.
let lastTrackedFilesSignature = null;
let allCompleteAnnouncedForSignature = false;

/**
 * Check all currently tracked files and handle completion.
 * - Marks completed files in csv-mappings.json
 * - Triggers restart if there are new ready files
 */
async function checkAndHandleCompletion() {
  try {
    // Get all file keys being tracked in Redis
    const fileKeys = await appRedis.keys("total-rows:*");
    
    if (fileKeys.length === 0) {
      // No files being tracked - check if there are ready files to start
      if (hasReadyFiles()) {
        logInfoToFile("📊 No files in progress but found ready files");
        triggerMainAppRestart();
      }
      return;
    }
    
    // If the tracked set changes (new file starts / cleared / etc.), allow a new
    // "all complete" announcement.
    const trackedFilesSignature = fileKeys.slice().sort().join("|");
    if (trackedFilesSignature !== lastTrackedFilesSignature) {
      lastTrackedFilesSignature = trackedFilesSignature;
      allCompleteAnnouncedForSignature = false;
    }

    let allComplete = true;
    const completedFileKeys = [];
    
    for (const key of fileKeys) {
      const fileKey = key.replace(/^total-rows:/, "");
      const { complete, stats } = await getFileCompletionStatus(fileKey);
      
      if (complete) {
        completedFileKeys.push(fileKey);

        // Only log completion once per file+stats signature to avoid spam when
        // periodic checks run but nothing has changed.
        const signature = `${stats.totalRows}|${stats.updated}|${stats.skipped}|${stats.failed}`;
        const lastSignature = announcedCompletedFileSignatures.get(fileKey);
        if (lastSignature !== signature) {
          announcedCompletedFileSignatures.set(fileKey, signature);
          logInfoToFile(
            `✅ File ${fileKey} complete: ${stats.updated} updated, ` +
            `${stats.skipped} skipped, ${stats.failed} failed`
          );
        }
      } else {
        allComplete = false;

        // If the file is no longer complete (e.g., restarted/cleared/reset),
        // allow a future completion log again.
        if (announcedCompletedFileSignatures.has(fileKey)) {
          announcedCompletedFileSignatures.delete(fileKey);
        }

        // Only log progress occasionally to avoid spam
        if (completionCheckCounter % 50 === 0 && stats.totalRows > 0) {
          logInfoToFile(
            `📊 File ${fileKey}: ${stats.processed}/${stats.totalRows} rows processed`
          );
        }
      }
    }
    
    // Mark completed files in csv-mappings.json
    for (const fileKey of completedFileKeys) {
      markFileAsCompletedInMappings(fileKey);
    }
    
    // If all tracked files are complete, check for new ready files
    if (allComplete && completedFileKeys.length > 0 && !allCompleteAnnouncedForSignature) {
      allCompleteAnnouncedForSignature = true;
      logInfoToFile(`🎉 All ${completedFileKeys.length} tracked file(s) completed!`);
      
      // Small delay to let things settle before checking for more work
      setTimeout(() => {
        if (hasReadyFiles()) {
          triggerMainAppRestart();
        } else {
          logInfoToFile("✅ All processing complete. No more files in queue.");
        }
      }, 3000);
    }
  } catch (error) {
    logErrorToFile(`Error in checkAndHandleCompletion: ${error.message}`);
  }
}

// =============================================================================
// WORKER DEFINITION
// =============================================================================

/**
 * BullMQ Worker instance.
 * 
 * This worker listens to the "batchQueue" and processes jobs as they arrive.
 * The worker function is called for each job with the job object.
 */
const batchWorker = new Worker(
  // Queue name - must match the queue name used when adding jobs
  QUEUE_NAME || "batchQueue",
  
  // Job processor function - called for each job
  async (job) => {
    try {
      // =========================================================================
      // STEP 1: Extract job data
      // =========================================================================
      /**
       * Job data structure (set in s3-helpers.js):
       * {
       *   batch: Array<Object>,        // Array of CSV row objects
       *   fileKey: string,             // S3 key or filename
       *   totalProductsInFile: number, // Total rows in the CSV
       *   startIndex: number,          // ⭐ Starting row index for this batch
       *   batchSize: number,           // Number of rows in this batch
       * }
       */
      const { 
        batch, 
        fileKey, 
        totalProductsInFile, 
        startIndex  // ⭐ RACE CONDITION FIX: Use this instead of reading checkpoint
      } = job.data;

      // =========================================================================
      // STEP 2: Validate job data
      // =========================================================================
      
      // Validate fileKey
      if (!fileKey || typeof fileKey !== "string") {
        logErrorToFile(`❌ Job ${job.id}: Missing or invalid fileKey`);
        throw new Error("Invalid job data: Missing fileKey");
      }

      // Validate totalProductsInFile
      if (!totalProductsInFile || isNaN(totalProductsInFile)) {
        logErrorToFile(`❌ Job ${job.id}: Missing or invalid totalProductsInFile`);
        throw new Error("Invalid job data: Missing totalProductsInFile");
      }

      // Validate batch array
      if (!Array.isArray(batch) || batch.length === 0) {
        logErrorToFile(`❌ Job ${job.id}: Missing or empty batch array`);
        throw new Error("Invalid job data: Missing or empty batch");
      }

      // =========================================================================
      // STEP 3: Determine starting row index
      // =========================================================================
      /**
       * ⭐ RACE CONDITION FIX
       * 
       * OLD CODE (buggy):
       *   let lastProcessedRow = getLastProcessedRow(fileKey);
       *   // Multiple workers could get the same value!
       * 
       * NEW CODE (fixed):
       *   Use startIndex from job.data, which was set at job creation time.
       *   Each job has a unique startIndex, so no overlap is possible.
       * 
       * BACKWARD COMPATIBILITY:
       *   If startIndex is missing (old jobs), fall back to reading checkpoint.
       *   This shouldn't happen for new jobs but handles edge cases.
       */
      let batchStartIndex;
      
      if (typeof startIndex === "number" && startIndex >= 0) {
        // ✅ New behavior: use startIndex from job data
        batchStartIndex = startIndex;
        logInfoToFile(
          `🚀 Job ${job.id}: Using startIndex from job data: ${batchStartIndex}`
        );
      } else {
        // ⚠️ Fallback for old jobs without startIndex
        // This maintains backward compatibility but may have race conditions
        logInfoToFile(
          `⚠️ Job ${job.id}: No startIndex in job data, falling back to checkpoint`
        );
        batchStartIndex = getLastProcessedRow(fileKey);
        logInfoToFile(
          `📌 Job ${job.id}: Retrieved lastProcessedRow=${batchStartIndex} from checkpoint`
        );
      }

      // =========================================================================
      // STEP 4: Log job start
      // =========================================================================
      
      logInfoToFile(
        `🚀 Processing job: ${job.id} | ` +
        `File: ${fileKey} | ` +
        `Rows: ${batchStartIndex}-${batchStartIndex + batch.length - 1} | ` +
        `Batch size: ${batch.length}`
      );

      // =========================================================================
      // STEP 5: Process the batch
      // =========================================================================
      /**
       * processBatch() does the actual work:
       *   - For each row in the batch:
       *     - Find the matching product in WooCommerce
       *     - Compare current vs new data
       *     - Update if needed, skip if no changes
       *   - Bulk update to WooCommerce API
       *   - Update Redis counters (updated, skipped, failed)
       * 
       * Parameters:
       *   - batch: Array of CSV row objects
       *   - batchStartIndex: Starting row number (for logging and tracking)
       *   - totalProductsInFile: Total rows (for progress calculation)
       *   - fileKey: File identifier (for logging and counters)
       */
      await processBatch(batch, batchStartIndex, totalProductsInFile, fileKey);
      
      logInfoToFile(`✅ Job ${job.id}: processBatch() completed`);

      // =========================================================================
      // STEP 6: Calculate ending row index
      // =========================================================================
      /**
       * The ending row is startIndex + number of rows processed.
       * 
       * Example:
       *   startIndex = 100
       *   batch.length = 20
       *   batchEndIndex = 120 (we've processed rows 100-119)
       * 
       * Note: batchEndIndex is EXCLUSIVE (the next batch starts here)
       */
      const batchEndIndex = batchStartIndex + batch.length;

      // Ensure we don't exceed total rows (edge case for last batch)
      const safeEndIndex = Math.min(batchEndIndex, totalProductsInFile);

      // =========================================================================
      // STEP 7: Save checkpoint (atomic to prevent race conditions)
      // =========================================================================
      /**
       * ⭐ RACE CONDITION FIX
       * 
       * OLD CODE (buggy):
       *   await saveCheckpoint(fileKey, batchEndIndex, totalProductsInFile);
       *   // If Worker B (processing later rows) finishes before Worker A,
       *   // Worker A would overwrite with a LOWER value!
       * 
       * NEW CODE (fixed):
       *   await saveCheckpointAtomic(fileKey, batchEndIndex, totalProductsInFile);
       *   // Only updates if batchEndIndex is HIGHER than current value.
       *   // Slower workers can't overwrite faster workers' progress.
       * 
       * Example:
       *   Worker A processes rows 100-120, takes 10 seconds
       *   Worker B processes rows 120-140, takes 5 seconds
       *   
       *   Without atomic save:
       *     Worker B finishes → saves 140
       *     Worker A finishes → saves 120 (OVERWRITES to lower value!)
       *   
       *   With atomic save:
       *     Worker B finishes → saves 140
       *     Worker A finishes → tries 120, but 140 > 120, SKIPPED ✅
       */
      await saveCheckpointAtomic(fileKey, safeEndIndex, totalProductsInFile);

      // =========================================================================
      // STEP 8: Log completion
      // =========================================================================
      
      logInfoToFile(
        `✅ Job ${job.id} completed successfully | ` +
        `Processed rows ${batchStartIndex}-${safeEndIndex - 1} | ` +
        `File: ${fileKey}`
      );

    } catch (error) {
      // =========================================================================
      // ERROR HANDLING
      // =========================================================================
      /**
       * If any error occurs during processing:
       *   1. Log the error
       *   2. Re-throw to trigger BullMQ's retry mechanism
       * 
       * BullMQ will retry failed jobs based on the settings in job-manager.js:
       *   - attempts: 5 (try up to 5 times)
       *   - backoff: exponential (wait longer between each retry)
       */
      logErrorToFile(
        `❌ Job ${job.id} failed: ${error.message}`,
        error.stack
      );
      
      // Re-throw to trigger BullMQ retry
      throw error;
    }
  },
  
  // Worker configuration options
  {
    // Redis connection settings (MUST match queue.js, including TLS/auth/db)
    connection: bullmqConnection,

    // Prefix MUST match queue.js when BULLMQ_PREFIX is customized
    prefix: BULLMQ_PREFIX || "bull",
    
    // How many jobs this worker processes in parallel
    concurrency: concurrency,
  }
);

// =============================================================================
// EVENT HANDLERS
// =============================================================================

/**
 * Handle job completion.
 * Logged for debugging and monitoring.
 * 
 * NEW: After each job completes, periodically check if all files are done.
 * If so, automatically restart to pick up any new ready files.
 */
batchWorker.on("completed", async (job) => {
  logInfoToFile(`✅ Job ${job.id} marked as completed by BullMQ`);
  
  // Increment counter and check completion periodically
  completionCheckCounter++;
  
  if (completionCheckCounter % COMPLETION_CHECK_INTERVAL === 0) {
    // Run async but don't block the event handler
    setImmediate(async () => {
      try {
        await checkAndHandleCompletion();
      } catch (error) {
        logErrorToFile(`Error in completion check: ${error.message}`);
      }
    });
  }
});

/**
 * Handle job failure (after all retries exhausted).
 * 
 * This fires when a job has failed all retry attempts.
 * At this point, the job is moved to the "failed" state in the queue.
 */
batchWorker.on("failed", (job, err) => {
  logErrorToFile(
    `⚠️ Job ${job.id} failed permanently after all retry attempts: ${err.message}`
  );
});

/**
 * Handle worker errors.
 * These are errors in the worker itself, not job-specific errors.
 */
batchWorker.on("error", (error) => {
  logErrorToFile(`❌ Worker error: ${error.message}`, error.stack);
});

// =============================================================================
// PROGRESS TRACKING HELPERS
// =============================================================================

/**
 * Check if all files have been fully processed.
 * 
 * Used during shutdown to determine if it's safe to exit,
 * or if there's still work in progress.
 * 
 * BUG FIX (2025): Improved fileKey extraction
 * 
 * NOTE: worker.js was already using .replace() which is correct.
 * Added explicit regex anchor for consistency and safety.
 * 
 * @returns {Promise<boolean>} True if all files are complete
 */
const checkAllFilesProcessed = async () => {
  try {
    // Get all file keys being tracked in Redis
    const fileKeys = await appRedis.keys("total-rows:*");
    
    // FIX: No files = KEEP WAITING, not "all done"
    // Returning true here causes shutdown → PM2 restart → infinite loop!
    if (fileKeys.length === 0) {
      logInfoToFile("📊 No files being tracked in Redis - waiting for work");
      return false;  // Keep running, don't shutdown
    }
    
    for (const key of fileKeys) {
      // Extract the fileKey from the Redis key pattern
      // Using regex with anchor (^) to only match at start
      const fileKey = key.replace(/^total-rows:/, "");
      
      // Get tracking data for this file
      const totalRows = parseInt((await appRedis.get(`total-rows:${fileKey}`)) || "0", 10);
      const updated = parseInt((await appRedis.get(`updated-products:${fileKey}`)) || "0", 10);
      const skipped = parseInt((await appRedis.get(`skipped-products:${fileKey}`)) || "0", 10);
      const failed = parseInt((await appRedis.get(`failed-products:${fileKey}`)) || "0", 10);

      const processedCount = updated + skipped + failed;

      // If any file has unprocessed rows, we're not done
      if (processedCount < totalRows) {
        logInfoToFile(
          `📊 File ${fileKey}: ${processedCount}/${totalRows} rows processed`
        );
        return false;
      }
    }

    // All files are complete
    return true;
  } catch (error) {
    logErrorToFile(`Error checking processing status: ${error.message}`);
    return false;
  }
};

// =============================================================================
// PERIODIC COMPLETION CHECK (Safety net for auto-pickup)
// =============================================================================

/**
 * Periodic completion check.
 * 
 * Runs every 60 seconds to catch any missed completions.
 * This is a safety net in case the per-job check misses something.
 */
const completionCheckInterval = setInterval(async () => {
  try {
    await checkAndHandleCompletion();
  } catch (error) {
    logErrorToFile(`Error in periodic completion check: ${error.message}`);
  }
}, 60000); // Check every 60 seconds

// =============================================================================
// GRACEFUL SHUTDOWN
// =============================================================================

/**
 * Graceful shutdown handler.
 * 
 * Properly closes connections to prevent data loss:
 *   1. Stop accepting new jobs
 *   2. Wait for current jobs to complete
 *   3. Close Redis connection
 *   4. Exit process
 */
const gracefulShutdown = async () => {
  logInfoToFile("🛑 Received shutdown signal. Cleaning up...");
  
  // Clear the periodic check interval
  clearInterval(completionCheckInterval);

  try {
    // Check if there's still work in progress
    const allProcessed = await checkAllFilesProcessed();
    
    if (!allProcessed) {
      logInfoToFile(
        "⚠️ Not all jobs processed. Progress saved - will resume on restart."
      );
    }

    // Close the worker (stops accepting new jobs, waits for current to finish)
    await batchWorker.close();
    logInfoToFile("✅ Worker closed");

    // Disconnect from Redis
    await appRedis.quit();
    logInfoToFile("✅ Redis connection closed");

    logInfoToFile("✅ Graceful shutdown complete");
    process.exit(0);
    
  } catch (error) {
    logErrorToFile(`❌ Error during shutdown: ${error.message}`);
    process.exit(1);
  }
};

// =============================================================================
// PROCESS SIGNAL HANDLERS
// =============================================================================

/**
 * Handle SIGTERM (sent by process managers like PM2, Kubernetes, etc.)
 */
process.on("SIGTERM", async () => {
  logInfoToFile("Received SIGTERM signal");
  await gracefulShutdown();
});

/**
 * Handle SIGINT (Ctrl+C in terminal)
 */
process.on("SIGINT", async () => {
  logInfoToFile("Received SIGINT signal (Ctrl+C)");
  await gracefulShutdown();
});

/**
 * Handle uncaught exceptions.
 * Log and exit to prevent undefined behavior.
 */
process.on("uncaughtException", (error) => {
  logErrorToFile(`Uncaught exception: ${error.message}`, error.stack);
  process.exit(1);
});

/**
 * Handle unhandled promise rejections.
 * Log and exit to prevent silent failures.
 */
process.on("unhandledRejection", (reason, promise) => {
  logErrorToFile(`Unhandled rejection at: ${promise}, reason: ${reason}`);
  process.exit(1);
});

// =============================================================================
// STARTUP LOG
// =============================================================================

logInfoToFile(
  `🚀 Worker started with AUTO-PICKUP enabled | ` +
  `Concurrency: ${concurrency} | ` +
  `Check interval: every ${COMPLETION_CHECK_INTERVAL} jobs + 60s periodic | ` +
  `Redis: ${process.env.REDIS_HOST || "127.0.0.1"}:${process.env.REDIS_PORT || 6379}`
);