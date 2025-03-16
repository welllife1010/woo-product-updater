const fs = require("fs");
const { batchQueue } = require('./queue'); // Bull Queue instance
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const { promisify } = require("util");
const { Readable, pipeline } = require("stream"); // Promisify the stream pipeline utility
const streamPipeline = promisify(pipeline); // Use async pipeline with stream promises
const csvParser = require("csv-parser");
const { logErrorToFile, logUpdatesToFile, logInfoToFile } = require("./logger");
const { redisClient } = require('./queue');
const { addBatchJob } = require('./job-manager');
const { handleError, createUniqueJobId } = require('./utils');
const { saveCheckpoint } = require('./checkpoint'); 

const executionMode = process.env.EXECUTION_MODE || 'production';

const initializeFileTracking = async (fileKey, totalRows) => {
  try {
    await redisClient.mSet({
      [`total-rows:${fileKey}`]: String(totalRows), // Store totalRows as a string to avoid Redis type issues
      [`updated-products:${fileKey}`]: "0",
      [`skipped-products:${fileKey}`]: "0",
      [`failed-products:${fileKey}`]: "0",
    });

    logInfoToFile(`✅ Debug: Successfully initialized tracking in Redis for ${fileKey}`);
  } catch (error) {
    logErrorToFile(`❌ Debug: Redis mSet failed in initializeFileTracking: ${error.message}`);
  }
};

// ✅ **** AWS S3 setup (using AWS SDK v3) ****
const s3Client = new S3Client({ 
  region: process.env.AWS_REGION_NAME,
  endpoint: process.env.AWS_ENDPOINT_URL, // Use specific bucket's region
  forcePathStyle: true, // This helps when using custom endpoints
  requestTimeout: 300000 // Set timeout to 10 minutes
});

const pattern = (executionMode === 'production') ? /^\d{2}-\d{2}-\d{4}\/$/ : /^\d{1,2}-\d{1,2}-\d{4}(-test)?\/?$/;

// Get the latest folder key (name) by sorting folders by date
const getLatestFolderKey = async (bucketName) => {
  try {
    const listParams = { Bucket: bucketName, Delimiter: '/' }; // Delimit by "/" to get folders
    const listData = await s3Client.send(new ListObjectsV2Command(listParams));

    if (!listData.CommonPrefixes) {
      logErrorToFile(`No folders found in bucket: ${bucketName}.`);
      return null;
    }

    // Extract folder names and trim spaces
    const rawFolders = (listData.CommonPrefixes || []).map(prefix => prefix.Prefix.trim());

    // 📌 **Log what AWS is returning**
    logInfoToFile(`✅ Raw S3 Folders from AWS: ${JSON.stringify(rawFolders)}`);

    // Apply regex pattern and extract valid folders
    const folders = rawFolders
      .filter(prefix => pattern.test(prefix)) // Match only valid folders
      .sort((a, b) => {
        // 📌 **Convert "MM-DD-YYYY" to Date for sorting**
        const dateA = new Date(a.slice(0, 10)); 
        const dateB = new Date(b.slice(0, 10));

        return dateB - dateA; // Sort newest to oldest
      });

    // 📌 **Log filtered folders**
    logInfoToFile(`✅ Filtered valid folders: ${JSON.stringify(folders)}`);

    // If no valid folders, log and return
    if (folders.length === 0) {
      logErrorToFile(`❌ No valid folders found in the bucket: ${bucketName}.`);
      return null;
    }

    // Log and return the latest folder
    logInfoToFile(`🚀 Selecting latest folder: ${folders[0]}`);

    return folders[0]; // Return the most recent folder
  } catch (error) {
    handleError(error, "getLatestFolderKey");
    return null;
  }
};

// Process CSV files within the latest folder
const processCSVFilesInS3LatestFolder = async (bucketName, batchSize) => {
  try {
    const latestFolder = await getLatestFolderKey(bucketName);
    if (!latestFolder) {
      logErrorToFile("No latest folder found, exiting.");
      return;
    }

    logInfoToFile(`📂 Processing files in the latest folder: ${latestFolder}`);
    const listParams = { Bucket: bucketName, Prefix: latestFolder };
    const listData = await s3Client.send(new ListObjectsV2Command(listParams));

    if (!listData.Contents) {
      logErrorToFile(`❌ No contents found in folder: ${latestFolder} of bucket: ${bucketName}`);
      return;
    }

    const csvFiles = listData.Contents.filter((file) => file.Key.toLowerCase().endsWith(".csv"));

    // 🚀 Log all CSV files being processed
    logInfoToFile(`Retrieved ${csvFiles.length} CSV files in folder: ${latestFolder}`);
    csvFiles.forEach(file => logInfoToFile(`Found file: ${file.Key}`));

    if (csvFiles.length === 0) {
      logErrorToFile(`❌ No CSV files found in folder: ${latestFolder} of bucket: ${bucketName}`);
      return;
    }

    const fileProcessingTasks = csvFiles.map(async (file) => {
      try {
          logInfoToFile(`🔄 Processing file: ${file.Key}`);
          await readCSVAndEnqueueJobs(bucketName, file.Key, batchSize);
      } catch (error) {
          logErrorToFile(`❌ 'processCSVFilesInS3LatestFolder()' - Error processing file ${file.Key}. Error: ${error.message}`, error.stack);
      }        
    });

    await Promise.all(fileProcessingTasks); // Wait for all files to process
    logUpdatesToFile("✅ 'processCSVFilesInS3LatestFolder()' - All CSV files in the latest folder have been read.");
  } catch (error) {
    logErrorToFile(`❌ 'processCSVFilesInS3LatestFolder()' - Error for bucket "${bucketName}": ${error.message}`, error.stack);
  }
};

// ✅ **Get Total Rows Directly From S3 (No Redis)**
const getTotalRowsFromS3 = async (bucketName, key) => {
  try {

    logInfoToFile(`🚀 Debug: getTotalRowsFromS3 called with bucketName=${bucketName}, key=${key}`);

    const data = await s3Client.send(new GetObjectCommand({ Bucket: bucketName, Key: key }));
    // ✅ Check if data and Body are valid
    if (!data || !data.Body) {
      logErrorToFile(`❌ Debug: getTotalRowsFromS3 - No data or Body received for file ${key}`);
      return 0;
    }

    const bodyContent = await data.Body.transformToString();
    // ✅ Check if bodyContent is a valid string
    if (!bodyContent) {
      logErrorToFile(`❌ Debug: getTotalRowsFromS3 - Empty content received for file ${key}`);
      return 0;
    }

    const rows = bodyContent.split('\n').filter(row => row.trim() !== ''); // Remove empty lines
    const totalRows = rows.length - 1; // Exclude header row

    // Ensure totalRows is an integer
    if (!Number.isInteger(totalRows) || totalRows < 0) {
      logErrorToFile(`❌ Invalid totalRows detected in getTotalRowsFromS3 for ${key}: ${JSON.stringify(totalRows)}`);
      return 0;
    }

    logInfoToFile(`✅ getTotalRowsFromS3: File ${key} has ${totalRows} rows.`);
    return totalRows;

  } catch (error) {
    logErrorToFile(`❌ Failed to fetch totalRows for ${key} from S3: ${error.message}`);
    return null;
  }
};

// ✅ **Check for Existing Jobs Across All States**
const checkExistingJobs = async (fileKey) => {
  const jobStates = ["waiting", "active", "delayed"];
  const jobs = await batchQueue.getJobs(jobStates);

  return jobs.some(job => job.data.fileKey === fileKey);
};

// ✅ **Check if File is Fully Processed**
const isFileFullyProcessed = (fileKey) => {
  // ✅ Ensure checkpoint file exists before reading
  if (!fs.existsSync("process_checkpoint.json")) {
    logInfoToFile(`⚠️ process_checkpoint.json not found. Creating a new one.`);
    fs.writeFileSync("process_checkpoint.json", JSON.stringify({}, null, 2));
    return false; // No checkpoints yet, so assume not processed
  }

  const checkpointData = JSON.parse(fs.readFileSync("process_checkpoint.json", "utf-8") || "{}");
  return checkpointData[fileKey]?.rowLevel?.remainingRows === 0;
};

// ✅ **Read CSV from S3 and enqueue jobs**
const readCSVAndEnqueueJobs = async (bucketName, key, batchSize) => {

  logInfoToFile(`🚀 Debug: 'readCSVAndEnqueueJobs()' called with bucketName=${bucketName}, key=${key}, batchSize=${batchSize}`);

  let totalRows = await getTotalRowsFromS3(bucketName, key);

  logInfoToFile(`🚀 Debug: 'readCSVAndEnqueueJobs()' - 'getTotalRowsFromS3()' Total rows fetched from S3 for ${key}: ${totalRows}`);

  if (totalRows === null || totalRows <= 0) {
    logErrorToFile(`❌ Skipping ${key} due to S3 read error.`);
    return;
  }

  // ✅ Check if the file has already been processed or exists in Redis
  try {
    const alreadyInQueue = await checkExistingJobs(key);
    logInfoToFile(`✅ Debug: checkExistingJobs returned ${alreadyInQueue} for ${key}`);
    if (alreadyInQueue) {
      logInfoToFile(`⚠️ Debug: ${key} is already in queue. Skipping execution.`);
      return;
    }
  } catch (error) {
    logErrorToFile(`❌ Debug: checkExistingJobs threw an error: ${error.message}`);
    return;
  }

  // ✅ Check if the file is already fully processed
  try {
    const fileProcessed = isFileFullyProcessed(key);
    logInfoToFile(`✅ Debug: isFileFullyProcessed returned ${fileProcessed} for ${key}`);
    if (fileProcessed) {
      logInfoToFile(`✅ Debug: ${key} is already fully processed. Skipping execution.`);
      return;
    }
  } catch (error) {
    logErrorToFile(`❌ Debug: isFileFullyProcessed threw an error: ${error.message}`);
    return;
  }

  logInfoToFile(`✅ Debug: Passed all checks in readCSVAndEnqueueJobs for ${key}, proceeding with initialization.`);

  // ✅ Initialize tracking for this file in Redis
  try {
    await initializeFileTracking(key, totalRows);
    logInfoToFile(`✅ Debug: initializeFileTracking completed for ${key}`);
  } catch (error) {
    logErrorToFile(`❌ Debug: initializeFileTracking threw an error: ${error.message}`);
    return;
  }

  logInfoToFile(`🚀 Processing file: ${key} | Total Rows: ${totalRows} | Checkpoints set up in Redis`);
  
  // ✅ **Check for Duplicate Jobs Across All States**
  const allExistingJobs = await batchQueue.getJobs(["waiting", "active", "delayed", "completed", "failed"]);

  // ✅ **Find the Last Processed Row by Checking Existing Jobs**
  const completedJobs = await batchQueue.getJobs(["completed"]);
  const existingJobNumbers = completedJobs.map(job => {
      const match = job.id.match(/row-(\d+)/);
      return match ? Number(match[1]) : 0;
  });
  let lastProcessedRow = existingJobNumbers.length > 0 ? Math.max(...existingJobNumbers) : 0;

  // ✅ Ensure lastProcessedRow is a valid number
  if (isNaN(lastProcessedRow) || lastProcessedRow === null || lastProcessedRow === undefined || lastProcessedRow < 0) {
      logErrorToFile(`❌ Debug: Invalid lastProcessedRow detected: ${JSON.stringify(lastProcessedRow)}. Resetting to 0.`);
      lastProcessedRow = 0;
  }

  // ✅ **Reset lastProcessedRow if no jobs exist**
  if (existingJobNumbers.length === 0) {
    logInfoToFile(`⚠️ No existing jobs found, resetting lastProcessedRow to 0.`);
    lastProcessedRow = 0;  // Ensure we start from the beginning if no jobs exist
  }

  // ✅ **Check if All Rows Have Been Processed**
  const totalCompleted = await redisClient.get(`updated-products:${key}`) || 0;
  const totalSkipped = await redisClient.get(`skipped-products:${key}`) || 0;
  const totalFailed = await redisClient.get(`failed-products:${key}`) || 0;
  const totalProcessed = parseInt(totalCompleted) + parseInt(totalSkipped) + parseInt(totalFailed);

  if (totalProcessed >= totalRows) {
      logInfoToFile(`✅ All rows in ${key} have been processed. Resetting lastProcessedRow.`);
      lastProcessedRow = 0;
  }

  logInfoToFile(`🚀 Processing ${key} | LastProcessedRow: ${lastProcessedRow} | Total Rows: ${totalRows}`);

  const remainder = lastProcessedRow % batchSize;
  const nextBatchStart = remainder === 0 ? lastProcessedRow : lastProcessedRow + (batchSize - remainder);

  if (nextBatchStart > totalRows) {
    if (lastProcessedRow < totalRows) {
      logInfoToFile(`⚠️ Small file detected (${totalRows} rows), adjusting batch processing.`);
    } else {
        logInfoToFile(`✅ Reached the end of file ${key}. No more jobs to enqueue.`);
        return;
    }
  }

  if (totalRows <= batchSize) {
    logInfoToFile(`⚠️ Small file detected (${totalRows} rows), forcing job enqueueing.`);
    lastProcessedRow = 0;
  }

  // ✅ **Generate Unique Job ID**
  const jobId = `jobId_${key}_row-${nextBatchStart}`;

  // ✅ **Check if Job is Already Queued**
  const activeJobs = await batchQueue.getJobs(["waiting", "active", "completed"]);
  const isDuplicate = activeJobs.some(job => job.id === jobId);
  if (isDuplicate) {
      logInfoToFile(`⚠️ Duplicate job detected: ${jobId}, skipping.`);
      return;
  }
  
  try {
    // ✅ Fetch CSV Data from S3
    const params = { Bucket: bucketName, Key: key };
    const data = await s3Client.send(new GetObjectCommand(params));
    const bodyContent = await data.Body.transformToString();
    let batch = [];

    await redisClient.set(`total-rows:${key}`, totalRows); // Store individual file's row count
    await redisClient.incrBy('overall-total-rows', totalRows); // Increment the overall total row count

    // Create a data stream for further row-level processing
    const dataStream = Readable.from(bodyContent);

    //  Use dataStream for processing rows
    await streamPipeline(
      dataStream,
      csvParser(),
      // Iterates over each row in the CSV asynchronously, allowing us to handle each chunk (row) as it arrives, without waiting for the entire file to load.
      async function* (source) {
        logInfoToFile(`Processing CSV: ${key}, on row ${lastProcessedRow + 1} / ${totalRows}`);

        for await (const chunk of source) {
          try {
            lastProcessedRow++;

            // Convert each row to an object with lowercase keys
            const normalizedData = Object.keys(chunk).reduce((acc, rawKey) => {
              const safeKey = rawKey.trim().toLowerCase().replace(/\s+/g, "_");
              acc[safeKey] = chunk[rawKey];
              return acc;
            }, {});
  
            batch.push(normalizedData);
  
            // Check if the batch size is reached
            if (batch.length >= batchSize) {

              // ✅ **Create Job Data**
              const jobData = {
                batch,
                fileKey: key,
                totalProductsInFile: totalRows,
                batchSize: batch.length
              };

              // Generate a unique jobId with row index
              const jobId = createUniqueJobId(key, "s3-helper_readCSVAndEnqueueJobs", lastProcessedRow);

              // ✅ **Check for duplicate job**
              if (allExistingJobs.some(job => job.id === jobId)) {
                logInfoToFile(`⚠️ Duplicate job detected: ${jobId}, skipping.`);
                return;
              }

              logInfoToFile(`🚀 Attempting to enqueue job: ${jobId} | File: ${key}`);
              
              // ✅ **Add Job to Queue**
              try {
                  const job = await addBatchJob(jobData, jobId);
                  if (!job) throw new Error(`❌ batchQueue.add() returned null/undefined for job ${jobId}`);
                  logInfoToFile(`✅ Job enqueued: ${job.id} | Rows: ${batch.length} | File: ${key}`);
              } catch (error) {
                logErrorToFile(`❌ batchQueue.add() failed for job ${jobId}. Error: ${error.message}`, error.stack);
              }         

              // ✅ **Save Progress to `process_checkpoint.json`**
              await saveCheckpoint(key, lastProcessedRow, totalRows);

              // Clear the batch after processing
              batch = [];  
            }
            
          } catch (error) {
            // Detailed error logging
            if (error.code === 'ENOTFOUND' || error.code === 'ECONNRESET') {
              logErrorToFile(`Network error: ${error.message}`, error.stack);
            } else if (error.name === 'CSVError') {  // Assuming csv-parser throws errors with name 'CSVError'
                logErrorToFile(`CSV parsing error at row ${totalRows} in file "${key}": ${error.message}`, error.stack);
            } else {
                logErrorToFile(`Error processing row ${totalRows} in file "${key}: ${error.message}"`, error.stack);
            }
          };
        }

        // If any rows remain in the batch after processing all rows
        if (batch.length > 0) {
          // Create a final job for any remaining data with totalRows included
          const jobData = {
            batch,
            fileKey: key,
            totalProductsInFile: totalRows, // Add totalRows to job data
            lastProcessedRow,
            batchSize: batch.length
          };

          const jobId = createUniqueJobId(key, "s3-helper_readCSVAndEnqueueJobs", String(lastProcessedRow));

          try {
            const existingFinalJob = activeJobs.some(job => job.id === jobId);
            if (existingFinalJob) {
                logInfoToFile(`⚠️ Final batch job already exists: ${jobId}, skipping.`);
                return;
            }

            // Use the centralized function to add the batch job
            const job = await addBatchJob(jobData, jobId);
            
            if (!job) throw new Error(`❌ batchQueue.add() returned null/undefined for job ${jobId}`);
        
            logInfoToFile(`✅ Job enqueued: ${job.id} | Rows: ${batch.length} | File: ${key}`);
        
            logInfoToFile(`Enqueued FINAL batch job for rows up to ${lastProcessedRow} in file: ${key}`);
            logInfoToFile(`DEBUG: Enqueued batch job with ID: ${job.id} for rows up to ${lastProcessedRow} in file: ${key}`);
        
            // ✅ **Save Progress to `process_checkpoint.json`**
            await saveCheckpoint(key, lastProcessedRow, totalRows);
          } catch (error) {
              logErrorToFile(`❌ Failed to enqueue final batch job for rows up to ${lastProcessedRow} in file: ${key}. Error: ${error.message}`, error.stack);
          }
        }
      }
    );

    logUpdatesToFile(`Completed reading the file: "${key}", total rows: ${totalRows}`);
  } catch (error) {
    try {
      handleError(error, `readCSVAndEnqueueJobs for ${key}`);
      logErrorToFile(`🛑 Debug: Invalid argument received in readCSVAndEnqueueJobs: ${JSON.stringify(error)}`);
      throw error; // Ensure any error bubbles up to be caught in Promise.all
    } catch (error) {
        logErrorToFile(`❌ Unexpected error: ${error.message}`, error.stack);
    }
  } 
};

module.exports = {
  getLatestFolderKey,
  processCSVFilesInS3LatestFolder,
  readCSVAndEnqueueJobs
};