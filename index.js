const dotenv = require("dotenv");
dotenv.config();

const { redisClient } = require('./queue');
const { batchQueue } = require('./queue');
const { getLatestFolderKey, processCSVFilesInS3LatestFolder } = require('./s3-helpers');
const { logger, logErrorToFile,logUpdatesToFile, logInfoToFile, logProgressToFile } = require("./logger");
const { createUniqueJobId } = require('./utils');
const { addBatchJob } = require('./job-manager');
const { performance } = require("perf_hooks"); // Import performance to track time
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter');
const { createBullBoard } = require('@bull-board/api');
const { ExpressAdapter } = require('@bull-board/express');

const express = require('express');
const app = express();
app.use(express.json()); // Needed to parse JSON request bodies

const getS3BucketName = (executionMode) => {
  return (executionMode === 'development') ? process.env.S3_TEST_BUCKET_NAME : process.env.S3_BUCKET_NAME;
};

// Set up BullMQ-compatible Bull Board
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');
app.use('/admin/queues', serverAdapter.getRouter());

createBullBoard({
  queues: [new BullMQAdapter(batchQueue)],
  serverAdapter: serverAdapter,
});

// Confirm Bull Board is receiving jobs
logInfoToFile("✅ Bull Board initialized and batchQueue registered.");

// Start time to track the whole process duration
const startTime = performance.now();

const executionMode = process.env.EXECUTION_MODE || 'production';
logInfoToFile(`Running in ${executionMode} mode`);

// ✅ Optional Bull Board setup, enabled only in development mode
if (executionMode === 'development') {
  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath('/admin/queues');

  app.use('/admin/queues', serverAdapter.getRouter());

  createBullBoard({
    queues: [new BullMQAdapter(batchQueue)],
    serverAdapter: serverAdapter,
  });

  // Confirm Bull Board is receiving jobs
  logInfoToFile("✅ Bull Board initialized and batchQueue registered.");
}

// ✅ Main process function to process CSV files
const mainProcess = async () => {
  try {
    const s3BucketName = getS3BucketName(executionMode);

    if (!s3BucketName) {
      logErrorToFile("Environment variable S3_BUCKET_NAME is not set.");
      return;
    }

    logger.info(`Starting process for S3 bucket: ${s3BucketName}`);

    const bucketName = process.env.S3_TEST_BUCKET_NAME;
    const latestFolder = await getLatestFolderKey(bucketName);
    console.log(`Latest folder detected: ${latestFolder}`);

    // ✅ Process files in the latest S3 folder, enqueuing each batch - second parameter is batch size
    await processCSVFilesInS3LatestFolder(s3BucketName, 20);

    // ✅ Log completion time
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(2);
    logUpdatesToFile(`"processCSVFilesInS3LatestFolder" function completed in ${duration} seconds.`);
  } catch (error) {
    logErrorToFile(`Unhandled error in mainProcess: ${error.message}`);
    handleProcessError(error);
  }
};

// ✅ Start the main process
mainProcess().catch(error => handleProcessError(error, "Critical error in main"));

// ✅ Periodic progress logging
const progressInterval = setInterval(async () => {
  try {
    const allComplete = await logProgressToFile();

    // If we've confirmed that all files are 100% done,
    // stop the interval from running again.
    if (allComplete) {
      console.log("All files are fully processed. Stopping further progress logs.");
      clearInterval(progressInterval);
    }
  } catch (error) {
    console.error(`Error during periodic progress logging: ${error.message}`);
  }
}, 1 * 60 * 1000); // 1 minute

// Unified error handling for process termination
const handleProcessError = (error, type = "Error") => {
  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  logger.error(`${type} occurred after ${duration} seconds:`, error);
  process.exit(1);
};

// ✅ Manual API trigger for batch job testing
app.post('/api/start-batch', async (req, res) => {
  try {
    const batchData = req.body.batchData; // Assuming batch data is passed in the request body

    // ✅ Generate a unique job ID for this batch job
    const jobId = createUniqueJobId();

    // ✅ Use the centralized function to add the batch job
    const job = await addBatchJob({ batch: batchData }, jobId);

    logger.info(`Enqueued batch job with ID: ${job.id}`);
    res.json({ jobId: job.id });
  } catch (error) {
    logErrorToFile(`Failed to enqueue batch: ${error.message}`, error);
    res.status(500).json({ error: "Failed to enqueue batch" });
  }
});

const PORT = process.env.PORT || 3000;
// After setting up your express app:
const server = app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  if (executionMode === 'development') {
    console.log('Bull Dashboard is available at http://localhost:3000/admin/queues');
  }
});

// Check if all files are processed (you can write your own logic)
const checkAllFilesProcessed = async () => {
  try {
    const fileKeys = await redisClient.keys("total-rows:*");
    for (const key of fileKeys) {
      const fileKey = key.split(":")[1];
      const totalRows = parseInt(await redisClient.get(`total-rows:${fileKey}`), 10);
      const updated = parseInt(await redisClient.get(`updated-products:${fileKey}`) || 0, 10);
      const skipped = parseInt(await redisClient.get(`skipped-products:${fileKey}`) || 0, 10);
      const failed = parseInt(await redisClient.get(`failed-products:${fileKey}`) || 0, 10);
      if (updated + skipped + failed < totalRows) {
        return false; // At least one file is still in progress
      }
    }
    return true; // All files are complete
  } catch (error) {
    console.error("Error checking processing status:", error.message);
    return false;
  }
};

// Shutdown check interval for index.js
// - Clears itself so it won’t run again.
// - Logs a message.
// - Closes the Express server.
// - Disconnects the Redis client.
// - Finally calls process.exit(0) to fully terminate the Node process.
const shutdownCheckInterval = setInterval(async () => {
  const allProcessed = await checkAllFilesProcessed();
  if (allProcessed) {
    clearInterval(shutdownCheckInterval);
    console.log("All processing complete in index.js. Shutting down...");
    // Close the Express server
    server.close(() => {
      // Disconnect from Redis
      redisClient.quit().then(() => process.exit(0));
    });
  }
}, 5000); // Check every 5 seconds

process.on('uncaughtException', (error) => handleProcessError(error, "Uncaught exception"));
process.on('unhandledRejection', (reason) => handleProcessError(reason, "Unhandled rejection"));