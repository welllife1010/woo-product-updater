const dotenv = require("dotenv");
dotenv.config();

const express = require("express");
const { performance } = require("perf_hooks"); // Track time for the whole run

const { appRedis } = require("./queue");
const { batchQueue } = require("./queue");
const {
  processReadyCsvFilesFromMappings,
} = require("./s3-helpers");
const {
  logger,
  logErrorToFile,
  logUpdatesToFile,
  logInfoToFile,
  logProgressToFile,
} = require("./logger");
const { createUniqueJobId } = require("./utils");
const { addBatchJob } = require("./job-manager");

const { BullMQAdapter } = require("@bull-board/api/bullMQAdapter");
const { createBullBoard } = require("@bull-board/api");
const { ExpressAdapter } = require("@bull-board/express");

// -------------------- Express setup --------------------
const app = express();
app.use(express.json()); // Needed to parse JSON request bodies

// Determine execution mode and bucket selection
const executionMode = process.env.EXECUTION_MODE || "production";
logInfoToFile(`Running in ${executionMode} mode`);

const getS3BucketName = (executionMode) => {
  return executionMode === "development"
    ? process.env.S3_TEST_BUCKET_NAME
    : process.env.S3_BUCKET_NAME;
};

// -------------------- Bull Board (dev only) --------------------
if (executionMode === "development") {
  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath("/admin/queues");
  app.use("/admin/queues", serverAdapter.getRouter());

  createBullBoard({
    queues: [new BullMQAdapter(batchQueue)],
    serverAdapter: serverAdapter,
  });

  logInfoToFile("✅ Bull Board initialized and batchQueue registered (development only).");
}

// -------------------- Timing & error handling --------------------
const startTime = performance.now();

/**
 * Unified error handler that logs how long the process ran
 * before failing and then exits the Node process.
 */
const handleProcessError = (error, type = "Error") => {
  const duration = ((performance.now() - startTime) / 1000).toFixed(2);
  logger.error(`${type} occurred after ${duration} seconds:`, error);
  process.exit(1);
};

// -------------------- Main process: enqueue jobs for READY CSVs --------------------
const mainProcess = async () => {
  try {
    const s3BucketName = getS3BucketName(executionMode);

    if (!s3BucketName) {
      logErrorToFile("Environment variable S3_BUCKET_NAME (or S3_TEST_BUCKET_NAME) is not set.");
      return;
    }

    logger.info(`Starting process for S3 bucket: ${s3BucketName}`);

    // ✅ NEW: Use the mapping-based flow
    // This will:
    //  - Read csv-mappings.json
    //  - Find files with status: "ready"
    //  - For each fileKey, call readCSVAndEnqueueJobs() with the correct mapping
    await processReadyCsvFilesFromMappings(s3BucketName, 20);

    // ✅ Log completion time
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(2);
    logUpdatesToFile(
      `"processReadyCsvFilesFromMappings" function completed in ${duration} seconds.`
    );
  } catch (error) {
    logErrorToFile(`Unhandled error in mainProcess: ${error.message}`);
    handleProcessError(error, "Unhandled error in mainProcess");
  }
};

// ✅ Start the main process
mainProcess().catch((error) =>
  handleProcessError(error, "Critical error in main")
);

// -------------------- Periodic progress logging --------------------
const progressInterval = setInterval(async () => {
  try {
    const allComplete = await logProgressToFile();

    // If we've confirmed that all files are 100% done,
    // stop the interval from running again.
    if (allComplete) {
      console.log(
        "All files are fully processed. Stopping further progress logs."
      );
      clearInterval(progressInterval);
    }
  } catch (error) {
    console.error(
      `Error during periodic progress logging: ${error.message}`
    );
  }
}, 1 * 60 * 1000); // 1 minute

// -------------------- Manual API trigger for batch job testing --------------------
app.post("/api/start-batch", async (req, res) => {
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

// -------------------- Start Express server --------------------
const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  if (executionMode === "development") {
    console.log(
      "Bull Dashboard is available at http://localhost:3000/admin/queues"
    );
  }
});

// -------------------- Processing-complete check & shutdown --------------------
// Check if all files are processed
const checkAllFilesProcessed = async () => {
  try {
    const fileKeys = await appRedis.keys("total-rows:*");
    for (const key of fileKeys) {
      const fileKey = key.split(":")[1];
      const totalRows = parseInt(
        await appRedis.get(`total-rows:${fileKey}`),
        10
      );
      const updated = parseInt(
        (await appRedis.get(`updated-products:${fileKey}`)) || 0,
        10
      );
      const skipped = parseInt(
        (await appRedis.get(`skipped-products:${fileKey}`)) || 0,
        10
      );
      const failed = parseInt(
        (await appRedis.get(`failed-products:${fileKey}`)) || 0,
        10
      );

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
      appRedis.quit().then(() => process.exit(0));
    });
  }
}, 5000); // Check every 5 seconds

// -------------------- Global error hooks --------------------
process.on("uncaughtException", (error) =>
  handleProcessError(error, "Uncaught exception")
);
process.on("unhandledRejection", (reason) =>
  handleProcessError(reason, "Unhandled rejection")
);
