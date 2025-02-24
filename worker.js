require("dotenv").config();
const { performance } = require("perf_hooks");
const { Worker } = require("bullmq");
const { logErrorToFile, logInfoToFile } = require("./logger");
const { redisClient } = require('./queue');
const { processBatch } = require('./batch-helpers'); 
const { getCheckpoint, saveCheckpoint } = require("./checkpoint");

// Initialize dynamic concurrency and batch size
let concurrency = parseInt(process.env.CONCURRENCY) || 2;
let batchSize = parseInt(process.env.BATCH_SIZE) || 10;

// ✅ Create BullMQ Worker
const batchWorker = new Worker(
    "batchQueue",
    async (job) => {
        try {
            const { batch, fileKey, totalProductsInFile } = job.data;

            if (!fileKey || typeof fileKey !== "string") {
                logErrorToFile(`❌ Job ${job.id} is missing a valid fileKey`);
                throw new Error("Invalid job data: Missing fileKey");
            }

            if (!totalProductsInFile || isNaN(totalProductsInFile)) {
                logErrorToFile(`❌ Job ${job.id} is missing a valid totalProductsInFile`);
                throw new Error("Invalid job data: Missing totalProductsInFile");
            }

            if (!Array.isArray(batch) || batch.length === 0) {
                logErrorToFile(`❌ Job ${job.id} has no valid 'batch' array`);
                throw new Error("Invalid job data: Missing batch");
            }

            //logInfoToFile(`batch: ${batch} | batchSize: ${batchSize} | fileKey: ${fileKey} | totalProductsInFile: ${totalProductsInFile}`);
            logInfoToFile(`🚀 "batchWorker" - Processing job: ${job.id} | File: ${fileKey}`);

            if (batch.length === 0) {
                logErrorToFile(`❌ Job ${job.id} has an empty batch. Something went wrong.`);
            }

            // 1) Get the current checkpoint from local file for this fileKey.
            let lastProcessedRow = getCheckpoint(fileKey);
            logInfoToFile(`📌 Last processed row (local file): ${lastProcessedRow} for fileKey=${fileKey}`);

            // 2) Process the batch
            //    The processBatch function can do the WooCommerce updates, etc.
            //    Note: processBatch may internally increment "updated"/"skipped"/"failed" in Redis.
            await processBatch(batch, lastProcessedRow, totalProductsInFile, fileKey);

            // 3) After successfully process the batch, update our lastProcessedRow.
            //    For example, if we processed 20 rows in this batch:
            let processedCount = batch.length;
            let updatedLastProcessedRow = lastProcessedRow + processedCount;
            updatedLastProcessedRow = Math.min(updatedLastProcessedRow, totalProductsInFile);

            // 4) Save new checkpoint to local file
            //    This will also optionally record row-level counters from Redis if you want.
            await saveCheckpoint(fileKey, updatedLastProcessedRow, totalProductsInFile);

            logInfoToFile(`✅ Job ${job.id} completed successfully. lastProcessedRow now = ${updatedLastProcessedRow}`);
        } catch (error) {
            logErrorToFile(`❌ Job ${job.id} failed: ${error.message}`);
            throw error;
        }
    },
    {
        connection: {
            host: process.env.REDIS_HOST || "127.0.0.1",
            port: process.env.REDIS_PORT || 6379,
        },
        concurrency, // Set concurrency for parallel job processing if desired
    }
);

// ✅ Handle job failures
batchWorker.on("failed", (job, err) => {
    logErrorToFile(`⚠️ Job ${job.id} failed after all attempts: ${err.message}`);
});

// Graceful shutdown handling
const gracefulShutdown = async () => {
    console.log("Received shutdown signal. Cleaning up...");
    clearInterval(shutdownCheckInterval);
    try {
        const allProcessed = await checkAllFilesProcessed();
        if (!allProcessed) {
            console.log("Not all jobs processed. Progress will resume on restart.");
        }
        await redisClient.quit(); // Disconnect from Redis
        console.log("Shutdown complete.");
    } catch (error) {
        console.error("Error during shutdown:", error.message);
    } finally {
        process.exit(0);
    }
};

process.on("SIGINT", gracefulShutdown); // Handle Ctrl+C
process.on("SIGTERM", gracefulShutdown); // Handle termination signals

// Check if all files have been processed
const checkAllFilesProcessed = async () => {
    try {
        const fileKeys = await redisClient.keys("total-rows:*"); // Get all file keys for processing

        for (const key of fileKeys) {
            const fileKey = key.split(":")[1];
            const totalRows = parseInt(await redisClient.get(`total-rows:${fileKey}`), 10);
            const successfulUpdates = parseInt(await redisClient.get(`updated-products:${fileKey}`) || 0, 10);
            const failedUpdates = parseInt(await redisClient.get(`failed-products:${fileKey}`) || 0, 10);
            const skippedUpdates = parseInt(await redisClient.get(`skipped-products:${fileKey}`) || 0, 10);

            // ✅ STOP if some rows are still processing
            if (successfulUpdates + failedUpdates + skippedUpdates < totalRows) {
                return false; // Processing still ongoing
            }
        }
        return true; // ✅ All files processed
    } catch (error) {
        logErrorToFile(`Error checking file processing status: ${error.message}`);
        return false;
    }
};

// ✅ Shutdown check loop
const shutdownCheckInterval = setInterval(async () => {
    const allProcessed = await checkAllFilesProcessed();
    if (allProcessed) {
        clearInterval(shutdownCheckInterval); // Stop checking
        console.log("All products across all files processed. Shutting down gracefully...");
        process.exit(0); // Shut down the process
    }
}, 5000); // Check every 5 seconds
