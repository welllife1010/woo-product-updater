const { batchQueue } = require('./queue'); // Bull Queue instance
const Bottleneck = require("bottleneck");
const { logErrorToFile, logInfoToFile } = require('./logger');

// Create a Bottleneck instance with appropriate settings
const limiter = new Bottleneck({
    maxConcurrent: 2, // Number of concurrent requests allowed - Limit to 2 concurrent 100-item requests at once
    minTime: 1000, // Minimum time between requests (in milliseconds) - 500ms between each request
});

// ✅ Add a batch job to BullMQ Queue
const addBatchJob = async (jobData, jobId) => {
    try {
        if (!batchQueue) {
            throw new Error("batchQueue is undefined! Check Redis connection.");
        }

        if (!jobData.fileKey) {
            throw new Error(`❌ addBatchJob error: Missing fileKey`);
        }
        
        if (!jobData.batch || jobData.batch.length === 0) {
            logInfoToFile(`⚠️ No valid products found in batch for ${jobData.fileKey}, skipping job enqueue.`);
            return;
        }

        logInfoToFile(`🚀 Adding batch job to queue: ${jobId} | File: ${jobData.fileKey}`);

        // ✅ Ensure all values in jobData are properly formatted
        const cleanedJobData = {
            batch: jobData.batch,
            fileKey: String(jobData.fileKey), // Ensure string format
            totalProductsInFile: Number(jobData.totalProductsInFile) || 0, // Ensure integer
            batchSize: Number(jobData.batchSize) || 0 // Ensure integer
        };

        

        // ✅ Add the job with proper BullMQ structure
        const job = await batchQueue.add(jobId, cleanedJobData, {
            removeOnComplete: 100, // Keep last 100 completed jobs
            removeOnFail: 50, // Keep last 50 failed jobs
            attempts: 5, // Retry failed jobs 5 times
            backoff: { type: 'exponential', delay: 5000 }, // Exponential backoff delay
            timeout: 300000 // 5-minute timeout
        }).catch(error => {
            logErrorToFile(`❌ batchQueue.add() failed for job ${jobId}. Error: ${error.message}`, error.stack);
        });

        if (!job) throw new Error(`Job creation returned null/undefined`);

        logInfoToFile(`✅ Successfully added batch job with ID: ${job.id}`);
        return job;
    } catch (error) {
        logErrorToFile(`❌ Failed to add batch job with ID: ${jobId}. Error: ${error.message}`, error.stack);
        throw error;
    }
};

// Schedule an API request using Bottleneck
const scheduleApiRequest = async (task, options) => {
    if (!limiter) {
        throw new Error('Limiter is not initialized');
    }

    try {
        const response = await limiter.schedule(options, task);
        logInfoToFile(`Successfully scheduled API request: ${options.id}`);
        return response;
    } catch (error) {
        logErrorToFile(`Failed to schedule API request: ${options.id}`, error);
        throw error;
    }
};

module.exports = {
    limiter,
    addBatchJob,
    scheduleApiRequest
};