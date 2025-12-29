/**
 * =============================================================================
 * FILE: job-manager.js
 * =============================================================================
 * 
 * PURPOSE:
 * Manages job creation for BullMQ queue and rate-limited API requests.
 * 
 * RESPONSIBILITIES:
 * 1. Add batch jobs to the BullMQ queue with proper configuration
 * 2. Schedule API requests through Bottleneck rate limiter
 * 
 * BUG FIX (2025):
 * - Added `startIndex` to cleanedJobData which is CRITICAL for the race
 *   condition fix. Without this, workers couldn't determine which rows
 *   they should process.
 * 
 * =============================================================================
 */

const { batchQueue } = require('./queue'); // Bull Queue instance
const Bottleneck = require("bottleneck");
const { logErrorToFile, logInfoToFile } = require('../utils/logger');

/**
 * Bottleneck rate limiter instance.
 * 
 * CONFIGURATION:
 * - maxConcurrent: 2 - Only allow 2 concurrent API requests
 *   This prevents overwhelming the WooCommerce server
 * 
 * - minTime: 1000ms - At least 1 second between requests
 *   This helps avoid rate limiting (429 errors)
 * 
 * WHY THESE VALUES?
 * WooCommerce APIs have rate limits. With these settings:
 *   - Maximum 2 requests/second sustained
 *   - Maximum 2 parallel requests at any time
 *   - Burst capacity is limited
 * 
 * TUNING:
 * If you see 429 errors, increase minTime or decrease maxConcurrent.
 * If processing is too slow, try increasing maxConcurrent cautiously.
 */
const limiter = new Bottleneck({
    maxConcurrent: 2,
    minTime: 1000,
});

/**
 * Add a batch job to the BullMQ Queue.
 * 
 * This function validates and sanitizes job data before enqueueing.
 * Jobs are processed by workers in worker.js.
 * 
 * @param {Object} jobData - The job data to enqueue
 * @param {Array<Object>} jobData.batch - Array of CSV row objects to process
 * @param {string} jobData.fileKey - The CSV file identifier
 * @param {number} jobData.totalProductsInFile - Total rows in the CSV file
 * @param {number} jobData.batchSize - Number of rows in this batch
 * @param {number} jobData.startIndex - Starting row index for this batch (CRITICAL for race-condition fix)
 * @param {string} jobId - Unique identifier for this job
 * @returns {Promise<Object|undefined>} The created job, or undefined if skipped
 * 
 * @throws {Error} If batchQueue is undefined or fileKey is missing
 * 
 * @example
 * const job = await addBatchJob({
 *   batch: [{ part_number: "ABC123", ... }],
 *   fileKey: "products.csv",
 *   totalProductsInFile: 1000,
 *   batchSize: 20,
 *   startIndex: 100
 * }, "job_products_100_1704067200000");
 */
const addBatchJob = async (jobData, jobId) => {
    try {
        // Validate queue connection
        if (!batchQueue) {
            throw new Error("batchQueue is undefined! Check Redis connection.");
        }

        // Validate required fields
        if (!jobData.fileKey) {
            throw new Error(`❌ addBatchJob error: Missing fileKey`);
        }
        
        // Skip empty batches
        if (!jobData.batch || jobData.batch.length === 0) {
            logInfoToFile(`⚠️ No valid products found in batch for ${jobData.fileKey}, skipping job enqueue.`);
            return;
        }

        logInfoToFile(`🚀 Adding batch job to queue: ${jobId} | File: ${jobData.fileKey}`);

        /**
         * BUG FIX (2025): Added `startIndex` to cleanedJobData
         * 
         * PROBLEM:
         * The original code didn't include `startIndex` in cleanedJobData.
         * This meant workers couldn't determine which rows they should process,
         * breaking the race-condition fix implemented in worker.js.
         * 
         * THE FIX:
         * Now we include `startIndex` from jobData. This is set by s3-helpers.js
         * when creating the job, and used by worker.js to know exactly which
         * rows this job is responsible for processing.
         * 
         * IMPORTANT:
         * If startIndex is undefined/null, we default to 0 for backward
         * compatibility, but log a warning since this shouldn't happen
         * with new code.
         */
        const cleanedJobData = {
            batch: jobData.batch,
            fileKey: String(jobData.fileKey),
            totalProductsInFile: Number(jobData.totalProductsInFile) || 0,
            batchSize: Number(jobData.batchSize) || 0,
            // ⭐ CRITICAL FIX: Include startIndex for race-condition prevention
            startIndex: typeof jobData.startIndex === 'number' ? jobData.startIndex : 0,
        };

        // Warn if startIndex wasn't provided (indicates old code path)
        if (typeof jobData.startIndex !== 'number') {
            logInfoToFile(
                `⚠️ Warning: addBatchJob called without startIndex for ${jobData.fileKey}. ` +
                `Defaulting to 0. This may cause race conditions in concurrent processing.`
            );
        }

        

        /**
         * Add the job with explicit BullMQ options.
         * 
         * BUG FIX (2025): Documented relationship with queue.js defaults
         * 
         * NOTE: These options OVERRIDE the defaultJobOptions in queue.js.
         * If you need to change retry behavior, update BOTH files to stay in sync.
         * 
         * OPTIONS EXPLAINED:
         * - removeOnComplete: Keep last 100 completed jobs (for debugging)
         * - removeOnFail: Keep last 50 failed jobs (for debugging)
         * - attempts: Total attempts before marking as permanently failed
         * - backoff: Wait time between retries (exponential: 5s, 10s, 20s, 40s, 80s)
         * - timeout: Maximum time for job to complete (prevents hung jobs)
         * 
         * SYNCED WITH: queue.js defaultJobOptions
         */
        const job = await batchQueue.add(jobId, cleanedJobData, {
            removeOnComplete: 100,
            removeOnFail: 50,
            attempts: 5,
            backoff: { type: 'exponential', delay: 5000 },
            timeout: 300000, // 5 minutes
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