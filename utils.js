/**
 * =============================================================================
 * FILE: utils.js
 * =============================================================================
 * 
 * PURPOSE:
 * Utility functions shared across the application.
 * 
 * BUG FIX (2025):
 * - Added missing import for `logErrorToFile` which was causing runtime errors
 *   when `handleError` was called.
 * 
 * =============================================================================
 */

// Import logging utility - REQUIRED for handleError function
const { logErrorToFile } = require("./logger");

/**
 * Creates a unique job ID for BullMQ jobs.
 * 
 * The job ID includes:
 *   - fileKey: identifies which CSV file this job processes
 *   - action: what operation is being performed
 *   - rowIndex: which row in the CSV (for debugging)
 *   - retryCount: how many times this job has been retried
 *   - timestamp: ensures uniqueness even for same row + retry
 * 
 * @param {string} fileKey - The CSV file identifier
 * @param {string} action - The action being performed (e.g., "processBatch")
 * @param {number|string} rowIndex - The row index being processed
 * @param {number|string} retryCount - The retry attempt number
 * @returns {string} A unique job ID
 * 
 * @example
 * createUniqueJobId("products.csv", "processBatch", 100, 0);
 * // Returns: "jobId_products.csv_processBatch_row-100_retry-0_1704067200000"
 */
const createUniqueJobId = (fileKey, action = "", rowIndex = 0, retryCount = "") => {
    // ✅ Ensure rowIndex and retryCount are valid integers or default to 0
    const validRowIndex = Number.isInteger(Number(rowIndex)) ? Number(rowIndex) : 0;
    const validRetryCount = Number.isInteger(Number(retryCount)) ? Number(retryCount) : 0;

    // ✅ Ensure fileKey and action are valid strings
    const validFileKey = typeof fileKey === "string" ? fileKey.replace(/\s+/g, "_") : "unknown-file";
    const validAction = typeof action === "string" && action ? `_${action.replace(/\s+/g, "_")}` : "";

    // ✅ Generate timestamp for uniqueness
    const timestamp = Date.now();

    // ✅ Construct Job ID safely
    let jobId = `jobId_${validFileKey}${validAction}_row-${validRowIndex}_retry-${validRetryCount}_${timestamp}`;

    return jobId;
};

/**
 * Centralized error handler with context-aware logging.
 * 
 * Categorizes errors by type and logs appropriate messages:
 *   - Network errors (ENOTFOUND, ECONNRESET)
 *   - CSV parsing errors
 *   - S3 file not found errors
 *   - All other unexpected errors
 * 
 * @param {Error} error - The error object to handle
 * @param {string} context - Where the error occurred (function/file name)
 * 
 * @example
 * try {
 *   await fetchFromS3(key);
 * } catch (error) {
 *   handleError(error, "s3-helpers.fetchFromS3");
 * }
 */
const handleError = (error, context = "Unknown") => {
  // Network connectivity errors
  if (error.code === "ENOTFOUND" || error.code === "ECONNRESET") {
    logErrorToFile(`🔴 Network error in ${context}: ${error.message}`, error.stack);
  } 
  // CSV parsing errors
  else if (error.name === "CSVError") {
    logErrorToFile(`📉 CSV Parsing Error in ${context}: ${error.message}`, error.stack);
  } 
  // AWS S3 file not found errors
  else if (error.name === "NoSuchKey") {
    logErrorToFile(`❌ S3 Error: File not found in ${context}: ${error.message}`, error.stack);
  } 
  // All other errors
  else {
    logErrorToFile(`❌ Unexpected Error in ${context}: ${error.message}`, error.stack);
  }
};

module.exports = { handleError, createUniqueJobId };
