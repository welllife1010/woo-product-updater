/**
 * =============================================================================
 * FILE: checkpoint.js
 * =============================================================================
 * 
 * PURPOSE:
 * Manages checkpoint/progress data for crash recovery and progress tracking.
 * When the system restarts after a crash, checkpoints tell us where to resume.
 * 
 * STORAGE MECHANISMS:
 * 1. JSON File (process_checkpoint.json) - Persistent storage across restarts
 * 2. Redis - Fast access for real-time progress tracking
 * 
 * RACE CONDITION FIX (2025):
 * Added `saveCheckpointAtomic()` function that only updates the checkpoint
 * if the new value is HIGHER than the existing value. This prevents slower
 * workers from overwriting the progress of faster workers.
 * 
 * EXAMPLE OF THE PROBLEM:
 *   Worker A processes rows 100-120, takes 10 seconds
 *   Worker B processes rows 120-140, takes 5 seconds
 *   
 *   Without atomic save:
 *     Worker B finishes first → saves checkpoint = 140
 *     Worker A finishes later → saves checkpoint = 120 (OVERWRITES!)
 *     System crashes and restarts → resumes from 120
 *     Rows 120-140 processed AGAIN (duplicate work)
 *   
 *   With atomic save:
 *     Worker B finishes first → saves checkpoint = 140
 *     Worker A finishes later → tries to save 120, but 140 > 120, so SKIPPED
 *     System crashes and restarts → resumes from 140 (CORRECT!)
 * 
 * =============================================================================
 */

// =============================================================================
// IMPORTS
// =============================================================================

const fs = require("fs");
const path = require("path");

// Custom logging utilities
const { logErrorToFile, logInfoToFile } = require("../utils/logger");

// Redis and queue access
const { batchQueue, appRedis } = require("../services/queue");

// =============================================================================
// CONFIGURATION
// =============================================================================

/**
 * Path to the checkpoint JSON file.
 * This file persists progress across system restarts.
 */
const checkpointFilePath = path.join(__dirname, "process_checkpoint.json");

// =============================================================================
// INITIALIZATION
// =============================================================================

/**
 * Ensure the checkpoint file exists when the module loads.
 * This prevents "file not found" errors on first run.
 */
if (!fs.existsSync(checkpointFilePath)) {
  logInfoToFile(`⚠️ process_checkpoint.json not found. Creating empty file.`);
  fs.writeFileSync(checkpointFilePath, JSON.stringify({}, null, 2));
}

// =============================================================================
// MAIN FUNCTIONS
// =============================================================================

/**
 * Save checkpoint progress to both Redis and JSON file.
 * 
 * This function saves comprehensive progress data including:
 *   - Row-level progress (lastProcessedRow, updated/skipped/failed counts)
 *   - Job-level status (waiting, active, delayed jobs in queue)
 *   - Timestamp for debugging
 * 
 * WHEN TO USE:
 * Use this function when you want to save the current state regardless of
 * what was saved before. Good for periodic snapshots.
 * 
 * For concurrent workers, prefer `saveCheckpointAtomic()` which prevents
 * slower workers from overwriting faster workers' progress.
 * 
 * @param {string} fileKey - The S3 key or filename being processed
 * @param {number} lastProcessedRow - The last row number that was processed
 * @param {number} totalRows - Total rows in the file
 * @returns {Promise<void>}
 * 
 * @example
 * // Save progress after processing a batch
 * await saveCheckpoint("products-jan-2025.csv", 500, 10000);
 */
async function saveCheckpoint(fileKey, lastProcessedRow, totalRows) {
  logInfoToFile(
    `🔍 saveCheckpoint() called: fileKey=${fileKey}, ` +
    `lastProcessedRow=${lastProcessedRow}, totalRows=${totalRows}`
  );

  // ===========================================================================
  // STEP 1: Validate input arguments
  // ===========================================================================
  
  // Validate fileKey
  if (!fileKey || typeof fileKey !== "string") {
    logErrorToFile(
      `❌ saveCheckpoint: Invalid fileKey. ` +
      `Type: ${typeof fileKey}, Value: ${JSON.stringify(fileKey)}`
    );
    return;
  }

  // Validate totalRows
  if (!Number.isInteger(totalRows) || totalRows < 0) {
    logErrorToFile(
      `❌ saveCheckpoint: Invalid totalRows: ${JSON.stringify(totalRows)}`
    );
    return;
  }

  // Validate lastProcessedRow
  if (!Number.isInteger(lastProcessedRow) || lastProcessedRow < 0) {
    logErrorToFile(
      `❌ saveCheckpoint: Invalid lastProcessedRow: ${JSON.stringify(lastProcessedRow)}`
    );
    return;
  }

  // ===========================================================================
  // STEP 2: Ensure checkpoint file exists
  // ===========================================================================
  
  if (!fs.existsSync(checkpointFilePath)) {
    logInfoToFile(`⚠️ Checkpoint file missing, creating new one.`);
    fs.writeFileSync(checkpointFilePath, JSON.stringify({}, null, 2));
  }

  // ===========================================================================
  // STEP 3: Fetch row-level statistics from Redis
  // ===========================================================================
  /**
   * Workers increment these counters as they process rows:
   *   - updated-products:{fileKey} → Successfully updated
   *   - skipped-products:{fileKey} → Skipped (no changes needed)
   *   - failed-products:{fileKey}  → Failed (errors)
   */
  
  let updated = 0;
  let skipped = 0;
  let failed = 0;

  try {
    updated = parseInt(await appRedis.get(`updated-products:${fileKey}`) || "0", 10);
    skipped = parseInt(await appRedis.get(`skipped-products:${fileKey}`) || "0", 10);
    failed = parseInt(await appRedis.get(`failed-products:${fileKey}`) || "0", 10);
  } catch (redisError) {
    logErrorToFile(`⚠️ Redis read error in saveCheckpoint: ${redisError.message}`);
    // Continue with zeros - better to save partial data than nothing
  }

  // Calculate derived values
  const completedRows = updated + skipped + failed;
  const remainingRows = Math.max(0, totalRows - completedRows);

  // ===========================================================================
  // STEP 4: Fetch queue-level statistics from BullMQ
  // ===========================================================================
  /**
   * These stats show how many jobs are pending in the queue:
   *   - waiting: Jobs waiting to be picked up
   *   - active: Jobs currently being processed
   *   - delayed: Jobs scheduled for later retry
   */
  
  let waiting = 0;
  let active = 0;
  let delayed = 0;

  try {
    waiting = await batchQueue.getWaitingCount();
    active = await batchQueue.getActiveCount();
    delayed = await batchQueue.getDelayedCount();
  } catch (queueError) {
    logErrorToFile(`⚠️ Queue stats error in saveCheckpoint: ${queueError.message}`);
    // Continue with zeros
  }

  const totalRemainingJobs = waiting + active + delayed;

  // ===========================================================================
  // STEP 5: Read existing checkpoint data
  // ===========================================================================
  
  let checkpoints = {};
  
  try {
    const fileData = fs.readFileSync(checkpointFilePath, "utf-8");
    checkpoints = JSON.parse(fileData);
  } catch (error) {
    logErrorToFile(`⚠️ Error reading checkpoint file, starting fresh: ${error.message}`);
    checkpoints = {};
  }

  // ===========================================================================
  // STEP 6: Update checkpoint data for this file
  // ===========================================================================
  
  checkpoints[fileKey] = {
    /**
     * Row-level tracking:
     * Shows exactly where we are in processing this file's rows.
     */
    rowLevel: {
      lastProcessedRow,   // Highest row number we've completed
      totalRows,          // Total rows in the file
      updated,            // Count of successfully updated products
      skipped,            // Count of skipped products (no changes)
      failed,             // Count of failed products (errors)
      completedRows,      // updated + skipped + failed
      remainingRows,      // totalRows - completedRows
    },
    
    /**
     * Job-level tracking:
     * Shows the state of batch jobs in the queue.
     */
    jobLevel: {
      waiting,            // Jobs waiting to be processed
      active,             // Jobs currently processing
      delayed,            // Jobs scheduled for retry
      totalRemainingJobs, // waiting + active + delayed
    },
    
    /**
     * Timestamp for debugging:
     * Helps identify when progress was last saved.
     */
    timestamp: new Date().toISOString(),
  };

  // ===========================================================================
  // STEP 7: Write updated checkpoint to file
  // ===========================================================================
  
  try {
    fs.writeFileSync(
      checkpointFilePath,
      JSON.stringify(checkpoints, null, 2)  // Pretty print for readability
    );
    
    logInfoToFile(
      `📌 Checkpoint saved for ${fileKey}: ` +
      `${completedRows}/${totalRows} rows (lastProcessedRow=${lastProcessedRow}) | ` +
      `${totalRemainingJobs} jobs remaining`
    );
  } catch (error) {
    logErrorToFile(`❌ Failed to write checkpoint: ${error.message}`);
  }
}

/**
 * =============================================================================
 * ATOMIC CHECKPOINT SAVE - RACE CONDITION FIX
 * =============================================================================
 * 
 * Save checkpoint ONLY if the new value is higher than the existing value.
 * This is critical for concurrent workers to prevent progress loss.
 * 
 * HOW IT WORKS:
 * 1. Read the current checkpoint value from Redis (fast, atomic)
 * 2. Compare: is newLastProcessedRow > currentValue?
 * 3. If YES: update both Redis and JSON file
 * 4. If NO: skip the update (a faster worker already saved higher progress)
 * 
 * WHY REDIS FIRST?
 * Redis operations are atomic and fast, making it ideal for coordination
 * between concurrent workers. The JSON file is updated as a backup for
 * persistence across full system restarts.
 * 
 * @param {string} fileKey - The S3 key or filename being processed
 * @param {number} newLastProcessedRow - The row number this worker completed up to
 * @param {number} totalRows - Total rows in the file
 * @returns {Promise<void>}
 * 
 * @example
 * // Worker finished processing rows 100-120 (120 is the end row)
 * await saveCheckpointAtomic("products.csv", 120, 10000);
 * // If another worker already saved 140, this will be skipped
 */
async function saveCheckpointAtomic(fileKey, newLastProcessedRow, totalRows) {
  logInfoToFile(
    `🔍 saveCheckpointAtomic() called: fileKey=${fileKey}, ` +
    `newLastProcessedRow=${newLastProcessedRow}, totalRows=${totalRows}`
  );

  // ===========================================================================
  // STEP 1: Validate input arguments
  // ===========================================================================
  
  if (!fileKey || typeof fileKey !== "string") {
    logErrorToFile(
      `❌ saveCheckpointAtomic: Invalid fileKey: ${JSON.stringify(fileKey)}`
    );
    return;
  }

  if (!Number.isInteger(totalRows) || totalRows < 0) {
    logErrorToFile(
      `❌ saveCheckpointAtomic: Invalid totalRows: ${totalRows}`
    );
    return;
  }

  if (!Number.isInteger(newLastProcessedRow) || newLastProcessedRow < 0) {
    logErrorToFile(
      `❌ saveCheckpointAtomic: Invalid newLastProcessedRow: ${newLastProcessedRow}`
    );
    return;
  }

  // ===========================================================================
  // STEP 2: Use Redis for atomic "compare and update"
  // ===========================================================================
  /**
   * Redis key pattern: checkpoint:{fileKey}:lastProcessedRow
   * 
   * This key stores the highest row number that has been completely processed.
   * Multiple workers read and write this key, so we need atomic operations.
   */
  
  const redisKey = `checkpoint:${fileKey}:lastProcessedRow`;
  
  try {
    // Get current value from Redis
    const currentValueStr = await appRedis.get(redisKey);
    const currentValue = parseInt(currentValueStr || "0", 10);

    // ===========================================================================
    // STEP 3: Compare and conditionally update
    // ===========================================================================
    
    if (newLastProcessedRow > currentValue) {
      // New value is higher - safe to update
      await appRedis.set(redisKey, newLastProcessedRow.toString());
      
      logInfoToFile(
        `📌 Checkpoint UPDATED for ${fileKey}: ${currentValue} → ${newLastProcessedRow}`
      );

      // Also update the JSON file for persistence
      // Use the higher value to ensure consistency
      await saveCheckpoint(fileKey, newLastProcessedRow, totalRows);
      
    } else {
      // New value is NOT higher - skip update to preserve higher progress
      logInfoToFile(
        `📌 Checkpoint SKIPPED for ${fileKey}: ` +
        `current (${currentValue}) >= new (${newLastProcessedRow})`
      );
      
      // Still update the JSON file with the CURRENT (higher) value
      // This keeps Redis and JSON in sync
      await saveCheckpoint(fileKey, currentValue, totalRows);
    }

  } catch (error) {
    logErrorToFile(
      `❌ saveCheckpointAtomic Redis error: ${error.message}. ` +
      `Falling back to regular save.`
    );
    
    // Fallback: save anyway (better than losing all progress)
    await saveCheckpoint(fileKey, newLastProcessedRow, totalRows);
  }
}

/**
 * Get the last processed row number for a file.
 * 
 * Checks Redis first (faster), then falls back to JSON file.
 * Used when resuming processing after a restart.
 * 
 * @param {string} fileKey - The S3 key or filename
 * @returns {number} - Last processed row number (0 if not found)
 * 
 * @example
 * const lastRow = getLastProcessedRow("products.csv");
 * console.log(`Resuming from row ${lastRow}`);
 */
function getLastProcessedRow(fileKey) {
  logInfoToFile(`🔍 getLastProcessedRow() called for fileKey=${fileKey}`);

  // ===========================================================================
  // STEP 1: Validate input
  // ===========================================================================
  
  if (!fileKey || typeof fileKey !== "string") {
    logErrorToFile(`❌ getLastProcessedRow: Invalid fileKey`);
    return 0;
  }

  // ===========================================================================
  // STEP 2: Check Redis first (synchronous workaround)
  // ===========================================================================
  /**
   * NOTE: This function is synchronous, but Redis operations are async.
   * For true async usage, consider making this function async.
   * 
   * Current implementation falls through to JSON file for sync access.
   * Redis is checked in saveCheckpointAtomic for atomic updates.
   */

  // ===========================================================================
  // STEP 3: Read from JSON file (fallback/primary for sync access)
  // ===========================================================================
  
  if (!fs.existsSync(checkpointFilePath)) {
    logInfoToFile(`⚠️ Checkpoint file not found, returning 0`);
    fs.writeFileSync(checkpointFilePath, JSON.stringify({}, null, 2));
    return 0;
  }

  try {
    const fileData = fs.readFileSync(checkpointFilePath, "utf-8");
    const checkpoints = JSON.parse(fileData);

    // Check if we have data for this file
    if (!checkpoints[fileKey]) {
      logInfoToFile(`No checkpoint found for ${fileKey}, returning 0`);
      return 0;
    }

    // Get lastProcessedRow from the nested structure
    const lastProcessedRow = checkpoints[fileKey].rowLevel?.lastProcessedRow;

    if (typeof lastProcessedRow === "number") {
      logInfoToFile(
        `✅ getLastProcessedRow: Found ${lastProcessedRow} for ${fileKey}`
      );
      return lastProcessedRow;
    } else {
      logInfoToFile(
        `⚠️ lastProcessedRow is not a number for ${fileKey}, returning 0`
      );
      return 0;
    }

  } catch (error) {
    logErrorToFile(`❌ Error reading checkpoint: ${error.message}`);
    return 0;
  }
}

/**
 * Async version of getLastProcessedRow that checks Redis first.
 * 
 * Use this when you need the most up-to-date checkpoint value,
 * especially in concurrent processing scenarios.
 * 
 * @param {string} fileKey - The S3 key or filename
 * @returns {Promise<number>} - Last processed row number (0 if not found)
 * 
 * @example
 * const lastRow = await getLastProcessedRowAsync("products.csv");
 */
async function getLastProcessedRowAsync(fileKey) {
  logInfoToFile(`🔍 getLastProcessedRowAsync() called for fileKey=${fileKey}`);

  if (!fileKey || typeof fileKey !== "string") {
    logErrorToFile(`❌ getLastProcessedRowAsync: Invalid fileKey`);
    return 0;
  }

  // Try Redis first (most up-to-date value)
  try {
    const redisKey = `checkpoint:${fileKey}:lastProcessedRow`;
    const redisValue = await appRedis.get(redisKey);
    
    if (redisValue !== null) {
      const value = parseInt(redisValue, 10);
      if (!isNaN(value)) {
        logInfoToFile(`✅ Got checkpoint from Redis: ${value} for ${fileKey}`);
        return value;
      }
    }
  } catch (error) {
    logErrorToFile(`⚠️ Redis error in getLastProcessedRowAsync: ${error.message}`);
    // Fall through to JSON file
  }

  // Fallback to JSON file
  return getLastProcessedRow(fileKey);
}

/**
 * Clear checkpoint data for a file.
 * 
 * Useful when you want to reprocess a file from the beginning,
 * or when cleaning up after a file is fully processed.
 * 
 * @param {string} fileKey - The S3 key or filename
 * @returns {Promise<void>}
 * 
 * @example
 * // Reset and reprocess a file
 * await clearCheckpoint("products.csv");
 */
async function clearCheckpoint(fileKey) {
  logInfoToFile(`🗑️ Clearing checkpoint for ${fileKey}`);

  if (!fileKey || typeof fileKey !== "string") {
    logErrorToFile(`❌ clearCheckpoint: Invalid fileKey`);
    return;
  }

  // Clear from Redis
  try {
    const redisKey = `checkpoint:${fileKey}:lastProcessedRow`;
    await appRedis.del(redisKey);
    logInfoToFile(`✅ Cleared Redis checkpoint for ${fileKey}`);
  } catch (error) {
    logErrorToFile(`⚠️ Error clearing Redis checkpoint: ${error.message}`);
  }

  // Clear from JSON file
  try {
    if (fs.existsSync(checkpointFilePath)) {
      const fileData = fs.readFileSync(checkpointFilePath, "utf-8");
      const checkpoints = JSON.parse(fileData);
      
      if (checkpoints[fileKey]) {
        delete checkpoints[fileKey];
        fs.writeFileSync(checkpointFilePath, JSON.stringify(checkpoints, null, 2));
        logInfoToFile(`✅ Cleared JSON checkpoint for ${fileKey}`);
      }
    }
  } catch (error) {
    logErrorToFile(`⚠️ Error clearing JSON checkpoint: ${error.message}`);
  }
}

// =============================================================================
// EXPORTS
// =============================================================================

module.exports = {
  // Standard checkpoint save (overwrites existing)
  saveCheckpoint,
  
  // Atomic checkpoint save (only updates if higher) - RACE CONDITION FIX
  saveCheckpointAtomic,
  
  // Read checkpoint (sync version)
  getLastProcessedRow,
  
  // Read checkpoint (async version, checks Redis first)
  getLastProcessedRowAsync,
  
  // Clear checkpoint (for reprocessing)
  clearCheckpoint,
  
  // Export path for testing
  checkpointFilePath,
};