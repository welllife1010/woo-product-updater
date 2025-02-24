const fs = require("fs");
const path = require("path");
const { logErrorToFile, logInfoToFile } = require("./logger");
const { batchQueue, redisClient } = require('./queue');
const checkpointFilePath = path.join(__dirname, "process_checkpoint.json");

/**
 * Save progress / checkpoint to a local JSON file.
 *
 * Single-worker scenario: We don't need to store `lastProcessedRow`
 * in Redis. Instead, we just pass it in or compute it, then write it
 * to `process_checkpoint.json`.
 */
async function saveCheckpoint(fileKey, lastProcessedRow, totalRows) {
    // Validate arguments
    if (!fileKey || typeof fileKey !== "string") {
      logErrorToFile(`❌ saveCheckpoint is missing a valid fileKey.`);
      return;
    }
    if (!Number.isInteger(totalRows)) {
      logErrorToFile(`❌ saveCheckpoint received invalid totalRows: ${totalRows}`);
      return;
    }
    if (!Number.isInteger(lastProcessedRow) || lastProcessedRow < 0) {
      logErrorToFile(`❌ saveCheckpoint received invalid lastProcessedRow: ${lastProcessedRow}`);
      return;
    }
  
    // ──────────────────────────────────────────────
    // 1) Fetch Row-Level Stats (Optional)
    //    If you still use Redis to track updated/skipped/failed, you can keep it
    // ──────────────────────────────────────────────
    const updated = parseInt(await redisClient.get(`updated-products:${fileKey}`) || 0, 10);
    const skipped = parseInt(await redisClient.get(`skipped-products:${fileKey}`) || 0, 10);
    const failed  = parseInt(await redisClient.get(`failed-products:${fileKey}`)  || 0, 10);
  
    const completedRows = updated + skipped + failed;
    const remainingRows = totalRows - completedRows;
  
    // ──────────────────────────────────────────────
    // 2) (Optional) Gather Queue-Wide Job Stats
    //    Even with 1 worker, you can store how many jobs are left.
    // ──────────────────────────────────────────────
    const waiting = await batchQueue.getWaitingCount();
    const active  = await batchQueue.getActiveCount();
    const delayed = await batchQueue.getDelayedCount();
    const totalRemainingJobs = waiting + active + delayed;
  
    // ──────────────────────────────────────────────
    // 3) Read Existing Checkpoint Data From JSON
    // ──────────────────────────────────────────────
    let checkpoints = {};
    if (fs.existsSync(checkpointFilePath)) {
      try {
        const fileData = fs.readFileSync(checkpointFilePath, "utf-8");
        checkpoints = JSON.parse(fileData);
      } catch (error) {
        logErrorToFile(`❌ Error reading checkpoint file: ${error.message}`);
        // If parse fails, we fallback to an empty object
        checkpoints = {};
      }
    }
    // ──────────────────────────────────────────────
    // 4) Update the Checkpoints Object
    // ──────────────────────────────────────────────
    checkpoints[fileKey] = {
      rowLevel: {
        lastProcessedRow,    // Single-worker approach: stored in local file
        totalRows,
        updated,
        skipped,
        failed,
        completedRows,
        remainingRows
      },
      jobLevel: {
        waiting,
        active,
        delayed,
        totalRemainingJobs
      },
      timestamp: new Date().toISOString()
    };
  
    // ──────────────────────────────────────────────
    // 5) Write Updated Checkpoints to File
    // ──────────────────────────────────────────────
    try {
      fs.writeFileSync(checkpointFilePath, JSON.stringify(checkpoints, null, 2));
      logInfoToFile(
        `📌 Progress saved for ${fileKey} => ` +
        `Rows: completed ${completedRows}/${totalRows} (lastProcessedRow=${lastProcessedRow}) | ` +
        `Jobs: remaining ${totalRemainingJobs}`
      );
    } catch (error) {
      logErrorToFile(`❌ Failed to save checkpoint for ${fileKey}: ${error.message}`);
    }
}

/**
 * getCheckpoint returns the lastProcessedRow stored in process_checkpoint.json.
 */
function getCheckpoint(fileKey) {
    if (!fileKey || typeof fileKey !== "string") {
      logErrorToFile(`❌ getCheckpoint missing valid fileKey`);
      return 0;
    }
  
    if (!fs.existsSync(checkpointFilePath)) {
      logInfoToFile(`Checkpoint file doesn't exist yet. Returning 0 for fileKey=${fileKey}`);
      return 0;
    }
  
    try {
      const fileData = fs.readFileSync(checkpointFilePath, "utf-8");
      const checkpoints = JSON.parse(fileData);
  
      if (!checkpoints[fileKey]) {
        logInfoToFile(`No checkpoint entry found for fileKey=${fileKey}, returning 0`);
        return 0;
      }
  
      // The structure here matches what we wrote in saveCheckpoint
      const lastProcessedRow = checkpoints[fileKey].rowLevel?.lastProcessedRow;
      if (typeof lastProcessedRow === "number") {
        return lastProcessedRow;
      } else {
        logInfoToFile(`No valid lastProcessedRow for fileKey=${fileKey} in checkpoint, returning 0`);
        return 0;
      }
    } catch (error) {
      logErrorToFile(`❌ Error reading getCheckpoint: ${error.message}`);
      return 0;
    }
}

module.exports = {
    saveCheckpoint,
    getCheckpoint,
};