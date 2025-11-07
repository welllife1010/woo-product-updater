const fs = require("fs");
const path = require("path");
const { logErrorToFile, logInfoToFile } = require("./logger");
const { batchQueue, appRedis } = require('./queue');
const checkpointFilePath = path.join(__dirname, "process_checkpoint.json");

// ✅ **Ensure `process_checkpoint.json` is always present at script start**
if (!fs.existsSync(checkpointFilePath)) {
  logInfoToFile(`⚠️ process_checkpoint.json not found. Creating a new one.`);
  fs.writeFileSync(checkpointFilePath, JSON.stringify({}, null, 2)); // Create an empty JSON file
}

/**
 * Save progress / checkpoint to a local JSON file.
 *
 * Single-worker scenario: We don't need to store `lastProcessedRow`
 * in Redis. Instead, we just pass it in or compute it, then write it
 * to `process_checkpoint.json`.
 */
async function saveCheckpoint(fileKey, lastProcessedRow, totalRows) {

  logInfoToFile(`🔍 Debug: saveCheckpoint() called!`);
  logInfoToFile(`🔍 Debug: saveCheckpoint called with fileKey=${fileKey} (${typeof fileKey}), lastProcessedRow=${lastProcessedRow} (${typeof lastProcessedRow}), totalRows=${totalRows} (${typeof totalRows})`);

  // Validate arguments with detailed logging
  if (!fileKey || typeof fileKey !== "string") {
    logErrorToFile(`❌ saveCheckpoint received invalid fileKey. Type: ${typeof fileKey}, Value: ${JSON.stringify(fileKey)}`);
    return;
  }
  if (!Number.isInteger(totalRows) || totalRows < 0) {
    logErrorToFile(`❌ saveCheckpoint received invalid totalRows: ${JSON.stringify(totalRows)}`);
    return;
  }
  if (!Number.isInteger(lastProcessedRow) || lastProcessedRow < 0) {
    logErrorToFile(`❌ saveCheckpoint received invalid lastProcessedRow: ${JSON.stringify(lastProcessedRow)}`);
    return;
  }

  // ✅ Ensure the checkpoint file exists before writing
  if (!fs.existsSync(checkpointFilePath)) {
    logInfoToFile(`⚠️ process_checkpoint.json not found. Creating a new one.`);
    fs.writeFileSync(checkpointFilePath, JSON.stringify({}, null, 2));
  }
  
  // ──────────────────────────────────────────────
  // 1) Fetch Row-Level Stats (Optional)
  //    Use Redis to track updated/skipped/failed products.
  // ──────────────────────────────────────────────
  const updated = parseInt(await appRedis.get(`updated-products:${fileKey}`) || 0, 10);
  const skipped = parseInt(await appRedis.get(`skipped-products:${fileKey}`) || 0, 10);
  const failed  = parseInt(await appRedis.get(`failed-products:${fileKey}`)  || 0, 10);

  const completedRows = updated + skipped + failed;
  const remainingRows = totalRows - completedRows;

  // ──────────────────────────────────────────────
  // 2) (Optional) Gather Queue-Wide Job Stats
  //    Even with 1 worker, we can store how many jobs are left.
  // ──────────────────────────────────────────────
  const waiting = await batchQueue.getWaitingCount();
  const active  = await batchQueue.getActiveCount();
  const delayed = await batchQueue.getDelayedCount();
  const totalRemainingJobs = waiting + active + delayed;

  // ──────────────────────────────────────────────
  // 3) Read Existing Checkpoint Data From JSON
  // ──────────────────────────────────────────────
  
  // ✅ Log before updating
  logInfoToFile(`📌 Debug: Updating process_checkpoint.json with new data.`);

  let checkpoints = {};
  try {
    const fileData = fs.readFileSync(checkpointFilePath, "utf-8");
    checkpoints = JSON.parse(fileData);
  } catch (error) {
    logErrorToFile(`❌ Error reading checkpoint file: ${error.message}`);
    // If parse fails, we fallback to an empty object
    checkpoints = {};
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
 * getLastProcessedRow returns the lastProcessedRow stored in `process_checkpoint.json`.
 */
function getLastProcessedRow(fileKey) {

  logInfoToFile(`"getLastProcessedRow" - Start to check the lastProcessRow value for fileKey=${fileKey}`);

    if (!fileKey || typeof fileKey !== "string") {
      logErrorToFile(`❌ getLastProcessedRow missing valid fileKey`);
      return 0;
    }
  
    // ✅ Ensure the checkpoint file exists
    if (!fs.existsSync(checkpointFilePath)) {
      logInfoToFile(`⚠️ process_checkpoint.json not found. Creating a new one.`);
      fs.writeFileSync(checkpointFilePath, JSON.stringify({}, null, 2));
      return 0; // No previous progress
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

      logInfoToFile(`"getLastProcessedRow" - lastProcessedRow=${lastProcessedRow} for fileKey=${fileKey}`);

      if (typeof lastProcessedRow === "number") {
        return lastProcessedRow;
      } else {
        logInfoToFile(`No valid lastProcessedRow for fileKey=${fileKey} in checkpoint, returning 0`);
        return 0;
      }
    } catch (error) {
      logErrorToFile(`❌ Error reading getLastProcessedRow: ${error.message}`);
      return 0;
    }
}

module.exports = {
    saveCheckpoint,
    getLastProcessedRow,
};