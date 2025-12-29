/**
 * File processing state / skip logic.
 * - Redis progress counters
 * - Detect jobs already queued
 * - Detect file already fully processed (checkpoint)
 */

const fs = require("fs");
const path = require("path");

const { batchQueue, appRedis } = require("../queue");
const { logErrorToFile, logInfoToFile } = require("../../utils/logger");

const initializeFileTracking = async (fileKey, totalRows) => {
  try {
    const totalKey = `total-rows:${fileKey}`;
    const updatedKey = `updated-products:${fileKey}`;
    const skippedKey = `skipped-products:${fileKey}`;
    const failedKey = `failed-products:${fileKey}`;

    const existingTotal = await appRedis.get(totalKey);

    // Always keep totalRows up-to-date (safe), but do NOT reset counters when resuming.
    await appRedis.set(totalKey, String(totalRows));

    if (existingTotal === null) {
      // Fresh start for this file.
      await appRedis.mSet({
        [updatedKey]: "0",
        [skippedKey]: "0",
        [failedKey]: "0",
      });
      logInfoToFile(
        `✅ Initialized Redis tracking for ${fileKey} (${totalRows} total rows)`
      );
      return;
    }

    // Resume path: ensure missing counters exist, but don't clobber existing progress.
    const [u, s, f] = await Promise.all([
      appRedis.get(updatedKey),
      appRedis.get(skippedKey),
      appRedis.get(failedKey),
    ]);

    const toInit = {};
    if (u === null) toInit[updatedKey] = "0";
    if (s === null) toInit[skippedKey] = "0";
    if (f === null) toInit[failedKey] = "0";
    if (Object.keys(toInit).length) {
      await appRedis.mSet(toInit);
    }

    logInfoToFile(
      `↩️ Redis tracking already exists for ${fileKey}; preserving counters and continuing (totalRows=${totalRows})`
    );
  } catch (error) {
    logErrorToFile(
      `❌ Redis mSet failed in initializeFileTracking: ${error.message}`
    );
  }
};

const checkExistingJobs = async (fileKey) => {
  try {
    const jobs = await batchQueue.getJobs(["waiting", "active", "delayed"]);
    const hasExisting = jobs.some((job) => job.data?.fileKey === fileKey);

    if (hasExisting) {
      logInfoToFile(`Jobs for ${fileKey} already in queue`);
    }

    return hasExisting;
  } catch (error) {
    logErrorToFile(`Error checking existing jobs: ${error.message}`);
    return false;
  }
};

const isFileFullyProcessed = (fileKey) => {
  // Support both legacy and current checkpoint locations.
  // - legacy: <repoRoot>/process_checkpoint.json
  // - current: <repoRoot>/src/batch/process_checkpoint.json
  const repoRoot = path.resolve(__dirname, "..", "..", "..");
  const checkpointCandidates = [
    path.join(repoRoot, "process_checkpoint.json"),
    path.join(repoRoot, "src", "batch", "process_checkpoint.json"),
  ];

  const checkpointPath = checkpointCandidates.find((p) => fs.existsSync(p));
  if (!checkpointPath) return false;

  try {
    const checkpointData = JSON.parse(
      fs.readFileSync(checkpointPath, "utf-8") || "{}"
    );

    return checkpointData[fileKey]?.rowLevel?.remainingRows === 0;
  } catch (error) {
    logErrorToFile(`Error reading checkpoint (${checkpointPath}): ${error.message}`);
    return false;
  }
};

module.exports = {
  initializeFileTracking,
  checkExistingJobs,
  isFileFullyProcessed,
};
