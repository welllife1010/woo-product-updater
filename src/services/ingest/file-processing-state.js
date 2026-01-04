/**
 * File processing state / skip logic.
 * - Redis progress counters
 * - Detect jobs already queued
 * - Detect file already fully processed (checkpoint)
 */

const fs = require("fs");
const path = require("path");

const { batchQueue, appRedis, progressKeys, CURRENT_ENV } = require("../queue");
const { logErrorToFile, logInfoToFile } = require("../../utils/logger");

const initializeFileTracking = async (fileKey, totalRows) => {
  try {
    const keys = progressKeys.allKeys(fileKey);

    const existingTotal = await appRedis.get(keys.totalRows);

    // Always keep totalRows up-to-date (safe), but do NOT reset counters when resuming.
    await appRedis.set(keys.totalRows, String(totalRows));

    if (existingTotal === null) {
      // Fresh start for this file.
      await appRedis.mSet({
        [keys.updated]: "0",
        [keys.skipped]: "0",
        [keys.failed]: "0",
        [keys.processing]: "0",
      });
      logInfoToFile(
        `✅ Initialized Redis tracking for ${fileKey} (${totalRows} total rows)`
      );
      return;
    }

    // Resume path: ensure missing counters exist, but don't clobber existing progress.
    const [u, s, f, p] = await Promise.all([
      appRedis.get(keys.updated),
      appRedis.get(keys.skipped),
      appRedis.get(keys.failed),
      appRedis.get(keys.processing),
    ]);

    const toInit = {};
    if (u === null) toInit[keys.updated] = "0";
    if (s === null) toInit[keys.skipped] = "0";
    if (f === null) toInit[keys.failed] = "0";
    if (p === null) toInit[keys.processing] = "0";
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
  // Each environment has its own checkpoint file to prevent cross-environment pollution.
  const repoRoot = path.resolve(__dirname, "..", "..", "..");
  const checkpointCandidates = [
    path.join(repoRoot, `process_checkpoint_${CURRENT_ENV}.json`),
    path.join(repoRoot, "src", "batch", `process_checkpoint_${CURRENT_ENV}.json`),
    // Legacy fallback (non-env-specific) - only check if env-specific doesn't exist
    path.join(repoRoot, "process_checkpoint.json"),
    path.join(repoRoot, "src", "batch", "process_checkpoint.json"),
  ];

  // Prefer environment-specific checkpoint files
  const checkpointPath = checkpointCandidates.find((p) => fs.existsSync(p));
  if (!checkpointPath) return false;

  // If we found a legacy (non-env-specific) file, don't trust it for this environment
  if (!checkpointPath.includes(`_${CURRENT_ENV}`)) {
    logInfoToFile(`⚠️ Found legacy checkpoint file ${checkpointPath}, ignoring for ${CURRENT_ENV} environment`);
    return false;
  }

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
