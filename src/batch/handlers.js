/*
================================================================================
FILE: src/batch/handlers.js
PURPOSE: Mode-specific update handlers + the bulk API executor.
WHY HERE?
- Separates decision-making (compare.js) from mutation mechanics.
================================================================================
*/

const { appRedis } = require("../services/queue");
const { wooApi } = require("../services/woo-helpers");
const { scheduleApiRequest } = require("../services/job-manager");
const { logErrorToFile, logInfoToFile } = require("../utils/logger");
const { createUniqueJobId } = require("../utils/utils");
const { isUpdateNeeded } = require("./compare");

/**
* @function handleQuantityUpdate
* @description Pushes a minimal update when quantity value actually changes.
* @returns {boolean} true if we queued an update; false if skipped.
*/
function handleQuantityUpdate(newData, currentData, toUpdate, productId, item) {
  const currentQuantity = currentData.meta_data.find((m) => m.key === "quantity")?.value || "0";
  const newQuantity = newData.meta_data.find((m) => m.key === "quantity")?.value || "0";

  if (currentQuantity === newQuantity) {
    logInfoToFile(`🔎 Skipping ${item.part_number}, quantity unchanged: ${currentQuantity}`);
    return false;
  }

  toUpdate.push({
    id: productId,
    manufacturer: item.manufacturer,
    meta_data: [{ key: "quantity", value: String(newQuantity) }],
  });

  return true;
}

/**
* @function handleFullUpdate
* @description Queues a full update (entire newData) only if comparison says so.
*/
function handleFullUpdate(newData, currentData, toUpdate, productId, item, fileKey) {
  if (!isUpdateNeeded(currentData, newData, undefined, undefined, item.part_number, fileKey)) {
    logInfoToFile(`Skipping ${item.part_number} (no changes detected).`);
    return false;
  }
  toUpdate.push(newData);
  return true;
}

/**
* @function executeBatchUpdate
* @description Sends the collected `toUpdate` payload to Woo via bulk endpoint.
* Retries with exponential backoff on errors.
* @param {Array} toUpdate - The array of WooUpdate objects.
* @param {string} fileKey - Used for Redis counters + job id.
* @param {number} MAX_RETRIES - Max attempts before surfacing fatal error.
* @effects
* - Increments Redis key `updated-products:<fileKey>` by the number of
* successfully updated items reported by Woo.
* @throws {Error} when all retries fail.
*/
async function executeBatchUpdate(toUpdate, fileKey, MAX_RETRIES) {
  if (toUpdate.length === 0) {
    logInfoToFile(`No valid products to update in this batch for ${fileKey}. Done.`);
    return;
  }

  let attempts = 0;
  while (attempts < MAX_RETRIES) {
    try {
      const jobId = createUniqueJobId(fileKey, "processBatch", 0, attempts);
      const response = await scheduleApiRequest(
        () => wooApi.put("products/batch", { update: toUpdate }),
        { id: jobId }
      );

      const updatedCount = response.data?.update?.length || 0;
      await appRedis.incrBy(`updated-products:${fileKey}`, updatedCount);
      return; // success
    } catch (err) {
      attempts++;
      logErrorToFile(`Batch update attempt ${attempts} for file="${fileKey}" failed: ${err.message}`);
      if (attempts >= MAX_RETRIES) throw new Error(`Batch update failed permanently after ${MAX_RETRIES} attempts.`);
      // Exponential backoff: 2^attempts seconds
      await new Promise((r) => setTimeout(r, Math.pow(2, attempts) * 1000));
    }
  }
}

module.exports = { handleQuantityUpdate, handleFullUpdate, executeBatchUpdate };
