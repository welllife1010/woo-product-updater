/*
================================================================================
FILE: src/batch/handlers.js
PURPOSE: Mode-specific update handlers + the bulk API executor.
WHY HERE?
- Separates decision-making from mutation mechanics.
- Uses centralized buildUpdatePayload for diff-based updates.
================================================================================
*/

const { appRedis } = require("../services/queue");
const { wooApi } = require("../services/woo-helpers");
const { scheduleApiRequest } = require("../services/job-manager");
const { logErrorToFile, logInfoToFile } = require("../utils/logger");
const { createUniqueJobId } = require("../utils/utils");
const { buildUpdatePayload, buildQuantityOnlyPayload } = require("./build-update-payload");

/**
* @function handleQuantityUpdate
* @description Pushes a minimal update when quantity value actually changes.
* Uses centralized buildQuantityOnlyPayload for consistency.
* @returns {boolean} true if we queued an update; false if skipped.
*/
function handleQuantityUpdate(newData, currentData, toUpdate, productId, item) {
  const { payload, changed } = buildQuantityOnlyPayload(
    currentData, 
    newData, 
    productId, 
    item.part_number
  );

  if (!changed || !payload) {
    logInfoToFile(`🔎 Skipping ${item.part_number}, quantity unchanged`);
    return false;
  }

  // Add manufacturer for reference
  payload.manufacturer = item.manufacturer;
  toUpdate.push(payload);
  return true;
}

/**
* @function handleFullUpdate
* @description Queues a diff-based update (only changed fields).
* Uses centralized buildUpdatePayload for all field-level decisions.
*/
function handleFullUpdate(newData, currentData, toUpdate, productId, item, fileKey) {
  const { payload, changedFields, skippedFields } = buildUpdatePayload(
    currentData,
    newData,
    item.part_number,
    fileKey
  );

  if (!payload || changedFields.length === 0) {
    logInfoToFile(`Skipping ${item.part_number} (no changes detected).`);
    return false;
  }

  toUpdate.push(payload);
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
      // Decrement processing counter for successfully updated products
      if (updatedCount > 0) {
        await appRedis.decrBy(`processing-products:${fileKey}`, updatedCount);
      }
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
