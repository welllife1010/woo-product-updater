/*
================================================================================
FILE: src/batch/process-batch.js
PURPOSE: Orchestrate a single batch lifecycle end-to-end.
FLOW:
1) Validate inputs
2) For each row: fetch → validate → build → handle (by mode)
3) Persist human-friendly status snapshots as we go
4) Fire one bulk update to Woo at the end
================================================================================
*/

const { appRedis } = require("../../queue")
const { logInfoToFile, logErrorToFile } = require("../../logger")

const { fetchProductData, validateProductMatch } = require("./fetch-validate")
const { createNewData } = require("./map-new-data")
const {
  handleQuantityUpdate,
  handleFullUpdate,
  executeBatchUpdate,
} = require("./handlers")
const { recordBatchStatus } = require("./io-status")

/**
 * @function processBatch
 * @description Processes a contiguous slice (batch) of CSV rows and pushes
 * a single bulk update to WooCommerce.
 * @param {Array<Object>} batch - Array of CSV rows to process.
 * @param {number} startIndex - Zero-based index into the full CSV file for row #1 in this batch.
 * @param {number} totalProductsInFile - Full file length; used for guards + logs.
 * @param {string} fileKey - The logical key (often S3 key) used for logs & counters.
 * @returns {Promise<void>}
 * @important
 * - Idempotency: comparison prevents unnecessary writes.
 * - Crash safety: recordBatchStatus() is called *inside* the loop so a crash
 * still leaves breadcrumbs about what happened up to that row.
 */
async function processBatch(batch, startIndex, totalProductsInFile, fileKey) {
  const updateMode = process.env.UPDATE_MODE || "full"
  const MAX_RETRIES = 5

  logInfoToFile(
    `Starting "processBatch()" startIndex=${startIndex}, fileKey=${fileKey}, Mode: ${updateMode}`
  )
  if (!Array.isArray(batch))
    throw new Error(
      `"processBatch()" - Expected batch to be an array, got ${typeof batch}`
    )

  // We collect pending updates and commit once via bulk API for efficiency.
  const toUpdate = []

  // Local counters for this batch run; mirrored in Redis for global view.
  let skipCount = 0
  let localFailCount = 0

  // Human-readable notes for JSON status file; these are appended every loop.
  const updatedParts = []
  const skippedParts = []
  const failedParts = []

  for (let i = 0; i < batch.length; i++) {
    const item = batch[i]
    const currentIndex = startIndex + i // absolute index into the CSV

    // Guard 1: stop if caller passed an overlong batch near EOF
    if (currentIndex >= totalProductsInFile) break

    // Guard 2: part_number is mandatory for all downstream lookups
    if (!item.part_number) {
      localFailCount++
      continue
    }

    try {
      // Step A) Resolve Woo product + current state
      const { productId, currentData } = await fetchProductData(
        item,
        currentIndex,
        totalProductsInFile,
        fileKe
      )
      if (!productId || !currentData) {
        localFailCount++
        continue
      }

      // Step B) Ensure identity still matches (safety against mis-resolves)
      if (!validateProductMatch(item, currentData, productId, fileKey)) {
        skipCount++
        skippedParts.push(
          `Row ${currentIndex + 1}: ${item.part_number} identity mismatch`
        )
        recordBatchStatus(fileKey, updatedParts, skippedParts, failedParts)
        continue
      }

      // Step C) Build the candidate update payload
      const newData = createNewData(item, productId, item.part_number)

      // Step D) Route by UPDATE_MODE
      if (updateMode === "quantity") {
        if (
          handleQuantityUpdate(newData, currentData, toUpdate, productId, item)
        ) {
          updatedParts.push(
            `Row ${currentIndex + 1}: ${item.part_number} quantity updated.`
          )
        } else {
          skipCount++
          skippedParts.push(
            `Row ${currentIndex + 1}: ${item.part_number} quantity skipped.`
          )
        }
      } else {
        if (
          handleFullUpdate(
            newData,
            currentData,
            toUpdate,
            productId,
            item,
            fileKey
          )
        ) {
          updatedParts.push(
            `Row ${currentIndex + 1}: ${item.part_number} fully updated.`
          )
        } else {
          skipCount++
          skippedParts.push(
            `Row ${currentIndex + 1}: ${item.part_number} no changes.`
          )
        }
      }
    } catch (err) {
      // Any unexpected failure on this row is recorded and we continue the loop.
      localFailCount++
      failedParts.push(
        `Row ${currentIndex + 1}: ${item.part_number} failed - ${err.message}`
      )
      logErrorToFile(
        `Error processing part_number=${item.part_number}: ${err.message}`,
        err.stack
      )
    }

    // Persist a snapshot after every row so partial progress is visible on disk.
    recordBatchStatus(fileKey, updatedParts, skippedParts, failedParts)
  }

  // Mirror batch-level counters to Redis for global dashboards.
  if (skipCount > 0)
    await appRedis.incrBy(`skipped-products:${fileKey}`, skipCount)
  if (localFailCount > 0)
    await appRedis.incrBy(`failed-products:${fileKey}`, localFailCount)

  // Fire the single bulk update call for all queued changes.
  await executeBatchUpdate(toUpdate, fileKey, MAX_RETRIES)
}

module.exports = { processBatch }
