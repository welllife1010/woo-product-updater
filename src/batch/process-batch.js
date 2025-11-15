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

// Fuzzy category resolver (Fuse.js + category-hierarchy-ref.csv)
const { resolveCategory } = require("../../category-map");

/**
 * Try to extract a "vendor category" hint from a CSV row.
 * Adjust these keys once you know the real column names
 * that vendors are using for category-ish info.
 */
function getVendorCategoryFromRow(item) {
  if (!item || typeof item !== "object") return "";

  // These are guesses – update to match your real CSV headers (after normalization)
  return (
    item.category ||
    item.product_category ||
    item["product_category"] ||
    item["category"] ||
    ""
  );
}

/**
 * Build a human-readable category path from a resolvedCategory object.
 * e.g. { main: "Cables, Wires", sub: "Fiber Optic Cables", sub2: null }
 *  -> "Cables, Wires > Fiber Optic Cables"
 */
function buildCategoryPath(resolvedCategory) {
  if (!resolvedCategory) return "";

  const parts = [
    resolvedCategory.main,
    resolvedCategory.sub,
    resolvedCategory.sub2,
  ].filter(Boolean); // drop null/undefined/empty

  return parts.join(" > ");
}

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
        fileKey
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

      // --------------------------------------------
      // Resolve category from vendor data (if any)
      // --------------------------------------------
      let resolvedCategory = null;

      // No need to fuzzy-match categories when we're only doing quantity updates
      if (updateMode === "full") {
        try {
          const vendorCategory = getVendorCategoryFromRow(item);

          if (vendorCategory) {
            resolvedCategory = await resolveCategory(vendorCategory);

            if (resolvedCategory) {
              const path = buildCategoryPath(resolvedCategory);
              logInfoToFile(
                `"processBatch()" - Category match for part_number=${item.part_number}: ` +
                `"${vendorCategory}" → "${path}" (score=${resolvedCategory.score.toFixed(
                  3
                )}, matchedOn=${resolvedCategory.matchedOn})`
              );
            } else {
              logInfoToFile(
                `"processBatch()" - No category match for part_number=${item.part_number}, vendorCategory="${vendorCategory}"`
              );
            }
          } else {
            logInfoToFile(
              `"processBatch()" - No vendorCategory field present for part_number=${item.part_number}`
            );
          }
        } catch (catErr) {
          logErrorToFile(
            `"processBatch()" - Category resolution error for part_number=${item.part_number}: ${catErr.message}`,
            catErr.stack
          );
        }
      }

      // --------------------------------------------
      // Generate new data for update (existing logic)
      // --------------------------------------------
      // Step C) Build the candidate update payload
      const newData = createNewData(item, productId, item.part_number)

      // --------------------------------------------
      // Attach proposed category info to meta_data
      // (safe: does NOT change actual Woo categories yet)
      // --------------------------------------------
      if (resolvedCategory && Array.isArray(newData.meta_data)) {
        const path = buildCategoryPath(resolvedCategory);

        newData.meta_data.push(
          {
            key: "proposed_category_main",
            value: resolvedCategory.main || "",
          },
          {
            key: "proposed_category_sub",
            value: resolvedCategory.sub || "",
          },
          {
            key: "proposed_category_sub2",
            value: resolvedCategory.sub2 || "",
          },
          {
            key: "proposed_category_path",
            value: path,
          }
        );
      }

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
            item
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
