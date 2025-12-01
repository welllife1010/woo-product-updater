/**
 * =============================================================================
 * FILE: src/batch/process-batch.js
 * =============================================================================
 * 
 * PURPOSE:
 * Orchestrates the processing of a single batch of CSV rows.
 * This is the core business logic that:
 *   1. Fetches existing product data from WooCommerce
 *   2. Compares CSV data with existing data
 *   3. Builds update payloads for changed products
 *   4. Sends bulk updates to WooCommerce
 * 
 * FLOW:
 *   For each row in batch:
 *     1. Fetch product from WooCommerce (by part_number + manufacturer)
 *     2. Validate product identity
 *     3. Resolve category from vendor data
 *     4. Build update payload
 *     5. Compare with existing data
 *     6. Queue for update if changed
 *   After all rows:
 *     7. Send bulk update to WooCommerce
 *     8. Update Redis counters
 * 
 * BUG FIXES (2025):
 * 
 * BUG #4 - Missing Error Handling for Category Resolution:
 *   PROBLEM: If resolveCategory() throws an error, the entire batch stops.
 *   FIX: Added try-catch around resolveCategory() call. If it fails,
 *        we log the error and continue processing without category assignment.
 * 
 * BUG #6 - Memory Leak in Large Batch Processing:
 *   PROBLEM: updatedParts, skippedParts, failedParts arrays grow indefinitely.
 *            For large batches, this consumes significant memory.
 *   FIX: Added periodic flushing of status arrays. Every STATUS_FLUSH_INTERVAL
 *        rows, we write to disk and clear the arrays.
 * 
 * =============================================================================
 */

// =============================================================================
// IMPORTS
// =============================================================================

// Redis client for progress tracking
const { appRedis } = require("../../queue");

// Logging utilities
const { logInfoToFile, logErrorToFile } = require("../../logger");

// Product lookup and validation
const { fetchProductData, validateProductMatch } = require("./fetch-validate");

// Payload builder
const { createNewData } = require("./map-new-data");

// Update handlers
const {
  handleQuantityUpdate,
  handleFullUpdate,
  executeBatchUpdate,
} = require("./handlers");

// Status recording
const { recordBatchStatus } = require("./io-status");

// Category resolution (fuzzy matching)
const { resolveCategory } = require("../../category-map");

// Category hierarchy creation in WooCommerce
const { ensureCategoryHierarchy } = require("../../category-woo");
const { log } = require("util");

// =============================================================================
// CONFIGURATION
// =============================================================================

/**
 * STATUS_FLUSH_INTERVAL: How often to flush status arrays to disk.
 * 
 * BUG #6 FIX:
 * Instead of keeping all status messages in memory until batch completion,
 * we periodically flush them to disk and clear the arrays.
 * 
 * Set this based on your memory constraints:
 *   - Lower value = less memory usage, more disk writes
 *   - Higher value = more memory usage, fewer disk writes
 * 
 * Default: 50 rows (flush every 50 rows processed)
 */
const STATUS_FLUSH_INTERVAL = parseInt(process.env.STATUS_FLUSH_INTERVAL) || 50;

/**
 * MAX_RETRIES: Maximum retry attempts for the bulk update call.
 */
const MAX_RETRIES = 5;

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Extract vendor category information from a CSV row.
 * 
 * Vendors may provide category info in various columns:
 *   - main_category, sub_category, 2nd_sub_category (explicit hierarchy)
 *   - category (single column with full path)
 *   - product_category (alternative naming)
 * 
 * @param {Object} item - A single CSV row (object with column names as keys)
 * @returns {string} - Category string for fuzzy matching, or empty string
 * 
 * @example
 * // Explicit columns:
 * getVendorCategoryFromRow({
 *   main_category: "Integrated Circuits",
 *   sub_category: "Embedded",
 *   "2nd_sub_category": "Microcontrollers"
 * });
 * // Returns: "Integrated Circuits > Embedded > Microcontrollers"
 * 
 * // Single column:
 * getVendorCategoryFromRow({ category: "ICs > MCUs" });
 * // Returns: "ICs > MCUs"
 */
function getVendorCategoryFromRow(item) {
  if (!item || typeof item !== "object") return "";

  const parts = [];

  // Try explicit hierarchy columns first (most specific)
  if (item.main_category) parts.push(item.main_category);
  if (item.sub_category) parts.push(item.sub_category);
  if (item["2nd_sub_category"]) parts.push(item["2nd_sub_category"]);

  // Fallback to single category columns
  if (!parts.length && item.category) parts.push(item.category);
  if (!parts.length && item.product_category) parts.push(item.product_category);

  // Join with " > " separator for fuzzy matching
  return parts.filter(Boolean).join(" > ");
}

/**
 * Build a human-readable category path from a resolved category object.
 * 
 * @param {Object} resolvedCategory - Object with main, sub, sub2 properties
 * @returns {string} - Formatted path like "Main > Sub > Sub2"
 * 
 * @example
 * buildCategoryPath({ main: "ICs", sub: "Embedded", sub2: "MCUs" });
 * // Returns: "ICs > Embedded > MCUs"
 */
function buildCategoryPath(resolvedCategory) {
  if (!resolvedCategory) return "";

  const parts = [
    resolvedCategory.main,
    resolvedCategory.sub,
    resolvedCategory.sub2,
  ].filter(Boolean);

  return parts.join(" > ");
}

// =============================================================================
// MAIN FUNCTION: processBatch
// =============================================================================

/**
 * Process a batch of CSV rows and send updates to WooCommerce.
 * 
 * This is the main orchestration function called by the worker for each job.
 * 
 * @param {Array<Object>} batch - Array of CSV row objects to process
 * @param {number} startIndex - Starting row index (for logging and tracking)
 * @param {number} totalProductsInFile - Total rows in the CSV file
 * @param {string} fileKey - File identifier for logging and Redis counters
 * @returns {Promise<void>}
 * 
 * @throws {Error} If batch is not an array (validation failure)
 * 
 * @example
 * await processBatch(
 *   [{ part_number: "ABC123", manufacturer: "Acme", quantity: 100 }],
 *   0,       // Starting at row 0
 *   1000,    // File has 1000 total rows
 *   "products.csv"
 * );
 */
async function processBatch(batch, startIndex, totalProductsInFile, fileKey) {
  // =========================================================================
  // CONFIGURATION
  // =========================================================================
  
  /**
   * UPDATE_MODE determines what data we update:
   *   - "quantity": Only update stock quantity (faster, less API load)
   *   - "full": Update all fields (name, description, categories, etc.)
   */
  const updateMode = process.env.UPDATE_MODE || "full";

  logInfoToFile(
    `processBatch() - Starting | ` +
    `startIndex=${startIndex} | ` +
    `batchSize=${batch.length} | ` +
    `fileKey=${fileKey} | ` +
    `mode=${updateMode}`
  );

  // =========================================================================
  // INPUT VALIDATION
  // =========================================================================
  
  if (!Array.isArray(batch)) {
    throw new Error(
      `processBatch() - Expected batch to be an array, got ${typeof batch}`
    );
  }

  // =========================================================================
  // INITIALIZE TRACKING VARIABLES
  // =========================================================================
  
  /**
   * toUpdate: Accumulator for products that need updating.
   * These are sent to WooCommerce in a single bulk API call at the end.
   */
  const toUpdate = [];

  /**
   * Counters for Redis (global progress tracking):
   *   - skipCount: Products skipped (no changes needed or validation failed)
   *   - localFailCount: Products that failed to process (errors)
   */
  let skipCount = 0;
  let localFailCount = 0;

  /**
   * BUG #6 FIX: Status arrays for human-readable logging.
   * 
   * PROBLEM:
   * These arrays were growing indefinitely for large batches, causing memory issues.
   * 
   * SOLUTION:
   * We now flush these arrays periodically (every STATUS_FLUSH_INTERVAL rows)
   * and clear them to free memory.
   */
  let updatedParts = [];
  let skippedParts = [];
  let failedParts = [];

  // =========================================================================
  // MAIN PROCESSING LOOP
  // =========================================================================
  
  for (let i = 0; i < batch.length; i++) {
    const item = batch[i];
    const currentIndex = startIndex + i; // Absolute row index in the CSV

    // -----------------------------------------------------------------------
    // Guard: Stop if we've exceeded the file size (shouldn't happen normally)
    // -----------------------------------------------------------------------
    if (currentIndex >= totalProductsInFile) {
      logInfoToFile(
        `processBatch() - Reached end of file at index ${currentIndex}. Stopping.`
      );
      break;
    }

    // -----------------------------------------------------------------------
    // Guard: Skip rows without part_number (required field)
    // -----------------------------------------------------------------------
    if (!item.part_number) {
      localFailCount++;
      failedParts.push(
        `Row ${currentIndex + 1}: Missing part_number - skipped`
      );
      continue;
    }

    // -----------------------------------------------------------------------
    // PROCESS THIS ROW
    // -----------------------------------------------------------------------
    try {
      // =====================================================================
      // STEP A: Fetch product from WooCommerce
      // =====================================================================
      /**
       * fetchProductData does the following:
       *   1. Looks up product by part_number + manufacturer
       *   2. If found, fetches the full product data
       *   3. If NOT found, records as "missing product" for later creation
       * 
       * Returns: { productId, currentData } or { null, null } if not found
       */
      const { productId, currentData } = await fetchProductData(
        item,
        currentIndex,
        totalProductsInFile,
        fileKey
      );

      if (!productId || !currentData) {
        // Product not found in WooCommerce - already recorded as missing
        // Count as skipped (not failed - this is expected for new products)
        skipCount++;
        skippedParts.push(
          `Row ${currentIndex + 1}: ${item.part_number} - product not found (saved to missing)`
        );
        continue;
      }

      // =====================================================================
      // STEP B: Validate product identity
      // =====================================================================
      /**
       * validateProductMatch ensures we're updating the RIGHT product.
       * This is a safety check against incorrect part_number matches.
       */
      if (!validateProductMatch(item, currentData, productId, fileKey)) {
        skipCount++;
        skippedParts.push(
          `Row ${currentIndex + 1}: ${item.part_number} - identity mismatch, skipped`
        );
        // Write status after validation failures
        recordBatchStatus(fileKey, updatedParts, skippedParts, failedParts);
        continue;
      }

      // =====================================================================
      // STEP C: Build update payload from CSV data
      // =====================================================================
      /**
       * createNewData transforms CSV row into WooCommerce update format:
       *   - Maps CSV columns to WooCommerce fields
       *   - Handles meta_data fields
       *   - Cleans and normalizes values
       */
      const newData = createNewData(
        item,
        productId,
        item.part_number,
        currentData
      );

      // =====================================================================
      // STEP D: Resolve category from vendor data
      // =====================================================================
      /**
       * BUG #4 FIX: Error handling for category resolution
       * 
       * PROBLEM:
       * If resolveCategory() threw an error (e.g., malformed category string),
       * the entire batch processing would stop.
       * 
       * SOLUTION:
       * Wrap in try-catch. If category resolution fails:
       *   - Log the error
       *   - Continue processing WITHOUT category assignment
       *   - The product will still be updated with other fields
       */
      let resolvedCategory = null;

      // Only resolve categories in "full" mode (not needed for quantity-only)
      if (updateMode === "full") {
        const vendorCategory = getVendorCategoryFromRow(item);

        if (vendorCategory) {
          try {
            // BUG #4 FIX: Wrapped in try-catch
            resolvedCategory = await resolveCategory(vendorCategory);

            if (resolvedCategory) {
              const categoryPath = buildCategoryPath(resolvedCategory);
              logInfoToFile(
                `processBatch() - Category resolved for ${item.part_number}: ` +
                `"${vendorCategory}" → "${categoryPath}" ` +
                `(score=${resolvedCategory.score?.toFixed(3) || "N/A"}, ` +
                `matchedOn=${resolvedCategory.matchedOn || "N/A"})`
              );
            } else {
              logInfoToFile(
                `processBatch() - No category match for ${item.part_number}: "${vendorCategory}"`
              );
            }
          } catch (categoryError) {
            // BUG #4 FIX: Log error but continue processing
            logErrorToFile(
              `processBatch() - ⚠️ Category resolution FAILED for ${item.part_number}: ` +
              `${categoryError.message}. Continuing without category.`,
              categoryError.stack
            );
            // resolvedCategory remains null - product will be updated without categories
          }
        }
      }

      // =====================================================================
      // STEP E: Apply resolved category to newData
      // =====================================================================
      if (resolvedCategory) {
        try {
          
          logInfoToFile(
            `processBatch() - Applying resolved category to ${item.part_number}`
          );

          const hierarchy = await ensureCategoryHierarchy(resolvedCategory);

          if (hierarchy && Array.isArray(hierarchy.ids) && hierarchy.ids.length) {
            // Merge with any existing categories
            const existingIds = (newData.categories || []).map((c) => c.id);
            const mergedIds = Array.from(
              new Set([...existingIds, ...hierarchy.ids])
            );

            newData.categories = mergedIds.map((id) => ({ id }));

            logInfoToFile(
              `processBatch() - Assigned categories [${mergedIds.join(", ")}] ` +
              `to ${item.part_number}`
            );
          }
        } catch (catApplyErr) {
          // Log but continue - product will be updated without new categories
          logErrorToFile(
            `processBatch() - Failed to apply category hierarchy for ${item.part_number}: ` +
            `${catApplyErr.message}`,
            catApplyErr.stack
          );
        }
      }

      // =====================================================================
      // STEP F: Route by update mode and queue for update
      // =====================================================================
      if (updateMode === "quantity") {
        // Quantity-only mode: Only update stock quantity
        if (handleQuantityUpdate(newData, currentData, toUpdate, productId, item)) {
          updatedParts.push(
            `Row ${currentIndex + 1}: ${item.part_number} - quantity updated`
          );
        } else {
          skipCount++;
          skippedParts.push(
            `Row ${currentIndex + 1}: ${item.part_number} - quantity unchanged`
          );
        }
      } else {
        // Full mode: Update all fields
        // Note: Passing fileKey for Bug #1 fix (was missing before)
        if (handleFullUpdate(newData, currentData, toUpdate, productId, item, fileKey)) {
          updatedParts.push(
            `Row ${currentIndex + 1}: ${item.part_number} - fully updated`
          );
        } else {
          skipCount++;
          skippedParts.push(
            `Row ${currentIndex + 1}: ${item.part_number} - no changes detected`
          );
        }
      }

    } catch (err) {
      // =====================================================================
      // ERROR HANDLING: Unexpected errors during row processing
      // =====================================================================
      localFailCount++;
      failedParts.push(
        `Row ${currentIndex + 1}: ${item.part_number} - FAILED: ${err.message}`
      );
      logErrorToFile(
        `processBatch() - Error processing ${item.part_number}: ${err.message}`,
        err.stack
      );
      // Continue to next row - don't let one failure stop the batch
    }

    // =========================================================================
    // BUG #6 FIX: Periodic status flush to prevent memory buildup
    // =========================================================================
    /**
     * PROBLEM:
     * For large batches (e.g., 1000+ rows), the status arrays would grow
     * continuously, consuming significant memory.
     * 
     * SOLUTION:
     * Every STATUS_FLUSH_INTERVAL rows, we:
     *   1. Write current status to disk
     *   2. Clear the arrays to free memory
     * 
     * This keeps memory usage bounded regardless of batch size.
     */
    if ((i + 1) % STATUS_FLUSH_INTERVAL === 0) {
      // Flush status to disk
      recordBatchStatus(fileKey, updatedParts, skippedParts, failedParts);
      
      // Clear arrays to free memory
      logInfoToFile(
        `processBatch() - 🧹 Flushed status arrays at row ${currentIndex + 1} ` +
        `(updated: ${updatedParts.length}, skipped: ${skippedParts.length}, failed: ${failedParts.length})`
      );
      
      updatedParts = [];
      skippedParts = [];
      failedParts = [];
    }
  }

  // =========================================================================
  // POST-LOOP: Final status flush
  // =========================================================================
  /**
   * Flush any remaining status messages that weren't caught by the
   * periodic flush (for batches not evenly divisible by STATUS_FLUSH_INTERVAL).
   */
  if (updatedParts.length > 0 || skippedParts.length > 0 || failedParts.length > 0) {
    recordBatchStatus(fileKey, updatedParts, skippedParts, failedParts);
    logInfoToFile(
      `processBatch() - 🧹 Final status flush ` +
      `(updated: ${updatedParts.length}, skipped: ${skippedParts.length}, failed: ${failedParts.length})`
    );
  }

  // =========================================================================
  // UPDATE REDIS COUNTERS
  // =========================================================================
  /**
   * Increment global counters in Redis for progress tracking.
   * These are used by the UI and checkpointing system.
   */
  if (skipCount > 0) {
    await appRedis.incrBy(`skipped-products:${fileKey}`, skipCount);
  }
  if (localFailCount > 0) {
    await appRedis.incrBy(`failed-products:${fileKey}`, localFailCount);
  }

  // =========================================================================
  // SEND BULK UPDATE TO WOOCOMMERCE
  // =========================================================================
  /**
   * All queued updates are sent in a single API call for efficiency.
   * This is much faster than updating products one-by-one.
   * 
   * executeBatchUpdate handles:
   *   - Retry logic for API failures
   *   - Incrementing the updated-products counter in Redis
   */
  await executeBatchUpdate(toUpdate, fileKey, MAX_RETRIES);

  logInfoToFile(
    `processBatch() - ✅ Completed | ` +
    `Rows: ${startIndex}-${startIndex + batch.length - 1} | ` +
    `Updates queued: ${toUpdate.length} | ` +
    `Skipped: ${skipCount} | ` +
    `Failed: ${localFailCount}`
  );
}

// =============================================================================
// EXPORTS
// =============================================================================

module.exports = { processBatch };