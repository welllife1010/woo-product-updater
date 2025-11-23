/*
================================================================================
FILE: src/batch/io-status.js
PURPOSE: File I/O for status bookkeeping and missing-product capture.
WHY A SEPARATE FILE?
- Avoids sprinkling fs logic and try/catch across the core loop.
- Easy to stub in unit tests.
================================================================================
*/

const fs = require("fs");
const path = require("path");
const { logErrorToFile, logInfoToFile } = require("../../logger");

/**
* @typedef {Object} BatchStatus
* @property {string[]} updated - Human-readable notes for updated rows.
* @property {string[]} skipped - Human-readable notes for skipped rows.
* @property {string[]} failed - Human-readable notes for failed rows.
*/

/**
* @function recordBatchStatus
* @description Merges incremental status arrays into a durable JSON file so you
* can inspect progress even if the process crashes mid-file.
* @param {string} fileKey - Source CSV key (e.g., 'vendor-x/file.csv').
* @param {string[]} updatedParts - Notes for updated rows accumulated so far.
* @param {string[]} skippedParts - Notes for skipped rows accumulated so far.
* @param {string[]} failedParts - Notes for failed rows accumulated so far.
* @effects Writes/creates: ./batch_status/<fileKey no .csv>/batch_status.json
* @failure Never throws; logs errors to file.
*/
const recordBatchStatus = (fileKey, updatedParts, skippedParts, failedParts) => {
  try {
    // Build directory path per fileKey (preserves vendor/subdir structure).
    const statusDir = path.join(
      __dirname, 
      "../../batch_status", 
      fileKey.replace(/\.csv$/, "")
    );

    if (!fs.existsSync(statusDir)) fs.mkdirSync(statusDir, { recursive: true }); // Create full path recursively

    // Define the file path inside the created subfolder
    const statusFilePath = path.join(statusDir, "batch_status.json");

    /** @type {BatchStatus} */
    let batchStatus = { updated: [], skipped: [], failed: [] };

    // Merge with existing on-disk state to preserve earlier iterations
    if (fs.existsSync(statusFilePath)) {
      try {
        batchStatus = JSON.parse(fs.readFileSync(statusFilePath, "utf-8"));
      } catch (err) {
        logErrorToFile(`❌ Error reading batch status file: ${err.message}`);
      }
    }

    // Append new part numbers to the respective lists
    batchStatus.updated.push(...updatedParts);
    batchStatus.skipped.push(...skippedParts);
    batchStatus.failed.push(...failedParts);

    // Remove duplicates (de-dup)
    batchStatus.updated = [...new Set(batchStatus.updated)];
    batchStatus.skipped = [...new Set(batchStatus.skipped)];
    batchStatus.failed = [...new Set(batchStatus.failed)];

    // Write the updated batch status back to the file
    fs.writeFileSync(statusFilePath, JSON.stringify(batchStatus, null, 2));
    logInfoToFile(`✅ Saved batch status to ${statusFilePath}`);
  } catch (err) {
    logErrorToFile(`❌ Error writing batch status file: ${err.message}`);
  }
};

/**
 * @function toSlug
 * @description Turn a name into a URL/file-safe slug.
 *   e.g. "Microcontrollers" → "microcontrollers"
 *        "LED Emitters (IR/UV)" → "led-emitters-ir-uv"
 */
function toSlug(str) {
  if (!str) return "unknown";
  return String(str)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-") // non-alphanumeric → "-"
    .replace(/^-+|-+$/g, "");    // trim leading/trailing "-"
}

/**
 * @function recordMissingProduct
 * @description
 *   Persist a full CSV row when we cannot resolve a matching WooCommerce
 *   productId for that row. This is used later by `create-missing-products.js`
 *   to actually create new Woo products for these "missing" items.
 *
 *   The row is grouped by:
 *     - its *source CSV file*       → via `fileKey`
 *     - its *leaf category (slug)* → derived from item.category / item.Category
 *
 *   This gives us a clear folder structure for follow-up:
 *
 *     ./missing-products/
 *       missing-[leafCategorySlug]/
 *         missing_products_[cleanFileKey].json
 *
 *   where:
 *     - leafCategorySlug:
 *         - the last segment of the category path from the CSV row
 *           e.g. "Integrated Circuits (ICs)>Embedded>Microcontrollers"
 *           → leaf name "Microcontrollers"
 *           → slug "microcontrollers"
 *
 *     - cleanFileKey:
 *         - `fileKey` with the trailing ".csv" removed
 *         - e.g. "product-microcontrollers-03112025_part4.csv"
 *           → "product-microcontrollers-03112025_part4"
 *
 * @param {string} fileKey
 *   The original CSV key / file identifier for this batch.
 *   Examples:
 *     - "LED-Emitters-IR-UV-Visible.csv"
 *     - "product-microcontrollers-03112025_part4.csv"
 *     - "vendor-x/ics/microcontrollers-part2.csv"
 *
 * @param {Object} item
 *   The raw CSV row (single row) object for the missing product. Must contain at least:
 *     - item.part_number (for logging)
 *     - item.category or item.Category (optional but recommended) in
 *       the form "Main>Sub>Leaf" so we can derive the leaf category.
 *
 * @effects
 *   - Ensures the folder:
 *       ./missing-products/missing-[leafCategorySlug]/
 *   - Appends `item` into:
 *       ./missing-products/missing-[leafCategorySlug]/missing_products_[cleanFileKey].json
 *   - Creates directories and files as needed.
 *
 * @failure
 *   - Never throws to the caller; logs any I/O problems via logErrorToFile().
 */
const recordMissingProduct = (fileKey, item) => {
  try {
    // 1) Clean the fileKey (drop .csv so we can reuse the base name)
    const cleanFileKey = fileKey.replace(/\.csv$/i, "");

    // 2) Derive the leaf category slug from the CSV row, if possible.
    //    Example:
    //      item.category = "Integrated Circuits (ICs)>Embedded>Microcontrollers"
    //      → parts = ["Integrated Circuits (ICs)", "Embedded", "Microcontrollers"]
    //      → leaf name = "Microcontrollers"
    //      → leafCategorySlug = "microcontrollers"
    const rawCategory = item.category || item.Category || "";
    let leafCategorySlug = "unknown";

    if (rawCategory) {
      const parts = String(rawCategory)
        .split(">")
        .map((p) => p.trim())
        .filter(Boolean);

      if (parts.length) {
        const leafName = parts[parts.length - 1]; // last segment = leaf category
        leafCategorySlug = toSlug(leafName);
      }
    }

    // 3) Build the folder + file path:
    //    ./missing-products/missing-[leafCategorySlug]/missing_products_[cleanFileKey].json
    const missingDir = path.join(
      __dirname,
      "../../missing-products",
      `missing-${leafCategorySlug}`
    );

    if (!fs.existsSync(missingDir)) {
      fs.mkdirSync(missingDir, { recursive: true });
    }

    const missingFilePath = path.join(
      missingDir,
      `missing_products_${cleanFileKey}.json`
    );

    // 4) Read any existing missing-products array for this [categorySlug, fileKey]
    let missingProducts = [];
    if (fs.existsSync(missingFilePath)) {
      try {
        missingProducts = JSON.parse(fs.readFileSync(missingFilePath, "utf8"));
      } catch (err) {
        logErrorToFile(
          `Error reading missing products file at ${missingFilePath}: ${err.message}`
        );
      }
    }

    // 5) Append the current CSV row to the list
    missingProducts.push(item);

    // 6) Write the updated array back to disk
    fs.writeFileSync(missingFilePath, JSON.stringify(missingProducts, null, 2));

    logInfoToFile(
      `Recorded missing product for part_number=${item.part_number} in file ${missingFilePath}`
    );
  } catch (err) {
    logErrorToFile(`Error writing missing products file: ${err.message}`);
  }
};

module.exports = { recordBatchStatus, recordMissingProduct };
