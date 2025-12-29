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
const { logErrorToFile, logInfoToFile } = require("../utils/logger");

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
 * @function recordMissingProduct
 * @description
 *   Save a CSV row to a JSON file so we can create this product later.
 *
 *   The caller (fetchProductData) is responsible for figuring out:
 *     - which leafCategorySlug (Woo-style slug) this row belongs to.
 *
 *   Grouping:
 *     ./missing-products/missing-[leafCategorySlug]/missing_products_[cleanFileKey].json
 *
 *   Example:
 *     fileKey          = "product-microcontrollers-03112025_part4.csv"
 *     leafCategorySlug = "microcontrollers"
 *
 *     → folder:
 *         ./missing-products/missing-microcontrollers/
 *       file:
 *         missing_products_product-microcontrollers-03112025_part4.json
 *
 * @param {string} fileKey
 *   CSV identifier (often the filename).
 *
 * @param {Object} item
 *   The raw CSV row object.
 *
 * @param {string} [leafCategorySlug="unknown"]
 *   The Woo-like leaf category slug decided by category-resolver.
 */
const recordMissingProduct = (
  fileKey,
  item,
  leafCategorySlug = "unknown"
) => {
  try {
    // 1) Drop the ".csv" extension so we can reuse the base filename.
    const cleanFileKey = fileKey.replace(/\.csv$/i, "").replace(/\//g, "_");

    // 2) Make sure slug is safe to use in a folder name.
    //    (If resolver gave us "", fall back to "unknown".)
    const safeSlug = leafCategorySlug || "unknown";

    // 3) Build:
    //      ./missing-products/missing-[safeSlug]/
    const missingDir = path.join(
      __dirname,
      "../../missing-products",
      `missing-${safeSlug}`
    );

    if (!fs.existsSync(missingDir)) {
      fs.mkdirSync(missingDir, { recursive: true });
    }

    // 4) JSON file inside that folder:
    //      missing_products_[cleanFileKey].json
    const missingFilePath = path.join(
      missingDir,
      `missing_products_${cleanFileKey}.json`
    );

    // 5) Load existing array if the file is already there.
    let missingProducts = [];
    if (fs.existsSync(missingFilePath)) {
      try {
        missingProducts = JSON.parse(
          fs.readFileSync(missingFilePath, "utf8")
        );
      } catch (err) {
        logErrorToFile(
          `Error reading missing products file at ${missingFilePath}: ${err.message}`
        );
      }
    }

    // 6) Append the current row.
    missingProducts.push(item);

    // 7) Write it back to disk (pretty-printed for debugging).
    fs.writeFileSync(
      missingFilePath,
      JSON.stringify(missingProducts, null, 2)
    );

    logInfoToFile(
      `Recorded missing product for part_number=${item.part_number} in file ${missingFilePath}`
    );
  } catch (err) {
    logErrorToFile(`Error writing missing products file: ${err.message}`);
  }
};

module.exports = { recordBatchStatus, recordMissingProduct };
