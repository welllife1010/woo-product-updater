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
 *   Grouping (NEW STRUCTURE):
 *     ./missing-products/[CSV-Filename-With-Dashes]/missing-[leafCategorySlug]/missing_products.json
 *
 *   Example:
 *     fileKey          = "test-uploads/Accuris Full Data Report.csv"
 *     leafCategorySlug = "microcontrollers"
 *
 *     → folder:
 *         ./missing-products/Accuris-Full-Data-Report/missing-microcontrollers/
 *       file:
 *         missing_products.json
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
    // 1) Extract just the filename, drop path and ".csv" extension
    //    e.g., "test-uploads/Accuris Full Data Report.csv" → "Accuris Full Data Report"
    const baseFilename = path.basename(fileKey, ".csv");
    
    // 2) Convert filename to dash-separated format
    //    e.g., "Accuris Full Data Report" → "Accuris-Full-Data-Report"
    const csvFolderName = baseFilename
      .replace(/[^a-zA-Z0-9\s-]/g, "") // Remove special chars except spaces and dashes
      .replace(/\s+/g, "-")            // Replace spaces with dashes
      .replace(/-+/g, "-")             // Collapse multiple dashes
      .trim();

    // 3) Make sure slug is safe to use in a folder name.
    //    (If resolver gave us "", fall back to "unknown".)
    const safeSlug = leafCategorySlug || "unknown";

    // 4) Build:
    //      ./missing-products/[CSV-Filename-With-Dashes]/missing-[safeSlug]/
    const missingDir = path.join(
      __dirname,
      "../../missing-products",
      csvFolderName,
      `missing-${safeSlug}`
    );

    if (!fs.existsSync(missingDir)) {
      fs.mkdirSync(missingDir, { recursive: true });
    }

    // 5) JSON file inside that folder (simplified name since folder has context):
    //      missing_products.json
    const missingFilePath = path.join(
      missingDir,
      `missing_products.json`
    );

    // 6) Load existing array if the file is already there.
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

    // 7) Append the current row.
    missingProducts.push(item);

    // 8) Write it back to disk (pretty-printed for debugging).
    fs.writeFileSync(
      missingFilePath,
      JSON.stringify(missingProducts, null, 2)
    );

    logInfoToFile(
      `Recorded missing product for part_number=${item.part_number} in ${csvFolderName}/missing-${safeSlug}/`
    );
  } catch (err) {
    logErrorToFile(`Error writing missing products file: ${err.message}`);
  }
};

module.exports = { recordBatchStatus, recordMissingProduct };
