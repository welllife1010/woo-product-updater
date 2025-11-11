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
* @function recordMissingProduct
* @description Persists a full CSV row when we cannot resolve a matching
* WooCommerce productId. Useful for follow-up insertion flows.
* @param {string} fileKey - The CSV key used for grouping.
* @param {Object} item - Raw CSV row object.
* @effects Writes/creates: ./missing_products_<fileKey no .csv>.json
* @failure Never throws; logs errors to file.
*/
const recordMissingProduct = (fileKey, item) => {
  // Define the path for missing products file  
  const cleanFileKey = fileKey.replace(/\.csv$/, "");
  const missingFilePath = path.join(
    __dirname, 
    `../../missing_products_${cleanFileKey}.json`
  );

  let missingProducts = [];
  if (fs.existsSync(missingFilePath)) {
    try {
      missingProducts = JSON.parse(fs.readFileSync(missingFilePath, "utf8"));
    } catch (err) {
      logErrorToFile(`Error reading missing products file: ${err.message}`);
    }
  }

  // Add the current item (from the CSV) to the array
  missingProducts.push(item);

  // Ensure the directory exists before writing the file
  const dir = path.dirname(missingFilePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  // Write the updated array back to the file
  fs.writeFileSync(missingFilePath, JSON.stringify(missingProducts, null, 2));
  logInfoToFile(`Recorded missing product for part_number=${item.part_number} in file ${missingFilePath}`);
};

module.exports = { recordBatchStatus, recordMissingProduct };
