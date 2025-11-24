/*
================================================================================
FILE: src/batch/fetch-validate.js
PURPOSE: Network lookups + product identity validation.
NOTES:
- Keeping remote calls in one place simplifies retry/test strategies later.
- This is the ONLY place that:
-   1) Tries to find the Woo productId for a CSV row.
-   2) Fetches the existing Woo product payload by id.
-   3) If productId cannot be found, records the row as "missing".
================================================================================
*/

const { logErrorToFile, logInfoToFile } = require("../../logger");
const {
  getProductById,
  getProductIdByPartNumber,
} = require("../../woo-helpers");

// Use the smart category resolver to get the leaf Woo slug
// NOTE: adjust the path if your category-resolver.js lives in src/batch.
// - If category-resolver.js is at project root: "../../category-resolver"
// - If category-resolver.js is in src/batch:     "./category-resolver"
const { resolveLeafSlugSmart } = require("../../category-resolver");

// Our local helper for writing missing-product JSON files
const { recordMissingProduct } = require("./io-status");
const { normalizeManufacturerName } = require("./manufacturer-map");

/**
 * @function fetchProductData
 * @description
 *   1. Use (part_number, manufacturer) to find the Woo productId.
 *   2. If we find it, fetch the current Woo product payload.
 *   3. If we CANNOT find it, we:
 *        - figure out which Woo category the row "should" belong to
 *          (using resolveLeafSlugSmart on item.category),
 *        - record the row into a missing-products JSON file grouped
 *          by that leaf category slug.
 *
 * @param {Object}  item
 *   One CSV row (already parsed to an object).
 *
 * @param {number}  currentIndex
 *   0-based position of this row inside the CSV file (for logging).
 *
 * @param {number}  totalProductsInFile
 *   Total number of rows in the CSV (for logging / progress).
 *
 * @param {string}  fileKey
 *   Identifier / filename for the CSV (e.g. "product-microcontrollers-03112025_part4.csv").
 *
 * @returns {Promise<{productId:number|null,currentData:object|null}>}
 *   - If everything is OK:
 *       { productId: 123, currentData: { ...Woo product payload... } }
 *   - If we cannot resolve the product:
 *       { productId: null, currentData: null }
 *     (we ALSO log and record the row as "missing" if productId is null)
 *
 * @failure
 *   Never throws to the caller; always resolves with an object.
 */
async function fetchProductData(
  item,
  currentIndex,
  totalProductsInFile,
  fileKey
) {
  // STEP 1: Try to find the Woo productId using part_number + manufacturer.
  //   - This is your "identity" lookup.

  const rawManufacturer = item.manufacturer || "";
  const normalizedManufacturer = normalizeManufacturerName(rawManufacturer);

  const productId = await getProductIdByPartNumber(
    item.part_number,
    normalizedManufacturer,
    currentIndex,
    totalProductsInFile,
    fileKey
  );

  // STEP 2: If we couldn't find a productId, treat this row as "missing".
  if (!productId) {
    // 2a. Grab the raw category text from the row (if present).
    //     Examples:
    //       "Integrated Circuits (ICs)>Embedded>Microcontrollers"
    //       "LED Emitters>IR/UV/Visible"
    const rawCategory = item.category || item.Category || "";

    // 2b. Default slug in case we can't resolve anything better.
    let leafCategorySlug = "unknown";

    // 2c. Ask the smart resolver to guess the BEST Woo leaf slug
    //     for this category text.
    //
    //     resolveLeafSlugSmart will:
    //       - Try Woo fuzzy (using Woo API categories + Fuse.js).
    //       - Fall back to CSV reference mapping.
    //       - Fall back to slugifying the last ">" segment.
    try {
      if (rawCategory) {
        const resolvedSlug = await resolveLeafSlugSmart(rawCategory);
        if (resolvedSlug) {
          leafCategorySlug = resolvedSlug;
        }
      }
    } catch (err) {
      // If something goes wrong, we still don't want to break batching.
      logErrorToFile(
        `resolveLeafSlugSmart failed for category="${rawCategory}": ${err.message}`
      );
    }

    // 2d. Record this row as a "missing product", grouped by:
    //       - fileKey (CSV source)
    //       - leafCategorySlug (Woo-like leaf slug)
    //
    //     Folder structure:
    //       ./missing-products/missing-[leafCategorySlug]/missing_products_[cleanFileKey].json
    //
    //     Example:
    //       leafCategorySlug = "microcontrollers"
    //       fileKey          = "product-microcontrollers-03112025_part4.csv"
    //       → ./missing-products/missing-microcontrollers/missing_products_product-microcontrollers-03112025_part4.json
    recordMissingProduct(fileKey, item, leafCategorySlug);

    // 2e. Log and return "no data".
    logErrorToFile(
      `"processBatch()" - Missing productId for part_number=${item.part_number}, marking as failed.`
    );
    return { productId: null, currentData: null };
  }

  // STEP 3: If we DID find a productId, fetch the product from Woo.
  const currentData = await getProductById(productId, fileKey, currentIndex);
  if (!currentData) {
    logErrorToFile(
      `❌ "processBatch()" - Could not find part_number=${item.part_number}, marking as failed.`
    );
    return { productId: null, currentData: null };
  }

  // STEP 4: Happy path – we have both productId and the current Woo payload.
  return { productId, currentData };
}

/**
 * @function validateProductMatch
 * @description
 *   Extra safety check before updating:
 *   Make sure that the Woo product we fetched STILL matches the CSV row
 *   for:
 *     - part_number
 *     - manufacturer
 *
 *   This prevents us from accidentally updating the WRONG product id
 *   if something weird happened in the lookup logic.
 *
 * @returns {boolean}
 *   - true  → OK to proceed with update
 *   - false → skip this row for safety
 */
function validateProductMatch(item, currentData, _productId, _fileKey) {
  // 1) Try to get "part_number" and "manufacturer" from meta_data.
  let currentPartNumber =
    currentData.meta_data.find(
      (m) => m.key.toLowerCase() === "part_number"
    )?.value?.trim() || "";
  let currentManufacturer =
    currentData.meta_data.find(
      (m) => m.key.toLowerCase() === "manufacturer"
    )?.value?.trim() || "";

  // 2) Some catalogs store part_number in the product name instead.
  //    If meta_data doesn't have it, use the name as a fallback.
  if (!currentPartNumber) {
    currentPartNumber = currentData.name?.trim() || "";
  }

  // 3) If either part_number or manufacturer do NOT match, we skip.
  if (
    item.part_number !== currentPartNumber ||
    item.manufacturer !== currentManufacturer
  ) {
    logInfoToFile(
      `"processBatch()" - Skipping update for part_number=${item.part_number}: WooCommerce data mismatch.`
    );
    return false;
  }

  return true;
}

module.exports = { fetchProductData, validateProductMatch };
