/*
================================================================================
FILE: src/batch/fetch-validate.js
PURPOSE: Network lookups + product identity validation.
NOTES:
- Keeping remote calls in one place simplifies retry/test strategies later.
================================================================================
*/

const { logErrorToFile, logInfoToFile } = require("../../logger");
const { getProductById, getProductIdByPartNumber } = require("../../woo-helpers");
const { recordMissingProduct } = require("./io-status");

/**
* @function fetchProductData
* @description Resolves Woo productId from (part_number, manufacturer) and
* retrieves the current Woo product payload for comparison.
* @returns {Promise<{productId:number|null,currentData:object|null}>}
* @failure Never throws; returns nulls and logs on failure.
*/
async function fetchProductData(item, currentIndex, totalProductsInFile, fileKey) {
  const productId = await getProductIdByPartNumber(
    item.part_number,
    item.manufacturer?.trim() || "",
    currentIndex,
    totalProductsInFile,
    fileKey
  );

  if (!productId) {
    recordMissingProduct(fileKey, item);
    logErrorToFile(`"processBatch()" - Missing productId for part_number=${item.part_number}, marking as failed.`);
    return { productId: null, currentData: null };
  }

  const currentData = await getProductById(productId, fileKey, currentIndex);
  if (!currentData) {
    logErrorToFile(`❌ "processBatch()" - Could not find part_number=${item.part_number}, marking as failed.`);
    return { productId: null, currentData: null };
  }

  return { productId, currentData };
}

/**
* @function validateProductMatch
* @description Extra safety check before updating: verifies that the resolved
* Woo product still matches the CSV row for part_number and
* manufacturer. Prevents writing to a wrong product id.
* @returns {boolean}
*/
function validateProductMatch(item, currentData, _productId, _fileKey) {
  let currentPartNumber = currentData.meta_data.find((m) => m.key.toLowerCase() === "part_number")?.value?.trim() || "";
  let currentManufacturer = currentData.meta_data.find((m) => m.key.toLowerCase() === "manufacturer")?.value?.trim() || "";

  // Some catalogs store part number in the product name; use as a fallback.
  if (!currentPartNumber) {
    currentPartNumber = currentData.name?.trim() || "";
  }

  if (item.part_number !== currentPartNumber || item.manufacturer !== currentManufacturer) {
    logInfoToFile(
      `"processBatch()" - Skipping update for part_number=${item.part_number}: WooCommerce data mismatch.`
    );
    return false;
  }
  return true;
}

module.exports = { fetchProductData, validateProductMatch };
