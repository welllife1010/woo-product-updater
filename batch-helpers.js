const path = require("path");
const { performance } = require("perf_hooks"); // Import performance to track time

const { logger, logErrorToFile, logUpdatesToFile, logInfoToFile } = require("./logger");
const { wooApi, getProductById, getProductIdByPartNumber } = require("./woo-helpers");
const { redisClient } = require('./queue');
const { scheduleApiRequest } = require('./job-manager');
const { createUniqueJobId } = require('./utils');

let stripHtml;
(async () => {
  stripHtml = (await import("string-strip-html")).stripHtml;
})();

// ***************************************************************************
// Helper - Normalize input texts
// ***************************************************************************
const normalizeText = (text) => {
  if (!text || typeof text !== "string") return ""; // ✅ Ensure text is a valid string
  let normalized = stripHtml(text)?.result.trim() || "";
  return normalized.replace(/\u00ac\u00c6/g, "®").replace(/&deg;/g, "°").replace(/\s+/g, " ");
};

// ***************************************************************************
// Helper - Check if meta key is missing
// ***************************************************************************
function isMetaKeyMissing(newMetaValue, currentMeta) {
  return (!newMetaValue && !currentMeta) || (!newMetaValue && !currentMeta?.value);
}

// ***************************************************************************
// Helper - Check if current meta is missing
// ***************************************************************************
function isCurrentMetaMissing(newMetaValue, currentMeta) {
  return newMetaValue && !currentMeta;
}

// ***************************************************************************
// Helper - Check if meta value is different
// ***************************************************************************
function isMetaValueDifferent(newMetaValue, currentMetaValue) {
  return normalizeText(currentMetaValue) !== normalizeText(newMetaValue);
}

// ***************************************************************************  
// Helper - Check if product update is needed
// ***************************************************************************
const isUpdateNeeded = (currentData, newData, currentIndex, totalProductsInFile, partNumber, fileName) => {
  const fieldsToUpdate = [];

  logInfoToFile(`"isUpdateNeeded()" - Checking for updates for Part Number: ${partNumber} in ${fileName}`);

  Object.keys(newData).forEach((key) => {
    if (key === "id" || key === "part_number") return;

    let newValue = newData[key];
    let currentValue = currentData[key];

    // Handle meta_data (custom fields) specifically, as it is an array of objects
    if (key === "meta_data") {
      if (!Array.isArray(newValue) || !Array.isArray(currentValue)) {
          logger.info(`DEBUG: meta_data is not an array in either current or new data for Part Number: ${partNumber} in ${fileName}.`);
          fieldsToUpdate.push(key);
          return true;
      }

      newValue.forEach((newMeta) => {
        const newMetaValue = newMeta.value;
        const currentMeta = currentValue.find(meta => meta.key === newMeta.key);
        const currentMetaValue = currentMeta?.value;

        // if (isMetaKeyMissing(newMetaValue, currentMeta)) {
        //     logInfoToFile(`No update needed for the key '${newMeta.key}'. No meta value for Part Number: ${partNumber} in file ${fileName}. \n`);
        // }

        // Special check for datasheet fields
        if (newMeta.key === "datasheet" || newMeta.key === "datasheet_url") {
          // If the new datasheet value contains "digikey", skip updating this field.
          if (newMetaValue.toLowerCase().includes("digikey")) {
            logInfoToFile(`Skipping update for ${newMeta.key} because new value contains "digikey"`);
            return;
          }
          // If the current datasheet value already contains "suntsu-products-s3-bucket", skip updating. Ensure update only happens if current value is different
          if (currentMetaValue && currentMetaValue.toLowerCase().includes("suntsu-products-s3-bucket") && currentMetaValue !== newMetaValue) {
            logInfoToFile(`Skipping update for ${newMeta.key} because current value contains "suntsu-products-s3-bucket"`);
            return;
          }
        }

        // **🚀 Special check for Image_Url containing "digikey.com"**
        if (newMeta.key === "image_url" && newMetaValue.includes("digikey.com")) {
          logInfoToFile(`⚠️ Skipping update for image_url as it contains "digikey.com"`);
          return; // **Skip updating this field**
        }
    
        // Check if the current meta is missing or if values differ
        if (isCurrentMetaMissing(newMetaValue, currentMeta)) {
            logInfoToFile(`DEBUG: Key '${newMeta.key}' missing in currentData meta_data for Part Number: ${partNumber} in file ${fileName}. Marking for update. \n`);
            fieldsToUpdate.push(`meta_data.${newMeta.key}`);
            return true;
        }
    
        if (isMetaValueDifferent(newMetaValue, currentMetaValue)) {
            fieldsToUpdate.push(`meta_data.${newMeta.key}`);
            logInfoToFile(`Update needed for key '${newMeta.key}' for Part Number: ${partNumber} in ${fileName}. \nCurrent value: '${currentMetaValue}', \nNew value: '${newMetaValue}' \n`);
        }
      })
    } else {
      // Normalize and compare general string fields
      if (typeof newValue === "string") {
          newValue = normalizeText(newValue);
          currentValue = currentValue ? normalizeText(currentValue) : "";
      }

      // Check if values are different or if current value is undefined
      if (currentValue === undefined || currentValue !== newValue) {
          fieldsToUpdate.push(key);
          logInfoToFile(`Update needed for key '${key}' for Part Number: ${partNumber} in ${fileName}. \nCurrent value: '${currentValue}', \nNew value: '${newValue}' \n`);
      }
    }
  });

  // Log updates for each field in fieldsToUpdate
  if (fieldsToUpdate.length > 0) {
    // DEBUG: Log all fields to update
    fieldsToUpdate.forEach(field => {
      const currentFieldValue = field.startsWith("meta_data.") 
          ? currentData.meta_data?.find(meta => meta.key === field.split(".")[1])?.value 
          : currentData[field];
          
      const newFieldValue = field.startsWith("meta_data.") 
          ? newData.meta_data?.find(meta => meta.key === field.split(".")[1])?.value 
          : newData[field];
      
      logInfoToFile(`Update needed for field '${field}' in Part Number: ${partNumber}. Current value: '${currentFieldValue}', New value: '${newFieldValue}'`);
    });

    return true;
  } else {
    logger.info(`No update required for Part Number: ${partNumber} in ${fileName}`);
    return false;
  }
};
// ***************************************************************************
// Helper - Normalize CSV headers
// ***************************************************************************
const normalizeCsvHeaders = (item) => {
  const normalizedRow = {};
  Object.keys(item).forEach((key) => {
    const normalizedKey = key.trim().toLowerCase().replace(/\s+/g, "_");
    normalizedRow[normalizedKey] = item[key];
  });
  return normalizedRow;
};

// ***************************************************************************
// Helper - Format ACF field names
// ***************************************************************************
const formatAcfFieldName = (name) => {
  return name
    .replace(/_/g, " ")
    .replace(/-/g, " ")
    .replace(/\b\w/g, (l) => l.toUpperCase()); // Capitalize each word
};

/**
 * Creates a new WooCommerce product data object formatted for bulk updates.
 *
 * This function takes an individual product's data from a CSV row, normalizes its keys,
 * maps known fields to WooCommerce meta_data fields, and processes additional unknown fields.
 * 
 * ### Key Features:
 * - **Normalizes CSV headers** (lowercase, underscores for spaces).
 * - **Maps CSV data** to WooCommerce's meta_data format.
 * - **Handles `datasheet_url` separately** to exclude Digikey links.
 * - **Extracts unknown fields** into `additional_key_information`.
 * - **Ensures valid SKU generation** using available part numbers.
 *
 * @param {Object} item - A single row from a CSV file, representing a product.
 * @param {number} productId - The WooCommerce product ID to update.
 * @param {string} part_number - The part number of the product (fallback for missing values).
 *
 * @returns {Object} A structured product update object formatted for WooCommerce's Bulk API.
 *
 * ### Example Input:
 * ```json
 * {
 *   "manufacturer": "Texas Instruments",
 *   "leadtime": "10 Weeks",
 *   "image_url": "https://example.com/image.jpg",
 *   "datasheet": "https://example.com/datasheet.pdf",
 *   "part_number": "ABC123",
 *   "quantity_available": "1000"
 * }
 * ```
 *
 * ### Example Output:
 * ```json
 * {
 *   "id": 12345,
 *   "part_number": "ABC123",
 *   "sku": "ABC123_Texas Instruments",
 *   "description": "",
 *   "meta_data": [
 *     { "key": "manufacturer", "value": "Texas Instruments" },
 *     { "key": "manufacturer_lead_weeks", "value": "10 Weeks" },
 *     { "key": "image_url", "value": "https://example.com/image.jpg" },
 *     { "key": "quantity", "value": "1000" },
 *     { "key": "additional_key_information", "value": "" }
 *   ]
 * }
 * ```
 */
const createNewData = (item, productId, part_number) => {
  const normalizedCsvRow = normalizeCsvHeaders(item);
  let additionalInfo = [];

  // Define mapping of CSV headers to WooCommerce meta_data fields
  const metaDataKeyMap = {
    manufacturer: "manufacturer",
    leadtime: "manufacturer_lead_weeks",
    image_url: "image_url",
    series: "series",
    quantity_available: "quantity",
    operating_temperature: "operating_temperature",
    voltage___supply: "voltage",
    package___case: "package",
    supplier_device_package: "supplier_device_package",
    mounting_type: "mounting_type",
    short_description: "short_description",
    part_description: "detail_description",
    reachstatus: "reach_status",
    rohsstatus: "rohs_status",
    moisturesensitivitylevel: "moisture_sensitivity_level",
    exportcontrolclassnumber: "export_control_class_number",
    htsuscode: "htsus_code"
  };

  // Process and map valid meta_data fields
  const productMetaData = Object.keys(metaDataKeyMap)
    .filter((csvKey) => normalizedCsvRow.hasOwnProperty(csvKey))
    .map((csvKey) => ({
      key: metaDataKeyMap[csvKey],
      value: normalizedCsvRow[csvKey] || "",
    }));

  // Handle datasheet separately, excluding Digikey links
  if (normalizedCsvRow.hasOwnProperty("datasheet")) {
    const datasheetUrl = normalizedCsvRow["datasheet"] || "";
    if (!datasheetUrl.toLowerCase().includes("digikey")) {
      productMetaData.push({ key: "datasheet", value: datasheetUrl });
      productMetaData.push({ key: "datasheet_url", value: datasheetUrl });
    }
  }

  // Capture unknown fields into `additional_key_information`
  // Object.keys(normalizedCsvRow).forEach((key) => {
  //   if (!metaDataKeyMap[key] && key !== "datasheet" && key !== "part_number") {
  //     let value = normalizedCsvRow[key] || "";
  //     if (value !== "" && value !== "NaN") {
  //       additionalInfo.push(`<strong>${formatAcfFieldName(key)}:</strong> ${value}<br>`);
  //     }
  //   }
  // });

  // Use additional_info directly if available
  additionalInfo = normalizedCsvRow["additional_info"] || "";

  // If additional_info is not present, fallback to manually constructing it
  if (!additionalInfo) {
    Object.keys(normalizedCsvRow).forEach((key) => {
      if (!metaDataKeyMap[key] && key !== "datasheet" && key !== "part_number" && key !== "additional_info") {
        let value = normalizedCsvRow[key] || "";
        if (value !== "" && value !== "NaN") {
          let formattedKey = formatAcfFieldName(key);

          // Ensure we do not duplicate known fields
          const excludedFields = [
            "Part Title", "Category", "Product Status", "RF Type", "Topology", "Circuit",
            "Frequency Range", "Isolation", "Insertion Loss", "Test Frequency", "P1dB",
            "IIP3", "Features", "Impedance", "Voltage – Supply", "Operating Temperature",
            "Mounting Type", "Package / Case", "Supplier Device Package"
          ];

          if (!excludedFields.includes(formattedKey)) {
            additionalInfo += `<strong>${formattedKey}:</strong> ${value}<br>`;
          }
        }
      }
    });
  }

  return {
    id: productId,
    part_number: normalizedCsvRow.part_number || part_number,
    sku: normalizedCsvRow.sku || `${normalizedCsvRow.part_number}_${normalizedCsvRow.manufacturer}` || normalizedCsvRow.part_number,
    description: normalizedCsvRow.part_description || "",
    meta_data: [
      ...productMetaData,
      { key: "additional_key_information", value: additionalInfo || "" },
    ],
  };
};

/**
 * Filters an existing WooCommerce product object, extracting only relevant fields for comparison.
 *
 * This function is used before checking if a product update is needed. 
 * It removes unnecessary metadata and keeps only fields that are tracked in WooCommerce.
 *
 * ### Key Features:
 * - **Extracts SKU and Description** for easy comparison.
 * - **Filters meta_data** to keep only required fields.
 * - **Used in the `isUpdateNeeded` function** for detecting required updates.
 *
 * @param {Object} product - The existing WooCommerce product object.
 * 
 * @returns {Object} A filtered product object containing only relevant fields.
 *
 * ### Example Input:
 * ```json
 * {
 *   "sku": "ABC123",
 *   "description": "Some electronic component",
 *   "meta_data": [
 *     { "key": "manufacturer", "value": "Texas Instruments" },
 *     { "key": "image_url", "value": "https://example.com/image.jpg" },
 *     { "key": "random_field", "value": "this should be removed" }
 *   ]
 * }
 * ```
 *
 * ### Example Output:
 * ```json
 * {
 *   "sku": "ABC123",
 *   "description": "Some electronic component",
 *   "meta_data": [
 *     { "key": "manufacturer", "value": "Texas Instruments" },
 *     { "key": "image_url", "value": "https://example.com/image.jpg" }
 *   ]
 * }
 * ```
 */
const filterCurrentData = (product) => {
  return {
    sku: product.sku,
    description: product.description,
    meta_data: product.meta_data.filter((meta) =>
      [
        "spq", "manufacturer", "image_url", "datasheet_url", "series_url", "series", "quantity",
        "operating_temperature", "voltage", "package", "supplier_device_package", "mounting_type",
        "short_description", "detail_description", "additional_key_information", "reach_status",
        "rohs_status", "moisture_sensitivity_level", "export_control_class_number", "htsus_code"
      ].includes(meta.key)
    ),
  };
};

// ***************************************************************************
// Helper - Record missing product data
// ***************************************************************************
const recordMissingProduct = (fileKey, item) => {
  const missingFilePath = path.join(__dirname, `missing_products_${fileKey}.json`);
  let missingProducts = [];
  
  // If the file exists, read its current content
  if (fs.existsSync(missingFilePath)) {
    try {
      missingProducts = JSON.parse(fs.readFileSync(missingFilePath, 'utf8'));
    } catch (err) {
      logErrorToFile(`Error reading missing products file: ${err.message}`);
    }
  }
  
  // Add the current item (from the CSV) to the array
  missingProducts.push(item);
  
  // Write the updated array back to the file
  fs.writeFileSync(missingFilePath, JSON.stringify(missingProducts, null, 2));
  logInfoToFile(`Recorded missing product for part_number=${item.part_number}`);
};

// ***************************************************************************
// Main Function - processBatch()
// Process a batch of products using WooCommerce Bulk API
// Example: parse partial updates from the response
// ***************************************************************************
async function processBatch(batch, startIndex, totalProductsInFile, fileKey) {
  const MAX_RETRIES = 5;
  let attempts = 0;
  const action = "processBatch";

  const batchStartTime = performance.now();
  logInfoToFile(`Starting "processBatch" with startIndex=${startIndex}, fileKey=${fileKey}`);

  if (!Array.isArray(batch)) {
    throw new Error(`Expected batch to be an array, got ${typeof batch}`);
  }

  logInfoToFile(`"processBatch()" - Processing batch of ${batch.length} items for fileKey=${fileKey}`);

  // ──────────────────────────────────────────────────────────
  // 1) Determine which items truly need an update
  // ──────────────────────────────────────────────────────────
  const toUpdate = [];
  let skipCount = 0;
  let localFailCount = 0;

  for (let i = 0; i < batch.length; i++) {

    let item = batch[i];
    let part_number = item.part_number;

    const currentIndex = startIndex + i;
    
    logInfoToFile(`DEBUG item keys: ${JSON.stringify(Object.keys(item))}`);
    logInfoToFile(`"processBatch()" - Processing item=${item}`);
    logInfoToFile(`"processBatch()" - item.part_number=${item.part_number}`);
    logInfoToFile(`"processBatch()" - Processing part_number=${part_number}`);
    logInfoToFile(`"processBatch()" - currentIndex >= totalProductsInFile=${currentIndex >= totalProductsInFile}`);

    
    if (currentIndex >= totalProductsInFile) break;
      
    if (!part_number) {
      localFailCount++;
      continue;
    }

    try {
      // 1a) Attempt to find the matching product
      const productId = await getProductIdByPartNumber(part_number, currentIndex, totalProductsInFile, fileKey);

      logInfoToFile(`"processBatch()" - For part_number=${part_number}, got productId=${productId}`);

      if (!productId) {
        recordMissingProduct(fileKey, item);  // Record missing product details
        localFailCount++;
        logErrorToFile(`Missing productId for part_number=${part_number}, marking as failed.`);
        continue;
      }

      // 1b) Fetch existing product from Woo to see if an update is needed
      const product = await getProductById(productId, fileKey, currentIndex);
      const newData = createNewData(item, productId, part_number);
      const currentData = filterCurrentData(product);

      logInfoToFile(`"processBatch()" - product: ${product} | newData: ${JSON.stringify(newData)} | currentData: ${JSON.stringify(currentData)}`);

      logInfoToFile(`"processBatch()" - Checking product data for part_number=${part_number}`);
      //logInfoToFile(`Current Data: ${JSON.stringify(currentData, null, 2)}`);
      //logInfoToFile(`New Data: ${JSON.stringify(newData, null, 2)}`);

      
      if (product && isUpdateNeeded(currentData, newData, currentIndex, totalProductsInFile, part_number, fileKey)) {
        toUpdate.push(newData);
        logInfoToFile(`Adding part_number=${part_number} to the update list`);
      } else {
        logInfoToFile(`No update needed for part_number=${part_number}`);  
        skipCount++;
      }
    } catch (err) {
      localFailCount++;
      logErrorToFile(`Error processing part_number=${part_number}: ${err.message}`, err.stack);
    }
  }

  // 1c) Increment skip/fail counters in Redis
  if (skipCount > 0) {
    await redisClient.incrBy(`skipped-products:${fileKey}`, skipCount);
  }
  if (localFailCount > 0) {
    await redisClient.incrBy(`failed-products:${fileKey}`, localFailCount);
  }

  // ──────────────────────────────────────────────────────────
  // 2) If we have nothing to update, exit
  // ──────────────────────────────────────────────────────────
  if (toUpdate.length === 0) {
    logInfoToFile(`No valid products to update in this batch for ${fileKey}. Done.`);
    return;
  }

  // ──────────────────────────────────────────────────────────
  // 3) Attempt the actual WooCommerce bulk update (with retries)
  // ──────────────────────────────────────────────────────────
  while (attempts < MAX_RETRIES) {
    try {
      const jobId = createUniqueJobId(fileKey, action, startIndex, attempts);

      const apiCallStart = performance.now();
      const response = await scheduleApiRequest(
        () => wooApi.put("products/batch", { update: toUpdate }),
        { id: jobId }
      );
      const apiCallEnd = performance.now();
      logInfoToFile(`WooCommerce API batch update took ${(apiCallEnd - apiCallStart).toFixed(2)} ms`);

      // 3a) Parse the response to see how many were updated
      //     Depending on your WooCommerce version, this might be in `response.data.update`.
      //     The official docs show something like { "update": [ {...}, {...} ] }, "create": [], "delete": [] }
      const data = response.data;
      if (!data || !data.update) {
        // Possibly an unexpected response structure => treat it as a partial or full fail
        logErrorToFile(`Unexpected response structure from batch update. No 'update' array found.`);
        await redisClient.incrBy(`failed-products:${fileKey}`, toUpdate.length);
      } else {
        const updatedCount = data.update.length;
        const expectedCount = toUpdate.length;

        // If partial
        if (updatedCount < expectedCount) {
          const diff = expectedCount - updatedCount;
          await redisClient.incrBy(`failed-products:${fileKey}`, diff);
          logErrorToFile(`Partial success: expected ${expectedCount} updates, got ${updatedCount}. Marking ${diff} as failed.`);
        }

        // Mark however many the API said were updated
        if (updatedCount > 0) {
          await redisClient.incrBy(`updated-products:${fileKey}`, updatedCount);
        }

        // If your store does partial success with an "errors" array:
        //   if (data.errors?.length) {
        //       await redisClient.incrBy(`failed-products:${fileKey}`, data.errors.length);
        //   }

        // 3b) Log each item that was "intended" to update
        for (const product of toUpdate) {
          logUpdatesToFile(
            `Updated part_number=${product.part_number} in file=${fileKey}`
          );
        }
      }

      // 3c) If we reach here, the call itself succeeded => break out of the retry loop
      return;
    } catch (err) {
      attempts++;
      logErrorToFile(`Batch update attempt ${attempts} for file="${fileKey}" failed: ${err.message}`);

      if (attempts >= MAX_RETRIES) {
        // If we exhaust all retries, consider them failed
        await redisClient.incrBy(`failed-products:${fileKey}`, toUpdate.length);
        throw new Error(`Batch update failed permanently after ${MAX_RETRIES} attempts. fileKey=${fileKey}`);
      }
      const delayMs = Math.pow(2, attempts) * 1000;
      logInfoToFile(`Retrying after ${delayMs / 1000} seconds...`);
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }

  const batchEndTime = performance.now();
  logInfoToFile(`Total time for processBatch(fileKey=${fileKey}, startIndex=${startIndex}): ${(batchEndTime - batchStartTime).toFixed(2)} ms`);
}

module.exports = {
  normalizeText,
  isUpdateNeeded,
  processBatch,
};