const fs = require("fs");
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
// Helper - Record batch status to JSON file
// ***************************************************************************
const recordBatchStatus = (fileKey, updatedParts, skippedParts, failedParts) => {
  try {
      // ✅ Extract folder from fileKey (Ensure we create the full subfolder)
      const statusDir = path.join(__dirname, "batch_status", fileKey.replace(/\.csv$/, ""));
      
      // ✅ Ensure the entire directory path exists
      if (!fs.existsSync(statusDir)) {
          fs.mkdirSync(statusDir, { recursive: true }); // Create full path recursively
      }

      // ✅ Define the file path inside the created subfolder
      const statusFilePath = path.join(statusDir, `batch_status.json`);

      // ✅ Initialize an empty batch status object
      let batchStatus = { updated: [], skipped: [], failed: [] };

      // ✅ If the file already exists, read its content and merge data
      if (fs.existsSync(statusFilePath)) {
          try {
              const fileData = fs.readFileSync(statusFilePath, "utf-8");
              batchStatus = JSON.parse(fileData);
          } catch (error) {
              logErrorToFile(`❌ Error reading batch status file: ${error.message}`);
          }
      }

      // ✅ Append new part numbers to the respective lists
      batchStatus.updated.push(...updatedParts);
      batchStatus.skipped.push(...skippedParts);
      batchStatus.failed.push(...failedParts);

      // ✅ Remove duplicates (optional)
      batchStatus.updated = [...new Set(batchStatus.updated)];
      batchStatus.skipped = [...new Set(batchStatus.skipped)];
      batchStatus.failed = [...new Set(batchStatus.failed)];

      // ✅ Write the updated batch status back to the file
      fs.writeFileSync(statusFilePath, JSON.stringify(batchStatus, null, 2));
      logInfoToFile(`✅ Saved batch status to ${statusFilePath}`);
  } catch (err) {
      logErrorToFile(`❌ Error writing batch status file: ${err.message}`);
  }
};

// ***************************************************************************  
// Helper - Check if product update is needed
// ***************************************************************************
const isUpdateNeeded = (currentData, newData, currentIndex, totalProductsInFile, partNumber, fileName) => {
  const updateMode = process.env.UPDATE_MODE || 'full'; // Default to full mode
  const fieldsToUpdate = [];
  logInfoToFile(`"isUpdateNeeded()" - Checking for updates for Part Number: ${partNumber} in ${fileName}`);

  // 🔍 Debug current vs new data comparison
  // logInfoToFile(`🔎 currentData: ${JSON.stringify(currentData, null, 2)}`);
  // logInfoToFile(`🔎 newData: ${JSON.stringify(newData, null, 2)}`);

  if (updateMode === "quantity") {
    const currentQuantity = currentData.meta_data?.find(meta => meta.key === "quantity")?.value || "0";
    const newQuantity = newData.meta_data?.find(meta => meta.key === "quantity")?.value || "0";

    if (currentQuantity !== newQuantity) {
        logInfoToFile(`✅ Quantity update needed for Part Number: ${partNumber}: "${currentQuantity}" → "${newQuantity}"`);
        return true;
    }

    logInfoToFile(`No quantity update needed for Part Number: ${partNumber}`);
    return false;
  }

  Object.keys(newData).forEach((key) => {
    if (key === "id" || key === "part_number") return;

    let newValue = newData[key];
    let currentValue = currentData[key];

    // Handle meta_data (custom fields) specifically, as it is an array of objects
    if (key === "meta_data") {
      if (!Array.isArray(newValue) || !Array.isArray(currentValue)) {
          fieldsToUpdate.push(key);
          return true;
      }

      newValue.forEach((newMeta) => {
        const newMetaValue = newMeta.value;
        const currentMeta = currentValue.find(meta => meta.key === newMeta.key);
        const currentMetaValue = currentMeta?.value || "";

        if (newMetaValue !== currentMetaValue) {
          fieldsToUpdate.push(`meta_data.${newMeta.key}`);
        }

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
        if (newMeta.key === "image_url" && (newMetaValue.includes("digikey.com") || newMetaValue.includes("mm.digikey.com"))) {
          logInfoToFile(`⚠️ Skipping update for image_url as it contains "digikey.com"`);
          return; // **Skip updating this field**
        }
    
        // Check if the current meta is missing or if values differ
        if (isCurrentMetaMissing(newMetaValue, currentMeta)) {
            logInfoToFile(`DEBUG: Key '${newMeta.key}' missing in currentData (old) meta_data for Part Number: ${partNumber} in file ${fileName}. Marking for update. \n`);
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
    });

    //logInfoToFile(logBuffer.join("\n"));
    logInfoToFile(`✅ "isUpdateNeeded()" return "true" - Update needed for Part Number: ${partNumber} in ${fileName}`);
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
  const updateMode = process.env.UPDATE_MODE || 'full';
  const normalizedCsvRow = normalizeCsvHeaders(item);
  let additionalInfo = [];

  if (updateMode === "quantity") {
    return {
        id: productId,
        part_number: normalizedCsvRow.part_number || part_number,
        meta_data: [
            { key: "quantity", value: normalizedCsvRow.quantity_available || "0" }
        ],
    };
  }

  // Full mode: Include all fields
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
    name: product.name,
    sku: product.sku,
    description: product.description,
    meta_data: product.meta_data.filter((meta) =>
      [
        "part_number", "spq", "manufacturer", "image_url", "datasheet_url", "series_url", "series", "quantity",
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

  // ✅ Ensure fileKey does not have .csv before creating the file path
  const cleanFileKey = fileKey.replace(/\.csv$/, ""); 

  // ✅ Construct the correct missing file path
  const missingFilePath = path.join(__dirname, `missing_products_${cleanFileKey}.json`);
  let missingProducts = [];
  
  // If the file exists, read its current content
  if (fs.existsSync(missingFilePath)) {
    try {
      missingProducts = JSON.parse(fs.readFileSync(missingFilePath, 'utf8'));
    } catch (err) {
      logErrorToFile(`Error reading missing products file: ${err.message}`);
    }
  }
  
  // ✅  Add the current item (from the CSV) to the array
  missingProducts.push(item);

   // ✅ Ensure the directory exists before writing the file
   const dir = path.dirname(missingFilePath);
   if (!fs.existsSync(dir)) {
       fs.mkdirSync(dir, { recursive: true });
   }
  
  // ✅ Write the updated array back to the file
  fs.writeFileSync(missingFilePath, JSON.stringify(missingProducts, null, 2));
  logInfoToFile(`Recorded missing product for part_number=${item.part_number} in file ${missingFilePath}`);
};

// ***************************************************************************
// Main Function - processBatch()
// Process a batch of products using WooCommerce Bulk API
// Example: parse partial updates from the response
// ***************************************************************************
async function processBatch(batch, startIndex, totalProductsInFile, fileKey) {
  const updateMode = process.env.UPDATE_MODE || 'full';
  const MAX_RETRIES = 5;
  const action = "processBatch";
  let attempts = 0;

  const batchStartTime = performance.now();
  //let logBuffer = [`Starting "processBatch()" for fileKey=${fileKey}, startIndex=${startIndex}`];
  logInfoToFile(`Starting "processBatch()" with startIndex=${startIndex}, fileKey=${fileKey}`);

  if (!Array.isArray(batch)) {
    throw new Error(`"processBatch()" - Expected batch to be an array, got ${typeof batch}`);
  }

  logInfoToFile(`"processBatch()" - Processing batch of ${batch.length} items for fileKey=${fileKey}`);
  //logBuffer.push(`"processBatch()" - Processing batch of ${batch.length} items for fileKey=${fileKey}`);

  // ──────────────────────────────────────────────────────────
  // 1) Determine which items truly need an update
  // ──────────────────────────────────────────────────────────
  const toUpdate = [];
  let skipCount = 0;
  let localFailCount = 0;

  const updatedParts = [];
  const skippedParts = [];
  const failedParts = [];

  for (let i = 0; i < batch.length; i++) {
    let item = batch[i];
    let part_number = item.part_number;
    let manufacturer = item.manufacturer?.trim() || "";

    const currentIndex = startIndex + i;
    
    logInfoToFile(`"processBatch()" - Processing part_number=${part_number}`);
    logInfoToFile(`"processBatch()" - currentIndex >= totalProductsInFile=${currentIndex >= totalProductsInFile}`);

    if (currentIndex >= totalProductsInFile) break;
    if (!part_number) {
      localFailCount++;
      continue;
    }

    try {
      // 1a) Attempt to find the matching product
      const productId = await getProductIdByPartNumber(part_number, manufacturer, currentIndex, totalProductsInFile, fileKey);
      if (!productId) {
        recordMissingProduct(fileKey, item);  // Record missing product details
        localFailCount++;
        failedParts.push(`Row ${currentIndex + 1}: No product found for ${part_number}`);
        logErrorToFile(`"processBatch()" - Missing productId for part_number=${part_number}, marking as failed.`);
        continue;
      }

      // 1b) Fetch existing product from Woo to see if an update is needed
      const product = await getProductById(productId, fileKey, currentIndex);
      if (!product) {
        localFailCount++;
        failedParts.push(`Row ${currentIndex + 1}: Product ID ${productId} not found`);
        logErrorToFile(`❌ "processBatch()" - Could not find part_number=${part_number}, marking as failed.`);
        continue;
      }

      // 🔎 Log the full WooCommerce product data before extracting part_number
      //logInfoToFile(`🔎 Full WooCommerce product data for productId=${productId}: ${JSON.stringify(product, null, 2)}`);

      const newData = createNewData(item, productId, part_number);
      const currentData = filterCurrentData(product);

      // ✅ Extract `part_number` from WooCommerce's meta_data field
      let currentPartNumber = currentData.meta_data.find(meta => meta.key.toLowerCase() === "part_number")?.value?.trim() || "";
      let currentManufacturer = currentData.meta_data.find(meta => meta.key.toLowerCase() === "manufacturer")?.value?.trim() || "";

      logInfoToFile(`🔎 Extracted: currentPartNumber="${currentPartNumber}" | newData.part_number="${newData.part_number}"`);
      logInfoToFile(`🔎 Extracted: currentManufacturer="${currentManufacturer}" | newData.manufacturer="${newData.manufacturer}"`);
      
      // ✅ If `part_number` is missing, use the product title as a fallback
      if (!currentPartNumber) {
        if (currentData.name && typeof currentData.name === "string") {
            currentPartNumber = currentData.name.trim();
            logInfoToFile(`✅ Using product name as fallback part_number="${currentPartNumber}" for productId=${productId}`);
        } else {
            logErrorToFile(`❌ Fallback failed: Product name is missing or invalid for productId=${productId}`);
        }
      }

      // 🚀 Ensure both "part_number" and "manufacturer" match exactly
      if (newData.part_number !== currentPartNumber || manufacturer !== currentManufacturer) {
        logInfoToFile(`"processBatch()" - Skipping update: newData.part_number="${newData.part_number}" (CSV) does not match currentPartNumber="${currentPartNumber}" (WooCommerce) ` +
           `OR newData.manufacturer="${manufacturer}" (CSV) does not match currentManufacturer="${currentManufacturer}" (WooCommerce)`);
        //logBuffer.push(`"processBatch()" - Skipping update: newData.part_number="${newData.part_number}" (CSV) does not match currentPartNumber="${currentPartNumber}" (WooCommerce) ` +
        //  `OR newData.manufacturer="${manufacturer}" (CSV) does not match currentManufacturer="${currentManufacturer}" (WooCommerce)`);
        skipCount++;
        skippedParts.push(`Row ${currentIndex + 1}: ${part_number} skipped due to mismatched part_number or manufacturer`);
        continue;
      }

      logInfoToFile(`"processBatch()" - Checking product data for part_number=${part_number}`);

      // ** Check if any update is needed **
      // 🚀 Use the isUpdateNeeded function to compare currentData and newData
      const updateNeeded = isUpdateNeeded(currentData, newData, currentIndex, totalProductsInFile, part_number, fileKey);
      if (!updateNeeded) {
          skipCount++;
          skippedParts.push(`Row ${currentIndex + 1}: ${part_number} skipped (no changes)`);
          continue;
      }

      const fieldsToUpdate = [];
      const changedFields = [];

      if (updateMode === "quantity") {

        

      } else if (updateMode === "full") {

      }

      
      // Iterate over meta_data and only add changed fields
      newData.meta_data.forEach(newMeta => {
        const currentMeta = currentData.meta_data.find(meta => meta.key === newMeta.key);
        const currentMetaValue = currentMeta?.value || "";
        const newMetaValue = newMeta.value;

        // 🚀 Skip updating image_url if it contains "digikey.com"
        if (newMeta.key === "image_url" && newMetaValue.includes("digikey.com")) {
          logInfoToFile(`"processBatch()" - Skipping update for image_url as it contains "digikey.com"`);
          //logBuffer.push(`"processBatch()" - Skipping update for image_url as it contains "digikey.com"`);
          return;
        }

        // 🚀 Skip updating datasheet_url if it contains "digikey.com"
        if ((newMeta.key === "datasheet_url" || newMeta.key === "datasheet") && newMetaValue.includes("digikey.com")) {
          logInfoToFile(`"processBatch()" - Skipping update for datasheet as it contains "digikey.com"`);
          //logBuffer.push(`"processBatch()" - Skipping update for datasheet as it contains "digikey.com"`);
          return;
        }

        if (isCurrentMetaMissing(newMetaValue, currentMeta) || isMetaValueDifferent(newMetaValue, currentMetaValue)) {
          fieldsToUpdate.push(newMeta);
          changedFields.push({ key: newMeta.key, oldValue: currentMetaValue, newValue: newMetaValue });
        }
      });

      if (fieldsToUpdate.length > 0) {

        toUpdate.push({ id: productId, part_number, meta_data: fieldsToUpdate });
        
        // 🚀 Log only the fields that are different
        logInfoToFile(
          `Fields updated for part_number="${newData.part_number}, id=${productId}":\n` +
          changedFields.map(field => `- ${field.key}: "${field.oldValue}" → "${field.newValue}"`).join("\n")
        );
        // logBuffer.push(`Fields updated for part_number="${newData.part_number}":\n` +
        // changedFields.map(field => `- ${field.key}: "${field.oldValue}" → "${field.newValue}"`).join("\n"));
      } else {
        skipCount++;
      }
    } catch (err) {
      localFailCount++;
      failedParts.push(`Row ${currentIndex + 1}: ${part_number} failed - ${err.message}`);
      logErrorToFile(`Error processing part_number=${part_number}: ${err.message}`, err.stack);
    }

    // ✅ Write final status JSON file
    recordBatchStatus(fileKey, updatedParts, skippedParts, failedParts);

  } // End of for loop - looping through each item in the "batch"

  // ──────────────────────────────────────────────────────────
  // 1c) Increment skip/fail counters in Redis
  // ──────────────────────────────────────────────────────────
  if (skipCount > 0) await redisClient.incrBy(`skipped-products:${fileKey}`, skipCount);
  if (localFailCount > 0) await redisClient.incrBy(`failed-products:${fileKey}`, localFailCount);

  // ──────────────────────────────────────────────────────────
  // 2) If we have nothing to update, exit
  // ──────────────────────────────────────────────────────────
  if (toUpdate.length === 0) {
    logInfoToFile(`No valid products to update in this batch for ${fileKey}. Done.`);
    //logBuffer.push(`No valid products to update in this batch for ${fileKey}. Done.`);
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
      //logBuffer.push(`WooCommerce API batch update took ${(apiCallEnd - apiCallStart).toFixed(2)} ms`);

      // 3a) Parse the response to see how many were updated
      //     Depending on the WooCommerce version, this might be in `response.data.update`.
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

        // Partial success with an "errors" array:
        //   if (data.errors?.length) {
        //       await redisClient.incrBy(`failed-products:${fileKey}`, data.errors.length);
        //   }

        // 3b) Log each item that was "intended" to update
        for (const product of toUpdate) {
          logUpdatesToFile(`Updated part_number=${product.part_number} in file=${fileKey}`);
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
      logInfoToFile(`"processBatch()" - Retrying after ${delayMs / 1000} seconds...`);
      //logBuffer.push(`Retrying after ${delayMs / 1000} seconds...`);
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  } // End of while loop

  const batchEndTime = performance.now();
  logInfoToFile(`Total time for processBatch(fileKey=${fileKey}, startIndex=${startIndex}): ${(batchEndTime - batchStartTime).toFixed(2)} ms`);
  //logBuffer.push(`Total time for processBatch(fileKey=${fileKey}, startIndex=${startIndex}): ${(batchEndTime - batchStartTime).toFixed(2)} ms`);
  
  //logInfoToFile(logBuffer.join("\n")); // ✅ Log once for the batch
}

module.exports = {
  normalizeText,
  isUpdateNeeded,
  processBatch,
};