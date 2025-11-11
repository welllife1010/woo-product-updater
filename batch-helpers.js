const fs = require("fs");
const path = require("path");
const { performance } = require("perf_hooks"); // Import performance to track time

const { logger, logErrorToFile, logUpdatesToFile, logInfoToFile } = require("./logger");
const { wooApi, getProductById, getProductIdByPartNumber } = require("./woo-helpers");
const { appRedis } = require('./queue');
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
  if (!text || typeof text !== "string") return ""; // Ensure text is a valid string
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
      // 1. Extract folder from fileKey (Ensure we create the full subfolder)
      const statusDir = path.join(__dirname, "batch_status", fileKey.replace(/\.csv$/, ""));
      
      // 2. Ensure the entire directory path exists
      if (!fs.existsSync(statusDir)) {
          fs.mkdirSync(statusDir, { recursive: true }); // Create full path recursively
      }

      // 3. Define the file path inside the created subfolder
      const statusFilePath = path.join(statusDir, `batch_status.json`);

      // 4. Initialize an empty batch status object
      let batchStatus = { updated: [], skipped: [], failed: [] };

      // 5. If the file already exists, read its content and merge data
      if (fs.existsSync(statusFilePath)) {
          try {
              const fileData = fs.readFileSync(statusFilePath, "utf-8");
              batchStatus = JSON.parse(fileData);
          } catch (error) {
              logErrorToFile(`❌ Error reading batch status file: ${error.message}`);
          }
      }

      // 6. Append new part numbers to the respective lists
      batchStatus.updated.push(...updatedParts);
      batchStatus.skipped.push(...skippedParts);
      batchStatus.failed.push(...failedParts);

      // 7. Remove duplicates (optional)
      batchStatus.updated = [...new Set(batchStatus.updated)];
      batchStatus.skipped = [...new Set(batchStatus.skipped)];
      batchStatus.failed = [...new Set(batchStatus.failed)];

      // 8. Write the updated batch status back to the file
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
  logInfoToFile(`[ isUpdateNeeded() ] - Checking for updates for Part Number: ${partNumber} in ${fileName}`);

  // 🔍 Debug current vs new data comparison
  // logInfoToFile(`[ isUpdateNeeded() ] - 🔎 currentData: ${JSON.stringify(currentData, null, 2)}`);
  // logInfoToFile(`[ isUpdateNeeded() ] - 🔎 newData: ${JSON.stringify(newData, null, 2)}`);

  if (updateMode === "quantity") {
    const currentQuantity = currentData.meta_data?.find(meta => meta.key === "quantity")?.value || "0";
    const newQuantity = newData.meta_data?.find(meta => meta.key === "quantity")?.value || "0";

    if (currentQuantity !== newQuantity) {
        logInfoToFile(`[ isUpdateNeeded() ] - Quantity update needed for Part Number: ${partNumber}: "${currentQuantity}" → "${newQuantity}"`);
        return true;
    }

    logInfoToFile(`[ isUpdateNeeded() ] - No quantity update needed for Part Number: ${partNumber}`);
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

        // -- Special check for "datasheet fields" --
        if (newMeta.key === "datasheet" || newMeta.key === "datasheet_url") {
          // If the new datasheet value contains "digikey", skip updating this field.
          if (newMetaValue.toLowerCase().includes("digikey")) {
            logInfoToFile(`[ isUpdateNeeded() ] - Skipping update for ${newMeta.key} because new value contains "digikey"`);
            return;
          }
          // If the current datasheet value already contains "suntsu-products-s3-bucket", skip updating. Ensure update only happens if current value is different
          if (currentMetaValue && currentMetaValue.toLowerCase().includes("suntsu-products-s3-bucket") && currentMetaValue !== newMetaValue) {
            logInfoToFile(`[ isUpdateNeeded() ] - Skipping update for ${newMeta.key} because current value contains "suntsu-products-s3-bucket"`);
            return;
          }
        }

        // -- Special check for "Image_Url" containing "digikey.com" --
        if (newMeta.key === "image_url" && (newMetaValue.includes("digikey.com") || newMetaValue.includes("mm.digikey.com"))) {
          logInfoToFile(`[ isUpdateNeeded() ] - Skipping update for image_url as it contains "digikey.com"`);
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
            logInfoToFile(`[ isUpdateNeeded() ] - Update needed for key '${newMeta.key}' for Part Number: ${partNumber} in ${fileName}. \nCurrent value: '${currentMetaValue}', \nNew value: '${newMetaValue}' \n`);
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
          logInfoToFile(`[ isUpdateNeeded() ] - Update needed for key '${key}' for Part Number: ${partNumber} in ${fileName}. \nCurrent value: '${currentValue}', \nNew value: '${newValue}' \n`);
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
    logInfoToFile(`[ isUpdateNeeded() ] - Update needed for Part Number: ${partNumber} in ${fileName}`);
    return true;
  } else {
    logger.info(`[ isUpdateNeeded() ] - No update required for Part Number: ${partNumber} in ${fileName}`);
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
 */
const createNewData = (item, productId, part_number) => {
  const updateMode = process.env.UPDATE_MODE || 'full';
  const normalizedCsvRow = normalizeCsvHeaders(item);
  let additionalInfo = [];

  if (updateMode === "quantity") {
    return {
        id: productId,
        part_number: normalizedCsvRow.part_number || part_number,
        manufacturer: normalizedCsvRow.manufacturer || "", 
        meta_data: [
            { key: "quantity", value: normalizedCsvRow.quantity_available || "0" }
        ],
    };
  }

  // Full mode: Include all fields - Define mapping of CSV headers to WooCommerce meta_data fields
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

  // 1. Ensure fileKey does not have .csv before creating the file path
  const cleanFileKey = fileKey.replace(/\.csv$/, ""); 

  // 2. Construct the correct missing file path
  const missingFilePath = path.join(__dirname, `missing_products_${cleanFileKey}.json`);
  let missingProducts = [];
  
  // 3. If the file exists, read its current content
  if (fs.existsSync(missingFilePath)) {
    try {
      missingProducts = JSON.parse(fs.readFileSync(missingFilePath, 'utf8'));
    } catch (err) {
      logErrorToFile(`Error reading missing products file: ${err.message}`);
    }
  }
  
  // 4. Add the current item (from the CSV) to the array
  missingProducts.push(item);

  // 5. Ensure the directory exists before writing the file
  const dir = path.dirname(missingFilePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  
  // 6. Write the updated array back to the file
  fs.writeFileSync(missingFilePath, JSON.stringify(missingProducts, null, 2));
  logInfoToFile(`Recorded missing product for part_number=${item.part_number} in file ${missingFilePath}`);
};

// 1️⃣ fetchProductData() → Fetch productId & existing WooCommerce data
async function fetchProductData(item, currentIndex, totalProductsInFile, fileKey) {
  const productId = await getProductIdByPartNumber(item.part_number, item.manufacturer?.trim() || "", currentIndex, totalProductsInFile, fileKey);
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

// 2️⃣ validateProductMatch() → Ensure the correct product is being updated
function validateProductMatch(item, currentData, productId, fileKey) {
  let currentPartNumber = currentData.meta_data.find(meta => meta.key.toLowerCase() === "part_number")?.value?.trim() || "";
  let currentManufacturer = currentData.meta_data.find(meta => meta.key.toLowerCase() === "manufacturer")?.value?.trim() || "";

  if (!currentPartNumber) {
      currentPartNumber = currentData.name?.trim() || "";
  }

  if (item.part_number !== currentPartNumber || item.manufacturer !== currentManufacturer) {
      logInfoToFile(`"processBatch()" - Skipping update for part_number=${item.part_number}: WooCommerce data mismatch.`);
      return false;
  }

  return true;
}

// 3️⃣ handleQuantityUpdate() → Process only quantity updates
function handleQuantityUpdate(newData, currentData, toUpdate, productId, item) {
  const currentQuantity = currentData.meta_data.find(meta => meta.key === "quantity")?.value || "0";
  const newQuantity = newData.meta_data.find(meta => meta.key === "quantity")?.value || "0";

  if (currentQuantity === newQuantity) {
      logInfoToFile(`🔎 Skipping update for part_number=${item.part_number} as quantity is unchanged: ${currentQuantity}`);
      return false;
  }

  toUpdate.push({
      id: productId,
      manufacturer: item.manufacturer,
      meta_data: [{ key: "quantity", value: String(newQuantity) }]
  });

  return true;
}

// 4️⃣ handleFullUpdate() → Process full product updates
function handleFullUpdate(newData, currentData, toUpdate, productId, item) {
  if (!isUpdateNeeded(currentData, newData)) {
      logInfoToFile(`Skipping update for part_number=${item.part_number} (no changes detected).`);
      return false;
  }

  toUpdate.push(newData);
  return true;
}

// 5️⃣ executeBatchUpdate() → Send bulk updates to WooCommerce
async function executeBatchUpdate(toUpdate, fileKey, MAX_RETRIES) {
  if (toUpdate.length === 0) {
      logInfoToFile(`No valid products to update in this batch for ${fileKey}. Done.`);
      return;
  }

  let attempts = 0;
  while (attempts < MAX_RETRIES) {
      try {
          const jobId = createUniqueJobId(fileKey, "processBatch", 0, attempts);
          const response = await scheduleApiRequest(() => wooApi.put("products/batch", { update: toUpdate }), { id: jobId });

          const updatedCount = response.data?.update?.length || 0;
          await appRedis.incrBy(`updated-products:${fileKey}`, updatedCount);
          return;
      } catch (err) {
          attempts++;
          logErrorToFile(`Batch update attempt ${attempts} for file="${fileKey}" failed: ${err.message}`);
          if (attempts >= MAX_RETRIES) throw new Error(`Batch update failed permanently after ${MAX_RETRIES} attempts.`);
          await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempts) * 1000));
      }
  }
}

// ***************************************************************************
// Main Function - processBatch()
// Process a batch of products using WooCommerce Bulk API
// Example: parse partial updates from the response
// ***************************************************************************
async function processBatch(batch, startIndex, totalProductsInFile, fileKey) {
  const updateMode = process.env.UPDATE_MODE || 'full';
  const MAX_RETRIES = 5;
  let attempts = 0;

  logInfoToFile(`Starting "processBatch()" with startIndex=${startIndex}, fileKey=${fileKey}, Mode: ${updateMode}`);

  if (!Array.isArray(batch)) {
      throw new Error(`"processBatch()" - Expected batch to be an array, got ${typeof batch}`);
  }

  const toUpdate = [];
  let skipCount = 0, localFailCount = 0;
  const updatedParts = [], skippedParts = [], failedParts = [];

  for (let i = 0; i < batch.length; i++) {
      let item = batch[i];
      const currentIndex = startIndex + i;

      if (currentIndex >= totalProductsInFile) break;
      if (!item.part_number) {
          localFailCount++;
          continue;
      }

      try {
          // Fetch product ID and WooCommerce data
          const { productId, currentData } = await fetchProductData(item, currentIndex, totalProductsInFile, fileKey);
          if (!productId || !currentData) {
              localFailCount++;
              continue;
          }

          // Validate product match (ensures correct product before updating)
          if (!validateProductMatch(item, currentData, productId, fileKey)) {
              skipCount++;
              continue;
          }

          // Generate new data for update
          const newData = createNewData(item, productId, item.part_number);

          // Handle update based on mode (quantity-only vs full update)
          if (updateMode === "quantity") {
              if (handleQuantityUpdate(newData, currentData, toUpdate, productId, item)) {
                  updatedParts.push(`Row ${currentIndex + 1}: ${item.part_number} quantity updated.`);
              } else {
                  skipCount++;
              }
          } else {
              if (handleFullUpdate(newData, currentData, toUpdate, productId, item)) {
                  updatedParts.push(`Row ${currentIndex + 1}: ${item.part_number} fully updated.`);
              } else {
                  skipCount++;
              }
          }
      } catch (err) {
          localFailCount++;
          failedParts.push(`Row ${currentIndex + 1}: ${item.part_number} failed - ${err.message}`);
          logErrorToFile(`Error processing part_number=${item.part_number}: ${err.message}`, err.stack);
      }

      recordBatchStatus(fileKey, updatedParts, skippedParts, failedParts);
  }

  // Log skip/fail counts
  if (skipCount > 0) await appRedis.incrBy(`skipped-products:${fileKey}`, skipCount);
  if (localFailCount > 0) await appRedis.incrBy(`failed-products:${fileKey}`, localFailCount);

  // Execute the batch update if there are changes
  await executeBatchUpdate(toUpdate, fileKey, MAX_RETRIES);
}

module.exports = {
  normalizeText,
  isUpdateNeeded,
  processBatch,
};