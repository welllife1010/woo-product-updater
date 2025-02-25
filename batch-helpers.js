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
    if (!text) return "";

    // Strip HTML tags
    let normalized = stripHtml(text)?.result.trim()|| "";

    // Replace special characters; Normalize whitespace and line breaks
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
          // If the current datasheet value already contains "suntsu-products-s3-bucket", skip updating.
          if (currentMetaValue && currentMetaValue.toLowerCase().includes("suntsu-products-s3-bucket")) {
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
// Helper - Create new data for WooCommerce update
// item: a single CSV row (passed as the item object)
// Build an object that matches the format required by the WooCommerce Bulk API for updating products.
// ***************************************************************************
const createNewData =  (item, productId, part_number) => {
  let additionalKeyInfo = [];

  // Normalize CSV headers by converting them to lowercase and removing special characters
  const normalizedItem = {};
  Object.keys(item).forEach(key => {
    const normalizedKey = key.trim().toLowerCase().replace(/\s+/g, "_"); // Normalize key names
    normalizedItem[normalizedKey] = item[key];
  });

  // Define valid meta_data mappings using normalized CSV keys
  const metaDataMappings = {
      "manufacturer": "manufacturer",
      "leadtime": "manufacturer_lead_weeks",
      "image_url": "image_url",
      // Remove the default datasheet mapping since we want custom logic for it:
      // "datasheet": "datasheet_url",
      "series": "series",
      "quantity_available": "quantity",
      "operating_temperature": "operating_temperature",
      "voltage___supply": "voltage",
      "package___case": "package",
      "supplier_device_package": "supplier_device_package",
      "mounting_type": "mounting_type",
      "short_description": "short_description",
      "part_description": "detail_description",
      "reachstatus": "reach_status",
      "rohsstatus": "rohs_status",
      "moisturesensitivitylevel": "moisture_sensitivity_level",
      "exportcontrolclassnumber": "export_control_class_number",
      "htsuscode": "htsus_code"
  };

  // Build the meta_data array from the CSV headers (excluding datasheet for now)
  let metaDataArray = Object.keys(metaDataMappings)
    .filter(csvKey => normalizedItem.hasOwnProperty(csvKey)) // Only include fields that exist in the CSV
    .map(csvKey => ({
      key: metaDataMappings[csvKey],
      value: normalizedItem[csvKey] || "" // Ensure missing values default to an empty string
    }));

  
  // Handle datasheet separately
  if (normalizedItem.hasOwnProperty("datasheet")) {
    const newDatasheet = normalizedItem["datasheet"] || "";
    // Only add datasheet fields if new value does NOT contain "digikey"
    if (!newDatasheet.toLowerCase().includes("digikey")) {
      // Add both datasheet and datasheet_url fields
      metaDataArray.push({ key: "datasheet", value: newDatasheet });
      metaDataArray.push({ key: "datasheet_url", value: newDatasheet });
    }
  }  

  // Format and store any unknown fields in additional_key_information
  const formatAcfFieldName = (name) => {
    return name
      .replace(/_/g, " ") // Replace underscores with spaces
      .replace(/-/g, " ") // Replace dashes with spaces
      .replace(/\b\w/g, l => l.toUpperCase()); // Capitalize each word
  };

  // Store unknown fields in additional_key_information
  Object.keys(normalizedItem).forEach(key => {
    if (!metaDataMappings[key] && key !== "datasheet" && key !== "part_number" &&
        formatAcfFieldName(key) != "Category" && formatAcfFieldName(key) != "Product Status") {
      let value = normalizedItem[key] || "";
      if (value !== "" && value !== "NaN") {
        let label = formatAcfFieldName(key); 
        additionalKeyInfo.push(`<strong>${label}:</strong> ${value}<br>`);
      }
    }
  });

  // Ensure `additional_key_information` is never empty
  const additionalKeyContent = additionalKeyInfo.length > 0 ? additionalKeyInfo.join("") : "";

  return {
    id: productId, // Required for Bulk API
    part_number: normalizedItem.part_number || part_number, // Use normalized key
    sku: normalizedItem.sku || `${normalizedItem.part_number}_${normalizedItem.manufacturer}` || normalizedItem.part_number,
    description: normalizedItem.part_description || "", // Corrected field mapping
    meta_data: [
      ...metaDataArray, // Only include valid CSV-based meta_data
      { key: "additional_key_information", value: additionalKeyContent } // Store only extra unknown fields
    ]
  };
}

// ***************************************************************************
// Helper - Filter current product data
// ***************************************************************************
const filterCurrentData = (product) => {
  return {
    sku: product.sku,
    description: product.description,
    meta_data: product.meta_data.filter((meta) =>
        ["spq", "manufacturer", "image_url", "datasheet_url", "series_url", "series", "quantity", "operating_temperature", "voltage", "package", "supplier_device_package", "mounting_type", "short_description", "detail_description", "additional_key_information", "reach_status", "rohs_status", "moisture_sensitivity_level", "export_control_class_number", "htsus_code"].includes(meta.key)
    ),
  };
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
      logInfoToFile(`Current Data: ${JSON.stringify(currentData, null, 2)}`);
      logInfoToFile(`New Data: ${JSON.stringify(newData, null, 2)}`);

      
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