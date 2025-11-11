/*
================================================================================
FILE: src/batch/compare.js
PURPOSE: Decide whether an update is needed by comparing current vs new data.
ADDED VALUE:
- Filters current meta to only fields we track (avoids noise)
- Encodes all special skip rules in one place
================================================================================
*/

const { logger, logInfoToFile } = require("../../logger");
const { normalizeText, isCurrentMetaMissing, isMetaValueDifferent } = require("./text-utils");

/**
* @function filterCurrentData
* @description Reduces a Woo product to only the fields relevant for comparison,
* preventing false positives from irrelevant meta keys.
*/
const filterCurrentData = (product) => ({
  name: product.name,
  sku: product.sku,
  description: product.description,
  meta_data: (product.meta_data || []).filter((meta) =>
    [
      "part_number", "spq", "manufacturer", "image_url", "datasheet_url", "series_url", "series", "quantity",
      "operating_temperature", "voltage", "package", "supplier_device_package", "mounting_type",
      "short_description", "detail_description", "additional_key_information", "reach_status",
      "rohs_status", "moisture_sensitivity_level", "export_control_class_number", "htsus_code",
    ].includes(meta.key)
  ),
});

/**
* @function isUpdateNeeded
* @description Returns true if any field differs, applying domain-specific rules
* (e.g., skip digikey images, don't replace S3 datasheets).
* @param {Object} currentData - Reduced product from filterCurrentData().
* @param {Object} newData - Newly built update payload (createNewData()).
* @param {number} [currentIndex]
* @param {number} [total]
* @param {string} [partNumber]
* @param {string} [fileName]
* @returns {boolean}
*/
const isUpdateNeeded = (currentData, newData, _currentIndex, _total, partNumber, fileName) => {
  const updateMode = process.env.UPDATE_MODE || "full";
  const fieldsToUpdate = [];
  logInfoToFile(`[ isUpdateNeeded() ] - Checking for updates for Part Number: ${partNumber} in ${fileName}`);

  // Quantity-only short circuit
  if (updateMode === "quantity") {
    const curQ = currentData.meta_data?.find((m) => m.key === "quantity")?.value || "0";
    const newQ = newData.meta_data?.find((m) => m.key === "quantity")?.value || "0";
    if (curQ !== newQ) {
      logInfoToFile(`[ isUpdateNeeded() ] - Quantity update needed for ${partNumber}: "${curQ}" → "${newQ}"`);
      return true;
    }
    logInfoToFile(`[ isUpdateNeeded() ] - No quantity update needed for ${partNumber}`);
    return false;
  }

  // Full-field comparison
  Object.keys(newData).forEach((key) => {
    if (key === "id" || key === "part_number") return;

    let newValue = newData[key];
    let currentValue = currentData[key];

    if (key === "meta_data") {
      // Structure or presence mismatch → update
      if (!Array.isArray(newValue) || !Array.isArray(currentValue)) {
        fieldsToUpdate.push(key);
        return;
      }

      newValue.forEach((newMeta) => {
        const newMetaValue = newMeta.value;
        const currentMeta = (currentValue || []).find((m) => m.key === newMeta.key);
        const currentMetaValue = currentMeta?.value || "";

        // --- SPECIAL SKIP RULES ------------------------------------------------
        // 1) Datasheet fields: never replace with digikey; don't override S3 values
        if (newMeta.key === "datasheet" || newMeta.key === "datasheet_url") {
          if (String(newMetaValue).toLowerCase().includes("digikey")) {
            logInfoToFile(`[ isUpdateNeeded() ] - Skipping ${newMeta.key} (contains "digikey")`);
            return;
          }
          if (
            currentMetaValue &&
            String(currentMetaValue).toLowerCase().includes("suntsu-products-s3-bucket") &&
            currentMetaValue !== newMetaValue
          ) {
            logInfoToFile(`[ isUpdateNeeded() ] - Skipping ${newMeta.key} (current value already S3)`);
            return;
          }
        }

        // 2) Image URL: skip digikey hosts to avoid hotlinking
        if (newMeta.key === "image_url" &&
            (String(newMetaValue).includes("digikey.com") || String(newMetaValue).includes("mm.digikey.com"))) {
          logInfoToFile(`[ isUpdateNeeded() ] - Skipping image_url (digikey host)`);
          return;
        }

        // ----------------------------------------------------------------------

        // Missing entry → add it
        if (isCurrentMetaMissing(newMetaValue, currentMeta)) {
          logInfoToFile(`DEBUG: Key '${newMeta.key}' missing in current meta_data. Marking for update.`);
          fieldsToUpdate.push(`meta_data.${newMeta.key}`);
          return;
        }

        // Value differs after normalization → update
        if (isMetaValueDifferent(newMetaValue, currentMetaValue)) {
          fieldsToUpdate.push(`meta_data.${newMeta.key}`);
          logInfoToFile(
            `[ isUpdateNeeded() ] - Update '${newMeta.key}'\nCurrent: '${currentMetaValue}'\nNew: '${newMetaValue}'\n`
          );
        }
      });
    } else {
      // Non-meta fields: normalize strings before compare
      if (typeof newValue === "string") {
        newValue = normalizeText(newValue);
        currentValue = currentValue ? normalizeText(currentValue) : "";
      }
      if (currentValue === undefined || currentValue !== newValue) {
        fieldsToUpdate.push(key);
        logInfoToFile(
          `[ isUpdateNeeded() ] - Update '${key}'\nCurrent: '${currentValue}'\nNew: '${newValue}'\n`
        );
      }
    }
  });

  if (fieldsToUpdate.length > 0) {
    logInfoToFile(`[ isUpdateNeeded() ] - Update needed for ${partNumber} in ${fileName}`);
    return true;
  }
  logger.info(`[ isUpdateNeeded() ] - No update required for ${partNumber} in ${fileName}`);
  return false;
};

module.exports = { isUpdateNeeded, filterCurrentData };
