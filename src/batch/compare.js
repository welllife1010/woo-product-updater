/*
================================================================================
FILE: src/batch/compare.js
================================================================================

PURPOSE:
Decide whether an update is needed by comparing current vs new data.

NOTE (2026): This file is now SECONDARY to build-update-payload.js
The primary update logic now uses buildUpdatePayload() which:
1. Returns ONLY fields that need updating (diff-based)
2. Has centralized protection rules for all fields
3. Is used by handlers.js for actual updates

This file (isUpdateNeeded, filterCurrentData) is kept for:
- Backwards compatibility
- Additional logging/debugging
- Legacy code that may still reference it

For new development, use build-update-payload.js instead.

================================================================================

ADDED VALUE:
- Filters current meta to only fields we track (avoids noise)
- Encodes all special skip rules in one place

BUG FIX (2025) - NULL SAFETY:

PROBLEM:
The original code didn't properly handle cases where:
1. product parameter to filterCurrentData could be null/undefined
2. currentData or newData in isUpdateNeeded could have null meta_data
3. Array methods were called on potentially undefined values

This caused intermittent crashes when WooCommerce returned incomplete
product data or when CSV parsing produced malformed rows.

THE FIX:
1. Added null checks before accessing object properties
2. Used optional chaining (?.) and nullish coalescing (??) operators
3. Added input validation at the start of functions
4. Defensive array handling with fallbacks to empty arrays

================================================================================
*/

const { logger, logInfoToFile, logErrorToFile } = require("../utils/logger");
const { normalizeText, isCurrentMetaMissing, isMetaValueDifferent } = require("./text-utils");

/**
 * List of meta_data keys we track for comparison.
 * 
 * Only these fields will be included when comparing current vs new data.
 * This prevents false-positive updates from irrelevant meta keys that
 * WooCommerce might add (like internal tracking fields).
 */
const TRACKED_META_KEYS = [
  "part_number",
  "spq",
  "manufacturer",
  "image_url",
  "datasheet_url",
  "series_url",
  "series",
  "quantity",
  "operating_temperature",
  "voltage",
  "package",
  "supplier_device_package",
  "mounting_type",
  "short_description",
  "detail_description",
  "additional_key_information",
  "reach_status",
  "rohs_status",
  "moisture_sensitivity_level",
  "export_control_class_number",
  "htsus_code",
  // Basic Product Info
  "manufacturer_lead_weeks",
  // Document & Media
  "pcn_design_specification",
  "pcn_assembly_origin",
  "pcn_packaging",
  "html_datasheet",
  "eda_models",
  // Environmental Info (general)
  "environmental_information",
];

/**
 * Reduces a WooCommerce product to only the fields relevant for comparison.
 * 
 * This prevents false positives from irrelevant meta keys that WooCommerce
 * might add (like internal tracking fields, plugin data, etc.)
 * 
 * @param {Object|null} product - The WooCommerce product object
 * @returns {Object} Filtered product with only tracked fields
 * 
 * @example
 * const filtered = filterCurrentData(wooProduct);
 * // Only contains name, sku, description, and tracked meta_data
 */
const filterCurrentData = (product) => {
  // BUG FIX: Handle null/undefined product
  if (!product || typeof product !== 'object') {
    logErrorToFile(`filterCurrentData: Received invalid product: ${typeof product}`);
    return {
      name: "",
      sku: "",
      description: "",
      meta_data: [],
    };
  }

  return {
    name: product.name || "",
    sku: product.sku || "",
    description: product.description || "",
    // BUG FIX: Safe array filtering with fallback
    meta_data: Array.isArray(product.meta_data)
      ? product.meta_data.filter((meta) => 
          meta && typeof meta === 'object' && TRACKED_META_KEYS.includes(meta.key)
        )
      : [],
  };
};

/**
 * Determines if a WooCommerce product needs to be updated.
 * 
 * Compares current product data against new CSV data, applying domain-specific
 * rules to avoid unnecessary updates.
 * 
 * SPECIAL RULES:
 * - Digikey images: Skipped to avoid hotlinking issues
 * - S3 datasheets: Never replaced with non-S3 URLs
 * - Digikey datasheets: Skipped entirely
 * 
 * @param {Object} currentData - Reduced product from filterCurrentData()
 * @param {Object} newData - Newly built update payload from createNewData()
 * @param {number} [_currentIndex] - Unused, kept for API compatibility
 * @param {number} [_total] - Unused, kept for API compatibility
 * @param {string} [partNumber] - Part number for logging
 * @param {string} [fileName] - File name for logging
 * @returns {boolean} True if update is needed, false otherwise
 * 
 * @example
 * if (isUpdateNeeded(currentProduct, newPayload, 0, 100, "ABC123", "products.csv")) {
 *   toUpdate.push(newPayload);
 * }
 */
const isUpdateNeeded = (currentData, newData, _currentIndex, _total, partNumber, fileName) => {
  // BUG FIX: Early validation of inputs
  if (!currentData || typeof currentData !== 'object') {
    logErrorToFile(
      `[ isUpdateNeeded() ] - Invalid currentData for ${partNumber}: ${typeof currentData}`
    );
    // If we can't compare, assume update is needed to be safe
    return true;
  }

  if (!newData || typeof newData !== 'object') {
    logErrorToFile(
      `[ isUpdateNeeded() ] - Invalid newData for ${partNumber}: ${typeof newData}`
    );
    // If we have nothing to update with, skip
    return false;
  }

  const updateMode = process.env.UPDATE_MODE || "full";
  const fieldsToUpdate = [];
  logInfoToFile(`[ isUpdateNeeded() ] - Checking for updates for Part Number: ${partNumber} in ${fileName}`);

  // Quantity-only short circuit
  if (updateMode === "quantity") {
    // BUG FIX: Safe access to meta_data arrays
    const currentMeta = Array.isArray(currentData.meta_data) ? currentData.meta_data : [];
    const newMeta = Array.isArray(newData.meta_data) ? newData.meta_data : [];
    
    const curQ = currentMeta.find((m) => m && m.key === "quantity")?.value || "0";
    const newQ = newMeta.find((m) => m && m.key === "quantity")?.value || "0";
    
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

        // 2) Image URL: protect existing valid images
        if (newMeta.key === "image_url") {
          // Skip digikey hosts to avoid hotlinking
          if (String(newMetaValue).includes("digikey.com") || String(newMetaValue).includes("mm.digikey.com")) {
            logInfoToFile(`[ isUpdateNeeded() ] - Skipping image_url (digikey host)`);
            return;
          }
          // Don't overwrite existing valid image with empty/invalid value
          if (currentMetaValue && (!newMetaValue || newMetaValue.trim() === "")) {
            logInfoToFile(`[ isUpdateNeeded() ] - Skipping image_url (would overwrite existing image with empty value)`);
            return;
          }
          // Don't overwrite existing internal images (Suntsu WordPress/S3/Staging) with external URLs
          const currentLower = String(currentMetaValue).toLowerCase();
          const isCurrentInternalImage = currentLower.includes("suntsu.com") || 
                                         currentLower.includes("suntsu-products-s3-bucket") ||
                                         currentLower.includes("kinsta.cloud");  // staging domain
          if (currentMetaValue && isCurrentInternalImage && currentMetaValue !== newMetaValue) {
            const newLower = String(newMetaValue).toLowerCase();
            const isNewInternalImage = newLower.includes("suntsu.com") || 
                                       newLower.includes("suntsu-products-s3-bucket") ||
                                       newLower.includes("kinsta.cloud");  // staging domain
            // Only allow update if new value is also an internal image AND on the same domain
            // This prevents production URLs from overwriting staging URLs and vice versa
            if (!isNewInternalImage) {
              logInfoToFile(`[ isUpdateNeeded() ] - Skipping image_url (current internal image, new is external: "${newMetaValue}")`);
              return;
            }
            // Prevent cross-environment overwrites (staging <-> production)
            const currentIsStaging = currentLower.includes("kinsta.cloud");
            const newIsStaging = newLower.includes("kinsta.cloud");
            if (currentIsStaging !== newIsStaging) {
              logInfoToFile(`[ isUpdateNeeded() ] - Skipping image_url (cross-environment: current="${currentMetaValue}", new="${newMetaValue}")`);
              return;
            }
          }
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
