/*
================================================================================
FILE: src/batch/build-update-payload.js
================================================================================

PURPOSE:
Centralized diff-based update payload builder. Compares current WooCommerce 
product data against candidate (new) data and returns ONLY the fields that 
actually need updating.

KEY BENEFITS:
1. Minimal payloads - only changed fields are sent to WooCommerce
2. Centralized protection rules - all field-specific logic in ONE place
3. Clear audit trail - easy to debug what changed
4. Better performance - reduces API payload size

FIELD PROTECTION RULES:
- image_url: Protects internal images, prevents cross-environment overwrites
- datasheet/datasheet_url: Protects S3 datasheets, skips digikey URLs
- additional_key_information: Merges new with existing (never loses data)

================================================================================
*/

const { logInfoToFile } = require("../utils/logger");
const { normalizeText } = require("./text-utils");
const { 
  mergeAdditionalKeyInfo, 
  parseAdditionalKeyInfo 
} = require("./map-new-data");

/**
 * List of meta_data keys we track for updates.
 * Only these fields will be considered for inclusion in the update payload.
 */
const TRACKED_META_KEYS = [
  "part_number",
  "spq",
  "manufacturer",
  "image_url",
  "datasheet",
  "datasheet_url",
  "series_url",
  "series",
  "quantity",
  "operating_temperature",
  "voltage",
  "package",
  "packaging",
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
  "manufacturer_lead_weeks",
  "pcn_design_specification",
  "pcn_assembly_origin",
  "pcn_packaging",
  "html_datasheet",
  "eda_models",
  "environmental_information",
];

/**
 * @function isProtectedImageUrl
 * @description Determines if an image_url update should be blocked.
 * @param {string} currentValue - Current image URL in WooCommerce
 * @param {string} newValue - New image URL from CSV
 * @returns {{ protect: boolean, reason?: string }}
 */
function isProtectedImageUrl(currentValue, newValue) {
  const currentLower = (currentValue || "").toLowerCase();
  const newLower = (newValue || "").toLowerCase();

  // Rule 1: Don't overwrite with empty value
  if (currentValue && (!newValue || newValue.trim() === "")) {
    return { protect: true, reason: "would overwrite with empty value" };
  }

  // Rule 2: Skip digikey images (hotlinking issues)
  if (newLower.includes("digikey.com") || newLower.includes("mm.digikey.com")) {
    return { protect: true, reason: "new value is digikey (hotlinking)" };
  }

  // Rule 3: Protect internal images from cross-environment overwrites
  const isCurrentInternal = currentLower.includes("suntsu.com") || 
                            currentLower.includes("suntsu-products-s3-bucket") ||
                            currentLower.includes("kinsta.cloud");

  if (currentValue && isCurrentInternal) {
    const isNewInternal = newLower.includes("suntsu.com") || 
                          newLower.includes("suntsu-products-s3-bucket") ||
                          newLower.includes("kinsta.cloud");

    // Don't overwrite internal with external
    if (!isNewInternal) {
      return { protect: true, reason: "current is internal, new is external" };
    }

    // Prevent cross-environment overwrites (staging <-> production)
    const currentIsStaging = currentLower.includes("kinsta.cloud");
    const newIsStaging = newLower.includes("kinsta.cloud");
    
    if (currentIsStaging !== newIsStaging) {
      return { protect: true, reason: "cross-environment overwrite blocked" };
    }
  }

  return { protect: false };
}

/**
 * @function isProtectedDatasheet
 * @description Determines if a datasheet URL update should be blocked.
 * @param {string} currentValue - Current datasheet URL in WooCommerce
 * @param {string} newValue - New datasheet URL from CSV
 * @returns {{ protect: boolean, reason?: string }}
 */
function isProtectedDatasheet(currentValue, newValue) {
  const currentLower = (currentValue || "").toLowerCase();
  const newLower = (newValue || "").toLowerCase();

  // Rule 1: Skip digikey datasheets
  if (newLower.includes("digikey")) {
    return { protect: true, reason: "new value contains digikey" };
  }

  // Rule 2: Don't overwrite S3 datasheets with non-S3
  if (currentValue && currentLower.includes("suntsu-products-s3-bucket")) {
    if (!newLower.includes("suntsu-products-s3-bucket")) {
      return { protect: true, reason: "current is S3, new is not" };
    }
  }

  return { protect: false };
}

/**
 * @function shouldUpdateField
 * @description Centralized field-level decision maker.
 * Determines whether a specific field should be updated and what value to use.
 * 
 * @param {string} key - The meta_data key
 * @param {string} currentValue - Current value in WooCommerce
 * @param {string} newValue - New value from CSV
 * @returns {{ update: boolean, value?: string, reason?: string }}
 */
function shouldUpdateField(key, currentValue, newValue) {
  // Normalize for comparison
  const normalizedCurrent = normalizeText(currentValue || "");
  const normalizedNew = normalizeText(newValue || "");

  // =========================================================================
  // FIELD-SPECIFIC RULES
  // =========================================================================

  // Rule: image_url protection
  if (key === "image_url") {
    const protection = isProtectedImageUrl(currentValue, newValue);
    if (protection.protect) {
      return { update: false, reason: `image_url: ${protection.reason}` };
    }
  }

  // Rule: datasheet/datasheet_url protection
  if (key === "datasheet" || key === "datasheet_url") {
    const protection = isProtectedDatasheet(currentValue, newValue);
    if (protection.protect) {
      return { update: false, reason: `${key}: ${protection.reason}` };
    }
  }

  // Rule: additional_key_information - merge instead of replace
  if (key === "additional_key_information") {
    // If no new value, don't update
    if (!newValue || newValue.trim() === "") {
      return { update: false, reason: "additional_key_information: new value empty" };
    }

    // Merge existing with new (preserves existing, adds new keys only)
    const mergedValue = mergeAdditionalKeyInfo(currentValue || "", newValue);
    
    // Check if merge actually added anything new
    const existingKeys = parseAdditionalKeyInfo(currentValue || "");
    const mergedKeys = parseAdditionalKeyInfo(mergedValue);
    
    if (mergedKeys.size === existingKeys.size && normalizedCurrent === normalizeText(mergedValue)) {
      return { update: false, reason: "additional_key_information: no new keys to add" };
    }
    
    return { update: true, value: mergedValue, reason: "additional_key_information: merged with new keys" };
  }

  // =========================================================================
  // DEFAULT RULE: Update if values differ
  // =========================================================================
  
  // Skip if both empty
  if (!normalizedCurrent && !normalizedNew) {
    return { update: false, reason: "both values empty" };
  }

  // Skip if values are the same after normalization
  if (normalizedCurrent === normalizedNew) {
    return { update: false, reason: "values identical after normalization" };
  }

  // Don't overwrite existing value with empty
  if (currentValue && (!newValue || newValue.trim() === "")) {
    return { update: false, reason: "would overwrite with empty value" };
  }

  // Values differ - update needed
  return { update: true, value: newValue };
}

/**
 * @function buildUpdatePayload
 * @description Main entry point. Compares current vs candidate data and 
 * returns a minimal payload containing ONLY the fields that need updating.
 * 
 * @param {Object} currentData - Current WooCommerce product data (with meta_data array)
 * @param {Object} candidateData - Candidate data from createNewData() 
 * @param {string} partNumber - Part number for logging
 * @param {string} fileKey - File identifier for logging
 * @returns {{ payload: Object|null, changedFields: string[], skippedFields: Object }}
 *   - payload: The minimal update payload, or null if no changes needed
 *   - changedFields: Array of field names that will be updated
 *   - skippedFields: Object mapping skipped field names to reasons
 */
function buildUpdatePayload(currentData, candidateData, partNumber, fileKey) {
  const changedFields = [];
  const skippedFields = {};
  
  // Initialize payload with required fields
  const payload = {
    id: candidateData.id,
    meta_data: [],
  };

  // Get current meta as a map for easy lookup
  const currentMetaMap = new Map();
  if (Array.isArray(currentData?.meta_data)) {
    for (const meta of currentData.meta_data) {
      if (meta && meta.key) {
        currentMetaMap.set(meta.key, meta.value || "");
      }
    }
  }

  // Process each meta field from candidate data
  if (Array.isArray(candidateData?.meta_data)) {
    for (const newMeta of candidateData.meta_data) {
      if (!newMeta || !newMeta.key) continue;
      
      const key = newMeta.key;
      
      // Skip if not in our tracked keys
      if (!TRACKED_META_KEYS.includes(key)) {
        skippedFields[key] = "not in TRACKED_META_KEYS";
        continue;
      }

      const currentValue = currentMetaMap.get(key) || "";
      const newValue = newMeta.value || "";

      // Apply field-specific rules
      const decision = shouldUpdateField(key, currentValue, newValue);

      if (decision.update) {
        payload.meta_data.push({ 
          key, 
          value: decision.value !== undefined ? decision.value : newValue 
        });
        changedFields.push(key);
        
        logInfoToFile(
          `[buildUpdatePayload] ${partNumber}: UPDATE '${key}'\n` +
          `  Current: "${currentValue?.substring(0, 100)}${currentValue?.length > 100 ? '...' : ''}"\n` +
          `  New: "${(decision.value || newValue)?.substring(0, 100)}${(decision.value || newValue)?.length > 100 ? '...' : ''}"`
        );
      } else {
        skippedFields[key] = decision.reason || "no change";
      }
    }
  }

  // =========================================================================
  // Handle non-meta fields (description, sku, categories)
  // =========================================================================
  
  // Description
  if (candidateData.description !== undefined) {
    const currentDesc = normalizeText(currentData?.description || "");
    const newDesc = normalizeText(candidateData.description || "");
    
    if (currentDesc !== newDesc && candidateData.description) {
      payload.description = candidateData.description;
      changedFields.push("description");
      logInfoToFile(`[buildUpdatePayload] ${partNumber}: UPDATE 'description'`);
    }
  }

  // SKU
  if (candidateData.sku !== undefined) {
    const currentSku = normalizeText(currentData?.sku || "");
    const newSku = normalizeText(candidateData.sku || "");
    
    if (currentSku !== newSku && candidateData.sku) {
      payload.sku = candidateData.sku;
      changedFields.push("sku");
      logInfoToFile(`[buildUpdatePayload] ${partNumber}: UPDATE 'sku'`);
    }
  }

  // Categories (if present in candidate)
  if (candidateData.categories && Array.isArray(candidateData.categories)) {
    payload.categories = candidateData.categories;
    changedFields.push("categories");
  }

  // =========================================================================
  // Final decision: return payload only if there are changes
  // =========================================================================
  
  const hasChanges = changedFields.length > 0;
  
  if (hasChanges) {
    logInfoToFile(
      `[buildUpdatePayload] ${partNumber} in ${fileKey}: ` +
      `${changedFields.length} field(s) to update: [${changedFields.join(", ")}]`
    );
  } else {
    logInfoToFile(
      `[buildUpdatePayload] ${partNumber} in ${fileKey}: No changes needed`
    );
  }

  return {
    payload: hasChanges ? payload : null,
    changedFields,
    skippedFields,
  };
}

/**
 * @function buildQuantityOnlyPayload
 * @description Specialized builder for quantity-only updates.
 * @param {Object} currentData - Current WooCommerce product data
 * @param {Object} candidateData - Candidate data from createNewData()
 * @param {number} productId - Product ID
 * @param {string} partNumber - Part number for logging
 * @returns {{ payload: Object|null, changed: boolean }}
 */
function buildQuantityOnlyPayload(currentData, candidateData, productId, partNumber) {
  const currentMeta = Array.isArray(currentData?.meta_data) ? currentData.meta_data : [];
  const newMeta = Array.isArray(candidateData?.meta_data) ? candidateData.meta_data : [];
  
  const currentQty = currentMeta.find((m) => m?.key === "quantity")?.value || "0";
  const newQty = newMeta.find((m) => m?.key === "quantity")?.value || "0";

  if (currentQty === newQty) {
    logInfoToFile(`[buildQuantityOnlyPayload] ${partNumber}: quantity unchanged (${currentQty})`);
    return { payload: null, changed: false };
  }

  logInfoToFile(
    `[buildQuantityOnlyPayload] ${partNumber}: quantity ${currentQty} → ${newQty}`
  );

  return {
    payload: {
      id: productId,
      meta_data: [{ key: "quantity", value: String(newQty) }],
    },
    changed: true,
  };
}

module.exports = {
  buildUpdatePayload,
  buildQuantityOnlyPayload,
  shouldUpdateField,
  isProtectedImageUrl,
  isProtectedDatasheet,
  TRACKED_META_KEYS,
};
