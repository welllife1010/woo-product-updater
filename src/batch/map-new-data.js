/*
================================================================================
FILE: src/batch/map-new-data.js
PURPOSE: Convert a CSV row into a WooCommerce "update" payload.
SCOPE:
- Header normalization
- Known-field mapping (→ meta_data)
- Special rules (datasheet, image_url, etc.)
- Fallback assembly of `additional_key_information`
================================================================================
*/

const { normalizeText } = require("./text-utils");
const Fuse = require("fuse.js");

// Map raw normalized CSV headers -> canonical keys your code uses everywhere
const FIELD_ALIASES = {
  // Column 1/2 name variants
  "manufacturer_part_number": "part_number",
  "mfr_part_number": "part_number",

  // Description variants (column 2 may be any of these)
  "product_description": "part_description",
  "short_product_description": "short_description",
  "detailed_product_description": "detail_description",

  // New or variant spec names
  "stock_quantity": "quantity",          // ACF "quantity"
  "quantity_available": "quantity",      // keep existing
  "voltage": "voltage",                  // keep it canonical
  "operating_temperature": "operating_temperature",
  "supplier_device_package": "supplier_device_package",
  "packaging": "packaging",              // new
  "rohs_compliance": "rohs_status",
  "reach_compliance": "reach_status",
  "hts_code": "htsus_code",
  "eccn": "export_control_class_number",
  "moisture_sensitivity_level": "moisture_sensitivity_level",

  // URL field variants (after asterisk removal)
  "datasheet_url": "datasheet",
  "image_attachment_url": "image_url",
  "image_url": "image_url"
};

// Apply aliases before mapping
const applyAliases = (normalizedRow) => {
  const out = {};
  for (const [k, v] of Object.entries(normalizedRow)) {
    const alias = FIELD_ALIASES[k] || k;
    out[alias] = v;
  }
  return out;
};

/**
 * @function normalizeCsvHeaders
 * @description Produces a stable, punctuation-tolerant row so vendor CSV
 * idiosyncrasies don't propagate.
 *
 * Example:
 *   "Voltage / Supply" -> "voltage_supply"
 *   "Package / Case"  -> "package_case"
 */
const normalizeCsvHeaders = (item) => {
  const out = {};
  Object.keys(item || {}).forEach((key) => {
    const normalizedKey = String(key || "")
      .replace(/\*/g, "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "_")
      .replace(/[^a-z0-9_]+/g, "_")
      .replace(/_+/g, "_")
      .replace(/^_+|_+$/g, "");

    if (!normalizedKey) return;
    out[normalizedKey] = item[key];
  });
  return out;
};

function dedupeMetaData(metaData) {
  if (!Array.isArray(metaData)) return [];

  // Last write wins.
  const byKey = new Map();
  for (const entry of metaData) {
    if (!entry || typeof entry !== "object") continue;
    const key = entry.key;
    if (!key) continue;
    byKey.set(String(key), entry);
  }
  return Array.from(byKey.values());
}

/**
* @function formatAcfFieldName
* @description Presentational helper to turn underscored keys into a readable label
* for inclusion inside additional_key_information HTML.
*/
const formatAcfFieldName = (name) =>
  name.replace(/_/g, " ").replace(/-/g, " ").replace(/\b\w/g, (l) => l.toUpperCase());

/**
 * @function parseAdditionalKeyInfo
 * @description Parses the existing additional_key_information HTML into a Map of key-value pairs.
 * Handles various HTML formats:
 *   - "<strong>Key:</strong> Value<br/>"
 *   - "<p><strong>Key:</strong> Value</p>"
 *   - "<b>Key:</b> Value<br>"
 *   - "Key: Value<br>"
 *   - Mixed formats with various wrappers
 * @param {string} html - The HTML string to parse
 * @returns {Map<string, {originalKey: string, value: string}>} Map of normalized keys to their original key and value
 */
const parseAdditionalKeyInfo = (html) => {
  const result = new Map();
  if (!html || typeof html !== "string") return result;

  // First, normalize the HTML to make parsing easier
  let normalized = html
    // Remove newlines and extra whitespace
    .replace(/\n/g, " ")
    .replace(/\s+/g, " ")
    // Normalize different bold tags to <strong>
    .replace(/<b>/gi, "<strong>")
    .replace(/<\/b>/gi, "</strong>")
    // Remove <p> tags but keep content (treat as line breaks)
    .replace(/<p[^>]*>/gi, "")
    .replace(/<\/p>/gi, "<br>")
    // Normalize <br> variants
    .replace(/<br\s*\/?>/gi, "<br>")
    // Remove other common wrappers but keep content
    .replace(/<span[^>]*>/gi, "")
    .replace(/<\/span>/gi, "")
    .replace(/<div[^>]*>/gi, "")
    .replace(/<\/div>/gi, "<br>");

  // Pattern 1: <strong>Key:</strong> Value (most common format)
  const strongPattern = /<strong>([^<]+):\s*<\/strong>\s*([^<]*?)(?:<br>|<strong>|$)/gi;
  let match;
  
  while ((match = strongPattern.exec(normalized)) !== null) {
    const key = match[1].trim();
    const value = match[2].trim();
    if (key) {
      const normalizedKey = normalizeKeyForComparison(key);
      result.set(normalizedKey, { originalKey: key, value });
    }
  }

  // Pattern 2: Plain text "Key: Value" (without HTML tags)
  // Only process if we haven't found matches with the strong pattern
  // Split by <br> and look for "Key: Value" patterns
  if (result.size === 0) {
    const plainPattern = /([^:<>]+):\s*([^<]*?)(?:<br>|$)/gi;
    while ((match = plainPattern.exec(normalized)) !== null) {
      const key = match[1].trim();
      const value = match[2].trim();
      // Skip if key looks like a URL or contains HTML artifacts
      if (key && !key.includes("http") && !key.includes("&") && key.length < 50) {
        const normalizedKey = normalizeKeyForComparison(key);
        if (!result.has(normalizedKey)) {
          result.set(normalizedKey, { originalKey: key, value });
        }
      }
    }
  }

  return result;
};

/**
 * @function normalizeKeyForComparison
 * @description Normalizes a key string for comparison purposes.
 * Handles variations like "Load Capacitance" vs "load_capacitance" vs "Load  Capacitance"
 * @param {string} key - The key to normalize
 * @returns {string} Normalized key for comparison
 */
const normalizeKeyForComparison = (key) => {
  if (!key || typeof key !== "string") return "";
  return key
    .toLowerCase()
    .replace(/[_\-]/g, " ")      // Replace underscores and hyphens with spaces
    .replace(/\s+/g, " ")         // Collapse multiple spaces
    .replace(/[^a-z0-9 ]/g, "")   // Remove special characters
    .trim();
};

/**
 * @function fuzzyKeyMatch
 * @description Uses Fuse.js to check if a new key fuzzy-matches any existing key.
 * Helps catch variations like:
 *   - "Load Capacitance" vs "Load Capacitence" (typo)
 *   - "Operating Temp" vs "Operating Temperature" (abbreviation)
 *   - "Freq Tolerance" vs "Frequency Tolerance" (abbreviation)
 * @param {string} newKey - The normalized new key to check
 * @param {string[]} existingKeys - Array of normalized existing keys
 * @param {number} threshold - Fuse.js threshold (0 = exact, 1 = match anything). Default 0.3
 * @returns {boolean} True if a fuzzy match is found
 */
const fuzzyKeyMatch = (newKey, existingKeys, threshold = 0.3) => {
  if (!newKey || !existingKeys || existingKeys.length === 0) return false;
  
  // First check exact match (faster)
  if (existingKeys.includes(newKey)) return true;
  
  // Use Fuse.js for fuzzy matching
  const fuse = new Fuse(existingKeys, {
    threshold: threshold,        // Lower = stricter matching
    distance: 100,               // How far to search for a match
    minMatchCharLength: 2,       // Minimum characters that must match
    includeScore: true,
  });
  
  const results = fuse.search(newKey);
  
  // Return true if we found a match with score below threshold
  return results.length > 0 && results[0].score <= threshold;
};

/**
 * @function mergeAdditionalKeyInfo
 * @description Merges existing additional_key_information with new values.
 * Preserves all existing key-value pairs and only adds new ones that don't exist.
 * Uses both normalized key comparison AND fuzzy matching to detect duplicates.
 * @param {string} existingHtml - The current additional_key_information HTML
 * @param {string} newHtml - The newly generated additional_key_information HTML
 * @returns {string} Merged HTML string
 */
const mergeAdditionalKeyInfo = (existingHtml, newHtml) => {
  const existingMap = parseAdditionalKeyInfo(existingHtml);
  const newMap = parseAdditionalKeyInfo(newHtml);
  
  // Get array of existing normalized keys for fuzzy matching
  const existingNormalizedKeys = Array.from(existingMap.keys());
  
  // Start with all existing content (preserve original formatting completely)
  let merged = existingHtml || "";
  
  // Ensure existing content ends properly for appending
  if (merged && merged.trim()) {
    merged = merged.trim();
    // Normalize ending - remove trailing breaks then add one consistent one
    merged = merged.replace(/(<br\s*\/?>)+$/gi, "");
    merged += "<br>";
  }
  
  // Add only NEW entries that don't exist in the current data
  // Uses both exact normalized match AND fuzzy match
  for (const [normalizedKey, { originalKey, value }] of newMap) {
    // Check 1: Exact normalized key match
    const hasExactMatch = existingMap.has(normalizedKey);
    
    // Check 2: Fuzzy match (catches typos, abbreviations, slight variations)
    const hasFuzzyMatch = fuzzyKeyMatch(normalizedKey, existingNormalizedKeys);
    
    if (!hasExactMatch && !hasFuzzyMatch) {
      merged += `<strong>${originalKey}:</strong> ${value}<br>`;
    }
  }
  
  return merged;
};

/**
* @typedef {Object} WooMeta
* @property {string} key
* @property {string|number} value
*
* @typedef {Object} WooUpdate
* @property {number} id
* @property {string} part_number
* @property {string} [sku]
* @property {string} [description]
* @property {WooMeta[]} meta_data
*/

/**
/**
* @function createNewData
* @description Builds the final Woo update object from a raw CSV row.
* @param {Object} item - Raw CSV row.
* @param {number} productId - Target Woo product ID.
* @param {string} part_number - Fallback part number if not in row.
* @param {Object} [currentData] - Current WooCommerce product data (for merging additional_key_information).
* @returns {WooUpdate}
* @behavior
* - If UPDATE_MODE=quantity → returns only quantity meta.
* - Else → maps known keys; adds datasheet (unless digikey), and composes
* `additional_key_information` from leftover fields (merging with existing).
*/
const createNewData = (item, productId, part_number, currentData = null) => {
  const updateMode = process.env.UPDATE_MODE || "full";
  const normalizedCsvRow = normalizeCsvHeaders(item);
  const row = applyAliases(normalizedCsvRow);

  const description =
  row.detail_description ||
  row.short_description ||
  row.part_description ||  // already mapped from product_description
  ""; 

  if (updateMode === "quantity") {
    return {
        id: productId,
        part_number: row.part_number || part_number,
        manufacturer: row.manufacturer || "",
        meta_data: [{
        key: "quantity",
        value: row.quantity || row.quantity_available || "0"
        }],
    };
    }

  // 1) Map CSV → meta_data known keys
  const metaDataKeyMap = {
    manufacturer: "manufacturer",
    leadtime: "manufacturer_lead_weeks",
    image_url: "image_url",
    series: "series",

    // Quantity – support both old and new
    quantity_available: "quantity",
    quantity: "quantity",

    operating_temperature: "operating_temperature",

    // Voltage – support both old and new
    voltage_supply: "voltage",
    voltage: "voltage",

    // Packaging / package
    package_case: "package",
    packaging: "packaging",

    supplier_device_package: "supplier_device_package",
    mounting_type: "mounting_type",
    short_description: "short_description",
    part_description: "detail_description",

    // Compliance / statuses – support both old and new
    reachstatus: "reach_status",
    reach_status: "reach_status",
    rohsstatus: "rohs_status",
    rohs_status: "rohs_status",

    moisturesensitivitylevel: "moisture_sensitivity_level",
    moisture_sensitivity_level: "moisture_sensitivity_level",

    exportcontrolclassnumber: "export_control_class_number",
    export_control_class_number: "export_control_class_number",

    htsuscode: "htsus_code",
    htsus_code: "htsus_code",

    // Basic Product Info
    manufacturer_lead_weeks: "manufacturer_lead_weeks",

    // Document & Media
    pcn_design_specification: "pcn_design_specification",
    pcn_design: "pcn_design_specification",
    pcn_assembly_origin: "pcn_assembly_origin",
    pcn_assembly: "pcn_assembly_origin",
    pcn_packaging: "pcn_packaging",
    html_datasheet: "html_datasheet",
    eda_models: "eda_models",

    // Environmental Info (general)
    environmental_information: "environmental_information",
    environmental_info: "environmental_information",
    };

  const productMetaData = Object.keys(metaDataKeyMap)
    .filter((csvKey) => Object.prototype.hasOwnProperty.call(row, csvKey))
    .map((csvKey) => ({ key: metaDataKeyMap[csvKey], value: row[csvKey] || "" }));

  // NOTE: Image URL protection is now handled in build-update-payload.js
  // The buildUpdatePayload() function applies field-specific rules including
  // cross-environment protection for image_url, datasheet, etc.

  // 2) Datasheet special handling (skip digikey sources)
  if (Object.prototype.hasOwnProperty.call(row, "datasheet")) {
    const url = row["datasheet"] || "";
    if (!String(url).toLowerCase().includes("digikey")) {
      productMetaData.push({ key: "datasheet", value: url });
      productMetaData.push({ key: "datasheet_url", value: url });
    }
  }

  // 3) additional_key_information: prefer existing field; otherwise compose
  let additionalInfo = row["additional_info"] || "";
  if (!additionalInfo) {
    // Helper to check if a value looks like a URL
    const isUrlValue = (val) => {
      if (typeof val !== "string") return false;
      const trimmed = val.trim().toLowerCase();
      return (
        trimmed.startsWith("http://") ||
        trimmed.startsWith("https://") ||
        trimmed.startsWith("https") ||  // catch malformed URLs like "https//..."
        trimmed.startsWith("www.") ||
        /^[a-z0-9.-]+\.[a-z]{2,}(\/|$)/i.test(trimmed) // matches domain-like patterns
      );
    };

    // Helper to check if a key is price-related
    const isPriceRelatedKey = (key) => {
      const lowerKey = key.toLowerCase();
      return (
        lowerKey.includes("price") ||
        lowerKey.includes("cost") ||
        lowerKey.includes("msrp") ||
        /^price_?\d*$/.test(lowerKey) ||  // matches price, price_1, price1, etc.
        /^unit_?price/.test(lowerKey)
      );
    };

    // Helper to check if a key is quantity-related
    const isQuantityRelatedKey = (key) => {
      const lowerKey = key.toLowerCase();
      return (
        lowerKey.includes("quantity") ||
        lowerKey.includes("qty") ||
        /^quantity_?\d*$/.test(lowerKey) ||  // matches quantity, quantity_1, etc.
        lowerKey.includes("order_quantity") ||
        lowerKey.includes("minimum_order") ||
        lowerKey.includes("multiple_order")
      );
    };

    // Helper to check if a key is stock-related
    const isStockRelatedKey = (key) => {
      const lowerKey = key.toLowerCase();
      return (
        lowerKey.includes("stock") ||
        lowerKey.includes("on_hand") ||
        lowerKey.includes("inventory")
      );
    };

    // Helper to check if a key is status-related (we have manufacturer_status elsewhere)
    const isStatusRelatedKey = (key) => {
      const lowerKey = key.toLowerCase();
      return (
        lowerKey.includes("status") ||
        lowerKey.includes("part_status") ||
        lowerKey.includes("product_status")
      );
    };

    // Helper to check if a key is rohs-related (already mapped elsewhere)
    const isRohsRelatedKey = (key) => {
      const lowerKey = key.toLowerCase();
      return (
        lowerKey.includes("rohs") ||
        lowerKey.includes("reach")
      );
    };

    // Helper to check if a key is currency-related
    const isCurrencyRelatedKey = (key) => {
      const lowerKey = key.toLowerCase();
      return lowerKey.includes("currency");
    };

    // Helper to check if a key is region-related
    const isRegionRelatedKey = (key) => {
      const lowerKey = key.toLowerCase();
      return lowerKey.includes("region");
    };

    // Helper to check if a key contains URL-related keywords
    const isUrlRelatedKey = (key) => {
      const lowerKey = key.toLowerCase();
      return (
        lowerKey.includes("url") ||
        lowerKey.includes("link") ||
        lowerKey.includes("_uri") ||
        lowerKey === "https" ||
        lowerKey === "http"
      );
    };

    Object.keys(row).forEach((key) => {
      // Only include keys not already mapped and not explicitly excluded
      if (!metaDataKeyMap[key] && key !== "datasheet" && key !== "part_number" && key !== "additional_info") {
        const value = row[key] || "";
        if (value !== "" && value !== "NaN") {
          // Skip price-related fields (we don't show price to customers)
          if (isPriceRelatedKey(key)) return;

          // Skip quantity-related fields (already handled in dedicated quantity field)
          if (isQuantityRelatedKey(key)) return;

          // Skip stock-related fields (already handled elsewhere)
          if (isStockRelatedKey(key)) return;

          // Skip status-related fields (we have manufacturer_status elsewhere)
          if (isStatusRelatedKey(key)) return;

          // Skip rohs/reach-related fields (already mapped to dedicated fields)
          if (isRohsRelatedKey(key)) return;

          // Skip currency-related fields
          if (isCurrencyRelatedKey(key)) return;

          // Skip region-related fields
          if (isRegionRelatedKey(key)) return;

          // Skip fields with URL-related keywords in key name
          if (isUrlRelatedKey(key)) return;

          // Skip fields where the value is a URL (don't display URL text)
          if (isUrlValue(value)) return;

          const formattedKey = formatAcfFieldName(key);
          const excluded = new Set([
            // Keep this curated: items you never want duplicated into additional info.
            "Part Title", "Category", "Product Status", "RF Type", "Topology", "Circuit",
            "Frequency Range", "Isolation", "Insertion Loss", "Test Frequency", "P1dB",
            "IIP3", "Features", "Impedance", "Voltage – Supply", "Operating Temperature",
            "Mounting Type", "Package / Case", "Supplier Device Package",
          ]);
          if (!excluded.has(formattedKey)) {
            additionalInfo += `<strong>${formattedKey}:</strong> ${value}<br>`;
          }
        }
      }
    });
  }

  // 4) Merge new additional_key_information with existing (if currentData provided)
  // This preserves existing key-value pairs and only adds new ones
  let finalAdditionalInfo = additionalInfo || "";
  if (currentData && Array.isArray(currentData.meta_data)) {
    const existingMeta = currentData.meta_data.find(
      (m) => m && m.key === "additional_key_information"
    );
    const existingValue = existingMeta?.value || "";
    if (existingValue) {
      // Merge: preserve existing, add only new keys
      finalAdditionalInfo = mergeAdditionalKeyInfo(existingValue, additionalInfo);
    }
  }

  // 5) Final shape for bulk endpoint
  return {
    id: productId,
    part_number: row.part_number || part_number,
    sku:
      row.sku ||
      `${row.part_number || part_number || ""}_${row.manufacturer || ""}`.replace(/^_+|_+$/g, "") ||
      row.part_number ||
      part_number,
    description: description,
    meta_data: dedupeMetaData([
      ...productMetaData,
      { key: "additional_key_information", value: finalAdditionalInfo || "" },
    ]),
  };
};

module.exports = { createNewData, normalizeCsvHeaders, formatAcfFieldName, parseAdditionalKeyInfo, mergeAdditionalKeyInfo };
