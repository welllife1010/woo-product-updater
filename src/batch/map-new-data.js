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
  "moisture_sensitivity_level": "moisture_sensitivity_level"
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
* @description Produces a case/space-insensitive row so upstream CSV idiosyncrasies
* don't propagate (e.g., "Voltage / Supply" → "voltage___supply").
*/
const normalizeCsvHeaders = (item) => {
  const out = {};
  Object.keys(item).forEach((key) => {
    const normalizedKey = key.trim().toLowerCase().replace(/\s+/g, "_");
    out[normalizedKey] = item[key];
  });
  return out;
};

/**
* @function formatAcfFieldName
* @description Presentational helper to turn underscored keys into a readable label
* for inclusion inside additional_key_information HTML.
*/
const formatAcfFieldName = (name) =>
  name.replace(/_/g, " ").replace(/-/g, " ").replace(/\b\w/g, (l) => l.toUpperCase());

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
* @returns {WooUpdate}
* @behavior
* - If UPDATE_MODE=quantity → returns only quantity meta.
* - Else → maps known keys; adds datasheet (unless digikey), and composes
* `additional_key_information` from leftover fields.
*/
const createNewData = (item, productId, part_number) => {
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
      meta_data: [{ key: "quantity", value: row.quantity_available || "0" }],
    };
  }

  // 1) Map CSV → meta_data known keys
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
    htsuscode: "htsus_code",
  };

  const productMetaData = Object.keys(metaDataKeyMap)
    .filter((csvKey) => Object.prototype.hasOwnProperty.call(row, csvKey))
    .map((csvKey) => ({ key: metaDataKeyMap[csvKey], value: row[csvKey] || "" }));

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
    Object.keys(row).forEach((key) => {
      // Only include keys not already mapped and not explicitly excluded
      if (!metaDataKeyMap[key] && key !== "datasheet" && key !== "part_number" && key !== "additional_info") {
        const value = row[key] || "";
        if (value !== "" && value !== "NaN") {
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

  // 4) Final shape for bulk endpoint
  return {
    id: productId,
    part_number: row.part_number || part_number,
    sku: row.sku || `${row.part_number}_${row.manufacturer}` || row.part_number,
    description: description,
    meta_data: [...productMetaData, { key: "additional_key_information", value: additionalInfo || "" }],
  };
};

module.exports = { createNewData, normalizeCsvHeaders, formatAcfFieldName };
