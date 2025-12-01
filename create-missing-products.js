/**
 * create-missing-products.js
 *
 * PURPOSE (high-level overview):
 * --------------------------------
 * During a normal CSV update run, some rows cannot be matched to existing
 * WooCommerce products (for example: a part number doesn't exist yet).
 *
 * Those rows are captured as "missing products" and written to JSON files.
 *
 * This script:
 *   1) Reads those "missing" JSON files.
 *   2) For each missing product row:
 *        - Figures out which category hierarchy it should belong to
 *          (using fuzzy category resolution).
 *        - Ensures those categories exist in WooCommerce (create if needed).
 *        - Builds a WooCommerce "create product" payload.
 *        - Calls the WooCommerce REST API to create the product.
 *   3) Logs success or failure for each product.
 *
 * Typical usage (from CLI):
 *
 *   node create-missing-products.js <categorySlug> <fileKey>
 *
 * Example:
 *   node create-missing-products.js microcontrollers product-microcontrollers-03112025_part4.csv
 */

const fs = require("fs");
const path = require("path");

const { wooApi } = require("./woo-helpers");
const { logInfoToFile, logErrorToFile } = require("./logger");

// Smart category resolver
const { resolveCategorySmart } = require("./category-resolver");
const { resolveManufacturerSmart } = require("./manufacturer-resolver");
const { ensureCategoryHierarchyIds } = require("./category-woo");

// Execution mode
const EXECUTION_MODE = process.env.EXECUTION_MODE || "production";

/**
 * getCleanFileKey(fileKey)
 * Normalize file key: remove .csv extension and replace slashes with underscores
 */
function getCleanFileKey(fileKey) {
  return fileKey.replace(/\.csv$/i, "").replace(/\//g, "_");
}

/**
 * FIELD_ALIASES - Maps CSV column names to ACF field names
 * 
 * Keys are lowercase with spaces converted to underscores.
 * Values are the canonical ACF field names.
 */
const FIELD_ALIASES = {
  // ========== PART NUMBER VARIANTS ==========
  "manufacturer_part_number": "part_number",
  "mfr_part_number": "part_number",
  "mpn": "part_number",
  "partnumber": "part_number",
  "part_no": "part_number",
  "part#": "part_number",
  "part_#": "part_number",
  "pn": "part_number",
  "sku": "part_number",  // Some vendors use SKU as part number
  
  // ========== MANUFACTURER VARIANTS ==========
  "mfr": "manufacturer",
  "mfg": "manufacturer",
  "brand": "manufacturer",
  "vendor": "manufacturer",
  "supplier": "manufacturer",
  "make": "manufacturer",
  
  // ========== CATEGORY VARIANTS ==========
  "cat": "category",
  "product_category": "category",
  "categorypath": "category",
  "category_path": "category",
  "categories": "category",
  "prod_category": "category",
  
  // ========== DESCRIPTION VARIANTS ==========
  "product_description": "part_description",
  "description": "part_description",
  "desc": "part_description",
  "part_desc": "part_description",
  "short_product_description": "short_description",
  "brief_description": "short_description",
  "summary": "short_description",
  "detailed_product_description": "detail_description",
  "long_description": "detail_description",
  "full_description": "detail_description",
  "extended_description": "detail_description",
  
  // ========== QUANTITY VARIANTS ==========
  "stock_quantity": "quantity",
  "quantity_available": "quantity",
  "qty": "quantity",
  "stock": "quantity",
  "available_qty": "quantity",
  "avail_qty": "quantity",
  "inventory": "quantity",
  "in_stock": "quantity",
  "qty_available": "quantity",
  
  // ========== COMPLIANCE FIELDS ==========
  // RoHS variants
  "rohs_compliance": "rohs_status",
  "rohs": "rohs_status",
  "rohscompliant": "rohs_status",
  "rohs_compliant": "rohs_status",
  "rohsstatus": "rohs_status",
  "rohs_certified": "rohs_status",
  
  // REACH variants
  "reach_compliance": "reach_status",
  "reach": "reach_status",
  "reachstatus": "reach_status",
  "reach_compliant": "reach_status",
  "reachcompliant": "reach_status",
  
  // ========== EXPORT/CUSTOMS FIELDS ==========
  // HTS/HTSUS variants
  "hts_code": "htsus_code",
  "hts": "htsus_code",
  "htsuscode": "htsus_code",
  "tariff_code": "htsus_code",
  "harmonized_code": "htsus_code",
  "hs_code": "htsus_code",
  "customs_code": "htsus_code",
  
  // ECCN variants
  "eccn": "export_control_class_number",
  "eccn_code": "export_control_class_number",
  "exportcontrolclassnumber": "export_control_class_number",
  "export_class": "export_control_class_number",
  "export_control": "export_control_class_number",
  
  // ========== URL FIELDS (including wildcard variants) ==========
  // Datasheet variants
  "datasheet_url": "datasheet",
  "datasheet_url*": "datasheet",
  "datasheeturl": "datasheet",
  "datasheeturl*": "datasheet",
  "pdf_url": "datasheet",
  "spec_sheet": "datasheet",
  "specsheet": "datasheet",
  "spec_url": "datasheet",
  "documentation_url": "datasheet",
  "doc_url": "datasheet",
  
  // Image variants
  "image_attachment_url": "image_url",
  "imageurl": "image_url",
  "product_image": "image_url",
  "image": "image_url",
  "photo_url": "image_url",
  "picture_url": "image_url",
  "img_url": "image_url",
  "thumbnail": "image_url",
  "product_image_url": "image_url",
  
  // ========== PHYSICAL SPECS ==========
  // Dimensions variants
  "size_/_dimension": "dimensions",
  "size_/_dimensions": "dimensions",
  "size": "dimensions",
  "package_size": "dimensions",
  "dim": "dimensions",
  "dimension": "dimensions",
  "physical_dimensions": "dimensions",
  "case_size": "dimensions",
  
  // Voltage variants
  "voltage_-_input_(max)": "voltage",
  "voltage_-_input": "voltage",
  "voltage_max": "voltage",
  "operating_voltage": "voltage",
  "supply_voltage": "voltage",
  "voltage___supply": "voltage",
  "input_voltage": "voltage",
  "vcc": "voltage",
  "v_supply": "voltage",
  
  // Capacitance variants
  "capacitance_@_frequency": "capacitance",
  "capacitance": "capacitance",
  "cap_value": "capacitance",
  "cap": "capacitance",
  
  // Temperature variants
  "operating_temperature": "operating_temperature",
  "temp_range": "operating_temperature",
  "temperature": "operating_temperature",
  "temp": "operating_temperature",
  "operating_temp": "operating_temperature",
  "op_temp": "operating_temperature",
  "temperature_range": "operating_temperature",
  
  // MSL variants
  "moisture_sensitivity_level": "moisture_sensitivity_level",
  "msl": "moisture_sensitivity_level",
  "moisturesensitivitylevel": "moisture_sensitivity_level",
  "moisture_level": "moisture_sensitivity_level",
  
  // Mounting variants
  "mounting_type": "mounting_type",
  "mount_type": "mounting_type",
  "mount": "mounting_type",
  "mounting": "mounting_type",
  "mounting_style": "mounting_type",
  
  // Package variants
  "package": "packaging",
  "package_type": "packaging",
  "packaging": "packaging",
  "pkg": "packaging",
  "case_package": "packaging",
  "package___case": "packaging",
  
  // Termination variants
  "termination_style": "termination_style",
  "termination": "termination_style",
  "lead_style": "termination_style",
  "lead_type": "termination_style",
  "terminal_type": "termination_style",
  
  // ========== OTHER FIELDS ==========
  // Series variants
  "series": "series",
  "product_series": "series",
  "product_line": "series",
  "family": "series",
  
  // Title variants
  "part_title": "part_title",
  "product_name": "part_title",
  "title": "part_title",
  "name": "part_title",
  "product_title": "part_title",
  
  // Lead time variants
  "leadtime": "manufacturer_lead_weeks",
  "lead_time": "manufacturer_lead_weeks",
  "lead_weeks": "manufacturer_lead_weeks",
  
  // SPQ variants
  "spq": "spq",
  "standard_package_qty": "spq",
  "standard_pack_qty": "spq",
  "min_qty": "spq",
  
  // Supplier device package
  "supplier_device_package": "supplier_device_package",
  "supplier_package": "supplier_device_package",
};

/**
 * normalizeProductData(productData)
 * 
 * Normalizes field names from CSV format to ACF format.
 * 
 * Steps:
 * 1. Remove asterisks from column names (e.g., "Datasheet URL*" → "Datasheet URL")
 * 2. Convert to lowercase
 * 3. Replace spaces with underscores
 * 4. Apply field aliases to get canonical ACF names
 * 
 * @param {Object} productData - Raw CSV row object
 * @returns {Object} - Normalized object with ACF field names
 */
function normalizeProductData(productData) {
  const normalized = {};
  
  for (const [key, value] of Object.entries(productData)) {
    // Skip null/undefined keys
    if (!key) continue;
    
    // Clean the key:
    // 1. Remove asterisks (wildcards in CSV headers)
    // 2. Trim whitespace
    // 3. Convert to lowercase
    // 4. Replace spaces and special chars with underscores
    let cleanKey = key
      .replace(/\*/g, "")        // Remove asterisks
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "_")      // Spaces to underscores
      .replace(/[()]/g, "")      // Remove parentheses
      .replace(/-/g, "_")        // Dashes to underscores
      .replace(/__+/g, "_");     // Multiple underscores to single
    
    // Apply alias to get canonical ACF field name
    const acfKey = FIELD_ALIASES[cleanKey] || cleanKey;
    
    // Clean the value
    let cleanValue = value;
    if (typeof cleanValue === "string") {
      cleanValue = cleanValue.trim();
      // Skip empty, null-like values
      if (cleanValue === "" || cleanValue === "N/A" || cleanValue === "-" || cleanValue === "—") {
        cleanValue = "";
      }
    }
    
    normalized[acfKey] = cleanValue;
  }
  
  return normalized;
}

/**
 * buildMetaData(data)
 * 
 * Builds the meta_data array for WooCommerce from normalized data.
 * Only includes fields that have non-empty values.
 * 
 * @param {Object} data - Normalized product data
 * @param {string} partNumber - Part number (already extracted)
 * @param {string} manufacturer - Manufacturer (already resolved)
 * @param {string} additionalInfo - Additional key information HTML
 * @returns {Array} - WooCommerce meta_data array
 */
function buildMetaData(data, partNumber, manufacturer, additionalInfo) {
  const metaFields = [
    { key: "part_number", value: partNumber },
    { key: "manufacturer", value: manufacturer },
    { key: "series", value: data.series || "" },
    { key: "quantity", value: data.quantity || "0" },
    { key: "short_description", value: data.short_description || data.part_description || "" },
    { key: "detail_description", value: data.detail_description || data.part_description || "" },
    { key: "reach_status", value: data.reach_status || "" },
    { key: "rohs_status", value: data.rohs_status || "" },
    { key: "moisture_sensitivity_level", value: data.moisture_sensitivity_level || "" },
    { key: "export_control_class_number", value: data.export_control_class_number || "" },
    { key: "htsus_code", value: data.htsus_code || "" },
    { key: "datasheet", value: data.datasheet || "" },
    { key: "datasheet_url", value: data.datasheet || "" },
    { key: "image_url", value: data.image_url || "" },
    { key: "operating_temperature", value: data.operating_temperature || "" },
    { key: "voltage", value: data.voltage || "" },
    { key: "dimensions", value: data.dimensions || "" },
    { key: "mounting_type", value: data.mounting_type || data.termination_style || "" },
    { key: "packaging", value: data.packaging || "" },
    { key: "capacitance", value: data.capacitance || "" },
    { key: "spq", value: data.spq || "" },
    { key: "manufacturer_lead_weeks", value: data.manufacturer_lead_weeks || "" },
    { key: "supplier_device_package", value: data.supplier_device_package || "" },
    { key: "additional_key_information", value: additionalInfo || "" },
  ]; 
  // Filter out empty values (optional - keeps payload smaller)
  // But keep manufacturer and part_number even if empty
  return metaFields.filter(field => 
    field.value !== "" || 
    field.key === "part_number" || 
    field.key === "manufacturer"
  );
}

/**
 * DEBUG: Log field mapping for troubleshooting
 * 
 * Call this to see how CSV columns are being mapped:
 * 
 * debugFieldMapping(rawProductData);
 */
function debugFieldMapping(productData) {
  console.log("\n========== FIELD MAPPING DEBUG ==========");
  console.log("Raw CSV columns → ACF fields:");
  console.log("----------------------------------------");
  
  for (const [key, value] of Object.entries(productData)) {
    let cleanKey = key
      .replace(/\*/g, "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "_")
      .replace(/[()]/g, "")
      .replace(/-/g, "_")
      .replace(/__+/g, "_");
    
    const acfKey = FIELD_ALIASES[cleanKey] || cleanKey;
    const mapped = FIELD_ALIASES[cleanKey] ? "✅" : "⚠️ ";
    
    console.log(`${mapped} "${key}" → "${cleanKey}" → "${acfKey}"`);
    if (value) {
      const preview = String(value).substring(0, 50);
      console.log(`   Value: ${preview}${value.length > 50 ? "..." : ""}`);
    }
  }
  
  console.log("==========================================\n");
}

/**
 * parseCategoryPath(rawCategory)
 * Fallback parser for "A>B>C" category strings
 */
function parseCategoryPath(rawCategory) {
  if (!rawCategory) return null;

  const parts = String(rawCategory)
    .split(">")
    .map((p) => p.trim())
    .filter(Boolean);

  if (!parts.length) return null;

  return {
    main: parts[0] || null,
    sub: parts[1] || null,
    sub2: parts[2] || null,
    score: 1,
    matchedOn: parts.length === 1 ? "main" : parts.length === 2 ? "sub" : "sub2",
  };
}

/**
 * buildCategoryPath(resolvedCategory)
 * Build a human-readable string from a resolved category object
 */
function buildCategoryPath(resolvedCategory) {
  if (!resolvedCategory) return "";
  const parts = [
    resolvedCategory.main,
    resolvedCategory.sub,
    resolvedCategory.sub2,
  ].filter(Boolean);
  return parts.join(" > ");
}

/**
 * buildAdditionalKeyInfo(data, excludeKeys)
 * Build additional_key_information from unmapped fields
 */
function buildAdditionalKeyInfo(data, excludeKeys) {
  const lines = [];
  for (const [key, value] of Object.entries(data)) {
    if (excludeKeys.includes(key) || !value) continue;
    // Format key nicely: replace underscores with spaces, capitalize
    const label = key.replace(/_/g, " ").replace(/\b\w/g, (l) => l.toUpperCase());
    lines.push(`<strong>${label}:</strong> ${value}`);
  }
  return lines.join("<br/>");
}

/**
 * processMissingProducts(categorySlug, fileKey)
 * Main entry function for processing missing products
 */
const processMissingProducts = async (categorySlug, fileKey) => {
  try {
    // 1) Locate the JSON file
    const cleanFileKey = getCleanFileKey(fileKey);
    const missingFilePath = path.join(
      __dirname,
      "missing-products",
      `missing-${categorySlug}`,
      `missing_products_${cleanFileKey}.json`
    );

    if (!fs.existsSync(missingFilePath)) {
      logInfoToFile(
        `[create-missing-products] No missing products file found for ${fileKey}, skipping.`
      );
      return;
    }

    // 2) Load and parse the JSON file
    let missingProducts = [];
    try {
      const rawJson = fs.readFileSync(missingFilePath, "utf8");
      missingProducts = JSON.parse(rawJson);
    } catch (err) {
      logErrorToFile(
        `[create-missing-products] ❌ Error reading missing products file "${missingFilePath}": ${err.message}`
      );
      return;
    }

    logInfoToFile(
      `[create-missing-products] 🚀 Processing ${missingProducts.length} missing products from ${missingFilePath}`
    );

    // 3) Loop through each missing product row
    for (const productData of missingProducts) {
      try {
        // Normalize field names from CSV to ACF format
        const data = normalizeProductData(productData);
        
        const partNumber = data.part_number || "";
        const rawManufacturer = data.manufacturer || "";
        const manufacturerResolved = resolveManufacturerSmart(rawManufacturer);
        const canonicalManufacturer = manufacturerResolved?.canonical || rawManufacturer || "";
        const rawCategory = data.category || "";

        // 3a) Resolve category hierarchy
        let resolvedCategory = null;
        if (rawCategory) {
          try {
            resolvedCategory = await resolveCategorySmart(rawCategory);
          } catch (err) {
            logErrorToFile(
              `[create-missing-products] Category resolve error for part_number=${partNumber}: ${err.message}`
            );
          }

          if (!resolvedCategory) {
            resolvedCategory = parseCategoryPath(rawCategory);
          }

          if (resolvedCategory) {
            const pathStr = buildCategoryPath(resolvedCategory);
            logInfoToFile(
              `[create-missing-products] Category for part_number=${partNumber}: "${rawCategory}" → "${pathStr}"`
            );
          } else {
            logInfoToFile(
              `[create-missing-products] No category resolved for part_number=${partNumber}, rawCategory="${rawCategory}"`
            );
          }
        }

        // 3b) Ensure categories exist in WooCommerce
        let categoryIds = [];
        if (resolvedCategory) {
          categoryIds = await ensureCategoryHierarchyIds(resolvedCategory);
        }

        // 3c) Build additional_key_information from unmapped fields
        const excludeKeys = [
          "part_number", "manufacturer", "category", "short_description",
          "part_description", "detail_description", "datasheet", "image_url",
          "rohs_status", "reach_status", "htsus_code", "export_control_class_number",
          "moisture_sensitivity_level", "quantity", "series"
        ];
        const additionalInfo = buildAdditionalKeyInfo(data, excludeKeys);

        // DEBUG: See exactly how fields are being mapped
        console.log("=== RAW productData ===");
        console.log(Object.keys(productData).slice(0, 10));
        console.log("=== NORMALIZED data ===");  
        console.log(JSON.stringify(data, null, 2).substring(0, 500));

       // 3d) Build the WooCommerce "create product" payload
        const newProduct = {
          name: data.part_title || data.part_number || partNumber,
          sku: data.sku || data.part_number || partNumber,
          description: data.detail_description || data.part_description || "",
          short_description: data.short_description || data.part_description || "",
          categories: categoryIds.map((id) => ({ id })),
          
          // USE the buildMetaData() function instead of manual construction!
          meta_data: buildMetaData(data, partNumber, canonicalManufacturer, additionalInfo),
        };

        // Add category path meta for debugging
        if (resolvedCategory) {
          const pathStr = buildCategoryPath(resolvedCategory);
          newProduct.meta_data.push(
            { key: "proposed_category_main", value: resolvedCategory.main || "" },
            { key: "proposed_category_sub", value: resolvedCategory.sub || "" },
            { key: "proposed_category_sub2", value: resolvedCategory.sub2 || "" },
            { key: "proposed_category_path", value: pathStr }
          );
        }

        // 3e) Actually create the product in WooCommerce
        if (EXECUTION_MODE === "test") {
          logInfoToFile(
            `[create-missing-products] (TEST MODE) Would create product part_number=${partNumber} with categories: [${categoryIds.join(", ")}]`
          );
          // continue;
        }

        const response = await wooApi.post("products", newProduct);

        logInfoToFile(
          `[create-missing-products] ✅ Created product part_number=${partNumber} (id=${response.data.id}) with categories: [${categoryIds.join(", ")}]`
        );
      } catch (error) {
        const status = error.response?.status;
        const errData = error.response?.data;

        logErrorToFile(
          `[create-missing-products] ❌ Error creating product for part_number=${productData.part_number || "unknown"}: status=${status || "n/a"}, message=${errData?.message || error.message}`
        );
      }
    }
  } catch (outerErr) {
    logErrorToFile(
      `[create-missing-products] ❌ Fatal error in processMissingProducts(${fileKey}): ${outerErr.message}`
    );
  }
};

module.exports = {
  FIELD_ALIASES,
  normalizeProductData,
  buildMetaData,
  debugFieldMapping,
  processMissingProducts
};

/**
 * CLI entry point
 */
if (require.main === module) {
  const categorySlug = process.argv[2];
  const fileKey = process.argv[3];

  if (!categorySlug || !fileKey) {
    console.error(
      "[create-missing-products] ❌ Usage: node create-missing-products.js <categorySlug> <fileKey>"
    );
    console.error(
      "Example: node create-missing-products.js microcontrollers product-microcontrollers-03112025_part4.csv"
    );
    process.exit(1);
  }

  (async () => {
    try {
      console.log(
        `[create-missing-products] ▶︎ Starting processMissingProducts("${categorySlug}", "${fileKey}")...`
      );
      await processMissingProducts(categorySlug, fileKey);
      console.log(
        `[create-missing-products] ✅ Finished processMissingProducts("${categorySlug}", "${fileKey}")`
      );
      process.exit(0);
    } catch (err) {
      console.error(
        `[create-missing-products] ❌ Uncaught error in CLI runner:`,
        err
      );
      process.exit(1);
    }
  })();
}
