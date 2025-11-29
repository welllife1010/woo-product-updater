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
 * Field name aliases to normalize CSV column names to ACF field names
 */
const FIELD_ALIASES = {
  // Part number variants
  "manufacturer_part_number": "part_number",
  "mfr_part_number": "part_number",
  
  // Description variants
  "product_description": "part_description",
  "short_product_description": "short_description",
  "detailed_product_description": "detail_description",
  
  // Quantity variants
  "stock_quantity": "quantity",
  "quantity_available": "quantity",
  
  // Compliance fields
  "rohs_compliance": "rohs_status",
  "reach_compliance": "reach_status",
  "hts_code": "htsus_code",
  "eccn": "export_control_class_number",
  
  // URL fields
  "datasheet_url": "datasheet",
  "image_attachment_url": "image_url",
  
  // Other spec fields
  "size_/_dimension": "dimensions",
  "voltage_-_input_(max)": "voltage",
  "capacitance_@_frequency": "capacitance",
  
  // Manufacturer variants
  "mfr": "manufacturer",
  "cat": "category",
};

/**
 * normalizeProductData(productData)
 * Normalize field names from CSV format to ACF format
 */
function normalizeProductData(productData) {
  const normalized = {};
  for (const [key, value] of Object.entries(productData)) {
    // Remove asterisks, trim, lowercase, replace spaces with underscores
    const cleanKey = key.replace(/\*/g, "").trim().toLowerCase().replace(/\s+/g, "_");
    const aliasKey = FIELD_ALIASES[cleanKey] || cleanKey;
    normalized[aliasKey] = value;
  }
  return normalized;
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

        // 3d) Build the WooCommerce "create product" payload
        const newProduct = {
          name: data.part_title || data.part_number || partNumber,
          sku: data.sku || data.part_number || partNumber,
          description: data.detail_description || data.part_description || "",
          short_description: data.short_description || data.part_description || "",
          categories: categoryIds.map((id) => ({ id })),
          
          meta_data: [
            { key: "part_number", value: partNumber },
            { key: "manufacturer", value: canonicalManufacturer },
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
            { key: "additional_key_information", value: additionalInfo },
          ],
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
          continue;
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

module.exports = { processMissingProducts };

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
