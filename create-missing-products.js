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
 *
 *   - categorySlug:
 *       The LEAF category slug (e.g. "microcontrollers") used in the folder name.
 *   - fileKey:
 *       The original CSV filename (or S3 key) used in the batch, e.g.:
 *         "product-microcontrollers-03112025_part4.csv"
 */

const fs = require("fs");
const path = require("path");

const { wooApi } = require("./woo-helpers");
const { logInfoToFile, logErrorToFile } = require("./logger");

// Smart category resolver:
// - First tries fuzzy match against EXISTING Woo categories.
// - If nothing found, falls back to CSV + fuzzy via category-map.js.
const { resolveCategorySmart } = require("./category-resolver");

const { resolveManufacturerSmart } = require("./manufacturer-resolver");

// Shared category creation helpers (canonical implementation):
// - ensureCategoryHierarchyIds({ main, sub, sub2 }) → [id1, id2, id3?]
const { ensureCategoryHierarchyIds } = require("./category-woo");

// Optional: execution mode (can be used later to skip "create" calls in tests)
const EXECUTION_MODE = process.env.EXECUTION_MODE || "production";

/**
 * getCleanFileKey(fileKey)
 *
 * GOAL:
 *   Normalize any kind of "file key" (local filename or S3 key) into a simple
 *   base name without extension. This is used to build a predictable JSON
 *   filename for the missing products file.
 *
 * EXAMPLES:
 *   - "product-microcontrollers-03112025_part4.csv"
 *       → "product-microcontrollers-03112025_part4"
 *
 *   - "vendor-x/ics/microcontrollers-part2.csv"
 *       → "microcontrollers-part2"
 *
 *   - "LED-Emitters-IR-UV-Visible.csv"
 *       → "LED-Emitters-IR-UV-Visible"
 *
 * HOW:
 *   - path.basename() removes folder prefixes.
 *   - regex replaces the final ".something" with "".
 */
function getCleanFileKey(fileKey) {
  const base = path.basename(fileKey);  // "folder/file.csv" → "file.csv"
  return base.replace(/\.[^.]+$/, "");  // remove last extension (".csv")
}

/**
 * parseCategoryPath(rawCategory)
 *
 * PURPOSE:
 *   Fallback parser when fuzzy resolution fails.
 *   Takes a raw category string like:
 *     "Integrated Circuits (ICs)>Embedded>Microcontrollers"
 *
 *   and turns it into:
 *     {
 *       main: "Integrated Circuits (ICs)",
 *       sub:  "Embedded",
 *       sub2: "Microcontrollers",
 *       score: 1,
 *       matchedOn: "sub2"   // we treat this as "full path provided"
 *     }
 *
 *   If there are fewer than 3 parts, the missing ones become null.
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
    matchedOn:
      parts.length === 1 ? "main" : parts.length === 2 ? "sub" : "sub2",
  };
}

/**
 * buildCategoryPath(resolvedCategory)
 *
 * PURPOSE:
 *   Build a human-readable string from a resolved category object.
 *
 * EXAMPLE:
 *   input: { main: "ICs", sub: "Embedded", sub2: "MCUs" }
 *   output: "ICs > Embedded > MCUs"
 *
 * Used primarily for logging and debugging.
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
 * processMissingProducts(categorySlug, fileKey)
 *
 * MAIN ENTRY FUNCTION (for other modules and CLI).
 *
 * RESPONSIBILITIES:
 *   1) Build the path to the missing-products JSON file using categorySlug + fileKey.
 *   2) Load and parse the JSON file.
 *   3) For each missing product:
 *        a. Determine the best category hierarchy:
 *           - Use resolveCategorySmart() (Woo fuzzy → CSV fuzzy).
 *           - Fallback to simple "A>B>C" parsing if needed.
 *        b. Ensure that hierarchy exists in WooCommerce:
 *           - Calls ensureCategoryHierarchyIds() to get a list of IDs.
 *        c. Build a WooCommerce "create product" payload.
 *        d. POST to WooCommerce REST API to actually create the product.
 *   4) Log results for success and error cases.
 *
 * PARAMETERS:
 *   @param {string} categorySlug
 *     - The leaf category slug used to store missing-product files by folder.
 *     - Example: "microcontrollers", "led-emitters-ir-uv-visible", "resistors".
 *
 *   @param {string} fileKey
 *     - The original CSV filename or key used in the batch run.
 *     - Example: "product-microcontrollers-03112025_part4.csv"
 *                "LED-Emitters-IR-UV-Visible.csv"
 *
 * RETURNS:
 *   @returns {Promise<void>}
 *     - Resolves when all rows have been processed (or skipped).
 *     - Errors are logged via logErrorToFile; this function itself
 *       does not throw in normal flow.
 */
const processMissingProducts = async (categorySlug, fileKey) => {
  try {
    // -------------------------------------------------------------------------
    // 1) Locate the JSON file for this categorySlug + fileKey
    // -------------------------------------------------------------------------
    const cleanFileKey = getCleanFileKey(fileKey);

    const missingFilePath = path.join(
      __dirname,
      "missing-products",
      `missing-${categorySlug}`,              // e.g. "missing-microcontrollers"
      `missing_products_${cleanFileKey}.json` // e.g. "missing_products_product-microcontrollers-03112025_part4.json"
    );

    // If there is no JSON file, there is simply nothing to do.
    if (!fs.existsSync(missingFilePath)) {
      logInfoToFile(
        `[create-missing-products] No missing products file found for ${fileKey}, skipping.`
      );
      return;
    }

    // -------------------------------------------------------------------------
    // 2) Load and parse the JSON file
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // 3) Loop through each missing product row
    // -------------------------------------------------------------------------
    for (const productData of missingProducts) {
      try {
        const partNumber = productData.part_number || "";
        const rawManufacturer =
          productData.manufacturer || productData.Manufacturer || "";
        const manufacturerResolved = resolveManufacturerSmart(rawManufacturer);
        const canonicalManufacturer =
          manufacturerResolved?.canonical || rawManufacturer || "";
        const rawCategory =
          productData.category || productData.Category || ""; // allow both cases

        // ---------------------------------------------------------------
        // 3a) Resolve category hierarchy (smart fuzzy + fallback)
        // ---------------------------------------------------------------
        let resolvedCategory = null;

        if (rawCategory) {
          try {
            // Smart category resolution:
            //   1) fuzzy match against existing Woo categories (website as truth)
            //   2) if not found, fuzzy against category-hierarchy-ref.csv
            resolvedCategory = await resolveCategorySmart(rawCategory);
          } catch (err) {
            logErrorToFile(
              `[create-missing-products] Category resolve error for part_number=${partNumber}: ${err.message}`
            );
          }

          // Fallback: if the smart resolver couldn't find anything,
          // try interpreting the raw string as a "A>B>C" path.
          if (!resolvedCategory) {
            resolvedCategory = parseCategoryPath(rawCategory);
          }

          if (resolvedCategory) {
            const pathStr = buildCategoryPath(resolvedCategory);
            logInfoToFile(
              `[create-missing-products] Category for part_number=${partNumber}: ` +
                `"${rawCategory}" → "${pathStr}" ` +
                `(matchedOn=${resolvedCategory.matchedOn || "n/a"})`
            );
          } else {
            logInfoToFile(
              `[create-missing-products] No category could be resolved for part_number=${partNumber}, rawCategory="${rawCategory}"`
            );
          }
        } else {
          logInfoToFile(
            `[create-missing-products] No category field present for part_number=${partNumber}`
          );
        }

        // ---------------------------------------------------------------
        // 3b) Ensure those categories exist in WooCommerce
        // ---------------------------------------------------------------
        let categoryIds = [];
        if (resolvedCategory) {
          // This calls into the shared category-woo.js helper.
          // It will:
          //   - find or create main/sub/sub2
          //   - return an array of IDs in hierarchical order.
          categoryIds = await ensureCategoryHierarchyIds(resolvedCategory);
        }

        // ---------------------------------------------------------------
        // 3c) Build the WooCommerce "create product" payload
        // ---------------------------------------------------------------
        const newProduct = {
          // Human-friendly product title:
          //   - use part_title if available
          //   - otherwise fall back to part_number
          name: productData.part_title || productData.part_number,

          // SKU:
          //   - if vendor provides a dedicated 'sku', use that
          //   - else use part_number as SKU
          sku: productData.sku || productData.part_number,

          // Long description (HTML is okay here, Woo handles it)
          description: productData.part_description || "",

          // Category assignment: list of { id }
          categories: categoryIds.map((id) => ({ id })),

          // Custom fields (stored as meta_data for ACF or other custom logic)
          meta_data: [
            { key: "manufacturer", value: canonicalManufacturer },

            // Keep existing database-style fields as ACF meta
            { key: "series", value: productData.series || "" },
            { key: "quantity", value: productData.quantity_available || "0" },
            {
              key: "short_description",
              value: productData.short_description || "",
            },
            {
              key: "detail_description",
              value: productData.part_description || "",
            },
            { key: "reach_status", value: productData.reachstatus || "" },
            { key: "rohs_status", value: productData.rohsstatus || "" },
            {
              key: "moisture_sensitivity_level",
              value: productData.moisturesensitivitylevel || "",
            },
            {
              key: "export_control_class_number",
              value: productData.exportcontrolclassnumber || "",
            },
            { key: "htsus_code", value: productData.htsuscode || "" },
            {
              key: "additional_key_information",
              value: productData.additional_info || "",
            },
          ],
        };

        // Optional: also store the resolved category path as meta for debugging
        if (resolvedCategory) {
          const pathStr = buildCategoryPath(resolvedCategory);
          newProduct.meta_data.push(
            {
              key: "proposed_category_main",
              value: resolvedCategory.main || "",
            },
            {
              key: "proposed_category_sub",
              value: resolvedCategory.sub || "",
            },
            {
              key: "proposed_category_sub2",
              value: resolvedCategory.sub2 || "",
            },
            {
              key: "proposed_category_path",
              value: pathStr,
            }
          );
        }

        // ---------------------------------------------------------------
        // 3d) Actually create the product in WooCommerce
        // ---------------------------------------------------------------
        if (EXECUTION_MODE === "test") {
          // In test mode, we can log the payload instead of creating real products.
          logInfoToFile(
            `[create-missing-products] (TEST MODE) Would create product part_number=${partNumber} with categories: [${categoryIds.join(
              ", "
            )}]`
          );
          continue; // skip the actual API call
        }

        const response = await wooApi.post("products", newProduct);

        logInfoToFile(
          `[create-missing-products] ✅ Created product part_number=${partNumber} (id=${
            response.data.id
          }) with categories: [${categoryIds.join(", ")}]`
        );
      } catch (error) {
        // Handle errors that occur *for this single product*
        const status = error.response?.status;
        const data = error.response?.data;

        logErrorToFile(
          `[create-missing-products] ❌ Error creating product for part_number=${
            productData.part_number
          }: status=${status || "n/a"}, message=${
            data?.message || error.message
          }, data=${JSON.stringify(data || {}, null, 2)}`
        );
      }
    }
  } catch (outerErr) {
    // Catch any fatal errors (e.g. file-level problems)
    logErrorToFile(
      `[create-missing-products] ❌ Fatal error in processMissingProducts(${fileKey}): ${outerErr.message}`
    );
  }
};

module.exports = { processMissingProducts };

/**
 * If this file is run directly via:
 *
 *   node create-missing-products.js <categorySlug> <fileKey>
 *
 * It will call processMissingProducts() with those arguments.
 */
if (require.main === module) {
  const categorySlug = process.argv[2]; // e.g. "microcontrollers"
  const fileKey = process.argv[3];      // e.g. "product-microcontrollers-03112025_part4.csv"

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
