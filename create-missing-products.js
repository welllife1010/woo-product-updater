/**
 * create-missing-products.js
 *
 * PURPOSE:
 *   - Read "missing product" JSON files produced by the updater.
 *   - For each missing product:
 *       1) Build a WooCommerce "create product" payload
 *       2) Derive a category hierarchy from productData.category
 *          (using fuzzy resolve + fallback to ">" split)
 *       3) Ensure those categories exist in WooCommerce (create if needed)
 *       4) Assign the new product to those categories
 */

const fs = require("fs");
const path = require("path");

const { wooApi } = require("./woo-helpers");
const { logInfoToFile, logErrorToFile } = require("./logger");
const { resolveCategory } = require("./category-map");

// -----------------------------------------------------------------------------
// Small helper: normalize a category name for comparison
// -----------------------------------------------------------------------------
function normalizeName(value) {
  if (!value) return "";

  return String(value)
    .replace(/\u00A0/g, " ") // non-breaking spaces → normal space
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");   // collapse multiple spaces
}

// -----------------------------------------------------------------------------
// In-memory cache of Woo product categories
// Each item: { id, name, parent, slug, ... }
// -----------------------------------------------------------------------------
let CATEGORY_CACHE = null;

/**
 * Load all WooCommerce product categories once, cache for this process.
 * This avoids repeated API calls when creating many missing products.
 */
async function loadAllCategories() {
  if (CATEGORY_CACHE) return CATEGORY_CACHE;

  const categories = [];
  let page = 1;
  const per_page = 100;

  while (true) {
    try {
      const res = await wooApi.get("products/categories", {
        per_page,
        page,
      });

      const data = res.data || [];
      if (!data.length) break;

      categories.push(...data);

      if (data.length < per_page) break; // last page
      page++;
    } catch (err) {
      logErrorToFile(
        `[create-missing-products] ❌ Failed to load categories (page=${page}): ${err.message}`
      );
      break;
    }
  }

  CATEGORY_CACHE = categories;
  logInfoToFile(
    `[create-missing-products] ✅ Loaded ${categories.length} existing product categories from WooCommerce`
  );
  return CATEGORY_CACHE;
}

/**
 * Find a category in the cached list by name + parent.
 *
 * @param {string} name       - category name to search
 * @param {number} parentId   - parent category ID (0 for root)
 * @returns {Object|null}     - the category object or null
 */
async function findCategoryByNameAndParent(name, parentId) {
  const all = await loadAllCategories();
  const targetNorm = normalizeName(name);

  return (
    all.find(
      (cat) =>
        normalizeName(cat.name) === targetNorm && Number(cat.parent) === Number(parentId)
    ) || null
  );
}

/**
 * Ensure a single category exists in WooCommerce.
 *  - If found by name+parent → return existing ID.
 *  - Else → create via Woo API, cache, and return new ID.
 *
 * @param {string} name
 * @param {number} parentId
 * @returns {Promise<number|null>} categoryId
 */
async function ensureCategory(name, parentId = 0) {
  if (!name) return null;

  // 1) Try to find an existing category
  const existing = await findCategoryByNameAndParent(name, parentId);
  if (existing) {
    return existing.id;
  }

  // 2) Create new category
  try {
    const res = await wooApi.post("products/categories", {
      name,
      parent: parentId || 0,
    });

    const created = res.data;
    logInfoToFile(
      `[create-missing-products] ✅ Created category "${name}" (id=${created.id}, parent=${parentId})`
    );

    // push into cache so subsequent lookups see it
    if (CATEGORY_CACHE) {
      CATEGORY_CACHE.push(created);
    } else {
      CATEGORY_CACHE = [created];
    }

    return created.id;
  } catch (err) {
    logErrorToFile(
      `[create-missing-products] ❌ Failed to create category "${name}" (parent=${parentId}): ${err.message}`
    );
    return null;
  }
}

/**
 * Given a "resolvedCategory" object (from resolveCategory or manual parsing),
 * ensure the whole hierarchy exists and return an array of category IDs
 * in order from top → lowest level.
 *
 * resolvedCategory = {
 *   main: "Integrated Circuits (ICs)",
 *   sub: "Embedded",
 *   sub2: "Microcontrollers",
 *   score: <optional>,
 *   matchedOn: <optional>
 * }
 */
async function ensureCategoryHierarchy(resolvedCategory) {
  if (!resolvedCategory) return [];

  const { main, sub, sub2 } = resolvedCategory;

  const ids = [];

  // Main
  const mainId = await ensureCategory(main, 0);
  if (mainId) ids.push(mainId);

  // Sub (child of main)
  let subId = null;
  if (sub && mainId) {
    subId = await ensureCategory(sub, mainId);
    if (subId) ids.push(subId);
  }

  // 2nd sub (child of sub)
  if (sub2 && subId) {
    const sub2Id = await ensureCategory(sub2, subId);
    if (sub2Id) ids.push(sub2Id);
  }

  return ids;
}

/**
 * Parse a raw category string like:
 *   "Integrated Circuits (ICs)>Embedded>Microcontrollers"
 * into an object { main, sub, sub2 }.
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
 * Build a human-readable path from a resolvedCategory object.
 * e.g. { main: "ICs", sub: "Embedded", sub2: "MCUs" }
 *   → "ICs > Embedded > MCUs"
 */
function buildCategoryPath(resolvedCategory) {
  if (!resolvedCategory) return "";

  const parts = [resolvedCategory.main, resolvedCategory.sub, resolvedCategory.sub2].filter(
    Boolean
  );
  return parts.join(" > ");
}

/**
 * Main entry:
 *   Given a "fileKey", read missing_products_${fileKey}.json and create products.
 *
 * @param {string} fileKey - usually the CSV key, e.g. "Microcontrollers-2025-03-11.csv"
 */
const processMissingProducts = async (fileKey) => {
  try {
    const missingFilePath = path.join(
      __dirname,
      `missing_products_${fileKey}.json`
    );

    if (!fs.existsSync(missingFilePath)) {
      logInfoToFile(
        `[create-missing-products] No missing products file found for ${fileKey}, skipping.`
      );
      return;
    }

    let missingProducts = [];
    try {
      missingProducts = JSON.parse(fs.readFileSync(missingFilePath, "utf8"));
    } catch (err) {
      logErrorToFile(
        `[create-missing-products] ❌ Error reading missing products file: ${err.message}`
      );
      return;
    }

    logInfoToFile(
      `[create-missing-products] 🚀 Processing ${missingProducts.length} missing products from ${missingFilePath}`
    );

    // Ensure category cache is loaded before the loop
    await loadAllCategories();

    for (const productData of missingProducts) {
      try {
        const partNumber = productData.part_number || "";
        const manufacturer = productData.manufacturer || "";
        const rawCategory =
          productData.category || productData.Category || ""; // support both cases

        // -------------------------------------------------
        // 1) Derive a resolved category (fuzzy + fallback)
        // -------------------------------------------------
        let resolvedCategory = null;

        if (rawCategory) {
          try {
            // Try fuzzy match using category-hierarchy-ref.csv
            resolvedCategory = await resolveCategory(rawCategory);
          } catch (err) {
            logErrorToFile(
              `[create-missing-products] Category resolve error for part_number=${partNumber}: ${err.message}`
            );
          }

          // Fallback: if fuzzy failed, use raw ">" path directly
          if (!resolvedCategory) {
            resolvedCategory = parseCategoryPath(rawCategory);
          }

          if (resolvedCategory) {
            const pathStr = buildCategoryPath(resolvedCategory);
            logInfoToFile(
              `[create-missing-products] Category for part_number=${partNumber}: "${rawCategory}" → "${pathStr}" (matchedOn=${resolvedCategory.matchedOn || "n/a"})`
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

        // -------------------------------------------------
        // 2) Ensure category hierarchy exists in Woo
        // -------------------------------------------------
        let categoryIds = [];
        if (resolvedCategory) {
          categoryIds = await ensureCategoryHierarchy(resolvedCategory);
        }

        // -------------------------------------------------
        // 3) Build the new product payload
        //    (you can expand meta_data mappings over time)
        // -------------------------------------------------
        const newProduct = {
          name: productData.part_title || productData.part_number,
          sku: productData.sku || productData.part_number,
          description: productData.part_description || "",
          categories: categoryIds.map((id) => ({ id })), // attach category IDs

          meta_data: [
            { key: "manufacturer", value: manufacturer },

            // Keep existing database-style fields as ACF meta
            { key: "series", value: productData.series || "" },
            { key: "quantity", value: productData.quantity_available || "0" },
            { key: "short_description", value: productData.short_description || "" },
            { key: "detail_description", value: productData.part_description || "" },
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

        // Optionally also store the *proposed* category path as meta for debugging
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

        // -------------------------------------------------
        // 4) Create the product via Woo API
        // -------------------------------------------------
        const response = await wooApi.post("products", newProduct);

        logInfoToFile(
          `[create-missing-products] ✅ Created product part_number=${partNumber} (id=${response.data.id}) with categories: [${categoryIds.join(
            ", "
          )}]`
        );
      } catch (error) {
        const status = error.response?.status;
        const data = error.response?.data;

        logErrorToFile(
          `[create-missing-products] ❌ Error creating product for part_number=${productData.part_number}: ` +
            `status=${status || "n/a"}, ` +
            `message=${data?.message || error.message}, ` +
            `data=${JSON.stringify(data || {}, null, 2)}`
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

// If this file is run directly via `node create-missing-products.js ...`
if (require.main === module) {
  const fileKey = process.argv[2];

  if (!fileKey) {
    console.error(
      "[create-missing-products] ❌ Usage: node create-missing-products.js <missing_products_file.json>"
    );
    process.exit(1);
  }

  (async () => {
    try {
      console.log(
        `[create-missing-products] ▶︎ Starting processMissingProducts("${fileKey}")...`
      );
      await processMissingProducts(fileKey);
      console.log(
        `[create-missing-products] ✅ Finished processMissingProducts("${fileKey}")`
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

