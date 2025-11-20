// src/batch/category-apply.js
//
// Responsibility:
// Given a resolved category hierarchy from category-map.js:
//   { main, sub, sub2 }
// talk to WooCommerce and ensure that hierarchy exists as
// product categories, then return the corresponding IDs.
//
// It:
// - avoids duplicates (checks by slug + parent)
// - caches results in-memory during one run so we don't spam the API.

const { wooApi } = require("../woo-helpers");
const { logInfoToFile, logErrorToFile } = require("../logger");

// Simple slug generator: "LED Emitters" -> "led-emitters"
function slugify(name) {
  return String(name || "")
    .trim()
    .toLowerCase()
    .replace(/[\/]/g, " ")       // replace "/" with space
    .replace(/[^a-z0-9\s-]/g, "") // remove weird chars
    .replace(/\s+/g, "-");        // spaces -> "-"
}

// Cache so we don't re-fetch or re-create the same category repeatedly.
// Key: `${parentId}:${slug}` → value: { id, name, slug, parent }
const categoryCache = new Map();

/**
 * Find an existing WooCommerce product category by slug + parent,
 * or create it if not found.
 *
 * @param {string} name - Category name (human readable).
 * @param {number} parentId - Parent category ID (0 for top-level).
 * @returns {Promise<{ id: number, name: string, slug: string, parent: number }>}
 */
async function getOrCreateCategory(name, parentId = 0) {
  if (!name) {
    throw new Error("getOrCreateCategory called with empty name");
  }

  const slug = slugify(name);
  const cacheKey = `${parentId}:${slug}`;

  // 1) Check in-memory cache first
  if (categoryCache.has(cacheKey)) {
    return categoryCache.get(cacheKey);
  }

  try {
    // 2) Try to find an existing category via REST API
    const params = {
      slug,              // search by slug
      per_page: 100,
      parent: parentId,  // only categories under this parent
      hide_empty: false,
    };

    const existing = await wooApi.get("products/categories", { params });
    const data = existing.data || [];

    if (data.length > 0) {
      const cat = data[0];
      logInfoToFile(
        `[CategoryApply] Reusing existing category "${name}" (slug="${slug}", id=${cat.id}, parent=${parentId})`
      );
      categoryCache.set(cacheKey, cat);
      return cat;
    }

    // 3) Not found -> create a new category
    logInfoToFile(
      `[CategoryApply] Creating new category "${name}" (slug="${slug}", parent=${parentId})`
    );

    const created = await wooApi.post("products/categories", {
      name,
      slug,
      parent: parentId,
    });

    const cat = created.data;
    categoryCache.set(cacheKey, cat);
    return cat;
  } catch (err) {
    logErrorToFile(
      `[CategoryApply] Failed to get/create category "${name}" (parent=${parentId}): ${err.message}`
    );
    throw err;
  }
}

/**
 * Ensure the full hierarchy from a resolvedCategory exists in WooCommerce:
 *
 * resolved = { main, sub, sub2 }
 *
 * Returns:
 *   {
 *     ids: number[],      // [mainId, subId?, sub2Id?]
 *     mainId: number,
 *     subId?: number,
 *     sub2Id?: number
 *   }
 */
async function ensureCategoryHierarchy(resolvedCategory) {
  if (!resolvedCategory || !resolvedCategory.main) {
    // Nothing we can do
    return null;
  }

  const { main, sub, sub2 } = resolvedCategory;

  // Step 1: main (top-level)
  const mainCat = await getOrCreateCategory(main, 0);

  // Step 2: sub (optional)
  let subCat = null;
  if (sub) {
    subCat = await getOrCreateCategory(sub, mainCat.id);
  }

  // Step 3: sub2 (optional)
  let sub2Cat = null;
  if (sub2 && subCat) {
    sub2Cat = await getOrCreateCategory(sub2, subCat.id);
  }

  const ids = [mainCat.id];
  if (subCat) ids.push(subCat.id);
  if (sub2Cat) ids.push(sub2Cat.id);

  return {
    ids,
    mainId: mainCat.id,
    subId: subCat ? subCat.id : undefined,
    sub2Id: sub2Cat ? sub2Cat.id : undefined,
  };
}

module.exports = {
  ensureCategoryHierarchy,
  getOrCreateCategory, // exported in case you need finer control / tests
};
