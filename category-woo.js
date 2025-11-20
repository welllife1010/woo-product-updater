// category-woo.js
//
// Canonical helpers for talking to WooCommerce product categories.
//
// RESPONSIBILITIES:
// - Given a category name + parent ID, find or create the Woo category.
// - Given a resolved hierarchy { main, sub, sub2 }, ensure that whole
//   chain exists in Woo and return the IDs.
//
// This is the ONE place that should contain category creation logic.
// Other files (batch scripts, missing product creator, etc.) should
// import from here instead of implementing their own versions.

const { wooApi } = require("./woo-helpers");
const { logInfoToFile, logErrorToFile } = require("./logger");

// Simple slug generator: "LED Emitters" -> "led-emitters"
function slugify(name) {
  return String(name || "")
    .trim()
    .toLowerCase()
    .replace(/[\/]/g, " ")        // replace "/" with space
    .replace(/[^a-z0-9\s-]/g, "") // remove weird chars
    .replace(/\s+/g, "-");        // spaces -> "-"
}

// Cache so we don't re-fetch or re-create the same category repeatedly.
// Key: `${parentId}:${slug}` → value: { id, name, slug, parent }
let categoryCache = []; // in-memory for this process run

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
        `[CategoryWoo] Reusing existing category "${name}" (slug="${slug}", id=${cat.id}, parent=${parentId})`
      );
      categoryCache.set(cacheKey, cat);
      return cat;
    }

    // 3) Not found -> create a new category
    logInfoToFile(
      `[CategoryWoo] Creating new category "${name}" (slug="${slug}", parent=${parentId})`
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
      `[CategoryWoo] Failed to get/create category "${name}" (parent=${parentId}): ${err.message}`
    );
    throw err;
  }
}

/**
 * Ensure the full hierarchy from a resolvedCategory exists in WooCommerce:
 *
 * resolvedCategory = { main, sub, sub2 }
 *
 * Returns:
 *   {
 *     ids: number[],      // [mainId, subId?, sub2Id?]
 *     mainId: number,
 *     subId?: number,
 *     sub2Id?: number
 *   } | null
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

/**
 * Convenience helper when you only care about the array of IDs.
 *
 * @param {Object} resolvedCategory
 * @returns {Promise<number[]>}
 */
async function ensureCategoryHierarchyIds(resolvedCategory) {
  const result = await ensureCategoryHierarchy(resolvedCategory);
  return result?.ids || [];
}

module.exports = {
  getOrCreateCategory,
  ensureCategoryHierarchy,
  ensureCategoryHierarchyIds,
};

// async function loadCategoriesOnce() {
//   if (categoryCache.length) return;
//   let page = 1;
//   while (true) {
//     const res = await wooApi.get("products/categories", {
//       per_page: 100,
//       page,
//       hide_empty: false,
//     });
//     const items = res.data || [];
//     if (!items.length) break;
//     categoryCache.push(...items);
//     page++;
//   }
// }

// /**
//  * Find or create a single category with optional parent.
//  */
// async function ensureCategory(name, parentId = 0) {
//   await loadCategoriesOnce();
//   const existing = categoryCache.find(
//     (cat) =>
//       cat.name.trim().toLowerCase() === name.trim().toLowerCase() &&
//       Number(cat.parent) === Number(parentId)
//   );
//   if (existing) return existing.id;

//   // Create it
//   const res = await wooApi.post("products/categories", {
//     name,
//     parent: parentId,
//   });
//   const created = res.data;
//   categoryCache.push(created);
//   return created.id;
// }

// /**
//  * Ensure whole main → sub → sub2 chain exists.
//  * Returns an array of IDs [mainId, subId?, sub2Id?].
//  */
// async function ensureCategoryHierarchy(resolved) {
//   if (!resolved) return [];

//   const mainId = await ensureCategory(resolved.main, 0);
//   let subId = null;
//   let sub2Id = null;

//   if (resolved.sub) {
//     subId = await ensureCategory(resolved.sub, mainId);
//   }
//   if (resolved.sub2 && subId) {
//     sub2Id = await ensureCategory(resolved.sub2, subId);
//   }

//   return [mainId, subId, sub2Id].filter(Boolean);
// }

// module.exports = { ensureCategoryHierarchy };
