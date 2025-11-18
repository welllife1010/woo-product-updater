// category-woo.js
const { wooApi } = require("./woo-helpers");

let categoryCache = []; // in-memory for this process run

async function loadCategoriesOnce() {
  if (categoryCache.length) return;
  let page = 1;
  while (true) {
    const res = await wooApi.get("products/categories", {
      per_page: 100,
      page,
      hide_empty: false,
    });
    const items = res.data || [];
    if (!items.length) break;
    categoryCache.push(...items);
    page++;
  }
}

/**
 * Find or create a single category with optional parent.
 */
async function ensureCategory(name, parentId = 0) {
  await loadCategoriesOnce();
  const existing = categoryCache.find(
    (cat) =>
      cat.name.trim().toLowerCase() === name.trim().toLowerCase() &&
      Number(cat.parent) === Number(parentId)
  );
  if (existing) return existing.id;

  // Create it
  const res = await wooApi.post("products/categories", {
    name,
    parent: parentId,
  });
  const created = res.data;
  categoryCache.push(created);
  return created.id;
}

/**
 * Ensure whole main → sub → sub2 chain exists.
 * Returns an array of IDs [mainId, subId?, sub2Id?].
 */
async function ensureCategoryHierarchy(resolved) {
  if (!resolved) return [];

  const mainId = await ensureCategory(resolved.main, 0);
  let subId = null;
  let sub2Id = null;

  if (resolved.sub) {
    subId = await ensureCategory(resolved.sub, mainId);
  }
  if (resolved.sub2 && subId) {
    sub2Id = await ensureCategory(resolved.sub2, subId);
  }

  return [mainId, subId, sub2Id].filter(Boolean);
}

module.exports = { ensureCategoryHierarchy };
