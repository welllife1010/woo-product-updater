// ============================================================================
// category-resolver.js
// PURPOSE:
//   Smart resolver for vendor category text.
//   1) Fuzzy-match against EXISTING WooCommerce categories first.
//   2) If no good Woo match, fall back to CSV + fuzzy (category-map.js).
//
// This file does NOT create categories. It only decides:
//   { main, sub, sub2, score, matchedOn }
// ============================================================================

const Fuse = require("fuse.js");
const { wooApi } = require("./woo-helpers");
const {
  resolveCategory: resolveFromCsv,
  // If you didn't export normalizeName from category-map.js,
  // just copy the normalizeName function below instead.
  normalizeName,
} = require("./category-map");

// -----------------------------
// Woo category caches
// -----------------------------
let WOO_CATEGORY_CACHE = [];
let WOO_FUSE = null;
let WOO_LOADED = false;

/**
 * Simple slugify helper to approximate Woo slugs from a name.
 * Example: "Microcontrollers / DRAM" -> "microcontrollers-dram"
 */
function toSlug(value) {
  if (!value) return "";
  return String(value)
    .toLowerCase()
    .trim()
    .replace(/\s+/g, "-")        // spaces → dash
    .replace(/[^a-z0-9\-]/g, ""); // remove non-alphanumeric/dash
}

/**
 * Load all WooCommerce product categories once (paginated).
 */
async function loadWooCategoriesOnce() {
  if (WOO_LOADED && WOO_CATEGORY_CACHE.length) return WOO_CATEGORY_CACHE;

  const cats = [];
  let page = 1;
  const per_page = 100;

  while (true) {
    const res = await wooApi.get("products/categories", {
      per_page,
      page,
      hide_empty: false,
    });

    const data = res.data || [];
    if (!data.length) break;

    cats.push(...data);

    if (data.length < per_page) break;
    page++;
  }

  WOO_CATEGORY_CACHE = cats;
  WOO_LOADED = true;
  buildWooFuseIndex();
  return WOO_CATEGORY_CACHE;
}

/**
 * Build a flat list of candidates for fuzzy search:
 * - one candidate per Woo category
 * - label = normalized name
 * - we also keep slug for debugging/fallback
 */
function buildWooCandidates() {
  return WOO_CATEGORY_CACHE.map((cat) => ({
    label: normalizeName(cat.name),
    nameRaw: cat.name,
    slug: cat.slug,
    id: cat.id,
    parent: cat.parent,
  }));
}

/**
 * Build Fuse index over Woo categories.
 */
function buildWooFuseIndex() {
  if (!WOO_CATEGORY_CACHE.length) {
    WOO_FUSE = null;
    return;
  }

  const candidates = buildWooCandidates();

  WOO_FUSE = new Fuse(candidates, {
    includeScore: true,
    keys: ["label", "slug"],
    threshold: 0.4, // stricter than CSV because we don't want wrong matches
  });
}

/**
 * Walk up the parent chain of a Woo category and build:
 *   { main, sub, sub2 }
 *
 * Example:
 *   Leaf: Microcontrollers (id=10, parent=5)
 *   Parent: Integrated Circuits (id=5, parent=0)
 *
 * Path: [Integrated Circuits, Microcontrollers]
 *  -> main = Integrated Circuits
 *     sub  = Microcontrollers
 *     sub2 = null
 */
function buildResolvedFromWooCategory(leafCategory, all) {
  if (!leafCategory) return null;

  const path = [];
  let current = leafCategory;

  while (current) {
    path.unshift(current); // prepend so we get [root, ..., leaf]
    if (!current.parent) break;
    current = all.find((c) => Number(c.id) === Number(current.parent));
    if (!current) break; // parent not found (safety)
  }

  const main = path[0] ? path[0].name : null;
  const sub = path[1] ? path[1].name : null;
  const sub2 = path[2] ? path[2].name : null;

  return {
    main,
    sub,
    sub2,
    score: 1,          // we trust the website's structure
    matchedOn: "woo",  // indicates this came from Woo fuzzy
  };
}

/**
 * Fuzzy-resolve a vendor category string against EXISTING Woo categories.
 *
 * Returns:
 *   - { main, sub, sub2, score, matchedOn: "woo" } on success
 *   - null if no good match
 */
async function resolveCategoryFromWooFuzzy(rawCategory) {
  if (!rawCategory) return null;

  const inputNorm = normalizeName(rawCategory);
  if (!inputNorm) return null;

  // Ensure Woo categories & Fuse index are loaded
  await loadWooCategoriesOnce();
  if (!WOO_FUSE) return null;

  const results = WOO_FUSE.search(inputNorm, { limit: 1 });

  if (!results || !results.length) return null;

  const { item, score } = results[0];
  const similarity = 1 - score; // 1 = perfect match

  // Guardrail: ignore weak matches (tune this as you like)
  const MIN_SIMILARITY = 0.5; // stricter than CSV, since this is live site
  if (similarity < MIN_SIMILARITY) {
    return null;
  }

  // Find the real Woo category object by id
  const leaf = WOO_CATEGORY_CACHE.find((c) => Number(c.id) === Number(item.id));
  if (!leaf) return null;

  // Build path based on real Woo parent chain
  const resolved = buildResolvedFromWooCategory(leaf, WOO_CATEGORY_CACHE);
  if (resolved) {
    resolved.score = similarity;
  }

  return resolved;
}

/**
 * Public Smart Resolver:
 *   1) Try fuzzy Woo categories first (honor website hierarchy).
 *   2) If not found, fall back to CSV + fuzzy via category-map.js.
 *
 * Returns:
 *   - { main, sub, sub2, score, matchedOn } or null
 */
async function resolveCategorySmart(rawCategory) {
  // 1) Website-first fuzzy (Woo)
  const fromWoo = await resolveCategoryFromWooFuzzy(rawCategory);
  if (fromWoo) return fromWoo;

  // 2) Fallback to CSV-based fuzzy
  const fromCsv = await resolveFromCsv(rawCategory);
  if (fromCsv) {
    fromCsv.matchedOn = fromCsv.matchedOn || "csv";
  }

  return fromCsv;
}

module.exports = {
  resolveCategorySmart,
  resolveCategoryFromWooFuzzy, // export if you want to test separately
};
