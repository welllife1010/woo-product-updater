// ============================================================================
// category-resolver.js
// PURPOSE (in plain English):
//   Take some "category-ish" text from a vendor (for example a CSV column like
//   "Integrated Circuits (ICs)>Embedded>Microcontrollers") and guess which
//   WooCommerce category it belongs to.
//
//   We try to be smart in this order:
//
//     1) Fuzzy match against EXISTING WooCommerce categories via the Woo API.
//        (Woo's own category tree is our main source of truth.)
//
//     2) If that fails, fall back to our CSV / reference mapping
//        implemented in category-map.js (this is where you can plug in
//        manufacturer-category tables, reference CSVs, etc.).
//
//   This file DOES NOT create categories in Woo. It only returns a
//   "decision object" describing which category we think the vendor
//   string belongs to.
//
//   The main outputs are:
//     - resolveCategorySmart(rawCategory)
//         → { main, sub, sub2, score, matchedOn, leafId, leafSlug } or null
//
//     - resolveCategoryFromWooFuzzy(rawCategory)
//         → same shape, but "matchedOn" is "woo"
//
//     - resolveLeafSlugSmart(rawCategory)
//         → BEST GUESS of the Woo leaf category slug (e.g. "microcontrollers")
//            for convenience in other scripts (like missing-products).
// ============================================================================

// External libraries
const Fuse = require("fuse.js");

// Local helper: WooCommerce API client (already configured elsewhere)
const { wooApi } = require("./woo-helpers");

// Local helper: CSV / reference-based mapping logic
//   category-map.js should export at least:
//     - resolveCategory(rawCategory) → { main, sub, sub2, score, matchedOn? } or null
//   It MAY also export a normalizeName function (optional).
const categoryMap = require("./category-map");
const resolveFromCsv = categoryMap.resolveCategory;
const csvNormalizeName = categoryMap.normalizeName || null;

// ============================================================================
// Small string helpers
// ============================================================================

/**
 * normalizeName(str)
 *
 * PURPOSE:
 *   Convert a category string into a "simple" form that's easier to compare.
 *   - Lowercase everything.
 *   - Replace separators (/ > -) with spaces.
 *   - Remove extra punctuation.
 *   - Collapse multiple spaces into one.
 *
 *   Example:
 *     "Integrated Circuits (ICs)>Embedded>Microcontrollers"
 *       → "integrated circuits ics embedded microcontrollers"
 */
function normalizeName(str) {
  if (!str) return "";

  // If category-map provided its own normalizeName, use that (so we have
  // only ONE place to tweak normalization for the whole system).
  if (typeof csvNormalizeName === "function") {
    return csvNormalizeName(str);
  }

  // Fallback: simple, generic normalization
  return String(str)
    .toLowerCase()
    // Turn common separators into spaces
    .replace(/[>/]/g, " ")
    // Remove most punctuation except letters, numbers, spaces
    .replace(/[^a-z0-9\s]+/g, " ")
    // Collapse multiple spaces to one
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * toSlug(str)
 *
 * PURPOSE:
 *   Turn a name into a URL/file-safe slug.
 *
 *   Example:
 *     "LED Emitters (IR/UV)" → "led-emitters-ir-uv"
 */
function toSlug(str) {
  if (!str) return "unknown";
  return String(str)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-") // non-alphanumeric → "-"
    .replace(/^-+|-+$/g, "");    // trim leading/trailing "-"
}

// ============================================================================
// In-memory cache of Woo categories (+ Fuse index for fuzzy search)
// ============================================================================

/**
 * WOO_CATEGORY_CACHE
 *
 * What this holds:
 *   - An ARRAY of raw Woo category objects exactly as returned by:
 *
 *       GET /wp-json/wc/v3/products/categories
 *
 *   - We load ALL pages once, keep them in memory, and reuse them for
 *     all fuzzy matches during this Node process.
 *
 *   Example of one item:
 *     {
 *       id: 34,
 *       name: "Microcontrollers",
 *       slug: "microcontrollers",
 *       parent: 12,
 *       description: "",
 *       ...
 *     }
 */
let WOO_CATEGORY_CACHE = [];

// Simple flag to avoid re-loading categories multiple times
let WOO_LOADED = false;

// Fuse.js instance used for fuzzy search over Woo categories
let WOO_FUSE = null;

// --------------------------------------------------------------------------
// buildWooCandidates()
//   Convert raw Woo categories into a simpler list of "candidate" objects
//   that Fuse.js knows how to search.
// --------------------------------------------------------------------------
function buildWooCandidates() {
  return WOO_CATEGORY_CACHE.map((cat) => ({
    // "label" is the normalized name we will fuzzy-match against
    label: normalizeName(cat.name),
    nameRaw: cat.name,      // original Woo category name
    slug: cat.slug,         // actual Woo slug
    id: cat.id,             // Woo category ID
    parent: cat.parent,     // parent ID (0 for root)
  }));
}

// --------------------------------------------------------------------------
// buildWooFuseIndex()
//   Build or rebuild the Fuse.js index from WOO_CATEGORY_CACHE.
// --------------------------------------------------------------------------
function buildWooFuseIndex() {
  const candidates = buildWooCandidates();

  // If there are no categories, just leave WOO_FUSE null
  if (!candidates.length) {
    WOO_FUSE = null;
    return;
  }

  WOO_FUSE = new Fuse(candidates, {
    // Which fields in each candidate should be fuzzy-searched?
    keys: ["label"],
    // Lower threshold = stricter match; 1 = almost anything
    threshold: 0.4,
  });
}

// --------------------------------------------------------------------------
// loadWooCategoriesOnce()
//   Fetch ALL Woo product categories one time and cache them in memory.
// --------------------------------------------------------------------------
async function loadWooCategoriesOnce() {
  if (WOO_LOADED && WOO_CATEGORY_CACHE.length) {
    // Already loaded, nothing to do.
    return WOO_CATEGORY_CACHE;
  }

  const cats = [];
  let page = 1;
  const per_page = 100;

  try {
    while (true) {
      // NOTE:
      //   We assume wooApi.get() follows the pattern:
      //     wooApi.get(endpoint, { per_page, page, hide_empty: false })
      const res = await wooApi.get("products/categories", {
        per_page,
        page,
        hide_empty: false,
      });

      const data = res.data || [];
      if (!data.length) break; // no more pages

      cats.push(...data);
      page++;
    }
  } catch (err) {
    // If this fails, we can't do Woo-based resolving.
    console.error(
      `[category-resolver] Error loading Woo categories: ${err.message}`
    );
  }

  WOO_CATEGORY_CACHE = cats;
  WOO_LOADED = true;
  buildWooFuseIndex();

  return WOO_CATEGORY_CACHE;
}

// ============================================================================
// Helpers to turn a Woo "leaf" category into main/sub/sub2 structure
// ============================================================================

/**
 * buildResolvedFromWooCategory(leafCategory, all)
 *
 * PURPOSE (plain English):
 *   We get a "leaf" Woo category from Fuse (for example "Microcontrollers",
 *   id=34, parent=12). We now want to know its full path, including its parents.
 *
 *   We walk up the parent chain:
 *     - Start at the leaf.
 *     - Look up parent by id.
 *     - Keep going until parent=0 (root).
 *
 *   Then we turn that into:
 *     { main, sub, sub2 }   (up to 3 levels deep)
 *
 *   We ALSO expose:
 *     - leafId   : the ID of the leaf Woo category
 *     - leafSlug : the slug of the leaf Woo category
 */
function buildResolvedFromWooCategory(leafCategory, all) {
  if (!leafCategory) return null;

  const path = [];
  let current = leafCategory;

  // Walk up the tree until we reach a category with parent=0
  while (current) {
    // Put current at the front of the array so root ends at index 0
    path.unshift(current);

    if (!current.parent) break; // parent = 0 ⇒ root
    current = all.find((c) => Number(c.id) === Number(current.parent));
    if (!current) break;
  }

  const main = path[0] ? path[0].name : null;
  const sub  = path[1] ? path[1].name : null;
  const sub2 = path[2] ? path[2].name : null;

  return {
    main,
    sub,
    sub2,
    score: 1,           // we will override this with real similarity later
    matchedOn: "woo",   // indicates this came from Woo fuzzy
    leafId: leafCategory.id,
    leafSlug: leafCategory.slug,
  };
}

// ============================================================================
// Public fuzzy resolver: Woo first, CSV fallback
// ============================================================================

/**
 * Fuzzy-resolve a vendor category string against EXISTING Woo categories.
 *
 * INPUT:
 *   rawCategory (string)
 *     e.g. "Integrated Circuits (ICs)>Embedded>Microcontrollers"
 *
 * RETURNS:
 *   - { main, sub, sub2, score, matchedOn: "woo", leafId, leafSlug } on success
 *   - null if no good match
 */
async function resolveCategoryFromWooFuzzy(rawCategory) {
  if (!rawCategory) return null;

  const inputNorm = normalizeName(rawCategory);
  if (!inputNorm) return null;

  // Ensure we have categories + Fuse index loaded
  await loadWooCategoriesOnce();
  if (!WOO_FUSE) return null;

  // Ask Fuse for the best match
  const results = WOO_FUSE.search(inputNorm, { limit: 1 });
  if (!results || !results.length) return null;

  const { item, score } = results[0];
  // Fuse returns a distance score: 0 = perfect match, 1 = worst.
  // We convert it to a similarity: 1 = perfect, 0 = terrible.
  const similarity = 1 - score;

  // Minimum similarity threshold; tweak if needed.
  const MIN_SIMILARITY = 0.5;
  if (similarity < MIN_SIMILARITY) {
    // Too weak; we don't trust this match.
    return null;
  }

  // Now we need the actual Woo category object (with name, slug, parent, etc.)
  const leaf = WOO_CATEGORY_CACHE.find(
    (c) => Number(c.id) === Number(item.id)
  );
  if (!leaf) return null;

  const resolved = buildResolvedFromWooCategory(leaf, WOO_CATEGORY_CACHE);
  if (!resolved) return null;

  // Update score to reflect the actual fuzzy similarity
  resolved.score = similarity;

  return resolved;
}

/**
 * resolveCategorySmart(rawCategory)
 *
 * PURPOSE:
 *   This is the "main" function most code should call.
 *
 *   It tries:
 *     1) Woo fuzzy (resolveCategoryFromWooFuzzy)
 *     2) If that fails, CSV mapping (resolveFromCsv from category-map.js)
 *
 * RETURN VALUE:
 *   - { main, sub, sub2, score, matchedOn, leafId?, leafSlug? } or null
 *
 *   NOTE:
 *     For CSV-based mapping, leafId / leafSlug will usually be undefined,
 *     because we don't know Woo's IDs from CSV alone.
 */
async function resolveCategorySmart(rawCategory) {
  if (!rawCategory) return null;

  // 1) Try Woo categories first
  const fromWoo = await resolveCategoryFromWooFuzzy(rawCategory);
  if (fromWoo) return fromWoo;

  // 2) Fallback to CSV-based fuzzy
  if (typeof resolveFromCsv === "function") {
    const fromCsv = await resolveFromCsv(rawCategory);
    if (fromCsv) {
      // Make sure matchedOn is set so callers know the source
      fromCsv.matchedOn = fromCsv.matchedOn || "csv";
      return fromCsv;
    }
  }

  return null;
}

// ============================================================================
// Convenience helper: resolveLeafSlugSmart
//   For cases where you ONLY care about the leaf Woo slug.
//   Example usage: deciding where to store "missing-products" JSON.
// ============================================================================

/**
 * resolveLeafSlugSmart(rawCategory)
 *
 * PURPOSE:
 *   Given vendor category text, return:
 *     - the BEST GUESS of the Woo leaf category slug (string), or
 *     - null if we really cannot figure it out.
 *
 * STRATEGY:
 *   1) Try Woo fuzzy (preferred; we get real Woo slug).
 *   2) If that fails, try CSV mapping and slugify its leaf.
 *   3) If that fails, slugify the last segment of rawCategory as a last resort.
 */
async function resolveLeafSlugSmart(rawCategory) {
  if (!rawCategory) return null;

  // 1) Woo fuzzy: if we get a leafSlug, that's the best answer.
  const fromWoo = await resolveCategoryFromWooFuzzy(rawCategory);
  if (fromWoo && fromWoo.leafSlug) {
    return fromWoo.leafSlug;
  }

  // 2) CSV fallback: use its leafmost part and slugify
  if (typeof resolveFromCsv === "function") {
    const fromCsv = await resolveFromCsv(rawCategory);
    if (fromCsv) {
      if (fromCsv.sub2) return toSlug(fromCsv.sub2);
      if (fromCsv.sub)  return toSlug(fromCsv.sub);
      if (fromCsv.main) return toSlug(fromCsv.main);
    }
  }

  // 3) Last resort: just take the last ">" segment and slugify it
  const parts = String(rawCategory)
    .split(">")
    .map((p) => p.trim())
    .filter(Boolean);

  if (parts.length) {
    const leafName = parts[parts.length - 1];
    return toSlug(leafName);
  }

  return null;
}

// ============================================================================
// Exports
// ============================================================================

module.exports = {
  resolveCategorySmart,
  resolveCategoryFromWooFuzzy, // exported for testing / debugging
  resolveLeafSlugSmart,        // convenience: get just the leaf slug
};
