// manufacturer-resolver.js
//
// Smart resolver for manufacturer names.
// - Uses:
//    - MANUFACTURER_ALIASES (hard-coded overrides)
//    - static CANONICAL_MANUFACTURERS
//    - dynamic custom-manufacturers.json (auto-extended at runtime)
//    - Fuse.js fuzzy matching
//
// - If we cannot confidently match, we treat the input as a brand-new
//   canonical manufacturer, append it to custom-manufacturers.json,
//   and return it as canonical.

const fs = require("fs");
const path = require("path");
const Fuse = require("fuse.js");
const {
  CANONICAL_MANUFACTURERS,
  MANUFACTURER_ALIASES,
} = require("./manufacturer-data");

// Where we store newly discovered manufacturers
const CUSTOM_MANUF_PATH = path.join(__dirname, "custom-manufacturers.json");

// Normalization helper for comparisons
function normalizeForCompare(str) {
  return String(str || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[.,]/g, "")
    .trim();
}

function loadCustomManufacturers() {
  if (!fs.existsSync(CUSTOM_MANUF_PATH)) {
    // Initialize file if missing
    fs.writeFileSync(CUSTOM_MANUF_PATH, JSON.stringify([], null, 2));
    return [];
  }

  try {
    const raw = fs.readFileSync(CUSTOM_MANUF_PATH, "utf8");
    const list = JSON.parse(raw);
    if (Array.isArray(list)) return list;
    return [];
  } catch {
    return [];
  }
}

function saveCustomManufacturers(list) {
  fs.writeFileSync(CUSTOM_MANUF_PATH, JSON.stringify(list, null, 2));
}

// Build the combined canonical list (static + dynamic)
function getAllCanonicalManufacturers() {
  const custom = loadCustomManufacturers();
  const all = [...CANONICAL_MANUFACTURERS, ...custom];

  // De-duplicate by normalized name
  const seen = new Set();
  const result = [];
  for (const name of all) {
    const key = normalizeForCompare(name);
    if (!seen.has(key)) {
      seen.add(key);
      result.push(name);
    }
  }
  return result;
}

let MANUF_FUSE = null;

function buildManufacturerIndex() {
  const allCanonical = getAllCanonicalManufacturers();
  const items = allCanonical.map((name) => ({ name }));
  MANUF_FUSE = new Fuse(items, {
    keys: ["name"],
    threshold: 0.3, // stricter = safer
  });
}

/**
 * appendNewManufacturerAsCanonical
 *
 * If this normalized name is not already present in:
 *  - static CANONICAL_MANUFACTURERS
 *  - dynamic custom-manufacturers.json
 * Then we append it into custom-manufacturers.json.
 */
function appendNewManufacturerAsCanonical(name) {
  const original = String(name || "").trim();
  if (!original) return;

  const norm = normalizeForCompare(original);

  // 1) check static
  const inStatic = CANONICAL_MANUFACTURERS.some(
    (m) => normalizeForCompare(m) === norm
  );
  if (inStatic) return;

  // 2) check dynamic
  const custom = loadCustomManufacturers();
  const inCustom = custom.some((m) => normalizeForCompare(m) === norm);
  if (inCustom) return;

  // 3) append and save
  custom.push(original);
  saveCustomManufacturers(custom);
}

/**
 * resolveManufacturerSmart(rawName)
 *
 * Input: raw manufacturer string from CSV.
 * Output: object describing how we resolved it:
 *
 *  {
 *    canonical: "NXP",
 *    score: 1,
 *    matchedOn: "alias"|"exact"|"fuzzy"|"new",
 *    isNew: false|true
 *  }
 */
function resolveManufacturerSmart(rawName) {
  const original = String(rawName || "").trim();
  if (!original) return null;

  const norm = normalizeForCompare(original);

  // 1) Check aliases first (strong override)
  if (MANUFACTURER_ALIASES[norm]) {
    return {
      canonical: MANUFACTURER_ALIASES[norm],
      score: 1,
      matchedOn: "alias",
      isNew: false,
    };
  }

  // 2) Exact-ish match in static + dynamic canonical
  const allCanonical = getAllCanonicalManufacturers();
  const exact = allCanonical.find(
    (name) => normalizeForCompare(name) === norm
  );
  if (exact) {
    return {
      canonical: exact,
      score: 1,
      matchedOn: "exact",
      isNew: false,
    };
  }

  // 3) Fuzzy match with Fuse.js
  if (!MANUF_FUSE) {
    buildManufacturerIndex();
  }
  const results = MANUF_FUSE.search(original, { limit: 1 });

  if (results.length) {
    const { item, score } = results[0];
    const similarity = 1 - score; // 1 = perfect, 0 = bad

    const MIN_SIMILARITY = 0.7; // tune this if needed
    if (similarity >= MIN_SIMILARITY) {
      return {
        canonical: item.name,
        score: similarity,
        matchedOn: "fuzzy",
        isNew: false,
      };
    }
  }

  // 4) No confident match: treat as brand-new canonical.
  appendNewManufacturerAsCanonical(original);

  return {
    canonical: original,
    score: 0,
    matchedOn: "new",
    isNew: true,
  };
}

module.exports = {
  resolveManufacturerSmart,
  appendNewManufacturerAsCanonical,
  normalizeForCompare, // mainly for unit tests if you add them later
};
