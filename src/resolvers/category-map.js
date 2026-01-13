// category-map.js
//
// Helper to load the category hierarchy CSV and provide a resolveCategory()
// function that finds the best match for a raw category string from
// a product CSV row, using Fuse.js for fuzzy search.

const fs = require("fs")
const path = require("path")
const csvParser = require("csv-parser")
const Fuse = require("fuse.js")

// In-memory caches so we only load/build once.
let CATEGORY_ROWS = [] // raw + normalized category hierarchy rows
let CANDIDATES = [] // flattened list of "matchable" category labels
let fuse = null // Fuse index instance
let IS_LOADED = false

/**
 * Normalize a category name for comparison:
 * - Handle null/undefined safely
 * - Convert non-breaking spaces to normal spaces
 * - Trim leading/trailing spaces
 * - Lowercase
 * - Collapse multiple spaces into one
 */
function normalizeName(value) {
  if (!value) return ""

  return String(value)
    .replace(/\u00A0/g, " ") // non-breaking spaces -> normal space
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ") // collapse multiple spaces
}

/**
 * Internal: build CATEGORY_ROWS from a CSV row (CSV from Mun for category mapping).
 * CSV is expected to have headers:
 *   "Main Category", "Sub category", "2nd sub category"
 */
function pushCategoryRowFromCsvRow(csvRow, rows) {
  const mainRaw = csvRow["Main Category"]
  const subRaw = csvRow["Sub category"]
  const sub2Raw = csvRow["2nd sub category"]

  const main = normalizeName(mainRaw)
  const sub = normalizeName(subRaw)
  const sub2 = normalizeName(sub2Raw)

  // Skip completely empty rows
  if (!main && !sub && !sub2) return

  rows.push({
    mainRaw,
    subRaw,
    sub2Raw,
    main,
    sub,
    sub2,
  })
}

/**
 * Load and cache the category hierarchy from CSV.
 * You can call this once on startup or let resolveCategory() call it lazily.
 */
function loadCategoryHierarchy(
  csvFilePath =
    process.env.CATEGORY_HIERARCHY_CSV_PATH ||
    path.join(__dirname, "category-hierarchy-ref.csv")
) {
  return new Promise((resolve) => {
    if (IS_LOADED) {
      return resolve(CATEGORY_ROWS)
    }

    // If the CSV is missing, disable category resolution but do not crash.
    // This is especially important in staging/production where the ref file
    // may be mounted/copied separately.
    if (!fs.existsSync(csvFilePath)) {
      console.warn(
        `[CategoryMap] ⚠️ Category hierarchy CSV not found at ${csvFilePath}. ` +
          "Category resolution will be disabled until the file is provided."
      )
      CATEGORY_ROWS = []
      IS_LOADED = true
      buildCandidatesFromRows()
      buildFuseIndex()
      return resolve(CATEGORY_ROWS)
    }

    const rows = []

    // IMPORTANT: attach error handler to the read stream itself.
    // Errors like ENOENT are emitted by the read stream, and do NOT reliably
    // propagate through .pipe() to the parser, which can otherwise cause an
    // uncaught exception.
    let done = false
    const finish = (loadedRows, err) => {
      if (done) return
      done = true

      if (err) {
        console.error(
          `[CategoryMap] ❌ Failed to load category CSV at ${csvFilePath}:`,
          err
        )
      }

      CATEGORY_ROWS = loadedRows
      IS_LOADED = true
      buildCandidatesFromRows()
      buildFuseIndex()

      if (!err) {
        console.log(
          `[CategoryMap] ✅ Loaded ${loadedRows.length} category rows from ${csvFilePath}`
        )
      }

      resolve(loadedRows)
    }

    const readStream = fs.createReadStream(csvFilePath)
    readStream.on("error", (err) => finish([], err))

    const parser = csvParser()
    parser.on("error", (err) => finish([], err))

    readStream
      .pipe(parser)
      .on("data", (row) => {
        pushCategoryRowFromCsvRow(row, rows)
      })
      .on("end", () => {
        finish(rows)
      })
  })
}

/**
 * Internal: flatten CATEGORY_ROWS into a list of "candidates" to search.
 *
 * For each row, we may create up to 3 candidates:
 *   - one for Main Category
 *   - one for Sub category
 *   - one for 2nd sub category
 *
 * Each candidate remembers:
 *   - label (normalized text used for matching)
 *   - labelRaw (original text)
 *   - level ("main" | "sub" | "sub2")
 *   - mainRaw, subRaw, sub2Raw for mapping back to the full path
 */
function buildCandidatesFromRows() {
  CANDIDATES = []

  for (const row of CATEGORY_ROWS) {
    const mainNorm = normalizeName(row.mainRaw || row.main)
    const subNorm = normalizeName(row.subRaw || row.sub)
    const sub2Norm = normalizeName(row.sub2Raw || row.sub2)

    if (mainNorm) {
      CANDIDATES.push({
        label: mainNorm,
        labelRaw: row.mainRaw || row.main,
        level: "main",
        mainRaw: row.mainRaw || row.main,
        subRaw: row.subRaw || row.sub || null,
        sub2Raw: row.sub2Raw || row.sub2 || null,
      })
    }

    if (subNorm) {
      CANDIDATES.push({
        label: subNorm,
        labelRaw: row.subRaw || row.sub,
        level: "sub",
        mainRaw: row.mainRaw || row.main,
        subRaw: row.subRaw || row.sub,
        sub2Raw: row.sub2Raw || row.sub2 || null,
      })
    }

    if (sub2Norm) {
      CANDIDATES.push({
        label: sub2Norm,
        labelRaw: row.sub2Raw || row.sub2,
        level: "sub2",
        mainRaw: row.mainRaw || row.main,
        subRaw: row.subRaw || row.sub || null,
        sub2Raw: row.sub2Raw || row.sub2,
      })
    }
  }
}

/**
 * Internal: build the Fuse.js index from CANDIDATES.
 *
 * We search only on the "label" field (normalized string), but
 * keep original hierarchy info in each candidate.
 */
function buildFuseIndex() {
  if (!CANDIDATES.length) {
    fuse = null
    return
  }

  const options = {
    includeScore: true,
    // We search over the "label" (normalized text).
    keys: ["label"],
    /**
     * Fuse threshold:
     * - 0.0 = only almost-exact matches
     * - 1.0 = everything is a match
     *
     * 0.6 is a good compromise for:
     * - "Circular Cable Assemblies (Shenzhen Signal)"
     *   vs "Circular Cable Assemblies"
     * - typos like "fiber optc cabls"
     */
    threshold: 0.6,
  }

  fuse = new Fuse(CANDIDATES, options)
}

/**
 * Try to resolve a raw category label (from product CSV) into a hierarchy entry.
 *
 * @param {string} rawCategory - The category string from the product data.
 * @returns {Promise<null | {
 *   main: string,
 *   sub: string | null,
 *   sub2: string | null,
 *   score: number,          // 0..1 similarity (1 = perfect)
 *   matchedOn: 'main' | 'sub' | 'sub2'
 * }>}
 */
async function resolveCategory(rawCategory) {
  if (!rawCategory) return null

  const inputNorm = normalizeName(rawCategory)
  if (!inputNorm) return null

  // Lazy-load CSV on first call, if not already loaded.
  if (!IS_LOADED) {
    await loadCategoryHierarchy()
  }

  if (!fuse) {
    return null
  }

  // Fuse.js returns an array of results sorted by score (lower score = better).
  const results = fuse.search(inputNorm, { limit: 1 })

  if (!results || results.length === 0) {
    return null
  }

  const { item, score } = results[0]

  // Convert Fuse "distance" score (0 = perfect match, higher = worse)
  // into a more intuitive similarity (1 = perfect, 0 = no match).
  const similarity = 1 - score

  /**
   * Guardrail: ignore very weak matches.
   *  - 0.0 = terrible match
   *  - 1.0 = perfect
   *
   * 0.4 means:
   *  - we still drop completely unrelated stuff
   *  - but allow noisy variants like
   *    "Circular Cable Assemblies (Shenzhen Signal)".
   */
  const MIN_SIMILARITY = 0.4

  if (similarity < MIN_SIMILARITY) {
    return null
  }

  return {
    main: item.mainRaw || null,
    sub: item.subRaw || null,
    sub2: item.sub2Raw || null,
    score: similarity,
    matchedOn: item.level, // "main" | "sub" | "sub2"
  }
}

/**
 * TEST-ONLY helper:
 * Allow Jest tests to inject a fake category hierarchy without reading the CSV.
 *
 * @param {Array<{ mainRaw: string, subRaw?: string, sub2Raw?: string }>} rows
 */
function __setCategoryRowsForTest(rows) {
  CATEGORY_ROWS = rows.map((r) => ({
    mainRaw: r.mainRaw,
    subRaw: r.subRaw || "",
    sub2Raw: r.sub2Raw || "",
    main: normalizeName(r.mainRaw),
    sub: normalizeName(r.subRaw),
    sub2: normalizeName(r.sub2Raw),
  }))

  IS_LOADED = true
  buildCandidatesFromRows()
  buildFuseIndex()
}

module.exports = {
  loadCategoryHierarchy,
  resolveCategory,
  __setCategoryRowsForTest, // for Jest tests
}
