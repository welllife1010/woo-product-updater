/**
 * =============================================================================
 * FILE: csv-mapping-store.js
 * =============================================================================
 * 
 * PURPOSE:
 * Helper for reading csv-mappings.json and returning the list of CSV files
 * that are ready to be processed.
 * 
 * BUG FIX (2025): Added try-catch to JSON parsing
 * 
 * PROBLEM:
 * The original loadMappings() didn't wrap JSON.parse() in a try-catch.
 * If csv-mappings.json contained invalid JSON (e.g., trailing comma, syntax
 * error from manual editing), the entire application would crash with an
 * unhandled exception.
 * 
 * THE FIX:
 * Wrap JSON.parse() in try-catch and return a safe default on error.
 * Also log the error so admins know the config file needs fixing.
 * 
 * =============================================================================
 */

const fs = require("fs");
const path = require("path");

// =============================================================================
// PATH RESOLUTION
// =============================================================================
//
// After the refactor/code-splitting, this module lives under `src/services/`.
// The canonical `csv-mappings.json` lives in the REPO ROOT (same file the UI
// reads/writes and the worker auto-completion logic updates).
//
// Previously this module accidentally looked for `src/services/csv-mappings.json`,
// which caused the main app (index.js -> s3-helpers) to see “no READY files”.
//
// We treat the repo-root file as the source of truth, with a best-effort
// fallback to the legacy location if it exists.

const REPO_ROOT = path.resolve(__dirname, "..", "..");
const PRIMARY_MAPPINGS_PATH = path.join(REPO_ROOT, "csv-mappings.json");
const LEGACY_MAPPINGS_PATH = path.join(__dirname, "csv-mappings.json");

function getMappingsPath() {
  // Prefer repo root mapping file.
  if (fs.existsSync(PRIMARY_MAPPINGS_PATH)) return PRIMARY_MAPPINGS_PATH;
  // Fall back to legacy location if present.
  if (fs.existsSync(LEGACY_MAPPINGS_PATH)) return LEGACY_MAPPINGS_PATH;
  // Default to repo root going forward.
  return PRIMARY_MAPPINGS_PATH;
}

/**
 * Load the csv-mappings.json configuration file.
 * 
 * This file contains the list of CSV files and their column mappings.
 * 
 * @returns {Object} The parsed mappings object, or { files: [] } on error
 * 
 * @example
 * const store = loadMappings();
 * store.files.forEach(file => console.log(file.fileKey));
 */
function loadMappings() {
  const mappingsPath = getMappingsPath();

  // If file doesn't exist, return empty structure
  if (!fs.existsSync(mappingsPath)) {
    console.log(
      `[csv-mapping-store] ${mappingsPath} not found, creating empty mappings at repo root`
    );
    try {
      fs.writeFileSync(mappingsPath, JSON.stringify({ files: [] }, null, 2));
    } catch (error) {
      console.error(
        `[csv-mapping-store] ❌ Failed to create ${mappingsPath}: ${error.message}`
      );
      return { files: [] };
    }
  }
  
  // BUG FIX: Wrap JSON.parse in try-catch to prevent crashes from invalid JSON
  try {
    const fileContent = fs.readFileSync(mappingsPath, "utf8");
    
    // Handle empty file case
    if (!fileContent || fileContent.trim() === "") {
      console.log(`[csv-mapping-store] ${mappingsPath} is empty, returning empty mappings`);
      return { files: [] };
    }
    
    const parsed = JSON.parse(fileContent);
    
    // Validate the parsed structure has the expected shape
    if (!parsed || typeof parsed !== 'object') {
      console.error(`[csv-mapping-store] Invalid structure in ${mappingsPath}: expected object`);
      return { files: [] };
    }
    
    // Ensure files array exists
    if (!Array.isArray(parsed.files)) {
      console.warn(`[csv-mapping-store] No 'files' array in ${mappingsPath}, initializing empty`);
      parsed.files = [];
    }
    
    return parsed;
    
  } catch (error) {
    // BUG FIX: Log error and return safe default instead of crashing
    console.error(
      `[csv-mapping-store] ❌ Failed to parse ${mappingsPath}: ${error.message}\n` +
      `Please check the file for valid JSON syntax (trailing commas, missing quotes, etc.)`
    );
    return { files: [] };
  }
}

function markFileAsCompleted(fileKey) {
  const mappingsPath = getMappingsPath();
  if (!fs.existsSync(mappingsPath)) return;
  
  try {
    const raw = fs.readFileSync(mappingsPath, "utf-8");
    let data = JSON.parse(raw);

    // Support both legacy array format and current { files: [] } format.
    let files = Array.isArray(data) ? data : (data.files || []);
    const file = files.find(f => f.fileKey === fileKey);
    
    if (file) {
      file.status = "completed";
      file.completedAt = new Date().toISOString();
      fs.writeFileSync(mappingsPath, JSON.stringify(data, null, 2));
    }
  } catch (error) {
    console.error(`Error marking file completed: ${error.message}`);
  }
}

// Add to module.exports

/**
 * getReadyCsvFiles()
 * @returns {Array<{fileKey:string, headers:string[], mapping:Object, uploadedAt:string}>}
 */
function getReadyCsvFiles() {
  const store = loadMappings();
  return (store.files || []).filter((f) => f.status === "ready");
}

/**
 * getMappingForFile(fileKey)
 */
function getMappingForFile(fileKey) {
  const store = loadMappings();
  return (store.files || []).find(
    (f) => f.fileKey === fileKey && f.status === "ready"
  );
}

module.exports = {
  loadMappings,
  getReadyCsvFiles,
  getMappingForFile,
  markFileAsCompleted,
};
