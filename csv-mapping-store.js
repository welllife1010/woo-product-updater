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

const MAPPINGS_PATH = path.join(__dirname, "csv-mappings.json");

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
  // If file doesn't exist, return empty structure
  if (!fs.existsSync(MAPPINGS_PATH)) {
    console.log(`[csv-mapping-store] ${MAPPINGS_PATH} not found, returning empty mappings`);
    return { files: [] };
  }
  
  // BUG FIX: Wrap JSON.parse in try-catch to prevent crashes from invalid JSON
  try {
    const fileContent = fs.readFileSync(MAPPINGS_PATH, "utf8");
    
    // Handle empty file case
    if (!fileContent || fileContent.trim() === "") {
      console.log(`[csv-mapping-store] ${MAPPINGS_PATH} is empty, returning empty mappings`);
      return { files: [] };
    }
    
    const parsed = JSON.parse(fileContent);
    
    // Validate the parsed structure has the expected shape
    if (!parsed || typeof parsed !== 'object') {
      console.error(`[csv-mapping-store] Invalid structure in ${MAPPINGS_PATH}: expected object`);
      return { files: [] };
    }
    
    // Ensure files array exists
    if (!Array.isArray(parsed.files)) {
      console.warn(`[csv-mapping-store] No 'files' array in ${MAPPINGS_PATH}, initializing empty`);
      parsed.files = [];
    }
    
    return parsed;
    
  } catch (error) {
    // BUG FIX: Log error and return safe default instead of crashing
    console.error(
      `[csv-mapping-store] ❌ Failed to parse ${MAPPINGS_PATH}: ${error.message}\n` +
      `Please check the file for valid JSON syntax (trailing commas, missing quotes, etc.)`
    );
    return { files: [] };
  }
}

function markFileAsCompleted(fileKey) {
  const mappingsPath = path.join(__dirname, "csv-mappings.json");
  if (!fs.existsSync(mappingsPath)) return;
  
  try {
    const raw = fs.readFileSync(mappingsPath, "utf-8");
    let data = JSON.parse(raw);
    
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
