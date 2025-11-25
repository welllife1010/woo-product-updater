// csv-mapping-store.js
//
// Small helper for reading csv-mappings.json and returning
// the list of CSV files that are ready to be processed.

const fs = require("fs");
const path = require("path");

const MAPPINGS_PATH = path.join(__dirname, "csv-mappings.json");

function loadMappings() {
  if (!fs.existsSync(MAPPINGS_PATH)) {
    return { files: [] };
  }
  return JSON.parse(fs.readFileSync(MAPPINGS_PATH, "utf8"));
}

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
};
