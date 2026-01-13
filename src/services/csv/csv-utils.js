/**
 * Small, pure CSV utilities.
 */

function normalizeHeaderKey(rawKey) {
  // Keep header normalization consistent across the whole pipeline.
  // We intentionally:
  // - lower-case
  // - trim
  // - remove wildcard asterisks (often present in vendor CSVs)
  // - replace ANY non [a-z0-9_] with underscores
  // - collapse duplicate underscores
  // - trim leading/trailing underscores
  //
  // This avoids mismatches like:
  //   "Voltage / Supply" -> "voltage_supply"
  // rather than a slash-containing key that downstream mapping can't match.
  return String(rawKey || "")
    .replace(/\*/g, "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function normalizeRowKeys(row) {
  return Object.keys(row || {}).reduce((acc, rawKey) => {
    const safeKey = normalizeHeaderKey(rawKey);
    acc[safeKey] = row[rawKey];
    return acc;
  }, {});
}

module.exports = {
  normalizeHeaderKey,
  normalizeRowKeys,
};
