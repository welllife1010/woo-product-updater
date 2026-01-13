// manufacturer-data.js
//
// Static manufacturer reference data.
//
// This module is intentionally simple:
// - CANONICAL_MANUFACTURERS: a baseline list used for exact/fuzzy matching
// - MANUFACTURER_ALIASES: normalized input -> canonical output overrides
//
// If you have an existing manufacturer list from the old project structure,
// paste it here. The resolver will also auto-extend at runtime via
// `custom-manufacturers.json`.

/**
 * @type {string[]}
 */
const CANONICAL_MANUFACTURERS = [
  // Example entries (safe defaults). Add your real canonical list as needed.
  // "Texas Instruments",
  // "Analog Devices",
  // "NXP",
];

/**
 * Keys MUST be normalized (lowercase, punctuation stripped) to match
 * manufacturer-resolver.js normalizeForCompare().
 *
 * @type {Record<string, string>}
 */
const MANUFACTURER_ALIASES = {
  // Example:
  // "ti": "Texas Instruments",
  // "analog devices inc": "Analog Devices",
};

module.exports = {
  CANONICAL_MANUFACTURERS,
  MANUFACTURER_ALIASES,
};
