/*
================================================================================
FILE: src/batch/text-utils.js
PURPOSE: Small, focused helpers for text normalization and meta-data comparison.
WHY A SEPARATE FILE?
- Keeps string/compare logic isolated and easily unit testable.
- Every other module depends on consistent normalization rules.


TEST IDEAS:
- normalizeText(`<b> 3.3 V &deg; </b>`) => "3.3 V °"
- isMetaValueDifferent(' A ', '<b>A</b>') => false (after normalization)
================================================================================
*/
const { logErrorToFile } = require("../../logger");

/**
* We dynamically import `string-strip-html` so this module can be required
* even when the package isn't yet installed (e.g., during partial builds).
* If import fails, we fall back to a no-op stripper to avoid hard crashes.
*/
let stripHtml;
(async () => {
  try {
    stripHtml = (await import("string-strip-html")).stripHtml;
  } catch (e) {
    logErrorToFile(`Failed to import string-strip-html: ${e.message}`);
    stripHtml = (s) => ({ result: String(s || "") });
  }
})();

/**
* @function normalizeText
* @description Canonicalizes arbitrary text so downstream comparisons are stable.
* This avoids false-positive updates caused by HTML tags, spacing
* differences, or encoded symbols.
* @param {unknown} text - Any value that might be a string.
* @returns {string} A cleaned string (never throws; returns "" for non-strings).
* @example
* normalizeText('<p> ACME&nbsp;Co.&trade; </p>') // 'ACME Co.™'
* @notes
* - Removes HTML tags using stripHtml
* - Trims surrounding whitespace
* - Replaces specific sequences (e.g., &deg;) to human symbols
* - Collapses all whitespace blocks to single spaces
*/
const normalizeText = (text) => {
  if (!text || typeof text !== "string") return "";
  const normalized = stripHtml(text)?.result.trim() || "";
  return normalized
    .replace(/\u00ac\u00c6/g, "®")
    .replace(/&deg;/g, "°")
    .replace(/\s+/g, " ");
};

/**
* @function isMetaKeyMissing
* @description Checks whether a given meta key is effectively missing on the
* current product given an incoming (new) meta value.
* @param {any} newMetaValue - Incoming value from CSV mapping.
* @param {{key:string,value:any}|undefined} currentMeta - Existing meta object.
* @returns {boolean}
*/
function isMetaKeyMissing(newMetaValue, currentMeta) {
  return (!newMetaValue && !currentMeta) || (!newMetaValue && !currentMeta?.value);
}

/**
* @function isCurrentMetaMissing
* @description True when the incoming CSV has a value but the product has no
* meta entry yet (we should add it).
*/
function isCurrentMetaMissing(newMetaValue, currentMeta) {
  return newMetaValue && !currentMeta;
}

/**
* @function isMetaValueDifferent
* @description Normalized inequality check between existing and incoming values.
* Prevents updates when only formatting differs.
*/
function isMetaValueDifferent(newMetaValue, currentMetaValue) {
  return normalizeText(currentMetaValue) !== normalizeText(newMetaValue);
}

module.exports = {
  normalizeText,
  isMetaKeyMissing,
  isCurrentMetaMissing,
  isMetaValueDifferent,
};
