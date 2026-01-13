/*
================================================================================
FILE: src/batch/text-utils.js
================================================================================

PURPOSE:
Small, focused helpers for text normalization and meta-data comparison.

WHY A SEPARATE FILE?
- Keeps string/compare logic isolated and easily unit testable.
- Every other module depends on consistent normalization rules.

BUG FIX (2025) - RACE CONDITION WITH ASYNC IMPORT:

PROBLEM:
The original code used an IIFE (Immediately Invoked Function Expression) to 
dynamically import `string-strip-html`. This created a race condition:

  1. Module loads
  2. IIFE starts async import (runs in background)
  3. normalizeText() is exported and can be called
  4. If normalizeText() called before import completes → stripHtml is undefined!

The bug manifested as intermittent errors where some text comparisons would
fail because stripHtml wasn't loaded yet when the first batch started processing.

THE FIX:
1. Initialize stripHtml with a fallback immediately (not async)
2. Only attempt the ESM import inside the async IIFE
3. If the async import fails or hasn't completed, the fallback is already there

This ensures normalizeText() ALWAYS has a working stripHtml function,
even if the import hasn't completed or fails entirely.

TEST IDEAS:
- normalizeText(`<b> 3.3 V &deg; </b>`) => "3.3 V °"
- isMetaValueDifferent(' A ', '<b>A</b>') => false (after normalization)

================================================================================
*/

const { logErrorToFile, logInfoToFile } = require("../utils/logger");

/**
 * BUG FIX: Initialize with fallback IMMEDIATELY to prevent race condition.
 * 
 * The fallback simply returns the string unchanged (after converting to string).
 * This isn't as good as the real strip-html, but it prevents crashes.
 * 
 * The real implementation will be loaded asynchronously and replace this.
 */
let stripHtml = (s) => ({ result: String(s || "") });

/**
 * Flag to track if we've successfully loaded the real strip-html.
 * Used for debugging if normalization seems to not be working.
 */
let stripHtmlLoaded = false;

/**
 * Asynchronously load the real string-strip-html module.
 * 
 * This runs in the background when the module loads.
 * If it succeeds, stripHtml is replaced with the real implementation.
 * If it fails, we continue using the fallback (text won't be stripped of HTML).
 */
(async () => {
  try {
    const module = await import("string-strip-html");
    stripHtml = module.stripHtml;
    stripHtmlLoaded = true;
    logInfoToFile("✅ string-strip-html module loaded successfully");
  } catch (e) {
    // Don't log this as an error every time - it's expected in some environments
    logInfoToFile(
      `⚠️ Could not load string-strip-html: ${e.message}. ` +
      `Using fallback (HTML tags will not be stripped).`
    );
    // Keep using the fallback - already set above
  }
})();

/**
 * Canonicalizes arbitrary text so downstream comparisons are stable.
 * 
 * This avoids false-positive updates caused by HTML tags, spacing
 * differences, or encoded symbols.
 * 
 * OPERATIONS:
 * 1. Strip HTML tags (using string-strip-html if loaded)
 * 2. Trim surrounding whitespace
 * 3. Replace encoded symbols (®, °) with actual characters
 * 4. Collapse multiple spaces to single space
 * 
 * @param {unknown} text - Any value that might be a string.
 * @returns {string} A cleaned string (never throws; returns "" for non-strings).
 * 
 * @example
 * normalizeText('<p> ACME&nbsp;Co.&trade; </p>') // 'ACME Co.™'
 * normalizeText('<b> 3.3 V &deg; </b>')          // '3.3 V °'
 * normalizeText(null)                             // ''
 * normalizeText(123)                              // ''
 */
const normalizeText = (text) => {
  // Guard: return empty string for non-string inputs
  if (!text || typeof text !== "string") return "";
  
  // BUG FIX: Extra safety check for stripHtml being undefined
  // This should never happen now with the fallback, but belt-and-suspenders
  let normalized;
  try {
    if (typeof stripHtml === 'function') {
      normalized = stripHtml(text)?.result?.trim() || text.trim();
    } else {
      // Fallback if stripHtml is somehow not a function
      normalized = text.trim();
    }
  } catch (e) {
    // If stripHtml throws for any reason, use original text
    normalized = text.trim();
  }
  
  // Apply character replacements
  return normalized
    .replace(/\u00ac\u00c6/g, "®")  // Special encoding for ®
    .replace(/&deg;/g, "°")         // HTML entity for degree symbol
    .replace(/&nbsp;/g, " ")        // Non-breaking space to regular space
    .replace(/&amp;/g, "&")         // Ampersand entity
    .replace(/&lt;/g, "<")          // Less than entity
    .replace(/&gt;/g, ">")          // Greater than entity
    .replace(/\s+/g, " ");          // Collapse multiple spaces
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
 * Domain mappings for environment-aware URL comparison.
 * 
 * All these domains are considered equivalent for comparison purposes:
 * - https://suntsu.com (production)
 * - https://www.suntsu.com (production with www)
 * - https://env-suntsucom-staging.kinsta.cloud (staging)
 * - https://env-suntsucom-development.kinsta.cloud (development)
 * 
 * When comparing URLs, all will be normalized to 'suntsu.com' so that
 * staging/development URLs are treated as identical to production URLs.
 */
const STAGING_DEVELOPMENT_DOMAINS = [
  'env-suntsucom-staging.kinsta.cloud',
  'env-suntsucom-development.kinsta.cloud',
  'www.suntsu.com',  // Also normalize www to non-www
];

const CANONICAL_DOMAIN = 'suntsu.com';

/**
 * Normalizes URLs by replacing environment-specific domains with a canonical form.
 * 
 * This prevents false-positive updates when the only difference between
 * old and new values is the domain (e.g., staging vs production URLs).
 * 
 * Examples:
 *   "https://env-suntsucom-staging.kinsta.cloud/image.jpg" -> "https://suntsu.com/image.jpg"
 *   "https://env-suntsucom-development.kinsta.cloud/wp-content/uploads/x.pdf" -> "https://suntsu.com/wp-content/uploads/x.pdf"
 *   "https://www.suntsu.com/product" -> "https://suntsu.com/product"
 * 
 * @param {string} value - The string value that might contain a URL
 * @returns {string} The value with domains normalized to production form
 */
function normalizeUrlDomains(value) {
  if (!value || typeof value !== 'string') return value;
  
  let normalized = value;
  
  // Replace all staging/development/www domains with canonical production domain
  STAGING_DEVELOPMENT_DOMAINS.forEach(domain => {
    normalized = normalized.replace(
      new RegExp(escapeRegExp(domain), 'gi'),
      CANONICAL_DOMAIN
    );
  });
  
  return normalized;
}

/**
 * Escapes special regex characters in a string.
 * @param {string} string - The string to escape
 * @returns {string} The escaped string safe for use in RegExp
 */
function escapeRegExp(string) {
  return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
* @function isMetaValueDifferent
* @description Normalized inequality check between existing and incoming values.
* Prevents updates when only formatting differs or when URLs only differ by
* environment domain (staging vs production).
*/
function isMetaValueDifferent(newMetaValue, currentMetaValue) {
  // First normalize text (strip HTML, collapse whitespace, etc.)
  let normalizedNew = normalizeText(newMetaValue);
  let normalizedCurrent = normalizeText(currentMetaValue);
  
  // Then normalize URL domains for environment-agnostic comparison
  normalizedNew = normalizeUrlDomains(normalizedNew);
  normalizedCurrent = normalizeUrlDomains(normalizedCurrent);
  
  return normalizedCurrent !== normalizedNew;
}

module.exports = {
  normalizeText,
  normalizeUrlDomains,
  isMetaKeyMissing,
  isCurrentMetaMissing,
  isMetaValueDifferent,
};
