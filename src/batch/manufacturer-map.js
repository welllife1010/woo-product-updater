// src/batch/manufacturer-map.js

// 1. A dictionary of known aliases → canonical name
const MANUFACTURER_ALIASES = {
  // NXP family
  "nxp": "NXP",
  "nxp semiconductors": "NXP",
  "nxp semiconductors n.v.": "NXP",
  "nxp usa inc.": "NXP",

  // Microchip
  "microchip": "Microchip Technology",
  "microchip technology": "Microchip Technology",

  // ST
  "stm": "STMicroelectronics",
  "st microelectronics": "STMicroelectronics",
  "stmicroelectronics": "STMicroelectronics",

  // AMD
  "advanced micro devices": "AMD",
  "analog devices inc./maxim integrated": "Analog Devices Inc.",

  // Renesas
  "renesas": "Renesas Electronics Corporation",
};

/**
 * normalizeManufacturerName(name)
 *
 * PURPOSE:
 *   Turn messy vendor manufacturer strings into a single
 *   canonical form that matches what you store in Woo.
 *
 * EXAMPLES:
 *   "NXP Semiconductors" → "NXP"
 *   "NXP"                → "NXP"
 *   "Microchip"          → "Microchip Technology"
 */
function normalizeManufacturerName(name) {
  if (!name) return "";

  const trimmed = String(name).trim();
  const key = trimmed.toLowerCase();

  // If we know this alias, return the canonical manufacturer
  if (MANUFACTURER_ALIASES[key]) {
    return MANUFACTURER_ALIASES[key];
  }

  // Otherwise, return the trimmed original
  return trimmed;
}

module.exports = {
  normalizeManufacturerName,
};
