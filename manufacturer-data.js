// manufacturer-data.js
//
// Static, hand-maintained data about manufacturers.
// - CANONICAL_MANUFACTURERS: the official names you use in Woo.
// - MANUFACTURER_ALIASES: special cases where vendor text is very
//   different from the canonical name.

const CANONICAL_MANUFACTURERS = [
  "AMD",
  "Altera",
  "Analog Devices Inc.",
  "ChromLED",
  "Fairchild Semiconductor",
  "Formerica OptoElectronic",
  "Infineon Technologies",
  "Intel",
  "Lattice Semiconductor Corporation",
  "Linear Technology",
  "Matsuo",
  "Microchip Technology",
  "NXP",
  "Nemco Electronics",
  "Nexperia USA Inc.",
  "OptiFuse",
  "Panasonic Electronic Components",
  "Qualcomm",
  "Renesas Electronics Corporation",
  "Rohm Semiconductor",
  "STMicroelectronics",
  "Silicon Labs",
  "Skyworks Solutions Inc.",
  "Suntsu Electronics, Inc.",
  "TAEJIN Technology",
  "Texas Instruments",
  "Zilog",
  "onsemi",
];

const MANUFACTURER_ALIASES = {
  // NXP family
  "nxp semiconductors": "NXP",
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

  // Analog Devices / Maxim
  "analog devices inc./maxim integrated": "Analog Devices Inc.",

  // Renesas
  "renesas": "Renesas Electronics Corporation",
};

module.exports = {
  CANONICAL_MANUFACTURERS,
  MANUFACTURER_ALIASES,
};
