/*
================================================================================
FILE: src/batch/index.js
PURPOSE: Public entrypoint for the batch helper modules. Keeps imports tidy.
================================================================================
*/

const { normalizeText } = require("./text-utils");
const { isUpdateNeeded } = require("./compare");
const { processBatch } = require("./process-batch");

module.exports = {
  normalizeText,
  isUpdateNeeded,
  processBatch,
};
