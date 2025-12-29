/**
 * Backwards-compatible facade for WooCommerce helpers.
 *
 * This file used to contain:
 *  - env/config + Woo REST client initialization
 *  - Bottleneck retry listener side-effects
 *  - product lookup functions
 *
 * It has been split into smaller modules under src/services/woo/**.
 * Keep importing from "../services/woo-helpers" in existing call sites.
 */

'use strict';

const { initWooLimiterRetryHandling } = require('./woo/limiter-retry-handler');
const { wooApi } = require('./woo/woo-api');
const { retriedProducts } = require('./woo/retry-tracking');
const { getProductById } = require('./woo/products/get-product-by-id');
const { getProductIdByPartNumber } = require('./woo/products/get-product-id-by-part-number');

// Preserve previous behavior: attaching retry handling on module load.
initWooLimiterRetryHandling();

module.exports = {
  wooApi,
  getProductById,
  getProductIdByPartNumber,
  retriedProducts,
};