'use strict';

require('dotenv').config();

const WooCommerceRestApi = require('woocommerce-rest-ts-api').default;
const { logInfoToFile } = require('../../utils/logger');

const {
  resolveAppEnv,
  getWooConfig,
  requireNonEmpty,
} = require('../../config/runtime-env');

const appEnv = resolveAppEnv(process.env);
const wooConfig = getWooConfig(process.env, appEnv);

// Fail fast with a clear error if credentials are missing.
requireNonEmpty(wooConfig.url, 'WOO_API_BASE_URL_*');
requireNonEmpty(wooConfig.consumerKey, 'WOO_API_CONSUMER_KEY_*');
requireNonEmpty(wooConfig.consumerSecret, 'WOO_API_CONSUMER_SECRET_*');

const wooApi = new WooCommerceRestApi({
  url: wooConfig.url,
  consumerKey: wooConfig.consumerKey,
  consumerSecret: wooConfig.consumerSecret,
  version: 'wc/v3',
  queryStringAuth: true,
  timeout: 60000,
});

logInfoToFile(`WooCommerce API initialized for ${appEnv} environment: ${wooConfig.url}`);

module.exports = {
  wooApi,
  wooConfig,
  appEnv,
  // Backward-compat exports
  executionMode: appEnv,
  getWooConfig,
};
