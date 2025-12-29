'use strict';

require('dotenv').config();

const WooCommerceRestApi = require('woocommerce-rest-ts-api').default;
const { logInfoToFile } = require('../../utils/logger');

const executionMode = process.env.EXECUTION_MODE || 'production';

function getWooConfig() {
  if (executionMode === 'test') {
    return {
      url: process.env.WOO_API_BASE_URL_TEST,
      consumerKey: process.env.WOO_API_CONSUMER_KEY_TEST,
      consumerSecret: process.env.WOO_API_CONSUMER_SECRET_TEST,
    };
  }

  if (executionMode === 'development') {
    return {
      url: process.env.WOO_API_BASE_URL_DEV,
      consumerKey: process.env.WOO_API_CONSUMER_KEY_DEV,
      consumerSecret: process.env.WOO_API_CONSUMER_SECRET_DEV,
    };
  }

  return {
    url: process.env.WOO_API_BASE_URL,
    consumerKey: process.env.WOO_API_CONSUMER_KEY,
    consumerSecret: process.env.WOO_API_CONSUMER_SECRET,
  };
}

const wooConfig = getWooConfig();

const wooApi = new WooCommerceRestApi({
  url: wooConfig.url,
  consumerKey: wooConfig.consumerKey,
  consumerSecret: wooConfig.consumerSecret,
  version: 'wc/v3',
  queryStringAuth: true,
  timeout: 60000,
});

logInfoToFile(`WooCommerce API initialized for ${executionMode} mode: ${wooConfig.url}`);

module.exports = {
  wooApi,
  wooConfig,
  executionMode,
  getWooConfig,
};
