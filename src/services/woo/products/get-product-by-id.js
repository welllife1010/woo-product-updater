'use strict';

const { scheduleApiRequest } = require('../../job-manager');
const { createUniqueJobId } = require('../../../utils/utils');
const { logErrorToFile, logInfoToFile } = require('../../../utils/logger');
const { wooApi } = require('../woo-api');

/**
 * Fetch a product by its WooCommerce product ID.
 * Returns the full product object (including meta_data) or null.
 */
async function getProductById(productId, fileKey, currentIndex) {
  const action = 'woo-helper_getProductById';
  let attempts = 0;
  const maxAttempts = 5;

  while (attempts < maxAttempts) {
    const jobId = createUniqueJobId(fileKey, action, currentIndex, attempts);

    try {
      const response = await scheduleApiRequest(
        () => wooApi.get(`products/${productId}`),
        {
          id: jobId,
          context: {
            file: 'woo-helpers.js',
            functionName: 'getProductById',
            part: `${productId}`,
          },
        }
      );

      return response.data;
    } catch (error) {
      attempts++;
      logErrorToFile(
        `getProductById() - Attempt ${attempts}/${maxAttempts} failed for product ID ${productId}: ${error.message}`
      );

      if (attempts >= maxAttempts) {
        logErrorToFile(
          `getProductById() - FAILED permanently after ${attempts} attempts for product ID ${productId}`
        );
        return null;
      }

      const delay = Math.pow(2, attempts) * 1000;
      logInfoToFile(`getProductById() - Retrying in ${delay / 1000}s...`);
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  return null;
}

module.exports = {
  getProductById,
};
