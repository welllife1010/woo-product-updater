'use strict';

const { scheduleApiRequest } = require('../../job-manager');
const { appRedis } = require('../../queue');
const { createUniqueJobId } = require('../../../utils/utils');
const { logErrorToFile, logInfoToFile } = require('../../../utils/logger');
const { retriedProducts } = require('../retry-tracking');
const { wooApi } = require('../woo-api');

/**
 * Find WooCommerce product ID by part_number + manufacturer.
 * Uses Redis caching and paginates based on x-wp-total header.
 */
async function getProductIdByPartNumber(
  partNumber,
  manufacturer,
  currentIndex,
  totalProducts,
  fileKey
) {
  const action = 'getProductIdByPartNumber';
  let attempts = 0;
  const maxAttempts = 5;

  const perPage = 10;

  const normalizedManufacturer =
    typeof manufacturer === 'string' ? manufacturer.trim().toLowerCase() : '';

  const cacheKey = `productId:${partNumber}:${normalizedManufacturer}`;

  const describeWooError = (err) => {
    const status = err?.response?.status;
    const code = err?.code;
    const retryAfter = err?.response?.headers?.['retry-after'];
    // Woo/WordPress often returns `{ message, code, data: { status } }`.
    const apiMessage =
      (typeof err?.response?.data?.message === 'string' && err.response.data.message) ||
      (typeof err?.response?.data === 'string' && err.response.data) ||
      '';

    const parts = [];
    if (status) parts.push(`HTTP ${status}`);
    if (code) parts.push(`code=${code}`);
    if (retryAfter) parts.push(`retry-after=${retryAfter}`);
    const head = parts.length ? parts.join(' | ') : 'unknown error';
    const msg = apiMessage ? ` | ${apiMessage}` : (err?.message ? ` | ${err.message}` : '');
    return `${head}${msg}`;
  };

  const getRetryDelayMs = (err, attempt) => {
    const retryAfter = err?.response?.headers?.['retry-after'];
    if (retryAfter) {
      const asNum = Number(retryAfter);
      if (!Number.isNaN(asNum) && asNum > 0) return asNum * 1000;
    }
    return Math.pow(2, attempt) * 1000;
  };

  const isRetryableWooError = (err) => {
    const status = err?.response?.status;
    if (status) {
      // Retryable HTTP statuses
      if ([408, 429, 500, 502, 503, 504].includes(status)) return true;
      // Most other 4xx are configuration/auth/validation problems
      return false;
    }

    const code = (err?.code || '').toString();
    if (/^(ECONNRESET|ETIMEDOUT|EAI_AGAIN|ENOTFOUND|ECONNREFUSED)$/.test(code)) return true;

    const msg = (err?.message || '').toString();
    return /(socket hang up|network error|timeout|timed out|502|503|504|429)/i.test(msg);
  };

  try {
    const cachedProductId = await appRedis.get(cacheKey);
    if (cachedProductId) {
      const parsedId = parseInt(cachedProductId, 10);
      if (!isNaN(parsedId) && parsedId > 0) {
        logInfoToFile(
          `getProductIdByPartNumber() - ✅ CACHE HIT: Product ID ${parsedId} ` +
            `for Part: ${partNumber} | Manufacturer: ${manufacturer}`
        );
        return parsedId;
      }

      logInfoToFile(
        `getProductIdByPartNumber() - ⚠️ Invalid cached value "${cachedProductId}" ` +
          `for ${partNumber}, querying API...`
      );
    }
  } catch (cacheError) {
    logInfoToFile(
      `getProductIdByPartNumber() - Cache miss for ${partNumber}, querying API...`
    );
  }

  while (attempts < maxAttempts) {
    const jobId = createUniqueJobId(fileKey, action, currentIndex, attempts);

    try {
      let page = 1;
      let totalProductsInWoo = null;
      let productsChecked = 0;

      while (true) {
        logInfoToFile(
          `getProductIdByPartNumber() - 🔍 Searching page ${page} for Part: ${partNumber}`
        );

        const response = await scheduleApiRequest(
          () =>
            wooApi.get('products', {
              search: partNumber,
              per_page: perPage,
              page: page,
            }),
          {
            id: `${jobId}_page${page}`,
            context: {
              file: 'woo-helpers.js',
              functionName: 'getProductIdByPartNumber',
              part: partNumber,
            },
          }
        );

        if (totalProductsInWoo === null) {
          const totalHeader = response.headers?.['x-wp-total'];
          totalProductsInWoo = totalHeader ? parseInt(totalHeader, 10) : 0;

          logInfoToFile(
            `getProductIdByPartNumber() - 📊 Total matching products in WooCommerce: ${totalProductsInWoo}`
          );

          if (totalProductsInWoo === 0) {
            logInfoToFile(
              `getProductIdByPartNumber() - ❌ No products found for Part: ${partNumber}`
            );
            return null;
          }
        }

        const products = response.data || [];

        if (products.length === 0) {
          logInfoToFile(
            `getProductIdByPartNumber() - ❌ No more products on page ${page}. ` +
              `Checked ${productsChecked}/${totalProductsInWoo} products.`
          );
          break;
        }

        for (const product of products) {
          productsChecked++;

          const productManufacturer =
            product.meta_data
              ?.find((meta) => meta.key === 'manufacturer')
              ?.value?.trim()
              .toLowerCase() || '';

          logInfoToFile(
            `getProductIdByPartNumber() - 🔎 [${productsChecked}/${totalProductsInWoo}] ` +
              `Checking Product ID ${product.id}: ` +
              `WooCommerce Manufacturer: "${productManufacturer}" ` +
              `vs CSV Manufacturer: "${normalizedManufacturer}"`
          );

          if (productManufacturer === normalizedManufacturer) {
            logInfoToFile(
              `getProductIdByPartNumber() - ✅ MATCH FOUND! ` +
                `Product ID ${product.id} for Part: ${partNumber} | Manufacturer: ${manufacturer}`
            );

            try {
              await appRedis.set(cacheKey, product.id, { EX: 86400 });
              logInfoToFile(
                `getProductIdByPartNumber() - ✅ Cached Product ID ${product.id} in Redis`
              );
            } catch (cacheError) {
              logErrorToFile(
                `getProductIdByPartNumber() - ⚠️ Failed to cache: ${cacheError.message}`
              );
            }

            return product.id;
          }
        }

        if (productsChecked >= totalProductsInWoo) {
          logInfoToFile(
            `getProductIdByPartNumber() - ✅ Checked all ${productsChecked} products. ` +
              `No manufacturer match found for Part: ${partNumber}`
          );
          break;
        }

        page++;

        const maxSafetyPages = 50;
        if (page > maxSafetyPages) {
          logErrorToFile(
            `getProductIdByPartNumber() - ⚠️ Safety limit reached (${maxSafetyPages} pages). ` +
              `Stopping pagination for Part: ${partNumber}`
          );
          break;
        }
      }

      logErrorToFile(
        `getProductIdByPartNumber() - ❌ No manufacturer match found for Part: ${partNumber} ` +
          `after checking ${productsChecked} products in WooCommerce.`
      );
      return null;
    } catch (error) {
      attempts++;
      retriedProducts.add(partNumber);

      const status = error?.response?.status;
      const retryable = isRetryableWooError(error);
      const details = describeWooError(error);

      // Always log the full reason to error log.
      logErrorToFile(
        `getProductIdByPartNumber() - Attempt ${attempts}/${maxAttempts} failed (retryable=${retryable}): ${details}`,
        error
      );

      // Fail fast on non-retryable errors (e.g., 401/403 auth, 400 validation, 404 wrong endpoint).
      if (!retryable) {
        const hint = status === 401 || status === 403
          ? ' (check Woo API keys/secrets + site URL for this environment)'
          : '';
        throw new Error(
          `WooCommerce lookup failed with non-retryable error: ${details}${hint}`
        );
      }

      if (attempts >= maxAttempts) {
        logErrorToFile(
          `getProductIdByPartNumber() - FAILED permanently after ${attempts} attempts ` +
            `for Part: ${partNumber}`
        );
        return null;
      }

      const delay = getRetryDelayMs(error, attempts);
      logInfoToFile(
        `getProductIdByPartNumber() - Retrying in ${Math.round(delay / 1000)}s... (reason: ${describeWooError(error)})`
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  return null;
}

module.exports = {
  getProductIdByPartNumber,
};
