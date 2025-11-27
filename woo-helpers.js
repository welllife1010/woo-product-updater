/**
 * =============================================================================
 * FILE: woo-helpers.js
 * =============================================================================
 * 
 * PURPOSE:
 * Provides helper functions for interacting with the WooCommerce REST API.
 * Handles product lookups, caching, and retry logic.
 * 
 * KEY FUNCTIONS:
 * - getProductById: Fetch a product by its WooCommerce ID
 * - getProductIdByPartNumber: Find a product by part_number + manufacturer
 * 
 * BUG #5 FIX (2025):
 * The pagination in getProductIdByPartNumber was limited to maxPages * perPage
 * results (5 * 5 = 25 products). If the correct product was beyond page 5,
 * it would never be found and incorrectly marked as "missing."
 * 
 * THE FIX:
 * Now we check the `x-wp-total` header from WooCommerce to know the actual
 * total number of matching products. We continue paginating until we've
 * checked all results OR found a match.
 * 
 * =============================================================================
 */

// =============================================================================
// IMPORTS
// =============================================================================

require("dotenv").config();

// WooCommerce API client
const WooCommerceRestApi = require("woocommerce-rest-ts-api").default;

// Custom logging utilities
const { logger, logErrorToFile, logInfoToFile } = require("./logger");

// Redis client for caching
const { appRedis } = require("./queue");

// Job scheduling with rate limiting (Bottleneck)
// The limiter is CRITICAL for preventing 429/504 errors from WooCommerce
const { scheduleApiRequest, limiter } = require("./job-manager");

// Utility for creating unique job IDs
const { createUniqueJobId } = require("./utils");

// =============================================================================
// RATE LIMITING CONFIGURATION
// =============================================================================

/**
 * Configure retry behavior for rate-limited requests.
 * 
 * When WooCommerce returns 429 (Too Many Requests) or 504 (Gateway Timeout),
 * we use exponential backoff to wait before retrying.
 * 
 * The limiter (Bottleneck) from job-manager.js handles:
 *   - maxConcurrent: 2 (only 2 requests at a time)
 *   - minTime: 1000ms (minimum 1 second between requests)
 * 
 * This listener adds additional retry logic for specific error types.
 */
limiter.on("failed", async (error, jobInfo) => {
  const { retryCount } = jobInfo;
  const jobId = jobInfo.options?.id || "unknown";
  const context = jobInfo.options?.context || {};
  const { file, functionName, part } = context;

  logErrorToFile(
    `Bottleneck job failed | Job: ${jobId} | File: ${file} | ` +
    `Function: ${functionName} | Part: ${part} | Retry: ${retryCount + 1} | ` +
    `Error: ${error.message}`
  );

  // Track retried products for debugging
  if (part) retriedProducts.add(part);

  /**
   * Retry logic for transient errors:
   *   - ECONNRESET: Connection was reset (network issue)
   *   - socket hang up: Connection closed unexpectedly
   *   - 502: Bad Gateway (server overload)
   *   - 504: Gateway Timeout (request took too long)
   *   - 429: Too Many Requests (rate limited)
   *   - 499: Client Closed Request (nginx-specific)
   */
  const retryableErrors = /(ECONNRESET|socket hang up|502|504|429|499)/i;
  
  if (retryCount < 5 && retryableErrors.test(error.message)) {
    // Exponential backoff: 2s, 4s, 8s, 16s, 32s
    const retryDelay = 1000 * Math.pow(2, retryCount + 1);
    
    logInfoToFile(
      `Bottleneck: Scheduling retry in ${retryDelay / 1000}s for job ${jobId}`
    );
    
    return retryDelay; // Returning a number tells Bottleneck to retry after this delay
  }

  // If we've exceeded retries or it's not a retryable error, don't retry
  if (retryCount >= 5) {
    logErrorToFile(
      `Bottleneck: Job ${jobId} FAILED permanently after ${retryCount + 1} attempts ` +
      `for part "${part}"`
    );
  }

  // Return undefined = don't retry
  return undefined;
});

// =============================================================================
// WOOCOMMERCE API CLIENT SETUP
// =============================================================================

/**
 * Determine which WooCommerce environment to connect to based on EXECUTION_MODE.
 * 
 * Environments:
 * - production: Live WooCommerce store
 * - development: Development/staging store
 * - test: Test store (for automated testing)
 */
const executionMode = process.env.EXECUTION_MODE || "production";

/**
 * Get the appropriate API credentials based on execution mode.
 */
function getWooConfig() {
  if (executionMode === "test") {
    return {
      url: process.env.WOO_API_BASE_URL_TEST,
      consumerKey: process.env.WOO_API_CONSUMER_KEY_TEST,
      consumerSecret: process.env.WOO_API_CONSUMER_SECRET_TEST,
    };
  } else if (executionMode === "development") {
    return {
      url: process.env.WOO_API_BASE_URL_DEV,
      consumerKey: process.env.WOO_API_CONSUMER_KEY_DEV,
      consumerSecret: process.env.WOO_API_CONSUMER_SECRET_DEV,
    };
  } else {
    // Production (default)
    return {
      url: process.env.WOO_API_BASE_URL,
      consumerKey: process.env.WOO_API_CONSUMER_KEY,
      consumerSecret: process.env.WOO_API_CONSUMER_SECRET,
    };
  }
}

const wooConfig = getWooConfig();

/**
 * Initialize the WooCommerce REST API client.
 */
const wooApi = new WooCommerceRestApi({
  url: wooConfig.url,
  consumerKey: wooConfig.consumerKey,
  consumerSecret: wooConfig.consumerSecret,
  version: "wc/v3",                    // WooCommerce API version
  queryStringAuth: true,               // Use query string auth (not headers)
  timeout: 60000,                      // 60 second timeout
});

logInfoToFile(`WooCommerce API initialized for ${executionMode} mode: ${wooConfig.url}`);

// =============================================================================
// RETRY TRACKING
// =============================================================================

/**
 * Set of part numbers that have been retried.
 * Used for debugging and monitoring retry patterns.
 */
const retriedProducts = new Set();

// =============================================================================
// API FUNCTIONS
// =============================================================================

/**
 * Get a product's full details by its WooCommerce product ID.
 * 
 * This is typically called after finding the product ID via getProductIdByPartNumber.
 * Returns the complete product object including all meta_data.
 * 
 * @param {number} productId - The WooCommerce product ID
 * @param {string} fileKey - File identifier for logging
 * @param {number} currentIndex - Current row index for logging
 * @returns {Promise<Object|null>} - Product object or null if not found/error
 * 
 * @example
 * const product = await getProductById(12345, "products.csv", 100);
 * if (product) {
 *   console.log(product.name, product.meta_data);
 * }
 */
const getProductById = async (productId, fileKey, currentIndex) => {
  const action = "woo-helper_getProductById";
  let attempts = 0;
  const maxAttempts = 5;

  while (attempts < maxAttempts) {
    // Create unique job ID for tracking and deduplication
    const jobId = createUniqueJobId(fileKey, action, currentIndex, attempts);

    try {
      // Use the centralized job scheduler (handles rate limiting)
      const response = await scheduleApiRequest(
        () => wooApi.get(`products/${productId}`),
        {
          id: jobId,
          context: {
            file: "woo-helpers.js",
            functionName: "getProductById",
            part: `${productId}`,
          },
        }
      );

      // Success - return the product data
      return response.data;

    } catch (error) {
      attempts++;
      logErrorToFile(
        `getProductById() - Attempt ${attempts}/${maxAttempts} failed for product ID ${productId}: ${error.message}`
      );

      // Check if we've exhausted all attempts
      if (attempts >= maxAttempts) {
        logErrorToFile(
          `getProductById() - FAILED permanently after ${attempts} attempts for product ID ${productId}`
        );
        return null;
      }

      // Exponential backoff before retry
      const delay = Math.pow(2, attempts) * 1000; // 2s, 4s, 8s, 16s
      logInfoToFile(`getProductById() - Retrying in ${delay / 1000}s...`);
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  return null;
};

/**
 * =============================================================================
 * Find a WooCommerce product ID by part_number and manufacturer.
 * =============================================================================
 * 
 * This is the PRIMARY lookup function for matching CSV rows to WooCommerce products.
 * 
 * HOW IT WORKS:
 * 1. Check Redis cache first (fast path)
 * 2. If not cached, search WooCommerce API by part_number
 * 3. For each result, check if manufacturer matches
 * 4. If found, cache in Redis and return the product ID
 * 5. If not found after checking ALL results, return null
 * 
 * BUG #5 FIX:
 * Previously, we limited pagination to maxPages (5 pages = 25 products).
 * If the correct product was on page 6+, it would never be found.
 * 
 * NOW:
 * We use the `x-wp-total` header from WooCommerce to know the ACTUAL total
 * number of matching products, and continue paginating until we've checked
 * all of them or found a match.
 * 
 * @param {string} partNumber - The part number to search for
 * @param {string} manufacturer - The manufacturer to match (case-insensitive)
 * @param {number} currentIndex - Current row index for logging
 * @param {number} totalProducts - Total products in file for logging
 * @param {string} fileKey - File identifier for logging and caching
 * @returns {Promise<number|null>} - WooCommerce product ID or null if not found
 * 
 * @example
 * const productId = await getProductIdByPartNumber(
 *   "STM32F103C8T6",
 *   "STMicroelectronics",
 *   50,
 *   1000,
 *   "microcontrollers.csv"
 * );
 */
const getProductIdByPartNumber = async (
  partNumber,
  manufacturer,
  currentIndex,
  totalProducts,
  fileKey
) => {
  const action = "getProductIdByPartNumber";
  let attempts = 0;
  const maxAttempts = 5;

  // =========================================================================
  // CONFIGURATION
  // =========================================================================
  
  /**
   * perPage: How many products to fetch per API call.
   * Higher values = fewer API calls but more data per call.
   * WooCommerce maximum is typically 100.
   */
  const perPage = 10;

  /**
   * REMOVED: maxPages limit
   * 
   * OLD CODE (Bug #5):
   *   let maxPages = 5; // Only checks first 25 products!
   * 
   * NEW CODE:
   *   We now use x-wp-total header to determine actual total,
   *   and continue until all products are checked.
   */

  // =========================================================================
  // NORMALIZE MANUFACTURER FOR COMPARISON
  // =========================================================================
  /**
   * Manufacturer names can vary:
   *   - "STMicroelectronics" vs "stmicroelectronics" vs " STMicroelectronics "
   * 
   * We normalize to lowercase and trim whitespace for reliable comparison.
   */
  const normalizedManufacturer =
    typeof manufacturer === "string" ? manufacturer.trim().toLowerCase() : "";

  // =========================================================================
  // STEP 1: CHECK REDIS CACHE
  // =========================================================================
  /**
   * If we've already looked up this part_number + manufacturer combination,
   * return the cached result to avoid redundant API calls.
   * 
   * Cache key format: productId:{partNumber}:{manufacturer}
   * TTL: 24 hours (86400 seconds)
   */
  const cacheKey = `productId:${partNumber}:${normalizedManufacturer}`;

  /**
   * BUG FIX (2025): Redis returns strings, not numbers
   * 
   * PROBLEM:
   * Redis stores all values as strings. When we cached a product ID (number)
   * and retrieved it, we returned a STRING instead of a NUMBER.
   * 
   * This caused subtle bugs downstream:
   *   - `productId === 12345` would fail because "12345" !== 12345
   *   - Type checks like `typeof productId === 'number'` would fail
   *   - Comparisons with other product IDs could fail
   * 
   * THE FIX:
   * Parse the cached value back to an integer before returning.
   * Also validate that the parsed value is a valid number.
   */
  try {
    const cachedProductId = await appRedis.get(cacheKey);
    if (cachedProductId) {
      // BUG FIX: Parse string to number before returning
      const parsedId = parseInt(cachedProductId, 10);
      
      // Validate the parsed value is a valid number
      if (!isNaN(parsedId) && parsedId > 0) {
        logInfoToFile(
          `getProductIdByPartNumber() - ✅ CACHE HIT: Product ID ${parsedId} ` +
          `for Part: ${partNumber} | Manufacturer: ${manufacturer}`
        );
        return parsedId;
      } else {
        // Invalid cached value - log and continue to API lookup
        logInfoToFile(
          `getProductIdByPartNumber() - ⚠️ Invalid cached value "${cachedProductId}" ` +
          `for ${partNumber}, querying API...`
        );
      }
    }
  } catch (cacheError) {
    // Cache miss or error - continue with API lookup
    logInfoToFile(
      `getProductIdByPartNumber() - Cache miss for ${partNumber}, querying API...`
    );
  }

  // =========================================================================
  // STEP 2: SEARCH WOOCOMMERCE API WITH PAGINATION
  // =========================================================================
  
  while (attempts < maxAttempts) {
    const jobId = createUniqueJobId(fileKey, action, currentIndex, attempts);

    try {
      let page = 1;
      let totalProductsInWoo = null;  // Will be set from x-wp-total header
      let productsChecked = 0;

      // =====================================================================
      // PAGINATION LOOP - Continue until all products checked or match found
      // =====================================================================
      while (true) {
        logInfoToFile(
          `getProductIdByPartNumber() - 🔍 Searching page ${page} for Part: ${partNumber}`
        );

        // Make the API request
        const response = await scheduleApiRequest(
          () => wooApi.get("products", {
            search: partNumber,
            per_page: perPage,
            page: page,
          }),
          {
            id: `${jobId}_page${page}`,
            context: {
              file: "woo-helpers.js",
              functionName: "getProductIdByPartNumber",
              part: partNumber,
            },
          }
        );

        // =================================================================
        // BUG #5 FIX: Get total count from x-wp-total header
        // =================================================================
        /**
         * WooCommerce returns these headers:
         *   - x-wp-total: Total number of matching products
         *   - x-wp-totalpages: Total number of pages
         * 
         * We use x-wp-total to know when we've checked all products.
         */
        if (totalProductsInWoo === null) {
          // First page - extract total from headers
          const totalHeader = response.headers?.["x-wp-total"];
          totalProductsInWoo = totalHeader ? parseInt(totalHeader, 10) : 0;

          logInfoToFile(
            `getProductIdByPartNumber() - 📊 Total matching products in WooCommerce: ${totalProductsInWoo}`
          );

          // If no products match this search at all, exit early
          if (totalProductsInWoo === 0) {
            logInfoToFile(
              `getProductIdByPartNumber() - ❌ No products found for Part: ${partNumber}`
            );
            return null;
          }
        }

        // Check if this page returned any results
        const products = response.data || [];
        
        if (products.length === 0) {
          // No more products to check
          logInfoToFile(
            `getProductIdByPartNumber() - ❌ No more products on page ${page}. ` +
            `Checked ${productsChecked}/${totalProductsInWoo} products.`
          );
          break;
        }

        // =================================================================
        // Check each product on this page for manufacturer match
        // =================================================================
        for (const product of products) {
          productsChecked++;

          // Extract manufacturer from product meta_data
          const productManufacturer =
            product.meta_data
              ?.find((meta) => meta.key === "manufacturer")
              ?.value?.trim()
              .toLowerCase() || "";

          logInfoToFile(
            `getProductIdByPartNumber() - 🔎 [${productsChecked}/${totalProductsInWoo}] ` +
            `Checking Product ID ${product.id}: ` +
            `WooCommerce Manufacturer: "${productManufacturer}" ` +
            `vs CSV Manufacturer: "${normalizedManufacturer}"`
          );

          // Check for manufacturer match
          if (productManufacturer === normalizedManufacturer) {
            // ✅ FOUND IT!
            logInfoToFile(
              `getProductIdByPartNumber() - ✅ MATCH FOUND! ` +
              `Product ID ${product.id} for Part: ${partNumber} | Manufacturer: ${manufacturer}`
            );

            // Cache the result in Redis (TTL: 24 hours)
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

        // =================================================================
        // BUG #5 FIX: Check if we've exhausted all products
        // =================================================================
        /**
         * OLD CODE (buggy):
         *   if (page >= maxPages) break;  // Only checked first 25!
         * 
         * NEW CODE:
         *   Check if we've looked at all products based on x-wp-total
         */
        if (productsChecked >= totalProductsInWoo) {
          logInfoToFile(
            `getProductIdByPartNumber() - ✅ Checked all ${productsChecked} products. ` +
            `No manufacturer match found for Part: ${partNumber}`
          );
          break;
        }

        // Move to next page
        page++;

        // Safety limit to prevent infinite loops (in case of API issues)
        const maxSafetyPages = 50; // 50 pages * 10 per page = 500 products max
        if (page > maxSafetyPages) {
          logErrorToFile(
            `getProductIdByPartNumber() - ⚠️ Safety limit reached (${maxSafetyPages} pages). ` +
            `Stopping pagination for Part: ${partNumber}`
          );
          break;
        }
      }

      // If we get here, we've checked all products and found no match
      logErrorToFile(
        `getProductIdByPartNumber() - ❌ No manufacturer match found for Part: ${partNumber} ` +
        `after checking ${productsChecked} products in WooCommerce.`
      );
      return null;

    } catch (error) {
      // =================================================================
      // RETRY LOGIC
      // =================================================================
      attempts++;
      retriedProducts.add(partNumber);

      logErrorToFile(
        `getProductIdByPartNumber() - Attempt ${attempts}/${maxAttempts} failed: ${error.message}`
      );

      if (attempts >= maxAttempts) {
        logErrorToFile(
          `getProductIdByPartNumber() - FAILED permanently after ${attempts} attempts ` +
          `for Part: ${partNumber}`
        );
        return null;
      }

      // Exponential backoff
      const delay = Math.pow(2, attempts) * 1000;
      logInfoToFile(
        `getProductIdByPartNumber() - Retrying in ${delay / 1000}s...`
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  return null;
};

// =============================================================================
// EXPORTS
// =============================================================================

module.exports = {
  wooApi,
  getProductById,
  getProductIdByPartNumber,
  retriedProducts,
};