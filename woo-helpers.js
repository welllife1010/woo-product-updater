const dotenv = require("dotenv");
dotenv.config();

const WooCommerceRestApi = require("woocommerce-rest-ts-api").default;
// const Bottleneck = require("bottleneck");
const { logger, logErrorToFile, logInfoToFile } = require("./logger");
const { limiter, scheduleApiRequest } = require('./job-manager');
const { createUniqueJobId } = require('./utils');
const { redisClient } = require('./queue');

// Function to get WooCommerce API credentials based on execution mode
const getWooCommerceApiCredentials = (executionMode) => {
    return (executionMode === 'development') ? {
        url: process.env.WOO_API_BASE_URL_TEST,
        consumerKey: process.env.WOO_API_CONSUMER_KEY_TEST,
        consumerSecret: process.env.WOO_API_CONSUMER_SECRET_TEST,
        version: "wc/v3",
        timeout: 300000 // Set a longer timeout (in milliseconds)
    } : {
        url: process.env.WOO_API_BASE_URL,
        consumerKey: process.env.WOO_API_CONSUMER_KEY,
        consumerSecret: process.env.WOO_API_CONSUMER_SECRET,
        version: "wc/v3",
        timeout: 300000
    };
};

const wooApi = new WooCommerceRestApi(getWooCommerceApiCredentials(process.env.EXECUTION_MODE));

// Define a Set to keep track of products that were retried
const retriedProducts = new Set();

// Configure retry options to handle 504 or 429 errors
limiter.on("failed", async (error, jobInfo) => {
    //const jobId = jobInfo.options.id || "<unknown>";
    const jobId = createUniqueJobId(jobInfo.options.id, "woo-helpers_retry-setting", "", jobInfo.retryCount) || "<unknown>";
    const { file = "<unknown file>", functionName = "<unknown function>", part = "<unknown part>" } = jobInfo.options.context || {};
    const retryCount = jobInfo.retryCount || 0;

    logErrorToFile(
        `Retrying job "${jobId}" due to ${error.message}. | File: ${file} | Function: ${functionName} | Retry count: ${retryCount + 1}`
    );

    // Add part number to retriedProducts if a retry occurs
    if (part) retriedProducts.add(part);

    if (retryCount < 5 && /(ECONNRESET|socket hang up|502|504|429)/.test(error.message)) {
        const retryDelay = 1000 * Math.pow(2, jobInfo.retryCount); // Exponential backoff
        logger.warn(`Applying delay of ${retryDelay / 1000}s before retrying job ${jobId}`);
        logErrorToFile(`Retrying job due to ${error.message}. Retry count: ${retryCount + 1}`);
        return retryDelay;
    }

    if (retryCount >= 5) {
        logErrorToFile(`Job "${jobId}" failed permanently for part "${part}" after maximum retries due to ${error.message}.`);
    }

});

// Get product details by product ID
const getProductById = async (productId, fileKey, currentIndex) => {
    let attempts = 0;
    const action = 'woo-helper_getProductById';

    while (attempts < 5) {
        // Create a unique job ID
        const jobId = createUniqueJobId(fileKey, action, currentIndex, attempts);

        try {

            // Use the centralized job scheduling function
            const response = await scheduleApiRequest(
                () => wooApi.get(`products/${productId}`), // Task function for API call
                { 
                    id: jobId,
                    context: { 
                        file: "woo-helpers.js", 
                        functionName: "getProductById", 
                        part: `${productId}`
                    }
                }
            );
        
            return response.data; // Return product details on success

        } catch (error) {
            attempts++;
            logErrorToFile(`Retry attempt ${attempts} failed for job ID: ${jobId}. Error: ${error.message}`);

            if (attempts >= 5) {
                logErrorToFile(`getProductById function failed permanently after ${attempts} attempts for job ID: ${jobId} \n Error fetching product with ID ${productId}: ${error.response ?? error.message }`);
                return null;
            }

            const delay = Math.pow(2, attempts) * 1000;
            logInfoToFile(`Retrying job ID: ${jobId} after ${delay / 1000}s...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
};
  
// Find product ID by custom field "part_number"
const getProductIdByPartNumber = async (partNumber, manufacturer, currentIndex, totalProducts, fileKey) => {
    let attempts = 0;
    const action = 'getProductIdByPartNumber';
    let page = 1; // Start pagination
    let perPage = 5; // ✅ Fetch 5 products at a time
    let maxPages = 5; // ✅ Limit search to 5 pages

    // ✅ Check Redis cache before making WooCommerce API calls
    const cacheKey = `productId:${partNumber}:${manufacturer}`;
    const cachedProductId = await redisClient.get(cacheKey);
    if (cachedProductId) {
        logInfoToFile(`✅ Using cached Product ID ${cachedProductId} for Part Number: ${partNumber} | Manufacturer: ${manufacturer}`);
        return cachedProductId; // ✅ Return cached result
    }

    while (attempts < 5) {
        // Create a unique job ID
        const jobId = createUniqueJobId(fileKey, action, currentIndex, attempts);

        try {

            while (page <= maxPages) { // ✅ Limit to 5 pages to prevent unnecessary API calls

                const response = await scheduleApiRequest(
                    () => wooApi.get("products", { search: partNumber, per_page: perPage, page }), // ✅ Fetch multiple products
                    { 
                        id: jobId,
                        context: { file: "woo-helpers.js", functionName: "getProductIdByPartNumber", part: `${partNumber}` }
                    }
                );
    
                if (!response.data.length) {
                    logErrorToFile(`❌ No exact manufacturer match found for Part Number: ${partNumber} in file "${fileKey}" after checking ${page - 1} pages.`);
                    return null;
                }
    
                // ✅ Loop through results to find the correct manufacturer match
                for (const product of response.data) {
                    const productManufacturer = product.meta_data.find(meta => meta.key === "manufacturer")?.value?.trim() || "";
    
                    if (productManufacturer === manufacturer) {
                        logInfoToFile(`✅ Found exact match for Part Number: ${partNumber} | Manufacturer: ${manufacturer} in file "${fileKey}".`);
    
                        // ✅ Store result in Redis with TTL (e.g., expire after 24 hours)
                        await redisClient.set(cacheKey, product.id, { EX: 86400 });
    
                        return product.id; // ✅ Return the correct product
                    }
                }
    
                logInfoToFile(`🔄 No manufacturer match on page ${page} for Part Number: ${partNumber}. Checking next page...`);
                page++; // ✅ Continue searching the next batch
            
            }
    
            logErrorToFile(`❌ Max page limit reached (${maxPages}) for Part Number: ${partNumber}. No exact manufacturer match found.`);
            return null;

        } catch (error) {
            attempts++;
            logErrorToFile(`Attempt ${attempts} failed for job ID: ${jobId}. Error: ${error.message}`);

            if (attempts >= 5) {
                logErrorToFile(`Failed permanently after ${attempts} attempts for job ID: ${jobId}`);
                return null;
            }

            const delay = Math.pow(2, attempts) * 1000;
            logInfoToFile(`Retrying job ID: ${jobId} after ${delay / 1000}s...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
};


  module.exports = {
    wooApi,
    getProductIdByPartNumber,
    getProductById,
    retriedProducts
  };