const dotenv = require("dotenv");
dotenv.config();

const WooCommerceRestApi = require("woocommerce-rest-ts-api").default;
// const Bottleneck = require("bottleneck");
const { logger, logErrorToFile, logInfoToFile } = require("./logger");
const { limiter, scheduleApiRequest } = require('./job-manager');
const { createUniqueJobId } = require('./utils');

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

// Create a Bottleneck instance with appropriate settings
// const limiter = new Bottleneck({
//     maxConcurrent: 2, // Number of concurrent requests allowed - Limit to 2 concurrent 100-item requests at once
//     minTime: 1000, // Minimum time between requests (in milliseconds) - 500ms between each request
// });

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
const getProductIdByPartNumber = async (partNumber, currentIndex, totalProducts, fileKey) => {
    let attempts = 0;
    const action = 'getProductIdByPartNumber';

    while (attempts < 5) {
        // Create a unique job ID
        const jobId = createUniqueJobId(fileKey, action, currentIndex, attempts);

        try {

            // Use the centralized job scheduling function
            const response = await scheduleApiRequest(
                () => wooApi.get("products", { search: partNumber, per_page: 1 }), // Task function for API call
                { 
                    id: jobId,
                    context: { 
                        file: "woo-helpers.js", 
                        functionName: "getProductIdByPartNumber", 
                        part: `${partNumber}`
                    }
                }
            );

            // Check if the product was found
            if (response.data.length) {
                logger.info(`${currentIndex} / ${totalProducts} - Product ID ${response.data[0].id} found for Part Number ${partNumber} in file "${fileKey}"`);
                return response.data[0].id;
            } else {
                logErrorToFile(`${currentIndex} / ${totalProducts} - No product found for Part Number ${partNumber} in file "${fileKey}"`)
                return null;
            }

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