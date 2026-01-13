'use strict';

const { logErrorToFile, logInfoToFile } = require('../../utils/logger');
const { limiter } = require('../job-manager');
const { retriedProducts } = require('./retry-tracking');

let isInitialized = false;

/**
 * Attaches the Bottleneck "failed" listener once.
 *
 * This used to live inside src/services/woo-helpers.js.
 * It is intentionally initialized as a side-effect from the woo-helpers facade
 * to preserve existing behavior.
 */
function initWooLimiterRetryHandling() {
  if (isInitialized) return;
  isInitialized = true;

  limiter.on('failed', async (error, jobInfo) => {
    const { retryCount } = jobInfo;
    const jobId = jobInfo.options?.id || 'unknown';
    const context = jobInfo.options?.context || {};
    const { file, functionName, part } = context;

    logErrorToFile(
      `Bottleneck job failed | Job: ${jobId} | File: ${file} | ` +
        `Function: ${functionName} | Part: ${part} | Retry: ${retryCount + 1} | ` +
        `Error: ${error.message}`
    );

    if (part) retriedProducts.add(part);

    const retryableErrors = /(ECONNRESET|socket hang up|502|504|429|499)/i;

    if (retryCount < 5 && retryableErrors.test(error.message)) {
      const retryDelay = 1000 * Math.pow(2, retryCount + 1);
      logInfoToFile(`Bottleneck: Scheduling retry in ${retryDelay / 1000}s for job ${jobId}`);
      return retryDelay;
    }

    if (retryCount >= 5) {
      logErrorToFile(
        `Bottleneck: Job ${jobId} FAILED permanently after ${retryCount + 1} attempts ` +
          `for part "${part}"`
      );
    }

    return undefined;
  });
}

module.exports = {
  initWooLimiterRetryHandling,
};
