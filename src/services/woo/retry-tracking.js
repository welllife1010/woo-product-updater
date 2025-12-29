'use strict';

// Tracks part numbers that have been retried (useful for debugging / monitoring)
// Kept as a shared singleton Set.
const retriedProducts = new Set();

module.exports = {
  retriedProducts,
};
