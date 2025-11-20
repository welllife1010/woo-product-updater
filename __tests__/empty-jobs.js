const { batchQueue } = require('../queue');

(async () => {
  try {
    await batchQueue.empty(); // This will remove all jobs (waiting, paused, delayed)
    console.log('All jobs have been removed from the queue.');
  } catch (error) {
    console.error('Error clearing jobs:', error);
  }
})();

// or we can use redis-cli to remove all jobs from the queue
// redis-cli FLUSHALL
