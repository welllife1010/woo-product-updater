const { batchQueue } = require('../queue');

(async () => {
    const job = await batchQueue.add({ batch: [{ test: 'data' }], fileKey: 'test-file', totalProductsInFile: 100, lastProcessedRow: 1, batchSize: 1 });
    console.log('Enqueued job ID:', job.id);
})();
