const { batchQueue } = require('../queue');

(async () => {
  const completedJobs = await batchQueue.getJobs(['completed']);
  const failedJobs = await batchQueue.getJobs(['failed']);
  const activeJobs = await batchQueue.getJobs(['active']);
  const waitingJobs = await batchQueue.getJobs(['waiting']);

  console.log(`Completed Jobs: ${completedJobs.length}`);
  console.log(`Failed Jobs: ${failedJobs.length}`);
  console.log(`Active Jobs: ${activeJobs.length}`);
  console.log(`Waiting Jobs: ${waitingJobs.length}`);
})();

(async () => {
    const failedJobs = await batchQueue.getJobs(['failed']);
    failedJobs.forEach(job => {
      console.log(`Job ID: ${job.id}, Reason: ${job.failedReason}`);
    });
})();

// Redis Server Health
// redis-cli info memory
// redis-cli info stats
// redis-cli info clients