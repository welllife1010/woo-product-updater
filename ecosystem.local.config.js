module.exports = {
  apps: [
    {
      name: 'woo-update-app',
      script: './index.js',
      node_args: ['-r', 'dotenv/config'],
      env: {
        NODE_ENV: 'development',
        EXECUTION_MODE: 'development',
        PORT: 3000,
      },
    },
    {
      name: 'woo-worker',
      script: './worker.js',
      node_args: ['-r', 'dotenv/config'],
      env: {
        NODE_ENV: 'development',
        EXECUTION_MODE: 'development',
      },
    },
    {
      name: 'csv-mapping-ui',
      script: './csv-mapping-server.js',
      node_args: ['-r', 'dotenv/config'],
      env: {
        NODE_ENV: 'development',
        EXECUTION_MODE: 'development',
        CSV_MAPPING_PORT: 4000,
      },
    },
  ],
};