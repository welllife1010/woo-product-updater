/**
 * =============================================================================
 * PM2 ECOSYSTEM CONFIGURATION
 * =============================================================================
 * 
 * USAGE:
 *   pm2 start ecosystem.config.js                    # Production (default)
 *   pm2 start ecosystem.config.js --env staging      # Staging/Test environment
 *   pm2 start ecosystem.config.js --env development  # Development environment
 * 
 * NOTES:
 *   - Credentials are loaded from .env file (not stored here for security)
 *   - EXECUTION_MODE tells the app which credentials to use from .env:
 *       'production'  → uses WOO_API_BASE_URL, WOO_API_CONSUMER_KEY, etc.
 *       'test'        → uses WOO_API_BASE_URL_TEST, WOO_API_CONSUMER_KEY_TEST, etc.
 *       'development' → uses WOO_API_BASE_URL_DEV, WOO_API_CONSUMER_KEY_DEV, etc.
 * 
 * =============================================================================
 */

module.exports = {
  apps: [
    // =========================================================================
    // APP 1: Main API Server (Express)
    // - Handles API endpoints
    // - Bull Dashboard at /admin/queues
    // - Creates batch jobs from CSV files
    // =========================================================================
    {
      name: 'woo-update-app',
      script: './index.js',
      cwd: '/home/ubuntu/woo-product-update',
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      watch: false,
      max_restarts: 10,
      node_args: ['-r', 'dotenv/config'],
      
      // Default environment (Production)
      env: {
        NODE_ENV: 'production',
        EXECUTION_MODE: 'production',
        PORT: 3000
      },
      
      // Staging environment (--env staging)
      // NOTE: Changed from env_stage to env_staging to match PM2 naming convention
      env_staging: {
        NODE_ENV: 'production',
        EXECUTION_MODE: 'test',  // 'test' uses WOO_API_*_TEST credentials
        PORT: 3000
      },
      
      // Development environment (--env development)
      env_development: {
        NODE_ENV: 'development',
        EXECUTION_MODE: 'development',  // uses WOO_API_*_DEV credentials
        PORT: 3000
      }
    },
    
    // =========================================================================
    // APP 2: Worker Process (BullMQ Job Processor)
    // - Processes batch jobs from the queue
    // - Updates WooCommerce products
    // =========================================================================
    {
      name: 'woo-worker',
      script: './worker.js',
      cwd: '/home/ubuntu/woo-product-update',
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      watch: false,
      max_restarts: 10,
      restart_delay: 5000,  // Wait 5s before restarting on crash
      node_args: ['-r', 'dotenv/config'],
      
      // Default environment (Production)
      env: {
        NODE_ENV: 'production',
        EXECUTION_MODE: 'production'
      },
      
      // Staging environment (--env staging)
      env_staging: {
        NODE_ENV: 'production',
        EXECUTION_MODE: 'test'
      },
      
      // Development environment (--env development)
      env_development: {
        NODE_ENV: 'development',
        EXECUTION_MODE: 'development'
      }
    },
    
    // =========================================================================
    // APP 3: CSV Mapping UI Server (NEW)
    // - Web interface for uploading CSVs
    // - Configure column mappings
    // - Accessible at port 4000
    // =========================================================================
    {
      name: 'csv-mapping-ui',
      script: './csv-mapping-server.js',
      cwd: '/home/ubuntu/woo-product-update',
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      watch: false,
      max_restarts: 10,
      node_args: ['-r', 'dotenv/config'],
      
      // Default environment (Production)
      env: {
        NODE_ENV: 'production',
        EXECUTION_MODE: 'production',
        CSV_MAPPING_PORT: 4000
      },
      
      // Staging environment (--env staging)
      env_staging: {
        NODE_ENV: 'production',
        EXECUTION_MODE: 'test',
        CSV_MAPPING_PORT: 4000
      },
      
      // Development environment (--env development)
      env_development: {
        NODE_ENV: 'development',
        EXECUTION_MODE: 'development',
        CSV_MAPPING_PORT: 4000
      }
    }
  ]
};