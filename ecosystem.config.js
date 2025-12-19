/**
 * =============================================================================
 * PM2 ECOSYSTEM CONFIGURATION
 * =============================================================================
 * 
 * This file configures PM2 process manager for the WooCommerce Product Updater.
 * 
 * ARCHITECTURE:
 * ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
 * │  index.js       │     │  worker.js      │     │ csv-mapping-    │
 * │  (Main App)     │────▶│  (Worker)       │     │ server.js (UI)  │
 * │                 │     │                 │     │                 │
 * │ - Reads CSV     │     │ - Processes     │     │ - Admin UI      │
 * │ - Creates jobs  │     │   batch jobs    │     │ - Upload CSVs   │
 * │ - Enqueues to   │     │ - Updates Woo   │     │ - Map columns   │
 * │   Redis         │     │ - Tracks        │     │                 │
 * └─────────────────┘     │   progress      │     └─────────────────┘
 *         │               └─────────────────┘              │
 *         │                       │                        │
 *         └───────────────────────┼────────────────────────┘
 *                                 ▼
 *                    ┌─────────────────────┐
 *                    │       Redis         │
 *                    │   (Job Queue +      │
 *                    │    Progress Data)   │
 *                    └─────────────────────┘
 * 
 * =============================================================================
 * USAGE:
 * =============================================================================
 * 
 *   # Start with production environment (DEFAULT - uses suntsu.com)
 *   pm2 start ecosystem.config.js
 * 
 *   # Start with staging environment (uses Kinsta staging site)
 *   pm2 start ecosystem.config.js --env staging
 * 
 *   # Start with development environment (uses WPEngine dev site)
 *   pm2 start ecosystem.config.js --env development
 * 
 *   # Other common commands:
 *   pm2 stop all                    # Stop all processes
 *   pm2 restart all                 # Restart all processes
 *   pm2 logs                        # View all logs
 *   pm2 logs woo-update-app         # View specific process logs
 *   pm2 monit                       # Real-time monitoring dashboard
 *   pm2 status                      # Check process status
 * 
 * =============================================================================
 * ENVIRONMENT MAPPING:
 * =============================================================================
 * 
 *   PM2 Environment    │ EXECUTION_MODE │ WooCommerce Site
 *   ───────────────────┼────────────────┼──────────────────────────────────
 *   (default/env)      │ production     │ https://suntsu.com
 *   --env staging      │ test           │ https://env-suntsucom-staging.kinsta.cloud
 *   --env development  │ development    │ https://suntsudev.wpenginepowered.com
 * 
 * =============================================================================
 * SECURITY NOTE:
 * =============================================================================
 * 
 *   ⚠️  API keys and secrets should be in a .env file on the server,
 *       NOT committed to git. Ensure:
 *       1. .env file exists on the server with all credentials
 *       2. This file references environment variables, not hardcoded values
 *       3. The EC2 instance has restricted access
 *       4. Consider migrating to AWS Secrets Manager in the future
 * 
 * =============================================================================
 */

module.exports = {
  apps: [
    // =========================================================================
    // APP 1: MAIN APPLICATION (index.js)
    // =========================================================================
    // 
    // PURPOSE:
    // - Reads CSV files from S3 bucket
    // - Parses CSV data and creates batch jobs
    // - Enqueues jobs to Redis for workers to process
    // - Runs the Express server on port 3000
    // - Provides Bull Board dashboard in development mode
    //
    // WHEN TO RESTART:
    // - After uploading new CSVs to S3
    // - After changing csv-mappings.json
    // - After code updates
    //
    // =========================================================================
    {
      name: 'woo-update-app',
      script: './index.js',
      
      // Working directory - where the app files are located on EC2
      cwd: '/home/ubuntu/woo-product-update',
      
      // Instance configuration
      // Only 1 instance needed - this is the job CREATOR, not processor
      instances: 1,
      
      // Execution mode: 'fork' for single instance, 'cluster' for multiple
      exec_mode: 'fork',
      
      // Auto-restart if the process crashes
      autorestart: true,
      
      // Don't watch files for changes (would cause unnecessary restarts)
      watch: false,
      
      // Maximum number of restarts before giving up
      max_restarts: 10,
      
      // Minimum time the process must be up to be considered "started"
      // Prevents rapid restart loops
      min_uptime: '10s',
      
      // Delay between restarts (in milliseconds)
      restart_delay: 5000,
      
      // Memory limit - restart if exceeded (prevents memory leaks)
      max_memory_restart: '500M',
      
      // Load .env file via Node.js -r flag
      node_args: ['-r', 'dotenv/config'],
      
      // Logging configuration
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      error_file: '/home/ubuntu/woo-product-update/logs/main-error.log',
      out_file: '/home/ubuntu/woo-product-update/logs/main-out.log',
      merge_logs: true,  // Merge logs from all instances into single file
      
      // =====================================================================
      // PRODUCTION ENVIRONMENT (default)
      // =====================================================================
      // Used when running: pm2 start ecosystem.config.js
      // Target: https://suntsu.com (LIVE PRODUCTION SITE)
      // ⚠️  BE CAREFUL - This affects real customer data!
      // =====================================================================
      env: {
        NODE_ENV: 'production',
        
        // CRITICAL: Must be 'production' to use production WooCommerce credentials
        EXECUTION_MODE: 'production',
        
        // Server port
        PORT: 3000,
      },
      
      // =====================================================================
      // STAGING ENVIRONMENT
      // =====================================================================
      // Used when running: pm2 start ecosystem.config.js --env staging
      // Target: https://env-suntsucom-staging.kinsta.cloud (Kinsta Staging)
      // ✅ SAFE FOR TESTING - Use this for real-world tests
      // =====================================================================
      env_staging: {
        NODE_ENV: 'production',  // Still production Node behavior
        
        // BUG FIX: Changed from 'stage' to 'test'
        // The codebase only recognizes: 'production', 'development', 'test'
        // 'test' mode uses WOO_API_*_TEST variables from .env
        EXECUTION_MODE: 'test',
        
        // Server port
        PORT: 3000,
      },
      
      // =====================================================================
      // DEVELOPMENT ENVIRONMENT
      // =====================================================================
      // Used when running: pm2 start ecosystem.config.js --env development
      // Target: https://suntsudev.wpenginepowered.com (WPEngine Dev)
      // ✅ SAFE FOR DEVELOPMENT - Isolated environment
      // =====================================================================
      env_development: {
        NODE_ENV: 'development',
        
        // 'development' mode uses WOO_API_*_DEV variables from .env
        EXECUTION_MODE: 'development',
        
        // Server port
        PORT: 3000,
      },
    },

    // =========================================================================
    // APP 2: WORKER PROCESS (worker.js)
    // =========================================================================
    // 
    // PURPOSE:
    // - Listens to Redis queue for batch jobs
    // - Processes each batch (fetches from Woo, compares, updates)
    // - Updates progress counters in Redis
    // - Saves checkpoints for crash recovery
    //
    // SCALING:
    // - Can run multiple worker instances for parallel processing
    // - Each worker has its own CONCURRENCY setting (jobs per worker)
    // - Total parallel jobs = instances × CONCURRENCY
    //
    // EXAMPLE:
    // - instances: 2, CONCURRENCY: 2 = 4 parallel jobs max
    //
    // =========================================================================
    {
      name: 'woo-worker',
      script: './worker.js',
      
      // Working directory
      cwd: '/home/ubuntu/woo-product-update',
      
      // Number of worker instances
      // Increase this for more parallel processing power
      // But watch memory usage and API rate limits!
      instances: 1,
      
      // Execution mode
      exec_mode: 'fork',
      
      // Auto-restart if the process crashes
      autorestart: true,
      
      // Don't watch files for changes
      watch: false,
      
      // Maximum number of restarts before giving up
      max_restarts: 10,
      
      // Minimum time the process must be up to be considered "started"
      min_uptime: '10s',
      
      // Wait 5s before restarting on crash
      restart_delay: 5000,
      
      // Memory limit - restart if exceeded
      max_memory_restart: '500M',
      
      // Load .env file via Node.js -r flag
      node_args: ['-r', 'dotenv/config'],
      
      // Logging configuration
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      error_file: '/home/ubuntu/woo-product-update/logs/worker-error.log',
      out_file: '/home/ubuntu/woo-product-update/logs/worker-out.log',
      merge_logs: true,
      
      // =====================================================================
      // PRODUCTION ENVIRONMENT (default)
      // =====================================================================
      env: {
        NODE_ENV: 'production',
        EXECUTION_MODE: 'production',
      },
      
      // =====================================================================
      // STAGING ENVIRONMENT
      // =====================================================================
      // BUG FIX: Changed 'stage' to 'test'
      // =====================================================================
      env_staging: {
        NODE_ENV: 'production',
        EXECUTION_MODE: 'test',
      },
      
      // =====================================================================
      // DEVELOPMENT ENVIRONMENT
      // =====================================================================
      env_development: {
        NODE_ENV: 'development',
        EXECUTION_MODE: 'development',
      },
    },

    // =========================================================================
    // APP 3: CSV MAPPING UI SERVER (csv-mapping-server.js)
    // =========================================================================
    // 
    // PURPOSE:
    // - Serves the admin web interface for CSV column mapping
    // - Allows uploading CSV files to S3
    // - Allows configuring column mappings (partNumber, category, manufacturer)
    // - Monitors batch processing progress
    // - Runs on port 4000
    //
    // ACCESS:
    // - http://your-ec2-ip:4000 (requires port 4000 open in security group)
    // - Or use SSH tunnel: ssh -L 4000:localhost:4000 ubuntu@your-ec2
    //
    // =========================================================================
    {
      name: 'csv-mapping-ui',
      script: './csv-mapping-server.js',
      
      // Working directory
      cwd: '/home/ubuntu/woo-product-update',
      
      // Only 1 instance needed for UI server
      instances: 1,
      
      // Execution mode
      exec_mode: 'fork',
      
      // Auto-restart if the process crashes
      autorestart: true,
      
      // Don't watch files for changes
      watch: false,
      
      // Maximum number of restarts before giving up
      max_restarts: 10,
      
      // Minimum time the process must be up to be considered "started"
      min_uptime: '10s',
      
      // Delay between restarts
      restart_delay: 5000,
      
      // UI server needs less memory than workers
      max_memory_restart: '200M',
      
      // Load .env file via Node.js -r flag
      node_args: ['-r', 'dotenv/config'],
      
      // Logging configuration
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      error_file: '/home/ubuntu/woo-product-update/logs/ui-error.log',
      out_file: '/home/ubuntu/woo-product-update/logs/ui-out.log',
      merge_logs: true,
      
      // =====================================================================
      // PRODUCTION ENVIRONMENT (default)
      // =====================================================================
      // All environments use the same UI settings
      // The UI only needs S3 access for uploading files
      // =====================================================================
      env: {
        NODE_ENV: 'production',
        EXECUTION_MODE: 'production',
        CSV_MAPPING_PORT: 4000,
      },
      
      // =====================================================================
      // STAGING ENVIRONMENT
      // =====================================================================
      env_staging: {
        NODE_ENV: 'production',
        EXECUTION_MODE: 'test',
        CSV_MAPPING_PORT: 4000,
      },
      
      // =====================================================================
      // DEVELOPMENT ENVIRONMENT
      // =====================================================================
      env_development: {
        NODE_ENV: 'development',
        EXECUTION_MODE: 'development',
        CSV_MAPPING_PORT: 4000,
      },
    },
  ],
};