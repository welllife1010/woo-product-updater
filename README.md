# WooCommerce Product Updater

A high-performance batch CSV processor for synchronizing product data with WooCommerce stores. Built with BullMQ for reliable job queuing and Redis for state management.

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  index.js       │     │  worker.js      │     │  server.js      │
│  (Main App)     │────▶│  (Worker)       │     │  (UI Server)    │
│                 │     │                 │     │                 │
│ - Reads CSV     │     │ - Processes     │     │ - Admin UI      │
│ - Creates jobs  │     │   batch jobs    │     │ - Upload CSVs   │
│ - Enqueues to   │     │ - Updates Woo   │     │ - Map columns   │
│   Redis         │     │ - Tracks        │     │                 │
└─────────────────┘     │   progress      │     └─────────────────┘
        │               └─────────────────┘              │
        │                       │                        │
        └───────────────────────┼────────────────────────┘
                                ▼
                   ┌─────────────────────┐
                   │       Redis         │
                   │   (Job Queue +      │
                   │    Progress Data)   │
                   └─────────────────────┘
```

## ✨ Features

- **Batch Processing**: Efficiently processes large CSV files in configurable batch sizes
- **S3 Integration**: Reads CSV files directly from Amazon S3
- **Job Queue Management**: BullMQ-based reliable job processing with automatic retries
- **Progress Tracking**: Real-time progress tracking with Redis-based checkpoints
- **Category Resolution**: Fuzzy matching for vendor categories to WooCommerce taxonomy
- **Rate Limiting**: Built-in Bottleneck rate limiter to respect WooCommerce API limits
- **Multi-Environment Support**: Production, staging, and development environments
- **Admin UI**: Web interface for uploading CSVs and mapping columns
- **Bull Board**: Visual queue monitoring dashboard
- **Auto-Recovery**: Checkpoint-based recovery from failures

## 📋 Prerequisites

- Node.js 18+
- Redis server
- PM2 (for production deployment)
- AWS S3 bucket (for CSV storage)
- WooCommerce store with REST API enabled

## 🚀 Quick Start

### 1. Install Dependencies

```bash
npm install
```

### 2. Configure Environment

Create a `.env` file in the project root:

```env
# WooCommerce API
WOO_API_URL=https://your-store.com
WOO_CONSUMER_KEY=ck_xxxxxx
WOO_CONSUMER_SECRET=cs_xxxxxx

# AWS S3
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-west-1
S3_BUCKET_NAME=your-bucket-name

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# App Configuration
APP_ENV=development  # production | staging | development
BATCH_SIZE=20
CONCURRENCY=2

# Bull Board Auth (production)
BULL_BOARD_USER=admin
BULL_BOARD_PASS=your-secure-password
```

### 3. Configure CSV Mappings

Copy the sample mappings file:

```bash
cp csv-mappings.json.sample csv-mappings.json
```

Edit `csv-mappings.json` to configure your CSV file mappings:

```json
{
  "files": [
    {
      "fileKey": "path/to/your-file.csv",
      "status": "ready",
      "headers": ["Part Number", "Category", "Manufacturer", ...],
      "mapping": {
        "partNumber": "Part Number",
        "category": "Category",
        "manufacturer": "Manufacturer"
      }
    }
  ]
}
```

### 4. Start the Application

**Development (single terminal):**

```bash
# Start main app (reads CSV, creates jobs)
npm start

# In another terminal - start worker
npm run start:worker

# In another terminal - start UI
npm run start:ui
```

**Production (with PM2):**

```bash
# Start all processes
npm run pm2:start

# Or with specific environment
npm run pm2:start:staging
npm run pm2:start:dev

# Monitor processes
npm run pm2:monit

# View logs
npm run pm2:logs
```

## 📁 Project Structure

```
├── index.js                 # Main app entry - job creation & queue management
├── worker.js                # BullMQ worker - processes batch jobs
├── server.js                # UI server entry (delegates to ui/server.js)
├── ecosystem.config.js      # PM2 configuration
├── csv-mappings.json        # CSV file configurations
│
├── src/
│   ├── batch/               # Batch processing logic
│   │   ├── process-batch.js # Core batch processor
│   │   ├── build-update-payload.js
│   │   ├── checkpoint.js    # Progress checkpointing
│   │   ├── compare.js       # Data comparison
│   │   ├── fetch-validate.js
│   │   ├── handlers.js      # Update handlers
│   │   └── map-new-data.js  # Data mapping
│   │
│   ├── config/
│   │   ├── runtime-env.js   # Environment configuration
│   │   └── update-mode.js   # Update mode settings
│   │
│   ├── resolvers/           # Data resolution
│   │   ├── category-map.js  # Fuzzy category matching
│   │   ├── category-resolver.js
│   │   ├── category-woo.js  # WooCommerce category creation
│   │   └── manufacturer-resolver.js
│   │
│   ├── services/
│   │   ├── queue.js         # BullMQ queue setup
│   │   ├── job-manager.js   # Job management
│   │   ├── s3-helpers.js    # S3 facade
│   │   ├── woo-helpers.js   # WooCommerce helpers
│   │   ├── csv/             # CSV utilities
│   │   ├── ingest/          # File ingestion
│   │   ├── s3/              # S3 client & operations
│   │   └── woo/             # WooCommerce API
│   │
│   └── utils/
│       ├── logger.js        # Logging with env prefixes
│       └── utils.js         # General utilities
│
├── ui/                      # Admin UI
│   ├── app.js               # Express app setup
│   ├── config.js            # UI configuration
│   ├── server.js            # UI server
│   ├── public/              # Static files
│   │   └── index.html       # Admin dashboard
│   ├── routes/              # API routes
│   └── services/            # UI services
│
├── scripts/
│   ├── deploy.sh            # Deployment script
│   └── reset-and-restart.sh # Reset script
│
├── output-files/            # Log files
│   ├── info-log.txt
│   ├── error-log.txt
│   └── updates-log.txt
│
└── __tests__/               # Jest test files
```

## 🖥️ Admin UI

Access the admin UI at `http://localhost:3001` (default port).

Features:
- Upload CSV files to S3
- Map CSV columns to product fields
- View processing progress
- Monitor job queue status
- View logs

## 📊 Bull Board Dashboard

Access the job queue dashboard at `http://localhost:3000/admin/queues`.

In production, this is protected by basic authentication using `BULL_BOARD_USER` and `BULL_BOARD_PASS` environment variables.

## 🚀 Deployment

### Using the Deploy Script

```bash
# Deploy to staging
npm run deploy:staging

# Deploy to production
npm run deploy:production
```

### Manual Deployment

1. SSH into your EC2 instance
2. Pull the latest code
3. Install dependencies: `npm install`
4. Restart PM2: `pm2 restart ecosystem.config.js`

## ⚙️ Configuration Options

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `APP_ENV` | Environment mode | `development` |
| `BATCH_SIZE` | Rows per batch job | `20` |
| `CONCURRENCY` | Worker concurrency | `2` |
| `REDIS_HOST` | Redis server host | `localhost` |
| `REDIS_PORT` | Redis server port | `6379` |
| `S3_BUCKET_NAME` | S3 bucket for CSVs | - |
| `WOO_API_URL` | WooCommerce store URL | - |
| `STATUS_FLUSH_INTERVAL` | Status array flush interval | `50` |

## 🧪 Testing

```bash
# Run all tests
npm test

# Run specific test file
npm test -- __tests__/s3-helpers.facade.test.js
```

## 📝 PM2 Commands

```bash
npm run pm2:start      # Start all processes
npm run pm2:stop       # Stop all processes
npm run pm2:restart    # Restart all processes
npm run pm2:delete     # Delete all processes
npm run pm2:logs       # View logs
npm run pm2:status     # Check status
npm run pm2:monit      # Real-time monitoring
```

## 🔧 Troubleshooting

### Jobs stuck in queue
1. Check Redis connection: `redis-cli ping`
2. View worker logs: `pm2 logs woo-update-worker`
3. Check Bull Board for error details

### Missing products
1. Check `missing-products/` directory for logs
2. Verify part number format in CSV
3. Check manufacturer mapping

### Rate limiting issues
1. Reduce `CONCURRENCY` value
2. Increase batch processing delays
3. Check WooCommerce API limits

## 📄 License

ISC

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `npm test`
5. Submit a pull request
