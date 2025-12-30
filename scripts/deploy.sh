#!/bin/bash
# =============================================================================
# DEPLOYMENT SCRIPT FOR WOO-PRODUCT-UPDATER
# =============================================================================
#
# This script handles deployment to EC2 instances.
#
# USAGE:
#   ./scripts/deploy.sh [staging|production]
#
# PREREQUISITES:
#   - SSH access to EC2 instance
#   - .env file configured on the server
#   - Redis running on the server
#
# =============================================================================

set -e  # Exit on any error

# =============================================================================
# CONFIGURATION - MODIFY THESE FOR YOUR ENVIRONMENT
# =============================================================================

# Default environment
ENV="${1:-staging}"

# =============================================================================
# EC2 CONFIGURATION - UPDATE THESE WITH YOUR ACTUAL VALUES!
# =============================================================================
# 
# Based on your ecosystem.config.js, your EC2 uses:
#   - User: ubuntu
#   - App Directory: /home/ubuntu/woo-product-update
#
# You need to fill in your EC2 hostname/IP below:
# =============================================================================

if [ "$ENV" == "production" ]; then
    # ⚠️  PRODUCTION - BE CAREFUL!
    EC2_HOST="18.144.155.64"  # TODO: Replace with actual IP
    EC2_USER="ubuntu"
    APP_DIR="/home/ubuntu/woo-product-update"
    PM2_ENV="production"
elif [ "$ENV" == "staging" ]; then
    # ✅ STAGING - Safe for testing
    EC2_HOST="18.144.155.64"  # TODO: Replace with actual IP
    EC2_USER="ubuntu"
    APP_DIR="/home/ubuntu/woo-product-update"
    PM2_ENV="staging"
else
    echo "❌ Unknown environment: $ENV"
    echo "Usage: ./scripts/deploy.sh [staging|production]"
    exit 1
fi

# SSH Key (modify if your key is in a different location)
SSH_KEY="~/.ssh/woo-product-update.pem"

echo "=============================================="
echo "🚀 Deploying to $ENV environment"
echo "   Host: $EC2_HOST"
echo "   User: $EC2_USER"
echo "   Directory: $APP_DIR"
echo "=============================================="

# =============================================================================
# STEP 1: Local Preparation
# =============================================================================

echo ""
echo "📦 Step 1: Preparing local files..."

# Ensure we're in the project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

echo "   Working directory: $(pwd)"

# =============================================================================
# STEP 2: Sync files to EC2
# =============================================================================

echo ""
echo "📤 Step 2: Syncing files to EC2..."

# Rsync options:
#   -avz: archive, verbose, compress
#   --exclude: skip files that shouldn't be deployed
rsync -avz \
    --exclude 'node_modules' \
    --exclude '.git' \
    --exclude 'logs' \
    --exclude 'output-files' \
    --exclude 'batch_status' \
    --exclude 'missing-products' \
    --exclude 'tmp-uploads' \
    --exclude 'process_checkpoint.json' \
    --exclude '*.log' \
    -e "ssh -i $SSH_KEY" \
    ./ "$EC2_USER@$EC2_HOST:$APP_DIR/"

echo "   ✅ Files synced successfully"

# Quick verification: ensure the UI API routes we rely on actually landed.
echo ""
echo "🔎 Step 2b: Verifying critical files on server..."

LOCAL_API_SHA=$(shasum -a 256 "$PROJECT_DIR/ui/routes/api.js" | awk '{print $1}')
REMOTE_API_SHA=$(ssh -i "$SSH_KEY" "$EC2_USER@$EC2_HOST" "shasum -a 256 $APP_DIR/ui/routes/api.js 2>/dev/null | awk '{print \$1}'")

echo "   Local ui/routes/api.js:  $LOCAL_API_SHA"
echo "   Remote ui/routes/api.js: $REMOTE_API_SHA"

if [ -z "$REMOTE_API_SHA" ]; then
    echo "   ⚠️  Could not read remote ui/routes/api.js checksum"
else
    if [ "$LOCAL_API_SHA" != "$REMOTE_API_SHA" ]; then
        echo "   ❌ Checksum mismatch: ui/routes/api.js did not sync correctly"
        echo "   (Aborting before restart so we don't run stale code)"
        exit 1
    fi
fi

HAS_PROGRESS_DELETE=$(ssh -i "$SSH_KEY" "$EC2_USER@$EC2_HOST" "grep -n 'router.delete(\"/progress/:fileKey\"' $APP_DIR/ui/routes/api.js >/dev/null 2>&1 && echo yes || echo no")
if [ "$HAS_PROGRESS_DELETE" != "yes" ]; then
    echo "   ❌ Missing DELETE /progress/:fileKey route on server (stale api.js?)"
    exit 1
fi

echo "   ✅ Verified ui/routes/api.js synced and includes DELETE /progress/:fileKey"

# =============================================================================
# STEP 3: Install dependencies and restart PM2
# =============================================================================

echo ""
echo "🔧 Step 3: Installing dependencies and restarting PM2..."

ssh -i "$SSH_KEY" "$EC2_USER@$EC2_HOST" << EOF
    cd $APP_DIR
    
    echo "   Installing npm dependencies..."
    npm install --production
    
    echo "   Creating required directories..."
    mkdir -p logs output-files batch_status missing-products tmp-uploads
    
    echo "   Checking Redis connection..."
    redis-cli ping || echo "⚠️  Redis may not be running!"
    
    echo "   Restarting PM2 processes..."
    pm2 stop ecosystem.config.js 2>/dev/null || true
    pm2 delete ecosystem.config.js 2>/dev/null || true
    pm2 start ecosystem.config.js --env $PM2_ENV
    
    echo "   Saving PM2 configuration..."
    pm2 save
    
    echo "   PM2 process status:"
    pm2 status
EOF

# =============================================================================
# STEP 4: Verify deployment
# =============================================================================

echo ""
echo "✅ Step 4: Verifying deployment..."

ssh -i "$SSH_KEY" "$EC2_USER@$EC2_HOST" << EOF
    echo "   Checking PM2 status..."
    pm2 status
    
    echo ""
    echo "   Checking if services are responding..."
    sleep 3
    
    # Check main process port
    curl -s http://localhost:3000 > /dev/null && echo "   ✅ Main server (3000) is responding" || echo "   ⚠️  Main server (3000) not responding"
    
    # Check UI port
    curl -s http://localhost:4000 > /dev/null && echo "   ✅ UI server (4000) is responding" || echo "   ⚠️  UI server (4000) not responding"
EOF

echo ""
echo "=============================================="
echo "🎉 Deployment to $ENV complete!"
echo ""
echo "📝 Next steps:"
echo "   1. SSH to server: ssh -i $SSH_KEY $EC2_USER@$EC2_HOST"
echo "   2. View logs: pm2 logs"
echo "   3. Monitor: pm2 monit"
echo "   4. Access UI: http://$EC2_HOST:4000"
echo "=============================================="
