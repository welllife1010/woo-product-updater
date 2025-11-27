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
    EC2_HOST="YOUR_PRODUCTION_EC2_IP_OR_HOSTNAME"  # TODO: Replace with actual IP
    EC2_USER="ubuntu"
    APP_DIR="/home/ubuntu/woo-product-update"
    PM2_ENV="production"
elif [ "$ENV" == "staging" ]; then
    # ✅ STAGING - Safe for testing
    EC2_HOST="YOUR_STAGING_EC2_IP_OR_HOSTNAME"  # TODO: Replace with actual IP
    EC2_USER="ubuntu"
    APP_DIR="/home/ubuntu/woo-product-update"
    PM2_ENV="staging"
else
    echo "❌ Unknown environment: $ENV"
    echo "Usage: ./scripts/deploy.sh [staging|production]"
    exit 1
fi

# SSH Key (modify if your key is in a different location)
SSH_KEY="~/.ssh/your-ec2-key.pem"

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
    --exclude '.env' \
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

