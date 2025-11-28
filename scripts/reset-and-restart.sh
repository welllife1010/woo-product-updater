#!/bin/bash
# =============================================================================
# RESET AND RESTART SCRIPT
# =============================================================================
# Usage: ./scripts/reset-and-restart.sh [--full]
#   --full : Also clears csv-mappings.json (requires re-upload of files)
# =============================================================================

set -e
cd /home/ubuntu/woo-product-update

echo "=============================================="
echo "🔄 WooCommerce Product Updater - Reset Script"
echo "=============================================="

# Parse arguments
FULL_RESET=false
if [ "$1" == "--full" ]; then
    FULL_RESET=true
    echo "⚠️  FULL RESET MODE - Will clear file registrations too"
fi

echo ""
echo "📦 Step 1: Stopping PM2 processes..."
pm2 stop all 2>/dev/null || true

echo ""
echo "🗑️  Step 2: Clearing batch status..."
rm -rf batch_status/*
echo "   ✅ batch_status/ cleared"

echo ""
echo "🗑️  Step 3: Clearing checkpoint file..."
rm -f process_checkpoint.json
echo "   ✅ process_checkpoint.json removed"

echo ""
echo "🗑️  Step 4: Clearing Redis databases..."
redis-cli FLUSHALL > /dev/null
echo "   ✅ Redis flushed"

echo ""
echo "🗑️  Step 5: Clearing missing-products folders..."
rm -rf missing-products/*
mkdir -p missing-products
echo "   ✅ missing-products/ cleared"

if [ "$FULL_RESET" = true ]; then
    echo ""
    echo "🗑️  Step 6: Resetting csv-mappings.json..."
    echo '{"files":[]}' > csv-mappings.json
    echo "   ✅ csv-mappings.json reset (you'll need to re-upload files)"
fi

echo ""
echo "📝 Step 7: Clearing log files..."
> output-files/info-log.txt
> output-files/error-log.txt
echo "   ✅ Log files cleared"

echo ""
echo "🔄 Step 8: Flushing PM2 logs..."
pm2 flush > /dev/null 2>&1 || true
echo "   ✅ PM2 logs flushed"

echo ""
echo "🚀 Step 9: Starting PM2 processes..."
pm2 delete all 2>/dev/null || true
pm2 start ecosystem.config.js --env staging
pm2 save

echo ""
echo "⏳ Waiting for processes to start..."
sleep 3

echo ""
echo "📊 Step 10: Current status:"
pm2 status

echo ""
echo "=============================================="
echo "✅ Reset complete!"
echo ""
echo "📝 Next steps:"
echo "   1. Upload CSV at: http://$(curl -s http://checkip.amazonaws.com):4000"
echo "   2. Configure mapping and save"
echo "   3. Watch logs: pm2 logs woo-worker"
echo "=============================================="
EOF

chmod +x ~/woo-product-update/scripts/reset-and-restart.sh
echo "✅ Script created at scripts/reset-and-restart.sh"