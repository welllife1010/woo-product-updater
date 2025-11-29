#!/bin/bash
# =============================================================================
# reset-and-restart.sh - Complete reset for WooCommerce Product Updater
# =============================================================================
# Usage:
#   ./scripts/reset-and-restart.sh              # Normal reset (keeps csv-mappings.json)
#   ./scripts/reset-and-restart.sh --full       # Full reset (clears everything)
#   ./scripts/reset-and-restart.sh <fileKey>    # Reset specific file only
# =============================================================================

set -e
cd /home/ubuntu/woo-product-update

echo "🛑 Stopping all PM2 processes..."
pm2 stop all

if [ "$1" == "--full" ]; then
    echo "🗑️  FULL RESET - Clearing everything including csv-mappings.json..."
    redis-cli FLUSHALL
    rm -f process_checkpoint.json
    rm -rf batch_status/*
    rm -rf missing-products/missing-*/*.json
    rm -f csv-mappings.json
    echo '{"files":[]}' > csv-mappings.json
    > output-files/info-log.txt
    > output-files/error-log.txt
elif [ -n "$1" ]; then
    echo "🔄 Resetting specific file: $1"
    CLEAN_KEY=$(echo "$1" | sed 's/\.csv$//' | tr '/' '_')
    
    # Clear Redis keys for this file
    redis-cli -n 0 EVAL "for _,k in ipairs(redis.call('keys','bull:*')) do redis.call('del',k) end" 0
    redis-cli -n 1 KEYS "*${CLEAN_KEY}*" | xargs -r redis-cli -n 1 DEL
    redis-cli -n 1 KEYS "*$1*" | xargs -r redis-cli -n 1 DEL
    
    # Clear checkpoint for this file
    if [ -f process_checkpoint.json ]; then
        node -e "
        const fs = require('fs');
        const cp = JSON.parse(fs.readFileSync('process_checkpoint.json', 'utf8'));
        delete cp['$1'];
        fs.writeFileSync('process_checkpoint.json', JSON.stringify(cp, null, 2));
        " 2>/dev/null || true
    fi
    
    # Clear batch_status for this file
    rm -rf "batch_status/${1%/*}" 2>/dev/null || true
    
    # Clear missing products for this file
    find missing-products/ -name "*${CLEAN_KEY}*" -delete 2>/dev/null || true
    
    # Set status to ready in csv-mappings.json
    node -e "
    const fs = require('fs');
    const mappings = JSON.parse(fs.readFileSync('csv-mappings.json', 'utf8'));
    const idx = mappings.files.findIndex(f => f.fileKey === '$1');
    if (idx !== -1) {
        mappings.files[idx].status = 'ready';
        fs.writeFileSync('csv-mappings.json', JSON.stringify(mappings, null, 2));
        console.log('✅ Set $1 status to ready');
    }
    " 2>/dev/null || true
else
    echo "🧹 Normal reset - Clearing state but keeping csv-mappings.json..."
    redis-cli FLUSHALL
    rm -f process_checkpoint.json
    rm -rf batch_status/*
    rm -rf missing-products/missing-*/*.json
fi

echo "🔄 Flushing PM2 logs..."
pm2 flush

echo "🚀 Starting all services..."
pm2 start woo-update-app woo-worker csv-mapping-ui

echo "⏳ Waiting for startup..."
sleep 5

echo ""
echo "✅ Reset complete!"
pm2 status
echo ""
echo "📊 Next steps:"
echo "   - View logs: pm2 logs"
echo "   - Check info: tail -f output-files/info-log.txt"
echo "   - UI: http://18.144.155.64:4000"
echo "   - Bull Dashboard: http://18.144.155.64:3000/admin/queues"