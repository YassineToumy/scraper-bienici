#!/bin/bash


echo "══════════════════════════════════════════════════"
echo "🇫🇷 Bienici Service Starting"
echo "   Time: $(date -u)"
echo "══════════════════════════════════════════════════"

# Export env vars so cron can access them
printenv | grep -v "no_proxy" >> /etc/environment

# Write crontab
cat > /etc/cron.d/bienici <<'CRON'
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin

# Scraper — every 24h at 02:00 UTC; cleaner chain automatically after
0 2 * * * root /app/runner.sh scraper >> /app/logs/cron.log 2>&1

CRON

chmod 0644 /etc/cron.d/bienici

echo "✅ Cron schedule installed:"
echo "   02:00  🕷️  Scraper → 🧹 Cleaner (chained, daily)"
echo ""

# Verify connections
echo "🔌 Testing MongoDB..."
python -c "
from pymongo import MongoClient
import os
c = MongoClient(os.environ['MONGODB_URI'], serverSelectionTimeoutMS=5000)
c.admin.command('ping')
print('  ✅ MongoDB OK')
c.close()
" || echo "  ❌ MongoDB connection failed"

echo ""
echo "🔄 Running initial pipeline on startup (scraper → cleaner)..."

# scraper chains cleaner — one call runs both
/app/runner.sh scraper || echo "⚠️  Pipeline startup failed — will retry via cron"

echo "══════════════════════════════════════════════════"
echo "✅ Startup complete — cron daemon starting"
echo "══════════════════════════════════════════════════"

exec cron -f
