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

# Scraper — every 24h at 02:00 UTC; cleaner + sync chain automatically after
0 2 * * * root /app/runner.sh scraper >> /app/logs/cron.log 2>&1

CRON

chmod 0644 /etc/cron.d/bienici

echo "✅ Cron schedule installed:"
echo "   02:00  🕷️  Scraper → 🧹 Cleaner → 🔄 Sync (chained, daily)"
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

echo "🔌 Testing PostgreSQL..."
python -c "
import psycopg2, os
conn = psycopg2.connect(os.environ['POSTGRES_DSN'])
cur = conn.cursor()
cur.execute('SELECT 1')
print('  ✅ PostgreSQL OK')
conn.close()
" || echo "  ❌ PostgreSQL connection failed"

echo ""
echo "🔄 Running initial pipeline on startup (scraper → cleaner → sync)..."

# scraper chains cleaner, cleaner chains sync — one call runs all three
/app/runner.sh scraper || echo "⚠️  Pipeline startup failed — will retry via cron"

echo "══════════════════════════════════════════════════"
echo "✅ Startup complete — cron daemon starting"
echo "══════════════════════════════════════════════════"

exec cron -f
