#!/bin/bash
set -e

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

# Scraper — every 24h at 02:00 UTC (incremental after first run)
0 2 * * * root /app/runner.sh scraper >> /app/logs/cron.log 2>&1

# Cleaner — every 24h at 06:00 UTC (after scraper)
0 6 * * * root /app/runner.sh cleaner >> /app/logs/cron.log 2>&1

# Sync to PostgreSQL — every 24h at 10:00 UTC (after cleaner)
0 10 * * * root /app/runner.sh sync >> /app/logs/cron.log 2>&1

CRON

chmod 0644 /etc/cron.d/bienici

echo "✅ Cron schedule installed:"
echo "   02:00  🕷️  Scraper (daily)"
echo "   06:00  🧹 Cleaner (daily)"
echo "   10:00  🔄 Sync to PostgreSQL (daily)"
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
echo "🔄 Running initial jobs on startup..."

# Run all 3 jobs on startup (in sequence: scrape → clean → sync)
/app/runner.sh scraper
/app/runner.sh cleaner
/app/runner.sh sync

echo "══════════════════════════════════════════════════"
echo "✅ Startup complete — cron daemon starting"
echo "══════════════════════════════════════════════════"

exec cron -f
