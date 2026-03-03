#!/usr/bin/env python3
"""
Bienici — MongoDB (locations_clean) -> PostgreSQL Sync + Archive Checker
Incremental: only syncs new docs, archives dead listings.

Usage:
    python sync.py          # Run once (sync + archive)
    python sync.py --loop   # Loop every CYCLE_SLEEP seconds
    python sync.py --sync-only
    python sync.py --archive-only
"""

import os
import re
import time
import logging
import argparse
import requests
from datetime import datetime, timezone

from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

MONGO_URI = os.environ["MONGODB_URI"]
MONGO_DB = os.getenv("MONGO_BIENICI_DB", "bienici")
MONGO_COL = os.getenv("MONGO_BIENICI_COL_CLEAN", "locations_clean")

PG_DSN = os.environ["POSTGRES_DSN"]
PG_TABLE = os.getenv("PG_TABLE_BIENICI", "bienici_listings")
PG_ARCHIVE = os.getenv("PG_ARCHIVE_BIENICI", "bienici_archive")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
CYCLE_SLEEP = int(os.getenv("CYCLE_SLEEP", "86400"))
ARCHIVE_DELAY = 2
ARCHIVE_TIMEOUT = 15

HOMEPAGE_REDIRECTS = {
    "https://www.bienici.com",
    "https://www.bienici.com/fr",
}

CHECK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bienici-sync")

# ============================================================
# HELPERS
# ============================================================

def to_pg_array(lst):
    if not lst or not isinstance(lst, list):
        return None
    cleaned = [str(x) for x in lst if x]
    return cleaned if cleaned else None


def to_pg_timestamp(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, dict) and "$date" in v:
        try:
            return datetime.fromisoformat(v["$date"].replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    return None


# ============================================================
# ROW BUILDER — reads from locations_clean (normalized fields)
# ============================================================

def build_row(d):
    """Build PG row from a cleaned bienici document (locations_clean)."""
    photos = to_pg_array(d.get("photos"))
    virtual_tours = to_pg_array(d.get("virtual_tours"))

    return (
        d.get("source_id"),                          # source_id
        d.get("reference"),                          # reference
        d.get("url"),                                # url
        d.get("city"),                               # city
        d.get("postal_code"),                        # postal_code
        d.get("department_code"),                    # department_code
        d.get("district_name"),                      # district_name
        d.get("insee_code"),                         # insee_code
        d.get("address_known", False),               # address_known
        d.get("latitude"),                           # latitude
        d.get("longitude"),                          # longitude
        d.get("property_type"),                      # property_type
        d.get("surface_m2"),                         # surface_m2
        d.get("floor"),                              # floor
        d.get("rooms"),                              # rooms
        d.get("bedrooms"),                           # bedrooms
        d.get("bathrooms"),                          # bathrooms
        d.get("shower_rooms") or 0,                  # shower_rooms
        d.get("terraces") or 0,                      # terraces
        d.get("balconies") or 0,                     # balconies
        d.get("parking_spots") or 0,                 # parking_spots
        d.get("cellars") or 0,                       # cellars
        d.get("is_new", False),                      # is_new
        d.get("is_furnished", False),                # is_furnished
        d.get("is_accessible", False),               # is_accessible
        d.get("has_elevator", False),                # has_elevator
        d.get("heating"),                            # heating
        d.get("optical_fiber"),                      # optical_fiber
        d.get("price"),                              # price
        d.get("currency", "EUR"),                    # currency
        d.get("charges"),                            # charges
        d.get("agency_fee"),                         # agency_fee
        d.get("price_decreased", False),             # price_decreased
        d.get("rent_excluding_charges"),             # rent_excluding_charges
        d.get("price_per_m2"),                       # price_per_m2
        d.get("energy_class"),                       # energy_class
        d.get("ghg_class"),                          # ghg_class
        d.get("energy_value"),                       # energy_value
        d.get("ghg_value"),                          # ghg_value
        d.get("energy_diag_date"),                   # energy_diag_date
        d.get("min_energy_cost"),                    # min_energy_cost
        d.get("max_energy_cost"),                    # max_energy_cost
        d.get("energy_numeric"),                     # energy_numeric
        d.get("ghg_numeric"),                        # ghg_numeric
        d.get("description"),                        # description
        photos,                                      # photos
        d.get("photos_count") or 0,                  # photos_count
        virtual_tours,                               # virtual_tours
        d.get("posted_by_pro", True),                # posted_by_pro
        d.get("agency_name"),                        # agency_name
        d.get("agency_id"),                          # agency_id
        d.get("is_exclusive", False),                # is_exclusive
        d.get("surface_per_room"),                   # surface_per_room
        d.get("surface_per_bedroom"),                # surface_per_bedroom
        d.get("equipment_score") or 0,               # equipment_score
        to_pg_timestamp(d.get("cleaned_at")),        # cleaned_at
    )


INSERT_SQL = """
    INSERT INTO bienici_listings (
        source_id, reference, url, city, postal_code, department_code,
        district_name, insee_code, address_known, latitude, longitude,
        property_type, surface_m2, floor, rooms, bedrooms,
        bathrooms, shower_rooms, terraces, balconies, parking_spots,
        cellars, is_new, is_furnished, is_accessible, has_elevator,
        heating, optical_fiber, price, currency, charges,
        agency_fee, price_decreased, rent_excluding_charges, price_per_m2,
        energy_class, ghg_class, energy_value, ghg_value, energy_diag_date,
        min_energy_cost, max_energy_cost, energy_numeric, ghg_numeric,
        description, photos, photos_count, virtual_tours,
        posted_by_pro, agency_name, agency_id, is_exclusive,
        surface_per_room, surface_per_bedroom, equipment_score, cleaned_at
    ) VALUES %s
    ON CONFLICT (source_id) DO NOTHING
"""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bienici_listings (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(100) UNIQUE NOT NULL,
    reference VARCHAR(100),
    url TEXT,
    city VARCHAR(200),
    postal_code VARCHAR(10),
    department_code VARCHAR(5),
    district_name VARCHAR(200),
    insee_code VARCHAR(10),
    address_known BOOLEAN DEFAULT FALSE,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    property_type VARCHAR(50),
    surface_m2 DOUBLE PRECISION,
    floor INTEGER,
    rooms INTEGER,
    bedrooms INTEGER,
    bathrooms INTEGER,
    shower_rooms INTEGER DEFAULT 0,
    terraces INTEGER DEFAULT 0,
    balconies INTEGER DEFAULT 0,
    parking_spots INTEGER DEFAULT 0,
    cellars INTEGER DEFAULT 0,
    is_new BOOLEAN DEFAULT FALSE,
    is_furnished BOOLEAN DEFAULT FALSE,
    is_accessible BOOLEAN DEFAULT FALSE,
    has_elevator BOOLEAN DEFAULT FALSE,
    heating VARCHAR(100),
    optical_fiber VARCHAR(50),
    price DOUBLE PRECISION,
    currency VARCHAR(5) DEFAULT 'EUR',
    charges DOUBLE PRECISION,
    agency_fee DOUBLE PRECISION,
    price_decreased BOOLEAN DEFAULT FALSE,
    rent_excluding_charges DOUBLE PRECISION,
    price_per_m2 DOUBLE PRECISION,
    energy_class VARCHAR(5),
    ghg_class VARCHAR(5),
    energy_value INTEGER,
    ghg_value INTEGER,
    energy_diag_date VARCHAR(50),
    min_energy_cost DOUBLE PRECISION,
    max_energy_cost DOUBLE PRECISION,
    energy_numeric INTEGER,
    ghg_numeric INTEGER,
    description TEXT,
    photos TEXT[],
    photos_count INTEGER DEFAULT 0,
    virtual_tours TEXT[],
    posted_by_pro BOOLEAN DEFAULT TRUE,
    agency_name VARCHAR(300),
    agency_id VARCHAR(100),
    is_exclusive BOOLEAN DEFAULT FALSE,
    surface_per_room DOUBLE PRECISION,
    surface_per_bedroom DOUBLE PRECISION,
    equipment_score INTEGER DEFAULT 0,
    cleaned_at TIMESTAMPTZ,
    synced_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_bienici_city ON bienici_listings(city);
CREATE INDEX IF NOT EXISTS idx_bienici_dept ON bienici_listings(department_code);
CREATE INDEX IF NOT EXISTS idx_bienici_price ON bienici_listings(price);
"""

# ============================================================
# SCHEMA + ARCHIVE
# ============================================================

def ensure_schema(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(SCHEMA_SQL)
    pg_conn.commit()
    log.info("✅ Table bienici_listings ensured")


def ensure_archive_table(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
        (PG_ARCHIVE,)
    )
    if cur.fetchone()[0]:
        return
    cur.execute(f"""
        CREATE TABLE {PG_ARCHIVE} (
            LIKE {PG_TABLE} INCLUDING DEFAULTS INCLUDING GENERATED
        )
    """)
    cur.execute(f"""
        ALTER TABLE {PG_ARCHIVE}
            ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ DEFAULT NOW(),
            ADD COLUMN IF NOT EXISTS archive_reason VARCHAR(50) DEFAULT 'listing_removed'
    """)
    # Drop unique constraints from archive
    cur.execute(f"""
        SELECT conname FROM pg_constraint
        WHERE conrelid = '{PG_ARCHIVE}'::regclass
        AND contype IN ('u', 'p')
        AND conname != '{PG_ARCHIVE}_pkey'
    """)
    for row in cur.fetchall():
        cur.execute(f"ALTER TABLE {PG_ARCHIVE} DROP CONSTRAINT IF EXISTS {row[0]}")
    pg_conn.commit()
    log.info(f"✅ Archive table {PG_ARCHIVE} created")


# ============================================================
# SYNC: MongoDB -> PostgreSQL
# ============================================================

def get_existing_ids(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(f"SELECT source_id FROM {PG_TABLE} WHERE source_id IS NOT NULL")
    return {str(row[0]) for row in cur.fetchall()}


def _flush_batch(pg_conn, batch, stats):
    try:
        cur = pg_conn.cursor()
        execute_values(cur, INSERT_SQL, batch)
        pg_conn.commit()
        stats["new_synced"] += len(batch)
    except Exception as e:
        pg_conn.rollback()
        log.error(f"Batch insert error: {e}")
        recovered = 0
        for row in batch:
            try:
                cur = pg_conn.cursor()
                execute_values(cur, INSERT_SQL, [row])
                pg_conn.commit()
                recovered += 1
            except Exception:
                pg_conn.rollback()
                stats["errors"] += 1
        stats["new_synced"] += recovered


def sync(mongo_col, pg_conn):
    stats = {"total_mongo": 0, "already_in_pg": 0, "new_synced": 0, "errors": 0}
    stats["total_mongo"] = mongo_col.count_documents({})

    existing_ids = get_existing_ids(pg_conn)
    stats["already_in_pg"] = len(existing_ids)

    log.info(f"  MongoDB (clean): {stats['total_mongo']} | PostgreSQL: {stats['already_in_pg']}")

    if stats["total_mongo"] == 0:
        log.info("  No documents in MongoDB — skipping")
        return stats

    batch = []
    for doc in mongo_col.find({}, batch_size=BATCH_SIZE):
        doc_id = doc.get("source_id")
        if not doc_id or str(doc_id) in existing_ids:
            continue
        try:
            batch.append(build_row(doc))
        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                log.warning(f"  Row build error (source_id={doc_id}): {e}")
        if len(batch) >= BATCH_SIZE:
            _flush_batch(pg_conn, batch, stats)
            batch = []
            log.info(f"  Synced {stats['new_synced']} so far...")

    if batch:
        _flush_batch(pg_conn, batch, stats)

    return stats


# ============================================================
# ARCHIVE: Check if listings still live
# ============================================================

def check_url_alive(url):
    try:
        resp = requests.head(
            url, headers=CHECK_HEADERS,
            timeout=ARCHIVE_TIMEOUT, allow_redirects=True
        )
        if resp.status_code in (404, 410):
            return False
        if resp.url.rstrip("/") in HOMEPAGE_REDIRECTS:
            return False
        if resp.status_code == 403:
            return True
        if resp.status_code < 400:
            return True
        if resp.status_code >= 500:
            return True
        return False
    except requests.RequestException:
        return True


def archive_listing(pg_conn, unique_val, reason="listing_removed"):
    cur = pg_conn.cursor()
    try:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND column_name NOT IN ('archived_at', 'archive_reason')
            ORDER BY ordinal_position
        """, (PG_TABLE,))
        columns = [r[0] for r in cur.fetchall()]
        cols_str = ", ".join(columns)

        cur.execute(f"""
            INSERT INTO {PG_ARCHIVE} ({cols_str}, archived_at, archive_reason)
            SELECT {cols_str}, NOW(), %s FROM {PG_TABLE} WHERE source_id = %s
        """, (reason, unique_val))
        cur.execute(f"DELETE FROM {PG_TABLE} WHERE source_id = %s", (unique_val,))
        pg_conn.commit()
        return True
    except Exception as e:
        pg_conn.rollback()
        log.error(f"Archive error for {unique_val}: {e}")
        return False


def archive_check(pg_conn):
    stats = {"checked": 0, "alive": 0, "archived": 0, "errors": 0}
    cur = pg_conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(f"SELECT source_id, url FROM {PG_TABLE} WHERE url IS NOT NULL")
    rows = cur.fetchall()
    log.info(f"  {len(rows)} listings to check")

    for i, row in enumerate(rows):
        url = row.get("url")
        if not url:
            continue

        alive = check_url_alive(url)
        stats["checked"] += 1

        if alive:
            stats["alive"] += 1
        else:
            if archive_listing(pg_conn, row["source_id"]):
                stats["archived"] += 1
                log.info(f"  📦 Archived: {row['source_id']}")
            else:
                stats["errors"] += 1

        if (i + 1) % 50 == 0:
            log.info(f"  Progress: {i+1}/{len(rows)} | alive={stats['alive']} archived={stats['archived']}")

        time.sleep(ARCHIVE_DELAY)

    return stats


# ============================================================
# MAIN CYCLE
# ============================================================

def run_cycle(mongo_col, pg_conn, do_sync=True, do_archive=True):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"\n{'='*60}")
    log.info(f"[🇫🇷 BIENICI] CYCLE START: {now}")
    log.info(f"{'='*60}")

    ensure_schema(pg_conn)
    ensure_archive_table(pg_conn)

    if do_sync:
        log.info("\n--- PHASE 1: SYNC MongoDB (clean) -> PostgreSQL ---")
        stats = sync(mongo_col, pg_conn)
        log.info(f"  ✅ +{stats['new_synced']} new | {stats['errors']} errors")

    if do_archive:
        log.info("\n--- PHASE 2: ARCHIVE CHECK ---")
        stats = archive_check(pg_conn)
        log.info(f"  ✅ {stats['checked']} checked | {stats['alive']} alive | "
                 f"{stats['archived']} archived | {stats['errors']} errors")

    cur = pg_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {PG_TABLE}")
    active = cur.fetchone()[0]
    try:
        cur.execute(f"SELECT COUNT(*) FROM {PG_ARCHIVE}")
        archived = cur.fetchone()[0]
    except Exception:
        pg_conn.rollback()
        archived = 0

    log.info(f"\n📊 {PG_TABLE}: {active} active | {PG_ARCHIVE}: {archived} archived")
    log.info("✅ CYCLE COMPLETE\n")


def main():
    parser = argparse.ArgumentParser(description="Bienici Sync + Archive")
    parser.add_argument("--sync-only", action="store_true")
    parser.add_argument("--archive-only", action="store_true")
    parser.add_argument("--loop", action="store_true", help="Loop continuously")
    args = parser.parse_args()

    do_sync = not args.archive_only
    do_archive = not args.sync_only

    log.info("Connecting to MongoDB...")
    mongo = MongoClient(MONGO_URI)
    mongo.admin.command("ping")
    log.info("✅ MongoDB connected")

    log.info("Connecting to PostgreSQL...")
    pg = psycopg2.connect(PG_DSN)
    log.info("✅ PostgreSQL connected")

    try:
        if args.loop:
            while True:
                try:
                    col = mongo[MONGO_DB][MONGO_COL]
                    run_cycle(col, pg, do_sync, do_archive)
                except Exception as e:
                    log.error(f"Cycle error: {e}")
                    try:
                        pg.close()
                    except Exception:
                        pass
                    pg = psycopg2.connect(PG_DSN)
                log.info(f"💤 Sleeping {CYCLE_SLEEP}s until next cycle...")
                time.sleep(CYCLE_SLEEP)
        else:
            col = mongo[MONGO_DB][MONGO_COL]
            run_cycle(col, pg, do_sync, do_archive)

    except KeyboardInterrupt:
        log.info("\n⚠️ Stopped by user")
    finally:
        pg.close()
        mongo.close()
        log.info("🔌 Connections closed")


if __name__ == "__main__":
    main()
