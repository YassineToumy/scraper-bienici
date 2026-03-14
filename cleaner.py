#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bien'ici Data Cleaner — Locations (Incremental)
Normalise les données brutes MongoDB → MongoDB clean collection.
Mode incrémental : ne traite que les annonces non encore nettoyées.

Usage:
    python cleaner.py              # Incremental (only new docs)
    python cleaner.py --dry-run    # Preview sans écriture
    python cleaner.py --sample 5   # Afficher N exemples après nettoyage
    python cleaner.py --full       # Force re-clean de tout (drop + recreate)
"""

import os
import re
import html
import unicodedata
import argparse
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv
from storage import upload_images
from datetime import datetime, timezone

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGO_BIENICI_DB", "bienici")
SOURCE_COLLECTION = os.getenv("MONGO_BIENICI_COL_RAW", "locations")
CLEAN_COLLECTION = os.getenv("MONGO_BIENICI_COL_CLEAN", "locations_clean")
BATCH_SIZE = 500

# Validation thresholds (monthly rent in EUR)
MIN_PRICE = 50
MAX_PRICE = 20_000
MIN_SURFACE = 5
MAX_SURFACE = 500
MAX_ROOMS = 20
MIN_PRICE_PER_M2 = 2
MAX_PRICE_PER_M2 = 200

# ============================================================
# CLEANING HELPERS
# ============================================================

RE_HTML = re.compile(r"<[^>]+>")
RE_BOILERPLATE = re.compile(
    r"\s*(?:Loyer de|Soit avec Assurance|Les honoraires|Vous pouvez consulter|"
    r"Montant estimé des dépenses|Prix moyens des énergies|"
    r"Les informations sur les risques|Service facultatif|"
    r"Contribution annuelle).*",
    re.DOTALL | re.IGNORECASE,
)

ENERGY_MAP = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7}


def clean_description(raw: str) -> str | None:
    """Strip HTML, decode entities, normalize accents, remove boilerplate."""
    if not raw:
        return None
    text = RE_HTML.sub(" ", raw)
    text = html.unescape(text)
    text = RE_BOILERPLATE.sub("", text)
    # Normalize accented chars: é→e, à→a, ç→c, etc.
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    return text if len(text) > 20 else None


def extract_photo_urls(photos: list, source_id: str = "") -> list[str]:
    if not photos or not isinstance(photos, list):
        return []
    seen = set()
    urls = []
    for p in photos:
        url = None
        if isinstance(p, dict):
            url = p.get("url") or p.get("url_photo")
        elif isinstance(p, str):
            url = p
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
    return upload_images("bienici", source_id, urls)


def extract_coordinates(doc: dict) -> tuple[float | None, float | None]:
    blur = doc.get("blurInfo")
    if not blur or not isinstance(blur, dict):
        return None, None
    pos = blur.get("position") or blur.get("centroid") or {}
    lat = pos.get("lat")
    lon = pos.get("lon")
    if lat is not None and lon is not None:
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return round(lat, 6), round(lon, 6)
    return None, None


def extract_virtual_tour_urls(tours: list) -> list[str]:
    if not tours or not isinstance(tours, list):
        return []
    return [t.get("url") for t in tours if isinstance(t, dict) and t.get("url")]


def flatten_district(district: dict) -> dict | None:
    if not district or not isinstance(district, dict):
        return None
    flat = {}
    for key in ("name", "libelle", "cp", "code_insee", "insee_code", "postal_code"):
        val = district.get(key)
        if val is not None:
            flat[key] = val
    return flat if flat else None


def derive_department_code(doc: dict) -> str | None:
    dept = doc.get("departmentCode")
    if dept:
        return str(dept)
    pc = doc.get("postalCode")
    if pc:
        pc_str = str(pc)
        if len(pc_str) == 5 and pc_str[:3] in ("971", "972", "973", "974", "976"):
            return pc_str[:3]
        if len(pc_str) >= 2:
            return pc_str[:2]
    return None


# ============================================================
# MAIN CLEAN FUNCTION
# ============================================================

def clean_document(doc: dict) -> dict:
    c = {}

    source_id = str(doc.get("id"))
    c["source_id"] = source_id
    c["source"] = "bienici"
    c["country"] = "FR"
    c["reference"] = doc.get("reference")
    c["transaction_type"] = "rent"

    # URL — derive from id if not in raw doc
    raw_url = doc.get("url")
    c["url"] = raw_url if raw_url else f"https://www.bienici.com/annonce/location/{source_id}"

    prop_type = doc.get("propertyType")
    type_map = {"flat": "apartment", "house": "house", "parking": "parking",
                "loft": "loft", "castle": "house", "townhouse": "house"}
    c["property_type"] = type_map.get(prop_type, prop_type)

    c["city"] = doc.get("city")
    c["postal_code"] = doc.get("postalCode")
    c["department_code"] = derive_department_code(doc)
    c["address_known"] = doc.get("addressKnown")

    district = flatten_district(doc.get("district"))
    if district:
        c["district_name"] = district.get("libelle") or district.get("name")
        c["insee_code"] = district.get("insee_code") or district.get("code_insee")

    lat, lon = extract_coordinates(doc)
    if lat is not None:
        c["latitude"] = lat
        c["longitude"] = lon

    c["surface_m2"] = doc.get("surfaceArea")
    c["floor"] = doc.get("floor")
    c["rooms"] = doc.get("roomsQuantity")
    c["bedrooms"] = doc.get("bedroomsQuantity")
    c["bathrooms"] = doc.get("bathroomsQuantity")
    c["shower_rooms"] = doc.get("showerRoomsQuantity")
    c["terraces"] = doc.get("terracesQuantity")
    c["balconies"] = doc.get("balconyQuantity")
    c["parking_spots"] = doc.get("parkingPlacesQuantity")
    c["cellars"] = doc.get("cellarsOrUndergroundsQuantity")
    c["is_new"] = doc.get("newProperty")
    c["is_furnished"] = doc.get("isFurnished")
    c["is_accessible"] = doc.get("isDisabledPeopleFriendly")
    c["has_elevator"] = doc.get("hasElevator")
    # heating: bienici API uses both "heating" and "heatingType" depending on version
    c["heating"] = doc.get("heating") or doc.get("heatingType")
    c["optical_fiber"] = doc.get("opticalFiberStatus")

    c["price"] = doc.get("price")
    c["currency"] = "EUR"
    c["charges"] = doc.get("charges")
    # agency_fee: numeric only — agencyFeeUrl / agencyRentalFee can be a URL string, skip it
    raw_fee = doc.get("agencyRentalFee") or doc.get("feePercentage")
    c["agency_fee"] = raw_fee if isinstance(raw_fee, (int, float)) else None
    c["price_decreased"] = doc.get("priceHasDecreased")

    c["energy_class"] = doc.get("energyClassification")
    c["ghg_class"] = doc.get("greenhouseGazClassification")
    c["energy_value"] = doc.get("energyValue")
    c["ghg_value"] = doc.get("greenhouseGazValue")
    # energy_diag_date: two field names seen in the API
    c["energy_diag_date"] = (doc.get("energyPerformanceDiagnosticDate")
                              or doc.get("energyDiagnosticDate"))
    # energy costs: two field name variants in the API
    c["min_energy_cost"] = (doc.get("minEnergyConsumption")
                             or doc.get("minEnergyConsumptionCost"))
    c["max_energy_cost"] = (doc.get("maxEnergyConsumption")
                             or doc.get("maxEnergyConsumptionCost"))

    c["description"] = clean_description(doc.get("description"))

    photo_urls = extract_photo_urls(doc.get("photos"), source_id)
    c["photos"] = photo_urls if photo_urls else []
    c["photos_count"] = len(photo_urls)

    tour_urls = extract_virtual_tour_urls(doc.get("virtualTours"))
    if tour_urls:
        c["virtual_tours"] = tour_urls

    # posted_by_pro: prefer adCreatedByPro, fallback to not-isPrivateSeller
    raw_posted_by_pro = doc.get("adCreatedByPro")
    if raw_posted_by_pro is not None:
        c["posted_by_pro"] = raw_posted_by_pro
    elif doc.get("isPrivateSeller") is not None:
        c["posted_by_pro"] = not doc.get("isPrivateSeller")

    # agency_name: try multiple field names
    c["agency_name"] = (doc.get("accountDisplayName")
                         or doc.get("agencyName")
                         or (doc.get("agency") or {}).get("name"))
    # agency_id: try multiple field names
    c["agency_id"] = (doc.get("customerId")
                       or doc.get("agencyId")
                       or (doc.get("agency") or {}).get("id"))
    # is_exclusive: two field names seen in the API
    c["is_exclusive"] = (doc.get("isBienIciExclusive")
                          or doc.get("isExclusiveSaleMandate"))

    surface = c.get("surface_m2")
    price = c.get("price")
    rooms = c.get("rooms")
    bedrooms = c.get("bedrooms")

    if surface and price and surface > 0:
        c["price_per_m2"] = round(price / surface, 2)
    if surface and rooms and rooms > 0:
        c["surface_per_room"] = round(surface / rooms, 2)
    if surface and bedrooms and bedrooms > 0:
        c["surface_per_bedroom"] = round(surface / bedrooms, 2)
    if price and c.get("charges"):
        c["rent_excluding_charges"] = round(price - c["charges"], 2)

    equip_bool = ["has_elevator", "is_furnished", "is_accessible"]
    equip_qty = ["terraces", "balconies", "parking_spots", "cellars"]
    score = sum(1 for f in equip_bool if c.get(f) is True)
    score += sum(1 for f in equip_qty if (c.get(f) or 0) > 0)
    c["equipment_score"] = score

    if c.get("energy_class"):
        c["energy_numeric"] = ENERGY_MAP.get(c["energy_class"].upper())
    if c.get("ghg_class"):
        c["ghg_numeric"] = ENERGY_MAP.get(c["ghg_class"].upper())

    c["cleaned_at"] = datetime.now(timezone.utc)

    return {k: v for k, v in c.items() if v is not None}


# ============================================================
# VALIDATION
# ============================================================

def validate(doc: dict) -> tuple[bool, str | None]:
    price = doc.get("price")
    if not price or price < MIN_PRICE or price > MAX_PRICE:
        return False, "invalid_price"

    surface = doc.get("surface_m2")
    if not surface or surface < MIN_SURFACE or surface > MAX_SURFACE:
        return False, "invalid_surface"

    if doc.get("property_type") not in ("apartment", "house"):
        return False, "invalid_type"

    if not doc.get("city") or not doc.get("postal_code"):
        return False, "missing_location"

    rooms = doc.get("rooms")
    if rooms and rooms > MAX_ROOMS:
        return False, "aberrant_rooms"

    ppm = doc.get("price_per_m2")
    if ppm and (ppm < MIN_PRICE_PER_M2 or ppm > MAX_PRICE_PER_M2):
        return False, "aberrant_price_m2"

    return True, None


# ============================================================
# PIPELINE
# ============================================================

def connect_db():
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DATABASE]
    return client, db


def ensure_clean_collection(db):
    """Ensure collection + indexes exist (incremental: never drops)."""
    col = db[CLEAN_COLLECTION]
    col.create_index([("source_id", ASCENDING)], unique=True, name="source_id_unique")
    col.create_index([("city", ASCENDING)])
    col.create_index([("postal_code", ASCENDING)])
    col.create_index([("department_code", ASCENDING)])
    col.create_index([("property_type", ASCENDING)])
    col.create_index([("price", ASCENDING)])
    col.create_index([("surface_m2", ASCENDING)])
    col.create_index([("is_furnished", ASCENDING)])
    col.create_index([("country", ASCENDING)])
    col.create_index([
        ("city", ASCENDING),
        ("property_type", ASCENDING),
        ("price", ASCENDING),
    ])
    return col


def setup_clean_collection_full(db):
    """Full mode: drop + recreate (only with --full flag)."""
    col = db[CLEAN_COLLECTION]
    col.drop()
    print(f"🗑️  '{CLEAN_COLLECTION}' reset (full mode)")
    return ensure_clean_collection(db)


def insert_batch(col, batch):
    ins, dup = 0, 0
    try:
        r = col.insert_many(batch, ordered=False)
        ins = len(r.inserted_ids)
    except BulkWriteError as e:
        ins = e.details.get("nInserted", 0)
        dup = len(batch) - ins
    return ins, dup


def run(source, clean, dry_run=False):
    total = source.count_documents({})
    print(f"📊 Source total: {total} docs")

    # Load already-cleaned source_ids into memory
    if not dry_run and clean is not None:
        print("🔍 Loading already-cleaned IDs...")
        existing_ids = {d["source_id"] for d in clean.find({}, {"source_id": 1, "_id": 0})}
        print(f"   ✅ Already cleaned: {len(existing_ids)}")
    else:
        existing_ids = set()

    pending = total - len(existing_ids)
    print(f"   ⏳ Pending (not yet cleaned): {pending}\n")

    if pending == 0:
        print("✅ Nothing new to clean.")
        return

    stats = {
        "total": total, "pending": pending, "cleaned": 0, "inserted": 0,
        "invalid_price": 0, "invalid_surface": 0, "invalid_type": 0,
        "missing_location": 0, "aberrant_rooms": 0, "aberrant_price_m2": 0,
        "duplicates": 0, "errors": 0,
    }

    batch = []
    last_id = None
    processed = 0

    # Paginate by _id — each batch is a short fresh query, no long-running cursor
    while True:
        page_query = {"_id": {"$gt": last_id}} if last_id else {}
        docs = list(source.find(page_query).sort("_id", 1).limit(BATCH_SIZE))
        if not docs:
            break
        last_id = docs[-1]["_id"]

        for doc in docs:
            # Skip already-cleaned docs (checked in Python, no $nin query)
            if doc.get("id") in existing_ids:
                continue

            try:
                cleaned = clean_document(doc)
                stats["cleaned"] += 1

                valid, reason = validate(cleaned)
                if not valid:
                    stats[reason] = stats.get(reason, 0) + 1
                    continue

                cleaned.pop("_id", None)

                if dry_run:
                    stats["inserted"] += 1
                    continue

                batch.append(cleaned)

                if len(batch) >= BATCH_SIZE:
                    ins, dup = insert_batch(clean, batch)
                    stats["inserted"] += ins
                    stats["duplicates"] += dup
                    batch = []
                    processed += BATCH_SIZE
                    pct = min(processed / max(pending, 1) * 100, 100)
                    print(f"   ⏳ ~{processed}/{pending} ({pct:.1f}%) — ✅ {stats['inserted']}",
                          end="\r", flush=True)

            except Exception as e:
                stats["errors"] += 1
                if stats["errors"] <= 5:
                    print(f"\n   ⚠️  Error on {doc.get('id')}: {str(e)[:100]}")

    if batch and not dry_run:
        ins, dup = insert_batch(clean, batch)
        stats["inserted"] += ins
        stats["duplicates"] += dup

    print_stats(stats, dry_run)


def print_stats(s, dry_run=False):
    ins = s["inserted"]
    rejected = s["cleaned"] - ins - s["duplicates"]

    print(f"\n\n{'='*60}")
    print(f"📊 CLEANING RESULTS {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*60}")
    print(f"   📥 Source total:  {s['total']}")
    print(f"   ⏳ Pending:       {s['pending']}")
    print(f"   🧹 Processed:     {s['cleaned']}")
    print(f"   ✅ Inserted:      {ins}")
    if rejected > 0:
        print(f"   ❌ Rejected:     {rejected}")
        print(f"      💰 Bad price:      {s.get('invalid_price',0)}")
        print(f"      📐 Bad surface:    {s.get('invalid_surface',0)}")
        print(f"      🏠 Bad type:       {s.get('invalid_type',0)}")
        print(f"      📍 No location:    {s.get('missing_location',0)}")
        print(f"      🔢 Bad rooms:      {s.get('aberrant_rooms',0)}")
        print(f"      💵 Bad price/m²:   {s.get('aberrant_price_m2',0)}")
    if s["duplicates"]:
        print(f"   🔁 Duplicates:   {s['duplicates']}")
    if s["errors"]:
        print(f"   ⚠️  Errors:       {s['errors']}")
    print(f"{'='*60}")


def show_sample(clean, n=3):
    print(f"\n📄 SAMPLE CLEANED DOCUMENTS ({n}):")
    for doc in clean.find({}, {"_id": 0}).limit(n):
        print("─" * 60)
        for k, v in doc.items():
            if k == "photos":
                print(f"   {k}: [{len(v)} urls]")
            elif k == "description":
                print(f"   {k}: {str(v)[:80]}...")
            else:
                print(f"   {k}: {v}")
    print("─" * 60)


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Bien'ici Cleaner — Incremental")
    parser.add_argument("--dry-run", action="store_true", help="Validate without writing")
    parser.add_argument("--full", action="store_true", help="Drop + recreate (full re-clean)")
    parser.add_argument("--sample", type=int, default=0, help="Show N sample docs after")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("🧹 BIEN'ICI CLEANER — LOCATIONS")
    print(f"   {SOURCE_COLLECTION} → {CLEAN_COLLECTION}")
    mode = "DRY RUN" if args.dry_run else ("FULL RE-CLEAN" if args.full else "INCREMENTAL")
    print(f"   Mode: {mode}")
    print("=" * 60 + "\n")

    client, db = connect_db()
    source = db[SOURCE_COLLECTION]

    if args.dry_run:
        run(source, None, dry_run=True)
    elif args.full:
        clean = setup_clean_collection_full(db)
        run(source, clean)
        if args.sample > 0:
            show_sample(clean, args.sample)
    else:
        clean = ensure_clean_collection(db)
        run(source, clean)
        if args.sample > 0:
            show_sample(clean, args.sample)
        print(f"\n✅ Done! '{CLEAN_COLLECTION}': {clean.count_documents({})} total docs")

    client.close()


if __name__ == "__main__":
    main()
