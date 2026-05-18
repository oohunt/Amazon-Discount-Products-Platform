# -*- coding: utf-8 -*-
"""
Oohunt Data Pipeline
====================
Fetches Amazon deals from the CJ/PartnerBoost API (with coupon filtering)
and upserts them into MongoDB Atlas.

Usage:
    python pipeline.py               # fetch all categories, all products
    python pipeline.py --coupon      # fetch only products with active coupons
    python pipeline.py --category Electronics --coupon
    python pipeline.py --schedule    # run every 3 hours indefinitely

Env vars required in .env:
    PARTNERBOOST_API_KEY
    PARTNERBOOST_PID
    PARTNERBOOST_CID
    MONGODB_URI
    MONGODB_DB   (optional, default: oohunt)
"""

import asyncio
import argparse
import os
import re
import sys
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from loguru import logger
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

load_dotenv()

# --- Config ------------------------------------------------------------------
API_KEY  = os.getenv("PARTNERBOOST_API_KEY", "")
PID      = os.getenv("PARTNERBOOST_PID", "")
CID      = os.getenv("PARTNERBOOST_CID", "")
BASE_URL = os.getenv("PARTNERBOOST_BASE_URL", "https://cj.partnerboost.com/api")

MONGODB_URI = os.getenv("MONGODB_URI", "")
MONGODB_DB  = os.getenv("MONGODB_DB", "oohunt")
COLLECTION  = "products"

SCHEDULE_INTERVAL_HOURS = 3

# Amazon category values used by PartnerBoost API
AMAZON_CATEGORIES = [
    "Electronics",
    "Clothing, Shoes & Jewelry",
    "Home & Kitchen",
    "Sports & Outdoors",
    "Beauty & Personal Care",
    "Toys & Games",
    "Books",
    "Automotive",
    "Health & Household",
    "Tools & Home Improvement",
    "Pet Supplies",
    "Patio, Lawn & Garden",
    "Baby",
    "Office Products",
]

# Map PartnerBoost category -> Oohunt slug
CATEGORY_SLUG_MAP = {
    "Electronics":                   "electronics",
    "Clothing, Shoes & Jewelry":     "clothing",
    "Home & Kitchen":                "home-kitchen",
    "Sports & Outdoors":             "sports",
    "Beauty & Personal Care":        "beauty",
    "Toys & Games":                  "toys",
    "Books":                         "books",
    "Automotive":                    "automotive",
    "Health & Household":            "health",
    "Tools & Home Improvement":      "tools",
    "Pet Supplies":                  "pets",
    "Patio, Lawn & Garden":          "garden",
    "Baby":                          "baby",
    "Office Products":               "office",
}

# Basic haram keyword filter (keep in sync with nextjs-app/app/lib/haram-filter.ts)
HARAM_KEYWORDS = {
    "alcohol", "beer", "wine", "vodka", "whiskey", "rum", "gin", "tequila",
    "pork", "bacon", "ham", "lard", "gelatin",
    "tobacco", "cigarette", "cigar", "vape", "e-cigarette",
    "gambling", "casino", "lottery", "poker chips",
    "adult", "xxx", "erotic",
    "halloween", "horror", "devil", "demon", "witch", "occult",
    "statue", "figurine", "idol",
    "astrology", "tarot", "ouija",
}


def is_haram(title: str, category: str = "") -> bool:
    text = "{} {}".format(title, category).lower()
    return any(kw in text for kw in HARAM_KEYWORDS)


def get_mongo_collection():
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_URI env var is not set. Update .env with your Atlas connection string.")
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGODB_DB]
    col = db[COLLECTION]
    # Ensure indexes
    col.create_index("product_id", unique=True)
    col.create_index("asin", sparse=True)
    col.create_index("category")
    col.create_index("coupon_type", sparse=True)
    col.create_index("fetched_at")
    # TTL index -- auto-delete documents after 7 days
    # Wrapped in try/except because an equivalent index may already exist with a different name
    try:
        existing = col.index_information()
        has_ttl = any(
            idx.get("expireAfterSeconds") is not None
            for idx in existing.values()
        )
        if not has_ttl:
            col.create_index("fetched_at", expireAfterSeconds=60 * 60 * 24 * 7, name="ttl_fetched_at")
    except Exception as e:
        logger.warning("TTL index skipped (already exists or conflict): {}".format(e))
    return col


async def fetch_page(
    session: aiohttp.ClientSession,
    category: str,
    cursor: str = "",
    have_coupon: int = 2,
    limit: int = 50,
) -> dict:
    """Fetch one page from the PartnerBoost API."""
    body = {
        "pid": PID,
        "cid": CID,
        "cursor": cursor,
        "limit": min(limit, 50),
        "country_code": "US",
        "brand_id": 0,
        "asins": "",
        "category": category,
        "subcategory": "",
        "is_featured_product": 2,
        "is_amazon_choice": 2,
        "have_coupon": have_coupon,
        "discount_min": 0,
    }
    headers = {
        "Authorization": "Bearer {}".format(API_KEY),
        "Content-Type": "application/json",
        "Request-Source": "cj",
    }
    async with session.post(
        "{}/get_products".format(BASE_URL),
        json=body,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=30),
    ) as resp:
        data = await resp.json()
    if data.get("code") != 0:
        raise RuntimeError("API error: {}".format(data.get("message", "unknown")))
    return data.get("data", {})


def transform_product(p: dict, category_slug: str, now: datetime) -> dict:
    """Transform a raw PartnerBoost product dict into a MongoDB document."""
    coupon_type  = None
    coupon_value = None
    raw_coupon   = p.get("coupon")
    coupon_raw   = str(raw_coupon).strip() if raw_coupon and str(raw_coupon).lower() not in ("none", "null", "") else ""
    if coupon_raw:
        if "%" in coupon_raw:
            coupon_type = "percentage"
            try:
                coupon_value = float(re.sub(r'[^0-9.]', '', coupon_raw))
            except ValueError:
                pass
        else:
            digits = re.sub(r'[^0-9.]', '', coupon_raw)
            if digits:
                coupon_type = "fixed"
                try:
                    coupon_value = float(digits)
                except ValueError:
                    pass

    return {
        "product_id":          str(p.get("product_id", "")),
        "product_name":        str(p.get("product_name", "")),
        "image":               str(p.get("image", "")),
        "url":                 str(p.get("url", "")),
        "brand_name":          str(p.get("brand_name", "")),
        "network":             "partnerboost",
        "advertiser":          "amazon",
        "category":            category_slug,
        "category_raw":        str(p.get("category", "")),
        "subcategory":         str(p.get("subcategory", "")),
        "original_price":      re.sub(r'[^0-9.]', '', str(p.get("original_price", "") or "")),
        "discount_price":      re.sub(r'[^0-9.]', '', str(p.get("discount_price", "") or "")),
        "discount":            re.sub(r'[^0-9.]', '', str(p.get("discount", "0") or "0")),
        "currency":            "USD",
        "asin":                str(p.get("asin", "")),
        "parent_asin":         str(p.get("parent_asin", "")),
        "variant_asin":        str(p.get("variant_asin", "")),
        "coupon":              coupon_raw,
        "discount_code":       "" if not p.get("discount_code") or str(p.get("discount_code")).lower() in ("none", "null") else str(p.get("discount_code", "")),
        "coupon_type":         coupon_type,
        "coupon_value":        coupon_value,
        "is_amazon_choice":    int(p.get("is_amazon_choice") or 0),
        "is_featured_product": int(p.get("is_featured_product") or 0),
        "country_code":        "US",
        "rating":              str(p.get("rating", "")),
        "reviews":             int(p.get("reviews", 0) or 0),
        "availability":        str(p.get("availability", "")),
        "commission":          "",
        "update_time":         str(p.get("update_time", "")),
        "brand_id":            int(p.get("brand_id", 0) or 0),
        "source":              "cj_crawler",
        "fetched_at":          now,
    }


async def crawl_category(
    session: aiohttp.ClientSession,
    category: str,
    have_coupon: int,
    max_pages: int = 10,
) -> list:
    """Crawl all pages for one category, return list of product dicts."""
    slug = CATEGORY_SLUG_MAP.get(category, "other")
    now  = datetime.now(timezone.utc)
    products = []
    cursor   = ""
    page     = 0

    while page < max_pages:
        try:
            data = await fetch_page(session, category, cursor=cursor, have_coupon=have_coupon)
        except Exception as e:
            logger.warning("  Page {} failed for '{}': {}".format(page + 1, category, e))
            break

        raw_list = data.get("list", [])
        if not raw_list:
            break

        for p in raw_list:
            title = str(p.get("product_name", ""))
            cat   = str(p.get("category", ""))
            img   = str(p.get("image", ""))
            if not img.startswith("https://"):
                continue
            if is_haram(title, cat):
                continue
            products.append(transform_product(p, slug, now))

        cursor   = data.get("cursor", "")
        has_more = bool(data.get("has_more", False))
        page    += 1

        short_cursor = cursor[:20] if cursor else "end"
        logger.info("  [{}] page {}: +{} items (cursor: {})".format(category, page, len(raw_list), short_cursor))

        if not has_more or not cursor:
            break

        await asyncio.sleep(0.3)

    return products


def upsert_to_mongo(col, docs: list) -> tuple:
    """Bulk upsert documents. Returns (upserted, modified)."""
    if not docs:
        return 0, 0
    ops = [
        UpdateOne(
            {"product_id": d["product_id"]},
            {"$set": d},
            upsert=True,
        )
        for d in docs
        if d.get("product_id")
    ]
    try:
        result = col.bulk_write(ops, ordered=False)
        return result.upserted_count, result.modified_count
    except BulkWriteError as e:
        logger.warning("Bulk write partial error: {}".format(e.details.get("writeErrors", [])[:3]))
        return 0, 0


async def run_pipeline(
    categories: Optional[list] = None,
    have_coupon: int = 2,
    max_pages: int = 10,
):
    """Main pipeline: crawl all categories and write to MongoDB."""
    if not API_KEY or not PID or not CID:
        logger.error("Missing API credentials. Set PARTNERBOOST_API_KEY, PARTNERBOOST_PID, PARTNERBOOST_CID in .env")
        sys.exit(1)

    col  = get_mongo_collection()
    cats = categories or AMAZON_CATEGORIES

    coupon_label = "coupon only" if have_coupon == 1 else "all products"
    logger.info("Starting pipeline -- {} categories, filter: {}".format(len(cats), coupon_label))

    total_upserted = 0
    total_modified = 0
    total_products = 0

    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        for cat in cats:
            logger.info("Crawling: {}".format(cat))
            docs = await crawl_category(session, cat, have_coupon, max_pages)
            total_products += len(docs)
            u, m = upsert_to_mongo(col, docs)
            total_upserted += u
            total_modified += m
            logger.success("  -> {} products | {} new, {} updated".format(len(docs), u, m))
            await asyncio.sleep(1)

    logger.success(
        "Pipeline complete: {} products processed | {} new, {} updated".format(
            total_products, total_upserted, total_modified
        )
    )


def run_scheduler(have_coupon: int):
    """Run the pipeline every SCHEDULE_INTERVAL_HOURS hours."""
    import time
    logger.info("Scheduler started -- running every {} hours".format(SCHEDULE_INTERVAL_HOURS))
    while True:
        logger.info("=== Starting scheduled pipeline run ===")
        asyncio.run(run_pipeline(have_coupon=have_coupon))
        logger.info("Next run in {} hours".format(SCHEDULE_INTERVAL_HOURS))
        time.sleep(SCHEDULE_INTERVAL_HOURS * 3600)


def main():
    parser = argparse.ArgumentParser(
        description="Oohunt data pipeline -- CJ/PartnerBoost -> MongoDB"
    )
    parser.add_argument("--coupon", action="store_true", help="Fetch only products with active coupons")
    parser.add_argument("--category", type=str, help="Fetch a single category only (e.g. Electronics)")
    parser.add_argument("--pages", type=int, default=10, help="Max pages per category (default: 10, up to 500 products)")
    parser.add_argument("--schedule", action="store_true", help="Run on a schedule every 3 hours")
    args = parser.parse_args()

    have_coupon = 1 if args.coupon else 2
    categories  = [args.category] if args.category else None

    if args.schedule:
        run_scheduler(have_coupon)
    else:
        asyncio.run(run_pipeline(categories=categories, have_coupon=have_coupon, max_pages=args.pages))


if __name__ == "__main__":
    main()
