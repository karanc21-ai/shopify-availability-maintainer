#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Unified Shopify app: Dual-site availability sync + Counters + Daily CSV (IST)

This script:
1) Scans India store (IN):
   - Tracks availability per product using variant inventory.
   - When availability drops:
       • bumps sales counters & sale dates metafields,
       • sets custom.start_manufacturing so ops can see it must be manufactured,
       • notifies manufacturing_handler (App B),
       • reverts temp discounts,
       • clamps negative inventory back to 0,
       • updates "Ready To Ship" badge and delivery_time metafields,
       • (optional) flips product ACTIVE/DRAFT based on stock.
   - Records, for each SKU, whether that product is "2-5 Days Across India" or "12-15 Days Across India".
     We persist that map to disk for the US sync.

2) Scans US store (US):
   - Mirrors "price in India" into custom.priceinindia on US products.
   - Mirrors the India readiness string into custom.status_in_india on US products.
     That lets the US PDP say "in stock in India, reaches you in ~10-12 days".
   - Only writes metafields if something changed.
   - Sleeps ONLY if it actually wrote, reducing rate-limit pressure.
   - Mirrors any US availability *drops* back into IN inventory (if MIRROR_US_INCREASES etc.), and can clamp.

3) Maintains per-SKU counters (views / ATC / sales) via lightweight /pixel/* endpoints.
   Rolls them into a CSV once per IST day and stores daily snapshots.

4) Provides /run-now, /rebuild-index, /diag, /health HTTP endpoints.
"""

import os
import sys
import time
import json
import csv
import random
import math
import hmac
import base64
import threading
import ipaddress
from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from flask import Flask, request, jsonify, make_response, send_file

from mfg_notify import notify_mfg_sale


# =============================== CONFIG (ENV) ===============================

API_VERSION = os.getenv("API_VERSION", "2024-10").strip()

PIXEL_SHARED_SECRET = os.getenv("PIXEL_SHARED_SECRET", "").strip()
if not PIXEL_SHARED_SECRET:
    raise SystemExit("PIXEL_SHARED_SECRET required")

# ---- Scheduler (Dual-site) ----
RUN_EVERY_MIN = int(os.getenv("RUN_EVERY_MIN", "5"))
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "0") == "1"

SLEEP_BETWEEN_PRODUCTS_MS = int(os.getenv("SLEEP_BETWEEN_PRODUCTS_MS", "120"))
SLEEP_BETWEEN_PAGES_MS = int(os.getenv("SLEEP_BETWEEN_PAGES_MS", "400"))
SLEEP_BETWEEN_SHOPS_MS = int(os.getenv("SLEEP_BETWEEN_SHOPS_MS", "800"))
MUTATION_SLEEP_SEC = float(os.getenv("MUTATION_SLEEP_SEC", "0.35"))

# ---- India shop (Dual-site) ----
IN_DOMAIN = os.getenv("IN_DOMAIN", "").strip()
IN_TOKEN = os.getenv("IN_TOKEN", "").strip()
BADGES_FORCE_TYPE = os.getenv("BADGES_FORCE_TYPE", "").strip()
DELIVERY_FORCE_TYPE = os.getenv("DELIVERY_FORCE_TYPE", "").strip()


# IMPORTANT:
# Respect existing env var names.
# Use IN_COLLECTION_HANDLES if set (your production var),
# fallback to IN_COLLECTIONS only if IN_COLLECTION_HANDLES is missing.
_in_coll_env = os.getenv("IN_COLLECTION_HANDLES", os.getenv("IN_COLLECTIONS", ""))
IN_COLLECTIONS = [c.strip() for c in _in_coll_env.split(",") if c.strip()]

IN_LOCATION_ID = int(os.getenv("IN_LOCATION_ID", "0") or "0")

# ---- US shop (Dual-site) ----
US_DOMAIN = os.getenv("US_DOMAIN", "").strip()
US_TOKEN = os.getenv("US_TOKEN", "").strip()

# Same treatment for US:
_us_coll_env = os.getenv("US_COLLECTION_HANDLES", os.getenv("US_COLLECTIONS", ""))
US_COLLECTIONS = [c.strip() for c in _us_coll_env.split(",") if c.strip()]

US_LOCATION_ID = int(os.getenv("US_LOCATION_ID", "0") or "0")

# ---- Metafields / keys ----
MF_NAMESPACE      = os.getenv("MF_NAMESPACE", "custom").strip()
MF_BADGES_KEY     = os.getenv("MF_BADGES_KEY", "badges").strip()
MF_DELIVERY_KEY   = os.getenv("MF_DELIVERY_KEY", "delivery_time").strip()
MF_PRICEIN_KEY    = os.getenv("MF_PRICEIN_KEY", "priceinindia").strip()  # used on US
TEMP_DISC_KEY     = os.getenv("TEMP_DISC_KEY", "temp_discount_active").strip()  # Integer metafield on IN

KEY_SALES         = os.getenv("KEY_SALES", "sales_total").strip()
KEY_DATES         = os.getenv("KEY_DATES", "sales_dates").strip()

KEY_VIEWS         = os.getenv("KEY_VIEWS", "view_counts").strip()
KEY_ATC           = os.getenv("KEY_ATC", "atc_counts").strip()
KEY_TOTAL_SALES   = os.getenv("KEY_TOTAL_SALES", "sales_counts").strip()
KEY_SALE_DATES    = os.getenv("KEY_SALE_DATES", "sale_dates").strip()

KEY_AGE_DAYS      = os.getenv("KEY_AGE_DAYS", "age_days").strip()
KEY_DOB           = os.getenv("KEY_DOB", "dob").strip()

# --- Metafield for manufacturing signal ---
MF_START_MFG_KEY  = os.getenv("MF_START_MFG_KEY", "start_manufacturing").strip()
START_MFG_VALUE   = os.getenv("START_MFG_VALUE", "Start Manufacturing").strip()
AUTO_CLEAR_START_MFG_ON_INCREASE = os.getenv("AUTO_CLEAR_START_MFG_ON_INCREASE", "0") == "1"

# --- Badge + delivery text values for India ---
BADGE_READY       = os.getenv("BADGE_READY", "Ready To Ship").strip()
DELIVERY_READY    = os.getenv("DELIVERY_READY", "2-5 Days Across India").strip()
DELIVERY_MTO      = os.getenv("DELIVERY_MTO", "12-15 Days Across India").strip()

# ---- Behavior toggles ----
USE_PRODUCT_CUSTOM_SKU   = os.getenv("USE_PRODUCT_CUSTOM_SKU", "1") == "1"
ONLY_ACTIVE_FOR_MAPPING  = os.getenv("ONLY_ACTIVE_FOR_MAPPING", "1") == "1"
MIRROR_US_INCREASES      = os.getenv("MIRROR_US_INCREASES", "0") == "1"
CLAMP_AVAIL_TO_ZERO      = os.getenv("CLAMP_AVAIL_TO_ZERO", "1") == "1"

ROUND_STEP_INR           = int(os.getenv("DISCOUNT_ROUND_STEP_INR", "5"))
ROUND_STEP_USD           = int(os.getenv("DISCOUNT_ROUND_STEP_USD", "5"))

# ---- Counters app: Admin host/token (for polling/inventory watcher) ----
ADMIN_HOST   = os.getenv("ADMIN_HOST", IN_DOMAIN or "silver-rudradhan.myshopify.com")
ADMIN_TOKEN  = os.getenv("ADMIN_TOKEN", IN_TOKEN)
if not ADMIN_TOKEN:
    raise SystemExit("ADMIN_TOKEN (or IN_TOKEN) required for counters")
ADMIN_TOKENS: Dict[str, str] = {ADMIN_HOST: ADMIN_TOKEN}

SHOPIFY_WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET", "").strip()
if not SHOPIFY_WEBHOOK_SECRET:
    print("[WARN] SHOPIFY_WEBHOOK_SECRET not set; /webhook/products_create verification will fail.")

# ---- Counters app polling ----
INVENTORY_POLL_SEC = int(os.getenv("INVENTORY_POLL_SEC", "0"))
AVAIL_POLL_PRODUCT_IDS: Set[str] = set(
    s.strip() for s in (os.getenv("AVAIL_POLL_PRODUCT_IDS", "") or "").split(",") if s.strip()
)

ALLOWED_PIXEL_HOSTS = set(
    h.strip().lower()
    for h in (os.getenv("ALLOWED_PIXEL_HOSTS","silver-rudradhan.myshopify.com").split(","))
)

# IP ignore list for pixel counters
IGNORE_IPS_ENV = [s.strip() for s in os.getenv("IGNORE_IPS", "").split(",")] if os.getenv("IGNORE_IPS") else []

# ---- Manufacturing handler bridge (App A -> App B) ----
ENABLE_MFG_NOTIFY      = os.getenv("ENABLE_MFG_NOTIFY", "1") == "1"
B_ENDPOINT_URL         = os.getenv("B_ENDPOINT_URL", "").strip()
A_TO_B_SHARED_SECRET   = os.getenv("A_TO_B_SHARED_SECRET", "").strip()
SOURCE_SITE            = os.getenv("SOURCE_SITE", "IN").strip()  # "IN" or "US" etc.

# ---- Other feature toggles from env (we don't always use them here, but we keep them so names stay stable)
IN_INCLUDE_UNTRACKED = (os.getenv("IN_INCLUDE_UNTRACKED", "1") == "1")
IN_CHANGE_STATUS = (os.getenv("IN_CHANGE_STATUS", "0") == "1")
SPECIAL_STATUS_HANDLE = os.getenv("SPECIAL_STATUS_HANDLE", "").strip()

# ---- Paths / persistence ----
DATA_DIR = os.getenv("DATA_DIR", ".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)

PORT = int(os.getenv("PORT", "10000"))
FLUSH_INTERVAL_SEC = int(os.getenv("FLUSH_INTERVAL_SEC", "5"))
LOG_TO_STDOUT = os.getenv("LOG_TO_STDOUT", "1") == "1"
QUIET_PUSH = os.getenv("QUIET_PUSH", "0") == "1"

def p(*parts): return os.path.join(DATA_DIR, *parts)

# "last seen" / index persistence
IN_LAST_SEEN   = p("in_last_seen.json")
US_LAST_SEEN   = p("us_last_seen.json")
STATE_PATH     = p("dual_state.json")
IN_SKU_INDEX   = p("in_sku_index.json")
IN_DELIVERY_MAP = p("in_delivery_map.json")  # SKU -> "2-5 Days..." / "12-15 Days..."
LOG_CSV        = p("dual_sync_log.csv")
LOG_JSONL      = p("dual_sync_log.jsonl")
DISC_STATE     = p("discount_state.json")

# counters persistence
EVENTS_CSV      = p("events.csv")
ATC_CSV         = p("atc_events.csv")
VIEWS_JSON      = p("view_counts.json")
ATC_JSON        = p("atc_counts.json")
SALES_JSON      = p("sales_counts.json")
SALE_DATES_JSON = p("sale_dates.json")
AGE_JSON        = p("age_days.json")
DOB_CACHE_JSON  = p("dob_cache.json")


# ======================================================================
# ========================= LOW-LEVEL HELPERS ==========================
# ======================================================================

def hdr(token: str) -> dict:
    return {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
    }

def _backoff_delay(attempt: int, base: float = 0.5, mx: float = 10.0) -> float:
    # jittered exponential backoff for throttling
    return min(mx, base * (2 ** (attempt-1))) + random.random() * 0.2

def gql(domain: str, token: str, query: str, variables: dict = None) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    last_err = None
    for attempt in range(1, 8):
        try:
            r = requests.post(
                url,
                headers=hdr(token),
                json={"query": query, "variables": variables or {}},
                timeout=60
            )
            if r.status_code in (429,) or r.status_code >= 500:
                time.sleep(_backoff_delay(attempt, base=0.4))
                continue
            if r.status_code != 200:
                raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")
            data = r.json()
            if data.get("errors"):
                # retry if throttled
                if any((e.get("extensions") or {}).get("code", "").upper() == "THROTTLED" for e in data["errors"]):
                    time.sleep(_backoff_delay(attempt, base=0.4))
                    continue
                raise RuntimeError(f"GQL errors: {data['errors']}")
            return data["data"]
        except Exception as e:
            last_err = e
            time.sleep(_backoff_delay(attempt, base=0.4))
    raise RuntimeError(str(last_err) if last_err else "GraphQL throttled/5xx repeatedly")

def rest_adjust_inventory(domain: str, token: str, inventory_item_id: int, location_id: int, delta: int) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/adjust.json"
    payload = {
        "inventory_item_id": int(inventory_item_id),
        "location_id": int(location_id),
        "available_adjustment": int(delta)
    }
    last_err = None
    for attempt in range(1, 6):
        r = requests.post(url, headers=hdr(token), json=payload, timeout=30)
        if r.status_code in (429,) or r.status_code >= 500:
            time.sleep(_backoff_delay(attempt))
            continue
        if r.status_code >= 400:
            last_err = RuntimeError(f"REST adjust inv {r.status_code}: {r.text}")
            time.sleep(_backoff_delay(attempt))
            continue
        return
    if last_err:
        raise last_err

def rest_get_variant(domain: str, token: str, variant_id_num: int) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/variants/{variant_id_num}.json"
    r = requests.get(url, headers=hdr(token), timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"REST get variant {variant_id_num} {r.status_code}: {r.text}")
    return r.json().get("variant") or {}

def rest_update_variant_price(domain: str, token: str, variant_id_num: int, price: str) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/variants/{variant_id_num}.json"
    payload = {"variant": {"id": int(variant_id_num), "price": str(price)}}
    last_err = None
    for attempt in range(1, 6):
        r = requests.put(url, headers=hdr(token), json=payload, timeout=30)
        if r.status_code in (429,) or r.status_code >= 500:
            time.sleep(_backoff_delay(attempt, base=0.3, mx=8.0))
            continue
        if r.status_code >= 400:
            last_err = RuntimeError(f"REST update price {r.status_code}: {r.text}")
            time.sleep(_backoff_delay(attempt, base=0.2))
            continue
        return
    if last_err:
        raise last_err

def now_ist():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

def now_ist_str():
    return now_ist().strftime("%Y-%m-%d %H:%M:%S")

def today_ist_str():
    return now_ist().strftime("%Y-%m-%d")

def sleep_ms(ms: int):
    time.sleep(float(ms) / 1000.0)

def gid_num(gid: str) -> str:
    # convert gid://shopify/Product/123456789 -> "123456789"
    try:
        return gid.rsplit("/", 1)[-1]
    except Exception:
        return str(gid)

def _as_int_or_none(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None

def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=0)
    os.replace(tmp, path)

def ensure_log_header():
    if not os.path.exists(LOG_CSV):
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ts","shop","note","product_id","variant_id","sku","before","after",
                "delta","title","message","collections"
            ])

def log_row(
    emoji_phase: str,
    shop: str,
    note: str,
    product_id: str = "",
    variant_id: str = "",
    sku: str = "",
    delta: str = "",
    message: str = "",
    extra: dict = None,
    title: str = "",
    before: str = "",
    after: str = "",
    collections: str = ""
):
    ensure_log_header()
    ts = now_ist_str()
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            ts, shop, note, product_id, variant_id, sku,
            before, after, delta, title, message, collections
        ])
    row = {
        "ts": ts,
        "shop": shop, "note": note,
        "product_id": product_id,
        "variant_id": variant_id,
        "sku": sku,
        "before": before,
        "after": after,
        "delta": delta,
        "title": title,
        "message": message,
        "collections": collections,
    }
    if extra:
        row.update(extra)
    with open(LOG_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    if LOG_TO_STDOUT:
        human = (
            f"{emoji_phase} {shop} {note} [SKU {sku}] "
            f"pid={product_id} vid={variant_id} {before}→{after} "
            f"Δ{delta} “{title}” — {message}"
        )
        print(human, flush=True)

# -------- IP tools (pixel) --------
def _parse_networks(items: List[str]):
    nets = []
    for s in items:
        s = (s or "").strip()
        if not s:
            continue
        try:
            if "/" in s:
                nets.append(ipaddress.ip_network(s, strict=False))
            else:
                nets.append(ipaddress.ip_network(s + "/32", strict=False))
        except Exception:
            pass
    return nets

IGNORE_NETS = _parse_networks(IGNORE_IPS_ENV)

def is_ignored_ip(ip_str: str) -> bool:
    try:
        ip_obj = ipaddress.ip_address(ip_str)
        for net in IGNORE_NETS:
            if ip_obj in net:
                return True
        return False
    except Exception:
        return False


# ========================= AVAILABILITY & METAFIELDS =========================

def compute_product_availability(variants: List[dict], include_untracked: bool) -> int:
    total = 0
    counted = False
    for v in variants:
        tracked = bool(((v.get("inventoryItem") or {}).get("tracked")))
        qty = int(v.get("inventoryQuantity") or 0)
        if include_untracked or tracked:
            counted = True
            total += qty
    # If none counted, treat availability as 0
    return int(total if counted else 0)

def desired_state(avail: int) -> Tuple[str, str, str]:
    """
    Return tuple of (badge, display_badge, delivery_time_value) based on availability.
    - If avail >= 1 => "Ready To Ship" & "2-5 Days Across India"
    - else          => "" (no badge) & "12-15 Days Across India"
    """
    if avail >= 1:
        return BADGE_READY, BADGE_READY, DELIVERY_READY
    else:
        return "", BADGE_READY, DELIVERY_MTO

def update_product_status(domain: str, token: str, product_gid: str, new_status: str) -> bool:
    q = """
    mutation($id:ID!, $status:ProductStatus!){
      productUpdate(input:{id:$id,status:$status}){
        product{ id status }
        userErrors{ field message }
      }
    }"""
    try:
        data = gql(domain, token, q, {"id": product_gid, "status": new_status})
        errs = (((data.get("productUpdate") or {}).get("userErrors")) or [])
        if errs:
            return False
        return True
    except Exception:
        return False

def _encode_value_for_type(mf_type: str, raw_str: str) -> str:
    """
    Given the metafield type string (e.g. 'single_line_text_field' or 'list.single_line_text_field')
    and the human value we WANT ('Ready To Ship' or '2-5 Days Across India' or ''),
    return the string we actually must send to Shopify in metafieldsSet.

    Rules:
    - list.* types must be valid JSON array string. e.g. '["Ready To Ship"]' or '[]'
    - scalar types are just the raw string.
    """
    mf_type = (mf_type or "").strip()
    val = (raw_str or "").strip()

    if mf_type.startswith("list."):
        if val == "":
            # clear list: send an empty JSON array
            return "[]"
        # single element list
        return json.dumps([val], ensure_ascii=False)
    else:
        # scalar metafield types take a bare string
        return val


def set_product_metafields(domain: str, token: str, product_gid: str,
                           badges_node: dict, dtime_node: dict,
                           new_badge_human: str, new_dtime_human: str) -> None:
    """
    Writes two metafields on a product:
      - custom.badges         (can be list.single_line_text_field on IN)
      - custom.delivery_time  (usually single_line_text_field on IN)

    We:
    1. Resolve the correct metafield type for each (env override wins).
    2. Encode the desired values according to that type.
    3. Compare encoded values to current values from Shopify.
    4. Only send metafieldsSet if something changed.
    """

    # --- resolve types ---
    # badges
    mf_badge_type = (
        BADGES_FORCE_TYPE
        or (badges_node or {}).get("type")
        or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_BADGES_KEY)
        or "single_line_text_field"
    )

    # delivery_time
    mf_dtime_type = (
        DELIVERY_FORCE_TYPE
        or (dtime_node or {}).get("type")
        or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_DELIVERY_KEY)
        or "single_line_text_field"
    )

    # --- current stored values on Shopify ---
    current_badge_raw = ((badges_node or {}).get("value") or "").strip()
    current_dtime_raw = ((dtime_node or {}).get("value") or "").strip()

    # --- desired encoded values (what metafieldsSet must send) ---
    want_badge_encoded = _encode_value_for_type(mf_badge_type, new_badge_human)
    want_dtime_encoded = _encode_value_for_type(mf_dtime_type, new_dtime_human)

    mfs = []

    # badge metafield: only push if different
    if want_badge_encoded != current_badge_raw:
        mfs.append({
            "ownerId": product_gid,
            "namespace": MF_NAMESPACE,
            "key": MF_BADGES_KEY,
            "type": mf_badge_type,
            "value": want_badge_encoded,
        })

    # delivery_time metafield: only push if different
    if want_dtime_encoded != current_dtime_raw:
        mfs.append({
            "ownerId": product_gid,
            "namespace": MF_NAMESPACE,
            "key": MF_DELIVERY_KEY,
            "type": mf_dtime_type,
            "value": want_dtime_encoded,
        })

    # If nothing changed, skip call entirely.
    if not mfs:
        return

    mutation = """
    mutation($mfs:[MetafieldsSetInput!]!){
      metafieldsSet(metafields:$mfs){
        userErrors{ field message }
      }
    }"""

    try:
        data = gql(domain, token, mutation, {"mfs": mfs})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_row("⚠️", domain, "SET_FAIL",
                    product_id=gid_num(product_gid),
                    message=str(errs))
        else:
            log_row("🏷️", domain, "SET_OK",
                    product_id=gid_num(product_gid),
                    message="updated badge/delivery")
    except Exception as e:
        log_row("⚠️", domain, "SET_ERR",
                product_id=gid_num(product_gid),
                message=str(e))

    time.sleep(MUTATION_SLEEP_SEC)

def bump_sales_in(domain: str, token: str, product_gid: str,
                  sales_node: dict, dates_node: dict,
                  sold_qty: int, today: str):
    """
    Increment 'sales_total' metafield and append today's date
    to 'sales_dates' metafield.
    """
    sold_qty = int(sold_qty or 0)
    if sold_qty <= 0:
        return
    old_total = int((sales_node or {}).get("value") or "0")
    new_total = old_total + sold_qty

    old_dates_raw = (dates_node or {}).get("value") or ""
    dates_list = [d.strip() for d in old_dates_raw.split(",") if d.strip()]
    dates_list.append(today)
    new_dates_val = ",".join(dates_list[-50:])  # clamp

    mf_sales_type = (sales_node or {}).get("type") \
        or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, KEY_SALES) \
        or "single_line_text_field"
    mf_dates_type = (dates_node or {}).get("type") \
        or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, KEY_DATES) \
        or "single_line_text_field"

    mutation = """
    mutation($mfs:[MetafieldsSetInput!]!){
      metafieldsSet(metafields:$mfs){
        userErrors{ field message }
      }
    }"""
    mfs = [
        {
            "ownerId": product_gid,
            "namespace": MF_NAMESPACE,
            "key": KEY_SALES,
            "type": mf_sales_type,
            "value": str(new_total),
        },
        {
            "ownerId": product_gid,
            "namespace": MF_NAMESPACE,
            "key": KEY_DATES,
            "type": mf_dates_type,
            "value": new_dates_val,
        },
    ]
    try:
        data = gql(domain, token, mutation, {"mfs": mfs})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_row("⚠️", "IN", "SALES_BUMP_FAIL",
                    product_id=gid_num(product_gid),
                    delta=str(sold_qty),
                    message=str(errs))
        else:
            log_row("🧾", "IN", "SALES_BUMP_OK",
                    product_id=gid_num(product_gid),
                    delta=str(sold_qty),
                    message=f"+{sold_qty} sold")
    except Exception as e:
        log_row("⚠️", "IN", "SALES_BUMP_ERR",
                product_id=gid_num(product_gid),
                delta=str(sold_qty),
                message=str(e))
    time.sleep(MUTATION_SLEEP_SEC)

def set_start_manufacturing_flag(domain: str, token: str, product_gid: str, flag_value: str):
    """
    Writes/clears custom.start_manufacturing metafield.
    """
    mf_type = get_mf_def_type(
        domain, token, "PRODUCT", MF_NAMESPACE, MF_START_MFG_KEY
    ) or "single_line_text_field"

    mutation = """
    mutation($mfs:[MetafieldsSetInput!]!){
      metafieldsSet(metafields:$mfs){
        userErrors{ field message }
      }
    }"""
    mfs = [{
        "ownerId": product_gid,
        "namespace": MF_NAMESPACE,
        "key": MF_START_MFG_KEY,
        "type": mf_type,
        "value": flag_value or ""
    }]
    try:
        data = gql(domain, token, mutation, {"mfs": mfs})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_row("⚠️", "IN", "START_MFG_FAIL",
                    product_id=gid_num(product_gid),
                    message=str(errs))
        else:
            if flag_value:
                log_row("🏁", "IN", "START_MFG_SET",
                        product_id=gid_num(product_gid),
                        message="flag set")
            else:
                log_row("🧼", "IN", "START_MFG_CLR",
                        product_id=gid_num(product_gid),
                        message="flag cleared")
    except Exception as e:
        log_row("⚠️", "IN", "START_MFG_ERR",
                product_id=gid_num(product_gid),
                message=str(e))
    time.sleep(MUTATION_SLEEP_SEC)


# ========================= TEMP DISCOUNT HELPERS =========================

def ceil_to_step(value: float, step: int) -> float:
    step = max(1, int(step))
    return math.ceil(value / step) * step

def discount_round_step_for_domain(domain: str) -> int:
    if domain == IN_DOMAIN:
        return ROUND_STEP_INR
    else:
        return ROUND_STEP_USD

def load_disc_state() -> dict:
    try:
        with open(DISC_STATE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_disc_state(state: dict) -> None:
    tmp = DISC_STATE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=0)
    os.replace(tmp, DISC_STATE)

def disc_key(shop_tag: str, product_id: int, variant_id: int) -> str:
    return f"{shop_tag}:{product_id}:{variant_id}"

def parse_percent(v: str) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return 0

def set_temp_discount_percent(shop_tag: str, domain: str, token: str,
                              product_node: dict, percent: int):
    """
    Update variant prices using REST, track original price in DISC_STATE.
    """
    pid = gid_num(product_node["id"])
    title = product_node.get("title") or ""
    disc_state = load_disc_state()
    changed_any = False
    step = discount_round_step_for_domain(domain)

    for v in ((product_node.get("variants") or {}).get("nodes") or []):
        vid = gid_num(v.get("id"))
        sku = v.get("sku") or ""
        cur_price = float(
            rest_get_variant(domain, token, int(vid)).get("price") or 0.0
        )
        key = disc_key(shop_tag, pid, vid)
        entry = disc_state.get(key)

        if percent <= 0:
            # "percent <=0" case is revert logic handled elsewhere
            continue

        try:
            if not entry:
                entry = {
                    "original_price": cur_price,
                    "applied_percent": percent
                }
                disc_state[key] = entry

            last_applied = int(entry.get("applied_percent") or 0)
            original = float(entry.get("original_price") or 0.0)

            if last_applied != percent:
                new_price = ceil_to_step(
                    original * (1.0 - (percent / 100.0)),
                    step
                )
                if new_price > 0 and abs(new_price - cur_price) >= 0.01:
                    rest_update_variant_price(
                        domain, token, int(vid), str(int(new_price))
                    )
                    changed_any = True
                    log_row("🏷️", "DISC", "APPLIED",
                            product_id=pid,
                            variant_id=vid,
                            sku=sku,
                            title=title,
                            message=f"Price {cur_price} → {new_price} (step {step})")
                    time.sleep(MUTATION_SLEEP_SEC)
                entry["applied_percent"] = percent
        except Exception as e:
            log_row("⚠️", "DISC", "WARN",
                    product_id=pid,
                    variant_id=vid,
                    sku=sku,
                    message=f"Apply discount error: {e}")

    if changed_any or percent > 0:
        save_disc_state(disc_state)

def maybe_apply_temp_discount_for_product(shop_tag: str, domain: str, token: str,
                                          product_node: dict, avail: int):
    """
    Check product_node.tdisc metafield (Integer metafield "temp_discount_active").
    If >0 => apply discount %.
    If ==0 => revert discount.
    """
    pid = gid_num(product_node["id"])
    title = product_node.get("title") or ""
    sku = (((product_node.get("metafield") or {}).get("value")) or "").strip()
    tdisc_node = product_node.get("tdisc") or {}
    percent = parse_percent(tdisc_node.get("value") or "0")

    if percent > 0:
        try:
            set_temp_discount_percent(shop_tag, domain, token, product_node, percent)
        except Exception as e:
            log_row("⚠️", "DISC", "WARN",
                    product_id=pid,
                    sku=sku,
                    title=title,
                    message=f"Apply discount pass error: {e}")
        return

    # else percent == 0 => revert
    try:
        revert_temp_discount_for_product(shop_tag, domain, token, product_node)
    except Exception as e:
        log_row("⚠️", "DISC", "WARN",
                product_id=pid,
                sku=sku,
                title=title,
                message=f"Revert discount pass error: {e}")

def revert_temp_discount_for_product(shop_tag: str, domain: str, token: str,
                                     product_node: dict):
    pid = gid_num(product_node["id"])
    title = product_node.get("title") or ""
    sku = (((product_node.get("metafield") or {}).get("value")) or "").strip()
    disc_state = load_disc_state()
    changed_any = False

    for v in ((product_node.get("variants") or {}).get("nodes") or []):
        vid = gid_num(v.get("id"))
        key = disc_key(shop_tag, pid, vid)
        orig = disc_state.get(key)
        if not orig:
            continue
        try:
            original_price = float(orig.get("original_price"))
            rest_update_variant_price(
                domain, token, int(vid), str(int(original_price))
            )
            changed_any = True
            log_row("↩️", "DISC", "REVERTED",
                    product_id=pid,
                    variant_id=vid,
                    sku=sku,
                    title=title,
                    message=f"Restored price to {original_price}")
            time.sleep(MUTATION_SLEEP_SEC)
            disc_state.pop(key, None)
        except Exception as e:
            log_row("⚠️", "DISC", "WARN",
                    product_id=pid,
                    variant_id=vid,
                    sku=sku,
                    message=f"Revert discount error: {e}")
    if changed_any:
        save_disc_state(disc_state)


# --- Helpers for priceinindia/status_in_india sync on US ---

def normalize_price_for_meta(value_rupees: float, meta_type: str) -> str:
    """
    We persist priceinindia as an integer string (rounded).
    """
    try:
        v_int = int(round(float(value_rupees)))
    except Exception:
        v_int = 0
    return str(v_int)

def sync_priceinindia_for_us_product(
    us_product_node: dict,
    idx: dict,
    delivery_status: Optional[str] = None
):
    """
    Sync BOTH:
      1. custom.priceinindia
      2. custom.status_in_india

    Behaviour:
    - Work out what each value *should* be based on India data.
    - Compare to current metafields on this US product (pricein, statusIndia).
    - Only write metafields if changed.
    - Only sleep if we wrote (reduces throttling).
    """

    if not idx:
        return

    wrote_anything = False
    pid_us = gid_num(us_product_node["id"])

    # Resolve SKU on US product: prefer custom.sku metafield, fall back to first variant SKU.
    sku = (((us_product_node.get("metafield") or {}).get("value")) or "").strip()
    if not sku:
        try:
            first_var = ((us_product_node.get("variants") or {}).get("nodes") or [])[0]
            sku = (first_var.get("sku") or "").strip()
        except Exception:
            sku = ""
    if not sku:
        return

    # --------------------------
    # PRICE SYNC (custom.priceinindia)
    # --------------------------
    in_variant_id = idx.get("variant_id")
    desired_price_val = None
    price_changed = False
    price_mf_type = None

    if in_variant_id:
        try:
            var = rest_get_variant(IN_DOMAIN, IN_TOKEN, int(in_variant_id))
            in_price = float(var.get("price") or 0.0)
        except Exception as e:
            log_row("⚠️", "US", "PRICEINDIA_WARN",
                    product_id=pid_us,
                    sku=sku,
                    message=f"read IN price error: {e}")
            in_price = None

        if in_price is not None:
            pin_node = us_product_node.get("pricein") or {}
            price_mf_type = (
                pin_node.get("type")
                or get_mf_def_type(
                    US_DOMAIN,
                    US_TOKEN,
                    "PRODUCT",
                    MF_NAMESPACE,
                    MF_PRICEIN_KEY
                )
                or "single_line_text_field"
            )
            current_price_val = (pin_node.get("value") or "").strip()
            desired_price_val = normalize_price_for_meta(in_price, price_mf_type)

            if desired_price_val != current_price_val:
                price_changed = True

    # --------------------------
    # STATUS SYNC (custom.status_in_india)
    # --------------------------
    status_changed = False
    status_mf_type = None

    if delivery_status:
        status_node = us_product_node.get("statusIndia") or {}
        status_mf_type = (
            status_node.get("type")
            or get_mf_def_type(
                US_DOMAIN,
                US_TOKEN,
                "PRODUCT",
                MF_NAMESPACE,
                "status_in_india"
            )
            or "single_line_text_field"
        )
        current_status_val = (status_node.get("value") or "").strip()

        if delivery_status != current_status_val:
            status_changed = True

    # --------------------------
    # BUILD ONE metafieldsSet CALL IF ANYTHING CHANGED
    # --------------------------
    mfs_inputs = []

    if price_changed and desired_price_val is not None and price_mf_type:
        mfs_inputs.append({
            "ownerId": us_product_node["id"],
            "namespace": MF_NAMESPACE,
            "key": MF_PRICEIN_KEY,
            "type": price_mf_type,
            "value": desired_price_val,
        })

    if status_changed and status_mf_type:
        mfs_inputs.append({
            "ownerId": us_product_node["id"],
            "namespace": MF_NAMESPACE,
            "key": "status_in_india",
            "type": status_mf_type,
            "value": delivery_status,
        })

    if mfs_inputs:
        mutation = (
            "mutation($mfs:[MetafieldsSetInput!]!){ "
            "metafieldsSet(metafields:$mfs){ userErrors{ field message } } }"
        )
        try:
            data = gql(US_DOMAIN, US_TOKEN, mutation, {"mfs": mfs_inputs})
            errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
            if errs:
                log_row("⚠️", "US", "PRICEINDIA_WARN",
                        product_id=pid_us,
                        sku=sku,
                        message=f"sync error: {errs}")
            else:
                if price_changed:
                    log_row("🏷️", "US", "PRICEINDIA_SET",
                            product_id=pid_us,
                            sku=sku,
                            message=f"Set US {MF_NAMESPACE}.{MF_PRICEIN_KEY} = {desired_price_val}")
                if status_changed:
                    log_row("📦", "US", "STATUSINDIA_SET",
                            product_id=pid_us,
                            sku=sku,
                            message=f"Set US {MF_NAMESPACE}.status_in_india = {delivery_status}")
            wrote_anything = True
        except Exception as e:
            log_row("⚠️", "US", "PRICEINDIA_WARN",
                    product_id=pid_us,
                    sku=sku,
                    message=f"sync error: {e}")

    # --------------------------
    # THROTTLE ONLY IF WE ACTUALLY WROTE
    # --------------------------
    if wrote_anything:
        time.sleep(0.55)


# ========================= INDEX BUILDER (IN) =========================

def build_in_sku_index():
    """
    Build an index of SKU -> { product_id, inventory_item_id, variant_id, title, status } for IN shop.
    Only ACTIVE products are included if ONLY_ACTIVE_FOR_MAPPING=1.
    """
    index: Dict[str, Dict[str, Any]] = {}
    for handle in IN_COLLECTIONS:
        cursor = None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                status = (p.get("status") or "").upper()
                if ONLY_ACTIVE_FOR_MAPPING and status != "ACTIVE":
                    continue
                sku = (((p.get("metafield") or {}).get("value")) or "").strip()
                if not sku:
                    continue
                variants = ((p.get("variants") or {}).get("nodes") or [])
                if not variants:
                    continue
                v0 = variants[0]
                inv_item_gid = (((v0.get("inventoryItem") or {}).get("id")) or "")
                if not inv_item_gid:
                    continue
                inv_item_id = int(gid_num(inv_item_gid) or "0")
                variant_id = int(gid_num(v0.get("id") or "") or "0")
                index[sku] = {
                    "product_id": gid_num(p["id"]),
                    "inventory_item_id": inv_item_id,
                    "variant_id": variant_id,
                    "title": p.get("title") or "",
                    "status": status,
                }
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break
    save_json(IN_SKU_INDEX, index)
    log_row("🗂️", "IN", "INDEX_BUILT",
            message=f"entries={len(index)}")


# ========================= MFG NOTIFIER =========================

def _mfg_hmac_b64(secret: str, raw: bytes) -> str:
    import hashlib, hmac, base64
    return base64.b64encode(
        hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()
    ).decode()

def send_to_manufacturing(*, sku: str, delta: int, before: int, after: int,
                          product_id: str = "", variant_id: str = "",
                          title: str = "") -> None:
    if not ENABLE_MFG_NOTIFY:
        return
    if not (B_ENDPOINT_URL and A_TO_B_SHARED_SECRET):
        print("[MFG] skipped (missing B_ENDPOINT_URL or A_TO_B_SHARED_SECRET)",
              flush=True)
        return

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    sku_compact = (sku or "").replace(" ", "")
    idem = f"{SOURCE_SITE}-{ts}-{sku_compact}-{after}"

    payload = {
        "event_type": "availability_drop",
        "source_site": SOURCE_SITE,
        "sku": sku,
        "delta": int(delta),
        "availability_before": int(before),
        "availability_after": int(after),
        "product_id": product_id,
        "variant_id": variant_id,
        "title": title,
        "at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "idempotency_key": idem,
    }

    body_bytes = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    sig = _mfg_hmac_b64(A_TO_B_SHARED_SECRET, body_bytes)

    try:
        resp = requests.post(
            B_ENDPOINT_URL,
            headers={
                "Content-Type": "application/json",
                "X-APP-AUTH": sig
            },
            data=body_bytes,
            timeout=10
        )
        if resp.status_code >= 400:
            print(f"[MFG] notify HTTP {resp.status_code}: {resp.text}",
                  flush=True)
        else:
            print(f"[MFG] notify OK sku={sku} delta={delta} {before}->{after}",
                  flush=True)
    except Exception as e:
        print(f"[MFG] notify error: {e}", flush=True)


# ========================= GraphQL Queries =========================

QUERY_COLLECTION_PAGE_IN = f"""
query ($handle:String!, $cursor:String) {{
  collectionByHandle(handle:$handle){{
    products(first: 60, after:$cursor){{
      pageInfo{{ hasNextPage endCursor }}
      nodes{{
        id
        status
        title

        metafield(namespace:"{MF_NAMESPACE}", key:"sku"){{ value }}

        tdisc: metafield(namespace:"{MF_NAMESPACE}", key:"{TEMP_DISC_KEY}"){{
          id
          value
          type
        }}

        badges: metafield(namespace:"{MF_NAMESPACE}", key:"{MF_BADGES_KEY}"){{
          id
          value
          type
        }}
        dtime: metafield(namespace:"{MF_NAMESPACE}", key:"{MF_DELIVERY_KEY}"){{
          id
          value
          type
        }}
        salesTotal: metafield(namespace:"{MF_NAMESPACE}", key:"{KEY_SALES}"){{
          id
          value
          type
        }}
        salesDates: metafield(namespace:"{MF_NAMESPACE}", key:"{KEY_DATES}"){{
          id
          value
          type
        }}

        variants(first: 100){{
          nodes{{
            id
            sku
            inventoryQuantity
            inventoryItem{{ id tracked }}
            inventoryPolicy
          }}
        }}
      }}
    }}
  }}
}}
"""

QUERY_COLLECTION_PAGE_US = f"""
query ($handle:String!, $cursor:String) {{
  collectionByHandle(handle:$handle){{
    products(first: 60, after:$cursor){{
      pageInfo{{ hasNextPage endCursor }}
      nodes{{
        id
        title

        metafield(namespace:"{MF_NAMESPACE}", key:"sku"){{ value }}

        tdisc: metafield(namespace:"{MF_NAMESPACE}", key:"{TEMP_DISC_KEY}"){{
          id
          value
          type
        }}

        pricein: metafield(namespace:"{MF_NAMESPACE}", key:"{MF_PRICEIN_KEY}"){{
          id
          value
          type
        }}

        statusIndia: metafield(namespace:"{MF_NAMESPACE}", key:"status_in_india"){{
          id
          value
          type
        }}

        variants(first: 100){{
          nodes{{
            id
            sku
            inventoryQuantity
            inventoryItem{{ id }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# metafield definition cache
_MF_DEF_CACHE: Dict[Tuple[str, str, str], str] = {}

def get_mf_def_type(domain: str, token: str, owner_type: str,
                    namespace: str, key: str) -> str:
    ck = (owner_type, namespace, key)
    if ck in _MF_DEF_CACHE:
        return _MF_DEF_CACHE[ck]
    q = """
    query($ownerType:MetafieldOwnerType!, $ns:String!, $key:String!){
      metafieldDefinitions(first:1,
                           ownerType:$ownerType,
                           namespace:$ns,
                           key:$key){
        edges{
          node{
            id
            type{ name }
          }
        }
      }
    }"""
    data = gql(domain, token, q, {
        "ownerType": owner_type,
        "ns": namespace,
        "key": key
    })
    edges = (((data.get("metafieldDefinitions") or {}).get("edges")) or [])
    tname = (
        (((edges[0] if edges else {}).get("node") or {}).get("type") or {})
    ).get("name") or ""
    _MF_DEF_CACHE[ck] = tname or "single_line_text_field"
    return _MF_DEF_CACHE[ck]


# ========================= INDIA / USA SCANNERS =========================

def scan_india_and_update(read_only: bool = False):
    """
    Walk through IN collections:
    - Update discounts.
    - Detect stock drops, log sales, set manufacturing flag, notify B.
    - Update metafields.badges / metafields.delivery_time.
    - Build/refresh delivery_idx[SKU] = "2-5 Days..." / "12-15 Days..."
      and persist to in_delivery_map.json for the US sync.
    """
    last_seen: Dict[str, int] = load_json(IN_LAST_SEEN, {})
    today = today_ist_str()

    # load old delivery map so we don't lose SKUs when scanning multiple collections
    delivery_idx: Dict[str, str] = load_json(IN_DELIVERY_MAP, {})

    for handle in IN_COLLECTIONS:
        cursor = None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN,
                       {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                log_row("⚠️", "IN", "WARN",
                        message=f"Collection not found: {handle}")
                break

            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})

            for p in prods:
                pid = gid_num(p["id"])
                title = p.get("title") or ""
                status = (p.get("status") or "").upper()
                sku = (((p.get("metafield") or {}).get("value")) or "").strip()

                variants = ((p.get("variants") or {}).get("nodes") or [])
                avail = compute_product_availability(
                    variants, IN_INCLUDE_UNTRACKED
                )

                # Apply / revert discounts based on tdisc metafield
                if not read_only:
                    try:
                        maybe_apply_temp_discount_for_product(
                            "IN", IN_DOMAIN, IN_TOKEN, p, avail
                        )
                    except Exception as e:
                        log_row("⚠️", "DISC", "WARN",
                                product_id=pid,
                                sku=sku,
                                message=f"Apply discount pass error: {e}")

                prev = _as_int_or_none(last_seen.get(pid))
                if prev is None:
                    prev = 0

                # Optional auto-clear manufacturing flag if stock increased
                if (not read_only
                    and AUTO_CLEAR_START_MFG_ON_INCREASE
                    and avail > prev):
                    try:
                        set_start_manufacturing_flag(
                            IN_DOMAIN, IN_TOKEN, p["id"], ""
                        )
                    except Exception as e:
                        log_row("⚠️", "IN", "START_MFG_WARN",
                                product_id=pid,
                                sku=sku,
                                message=f"clear flag error: {e}")

                # Availability DROP => treat as sales
                if not read_only and avail < prev:
                    sold = prev - avail

                    bump_sales_in(
                        IN_DOMAIN, IN_TOKEN,
                        p["id"],
                        p.get("salesTotal") or {},
                        p.get("salesDates") or {},
                        sold,
                        today
                    )
                    log_row("🧾➖", "IN", "SALES_BUMP",
                            product_id=pid,
                            sku=sku,
                            delta=str(sold),
                            title=title,
                            message=f"({prev}->{avail}) (sold={sold})",
                            before=str(prev),
                            after=str(avail))

                    # Set manufacturing flag
                    try:
                        set_start_manufacturing_flag(
                            IN_DOMAIN, IN_TOKEN, p["id"], START_MFG_VALUE
                        )
                    except Exception as e:
                        log_row("⚠️", "IN", "START_MFG_WARN",
                                product_id=pid,
                                sku=sku,
                                message=f"set flag error: {e}")

                    # Notify manufacturing handler (App B)
                    try:
                        v0 = variants[0] if variants else {}
                        variant_id = gid_num(v0.get("id") or "")
                        sku_eff = (sku or v0.get("sku") or "").strip()

                        print(
                            f'[MFG] DROP DETECTED site={SOURCE_SITE} '
                            f'sku="{sku_eff}" {prev}->{avail} '
                            f'delta={avail - prev}',
                            flush=True
                        )

                        if ENABLE_MFG_NOTIFY and (avail < prev):
                            ok = notify_mfg_sale(
                                endpoint_url=B_ENDPOINT_URL or None,
                                shared_secret=A_TO_B_SHARED_SECRET or None,
                                source_site=SOURCE_SITE,
                                sku=sku_eff,
                                product_id=str(pid),
                                variant_id=str(variant_id or 0),
                                prev_avail=int(prev),
                                new_avail=int(avail),
                                title=title or "",
                                image_url=None,
                                body=None,
                                max_retries=4,
                            )
                            if not ok:
                                print(
                                    "[MFG] notify returned False "
                                    "(will rely on retries next scan)",
                                    flush=True
                                )

                    except Exception as e:
                        print(f"[MFG] invoke error: {e}", flush=True)

                    # Revert temp discount after sale
                    try:
                        revert_temp_discount_for_product(
                            "IN", IN_DOMAIN, IN_TOKEN, p
                        )
                    except Exception as e:
                        log_row("⚠️", "DISC", "WARN",
                                product_id=pid,
                                sku=sku,
                                message=f"Revert on sale error: {e}")

                # Clamp negatives to zero if configured
                if (not read_only
                    and CLAMP_AVAIL_TO_ZERO
                    and avail < 0
                    and variants):
                    inv_item_gid = (
                        ((variants[0].get("inventoryItem") or {}).get("id"))
                        or ""
                    )
                    inv_item_id = int(gid_num(inv_item_gid) or "0")
                    try:
                        rest_adjust_inventory(
                            IN_DOMAIN, IN_TOKEN,
                            inv_item_id,
                            int(IN_LOCATION_ID),
                            -avail
                        )
                        log_row("🧰0️⃣", "IN", "CLAMP_TO_ZERO",
                                product_id=pid,
                                sku=sku,
                                title=title,
                                message=f"inventory_item_id={inv_item_id}",
                                before=str(avail),
                                after="0")
                        avail = 0
                    except Exception as e:
                        log_row("⚠️", "IN", "WARN",
                                product_id=pid,
                                sku=sku,
                                message=f"Clamp error: {e}")

                # Desired metafield state for India PDP
                _, target_badge, target_delivery = desired_state(avail)

                # Track delivery status for this SKU for USA sync:
                if sku:
                    delivery_idx[sku] = target_delivery

                # Update India product metafields (badge + delivery_time)
                if not read_only:
                    set_product_metafields(
                        IN_DOMAIN,
                        IN_TOKEN,
                        p["id"],
                        p.get("badges") or {},
                        p.get("dtime") or {},
                        None,               # leave badges alone           
                        target_delivery,
                        sku_for_log=sku     # <- NEW: include SKU in log rows
                    )

                # Optionally flip ACTIVE/DRAFT if this is a special collection
                if (not read_only
                    and IN_CHANGE_STATUS
                    and SPECIAL_STATUS_HANDLE
                    and (handle == SPECIAL_STATUS_HANDLE)):
                    if avail < 1:
                        if status == "ACTIVE":
                            ok = update_product_status(
                                IN_DOMAIN, IN_TOKEN, p["id"], "DRAFT"
                            )
                            log_row("🛑", "IN", "STATUS_TO_DRAFT",
                                    product_id=pid,
                                    sku=sku,
                                    delta=str(avail),
                                    title=title,
                                    message=f"handle={handle}")
                    else:
                        if status == "DRAFT":
                            ok = update_product_status(
                                IN_DOMAIN, IN_TOKEN, p["id"], "ACTIVE"
                            )
                            log_row("✅", "IN", "STATUS_TO_ACTIVE",
                                    product_id=pid,
                                    sku=sku,
                                    delta=str(avail),
                                    title=title,
                                    message=f"handle={handle}")

                last_seen[pid] = max(0, int(avail))
                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            # end products loop

            save_json(IN_LAST_SEEN, last_seen)
            save_json(IN_DELIVERY_MAP, delivery_idx)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)

            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

def scan_usa_and_mirror_to_india(read_only: bool = False):
    """
    Walk through US collections:
    - Apply/revert temp discounts (US side) based on metafield.
    - Sync custom.priceinindia and custom.status_in_india from India data,
      but only if changed (conditional write).
    - Mirror US stock *drops* back into IN's inventory if configured.
    - Clamp negatives on US.
    """
    last_seen: Dict[str, int] = load_json(US_LAST_SEEN, {})
    in_index: Dict[str, Any] = load_json(IN_SKU_INDEX, {})
    delivery_idx: Dict[str, str] = load_json(IN_DELIVERY_MAP, {})

    for handle in US_COLLECTIONS:
        cursor = None
        while True:
            data = gql(US_DOMAIN, US_TOKEN, QUERY_COLLECTION_PAGE_US,
                       {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                break

            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})

            for p in prods:
                p_sku = (((p.get("metafield") or {}).get("value")) or "").strip()
                title = p.get("title") or ""

                # Calculate US total avail
                us_avail = 0
                for v0 in ((p.get("variants") or {}).get("nodes") or []):
                    us_avail += int(v0.get("inventoryQuantity") or 0)

                # US temp discounts
                if not read_only:
                    try:
                        maybe_apply_temp_discount_for_product(
                            "US", US_DOMAIN, US_TOKEN, p, us_avail
                        )
                    except Exception as e:
                        log_row("⚠️", "DISC", "WARN",
                                product_id=gid_num(p["id"]),
                                message=f"US apply discount pass error: {e}")

                # Sync priceinindia + status_in_india to US metafields
                if not read_only and p_sku and p_sku in in_index:
                    try:
                        sync_priceinindia_for_us_product(
                            p,
                            in_index.get(p_sku),
                            delivery_idx.get(p_sku)
                        )
                    except Exception as e:
                        log_row("⚠️", "US", "PRICEINDIA_WARN",
                                product_id=gid_num(p["id"]),
                                sku=p_sku,
                                message=f"sync error: {e}")

                # Mirror US availability drops back into IN (per variant)
                for v in ((p.get("variants") or {}).get("nodes") or []):
                    vid = gid_num(v.get("id"))
                    raw_sku = v.get("sku") or p_sku
                    sku_exact = (raw_sku or "").strip()
                    qty = int(v.get("inventoryQuantity") or 0)

                    prev = _as_int_or_none(last_seen.get(vid))
                    if prev is None:
                        last_seen[vid] = qty
                        continue

                    delta = qty - int(prev)
                    if delta == 0:
                        last_seen[vid] = qty
                        continue

                    # qty increased in US
                    if delta > 0:
                        if not MIRROR_US_INCREASES:
                            log_row("🙅‍♂️➕", "US→IN", "IGNORE_INCREASE",
                                    variant_id=vid,
                                    sku=sku_exact,
                                    before=str(prev),
                                    after=str(qty),
                                    message="us>in increase; mirroring disabled")
                            last_seen[vid] = qty
                            continue

                    # qty decreased in US
                    if not read_only and delta < 0 and sku_exact:
                        idx_info = in_index.get(sku_exact)
                        if not idx_info:
                            log_row("⚠️", "US→IN", "WARN_SKU",
                                    variant_id=vid,
                                    sku=sku_exact,
                                    delta=str(delta),
                                    title=title,
                                    message="No matching SKU in India index")
                        else:
                            in_inv_item_id = int(idx_info.get("inventory_item_id") or 0)
                            try:
                                rest_adjust_inventory(
                                    IN_DOMAIN,
                                    IN_TOKEN,
                                    int(in_inv_item_id),
                                    int(IN_LOCATION_ID),
                                    int(delta)
                                )
                                log_row("🔁", "US→IN", "MIRROR",
                                        variant_id=vid,
                                        sku=sku_exact,
                                        delta=str(delta),
                                        title=title,
                                        message=f"Mirrored {delta} to IN inv_item_id={in_inv_item_id}")
                            except Exception as e:
                                log_row("⚠️", "US→IN", "WARN",
                                        variant_id=vid,
                                        sku=sku_exact,
                                        delta=str(delta),
                                        title=title,
                                        message=f"Mirror error: {e}")

                        # clamp US negative inventory to zero if configured
                        if (not read_only
                            and CLAMP_AVAIL_TO_ZERO
                            and qty < 0):
                            try:
                                inv_item_gid = (
                                    ((v.get("inventoryItem") or {}).get("id"))
                                    or ""
                                )
                                rest_adjust_inventory(
                                    US_DOMAIN,
                                    US_TOKEN,
                                    int(gid_num(inv_item_gid)),
                                    int(US_LOCATION_ID),
                                    -qty
                                )
                                log_row("🧰0️⃣", "US", "CLAMP_TO_ZERO",
                                        variant_id=vid,
                                        sku=sku_exact,
                                        title=title,
                                        message=f"inventory_item_id={gid_num(inv_item_gid)}",
                                        before=str(qty),
                                        after="0")
                                qty = 0
                            except Exception as e:
                                log_row("⚠️", "US→IN", "WARN",
                                        variant_id=vid,
                                        sku=sku_exact,
                                        title=title,
                                        message=f"US clamp error: {e}")

                    last_seen[vid] = qty
                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            # end products loop

            save_json(US_LAST_SEEN, last_seen)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)

            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break


# ========================= COUNTERS & DAILY CSV =========================

def _load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=0)
    os.replace(tmp, path)

def _persist_all(state: dict):
    _save_json(VIEWS_JSON, state.get("views") or {})
    _save_json(ATC_JSON, state.get("atc") or {})
    _save_json(SALES_JSON, state.get("sales") or {})
    _save_json(SALE_DATES_JSON, state.get("sale_dates") or {})
    _save_json(AGE_JSON, state.get("age_days") or {})
    _save_json(DOB_CACHE_JSON, state.get("dob_cache") or {})

def _ist_today_date_str() -> str:
    return now_ist().strftime("%Y-%m-%d")

def _load_today_state() -> dict:
    st = {
        "views": _load_json(VIEWS_JSON, {}),
        "atc": _load_json(ATC_JSON, {}),
        "sales": _load_json(SALES_JSON, {}),
        "sale_dates": _load_json(SALE_DATES_JSON, {}),
        "age_days": _load_json(AGE_JSON, {}),
        "dob_cache": _load_json(DOB_CACHE_JSON, {}),
        "date": _ist_today_date_str(),
    }
    return st

def _save_today_state(st: dict):
    st["date"] = _ist_today_date_str()
    _persist_all(st)

def _ensure_daily_csv_header(path: str):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "date","sku","views","atc","sales",
                "last_sale_date","age_days","dob"
            ])

def _write_daily_csv_for_date(path: str, st: dict, date_str: str):
    views = st.get("views", {})
    atc = st.get("atc", {})
    sales = st.get("sales", {})
    sale_dates = st.get("sale_dates", {})
    age_days = st.get("age_days", {})
    dob_cache = st.get("dob_cache", {})

    rows = []
    all_skus = (
        set(views.keys())
        | set(atc.keys())
        | set(sales.keys())
        | set(sale_dates.keys())
        | set(age_days.keys())
        | set(dob_cache.keys())
    )
    for sku in sorted(all_skus):
        rows.append([
            date_str,
            sku,
            views.get(sku, 0),
            atc.get(sku, 0),
            sales.get(sku, 0),
            sale_dates.get(sku, ""),
            age_days.get(sku, 0),
            dob_cache.get(sku, ""),
        ])

    _ensure_daily_csv_header(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for row in rows:
            w.writerow(row)

def _reset_today_counters(st: dict):
    st["views"] = {}
    st["atc"] = {}
    st["sales"] = {}
    st["sale_dates"] = {}
    # keep age_days, dob_cache
    st["date"] = _ist_today_date_str()

def _rollover_if_needed(st: dict, csv_path: str):
    cur = _ist_today_date_str()
    if st.get("date") != cur:
        prev_date = st.get("date")
        _write_daily_csv_for_date(csv_path, st, prev_date)
        _reset_today_counters(st)
        _save_today_state(st)

def daily_csv_worker():
    st = _load_today_state()
    _rollover_if_needed(st, LOG_CSV)
    _save_today_state(st)

def to_iso8601(ts: float) -> str:
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%SZ")

def record_counter_event(kind: str, sku: str,
                         ip_addr: str = "", user_agent: str = ""):
    """
    kind in {"view","atc","sale"}.
    """
    if not sku:
        return

    if ip_addr and is_ignored_ip(ip_addr):
        return

    st = _load_today_state()
    if kind == "view":
        st["views"][sku] = st["views"].get(sku, 0) + 1
    elif kind == "atc":
        st["atc"][sku] = st["atc"].get(sku, 0) + 1
    elif kind == "sale":
        st["sales"][sku] = st["sales"].get(sku, 0) + 1
        st["sale_dates"][sku] = _ist_today_date_str()
    _save_today_state(st)


# ========================= AVAILABILITY POLLER (COUNTERS APP) =========================

def GET_VARIANT_AVAIL(domain: str, token: str, product_id: str) -> Dict[str, int]:
    q = f"""
    query($id:ID!){{
      product(id:$id){{
        variants(first:100){{
          nodes{{
            id
            inventoryQuantity
            inventoryItem{{ tracked }}
          }}
        }}
      }}
    }}
    """
    data = gql(domain, token, q, {"id": f"gid://shopify/Product/{product_id}"})
    result = {}
    try:
        vs = (((data.get("product") or {}).get("variants") or {}).get("nodes") or [])
        for v in vs:
            tracked = bool(((v.get("inventoryItem") or {}).get("tracked")))
            if not tracked:
                continue
            vid = gid_num(v.get("id") or "")
            qty = int(v.get("inventoryQuantity") or 0)
            result[vid] = qty
    except Exception:
        pass
    return result

def _availability_poll_once():
    if INVENTORY_POLL_SEC <= 0:
        return

    st = _load_today_state()
    polled_cache = load_json(STATE_PATH, {})
    changed_any = False

    for product_id in AVAIL_POLL_PRODUCT_IDS:
        if not product_id:
            continue
        live_map = GET_VARIANT_AVAIL(
            ADMIN_HOST,
            ADMIN_TOKENS[ADMIN_HOST],
            product_id
        )
        old_map = polled_cache.get(product_id, {})
        for vid, qty in live_map.items():
            old_qty = int(old_map.get(vid, qty))
            if qty < old_qty:
                sold = old_qty - qty
                sku_placeholder = f"{product_id}:{vid}"
                st["sales"][sku_placeholder] = st["sales"].get(sku_placeholder, 0) + sold
                st["sale_dates"][sku_placeholder] = _ist_today_date_str()
                changed_any = True
        polled_cache[product_id] = live_map

    if changed_any:
        save_json(STATE_PATH, polled_cache)
        _save_today_state(st)

def availability_poller():
    while True:
        try:
            _availability_poll_once()
        except Exception as e:
            print(f"[POLL] error: {e}", file=sys.stderr)
        time.sleep(max(1, INVENTORY_POLL_SEC))


# ========================= FLASK APP =========================

app = Flask(__name__)

def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Signature, X-Shop-Domain"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp

@app.route("/health")
def health():
    return jsonify({"ok": True, "ts": now_ist_str()})

@app.route("/diag")
def diag():
    st = _load_today_state()
    info = {
        "now_ist": now_ist_str(),
        "today": today_ist_str(),
        "run_every_min": RUN_EVERY_MIN,
        "enable_scheduler": ENABLE_SCHEDULER,
        "in_collections": IN_COLLECTIONS,
        "us_collections": US_COLLECTIONS,
        "in_last_seen_count": len(load_json(IN_LAST_SEEN, {})),
        "us_last_seen_count": len(load_json(US_LAST_SEEN, {})),
        "in_sku_index_count": len(load_json(IN_SKU_INDEX, {})),
        "in_delivery_map_count": len(load_json(IN_DELIVERY_MAP, {})),
        "poll_interval_sec": INVENTORY_POLL_SEC,
        "st_date": st.get("date"),
        "views": sum(st.get("views", {}).values()),
        "atc": sum(st.get("atc", {}).values()),
        "sales": sum(st.get("sales", {}).values()),
    }
    return jsonify(info)

@app.route("/debug/flush-now", methods=["POST"])
def debug_flush_now():
    daily_csv_worker()
    return jsonify({"ok": True, "ts": now_ist_str()})

@app.route("/rebuild-index", methods=["POST"])
def rebuild_index():
    try:
        build_in_sku_index()
        return jsonify({"ok": True, "ts": now_ist_str()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/run-now", methods=["POST"])
def run_now():
    try:
        run_cycle()
        return jsonify({"ok": True, "ts": now_ist_str()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# --- pixel endpoints (view/atc/sale) with shared-secret auth ---

def verify_hmac(raw_body: bytes, sent_sig_b64: str, secret: str) -> bool:
    try:
        expected = base64.b64encode(
            hmac.new(secret.encode("utf-8"), raw_body, digestmod="sha256").digest()
        ).decode()
        return hmac.compare_digest(expected, sent_sig_b64 or "")
    except Exception:
        return False

@app.route("/pixel/view", methods=["POST", "OPTIONS"])
def pixel_view():
    if request.method == "OPTIONS":
        return _cors(make_response())
    sig = request.headers.get("X-Signature", "")
    body = request.data or b""
    if not verify_hmac(body, sig, PIXEL_SHARED_SECRET):
        return _cors(make_response(jsonify({"ok": False, "error": "bad-signature"}), 403))
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        payload = {}
    shop_domain = (payload.get("shop_domain") or "").strip().lower()
    sku = (payload.get("sku") or "").strip()
    ip_addr = request.remote_addr or ""
    ua = request.headers.get("User-Agent", "")
    if shop_domain not in ALLOWED_PIXEL_HOSTS:
        return _cors(make_response(jsonify({"ok": False, "error":"forbidden-shop"}), 403))
    record_counter_event("view", sku, ip_addr, ua)
    return _cors(make_response(jsonify({"ok": True})))

@app.route("/pixel/atc", methods=["POST", "OPTIONS"])
def pixel_atc():
    if request.method == "OPTIONS":
        return _cors(make_response())
    sig = request.headers.get("X-Signature", "")
    body = request.data or b""
    if not verify_hmac(body, sig, PIXEL_SHARED_SECRET):
        return _cors(make_response(jsonify({"ok": False, "error": "bad-signature"}), 403))
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        payload = {}
    shop_domain = (payload.get("shop_domain") or "").strip().lower()
    sku = (payload.get("sku") or "").strip()
    ip_addr = request.remote_addr or ""
    ua = request.headers.get("User-Agent", "")
    if shop_domain not in ALLOWED_PIXEL_HOSTS:
        return _cors(make_response(jsonify({"ok": False, "error":"forbidden-shop"}), 403))
    record_counter_event("atc", sku, ip_addr, ua)
    return _cors(make_response(jsonify({"ok": True})))

@app.route("/pixel/sale", methods=["POST", "OPTIONS"])
def pixel_sale():
    if request.method == "OPTIONS":
        return _cors(make_response())
    sig = request.headers.get("X-Signature", "")
    body = request.data or b""
    if not verify_hmac(body, sig, PIXEL_SHARED_SECRET):
        return _cors(make_response(jsonify({"ok": False, "error":"bad-signature"}), 403))
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        payload = {}
    shop_domain = (payload.get("shop_domain") or "").strip().lower()
    sku = (payload.get("sku") or "").strip()
    ip_addr = request.remote_addr or ""
    ua = request.headers.get("User-Agent","")
    if shop_domain not in ALLOWED_PIXEL_HOSTS:
        return _cors(make_response(jsonify({"ok": False, "error":"forbidden-shop"}), 403))
    record_counter_event("sale", sku, ip_addr, ua)
    return _cors(make_response(jsonify({"ok": True})))


# ========================= SCHEDULER =========================

def load_state() -> dict:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(st: dict):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=0)
    os.replace(tmp, STATE_PATH)

def run_cycle():
    """
    One full scan:
      1) scan_india_and_update()
      2) scan_usa_and_mirror_to_india()
      3) daily_csv_worker()
    """
    # INDIA
    if IN_DOMAIN and IN_TOKEN and IN_COLLECTIONS:
        try:
            scan_india_and_update(read_only=False)
        except Exception as e:
            print(f"[CYCLE] IN scan error: {e}", file=sys.stderr)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

    # USA
    if US_DOMAIN and US_TOKEN and US_COLLECTIONS:
        try:
            scan_usa_and_mirror_to_india(read_only=False)
        except Exception as e:
            print(f"[CYCLE] US scan error: {e}", file=sys.stderr)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

    # counters daily roll
    try:
        daily_csv_worker()
    except Exception as e:
        print(f"[CYCLE] daily_csv_worker error: {e}", file=sys.stderr)

def scheduler_loop():
    """
    If ENABLE_SCHEDULER=1, loop forever calling run_cycle()
    every RUN_EVERY_MIN. Otherwise we run once and then
    just serve Flask.
    """
    if not ENABLE_SCHEDULER:
        print("[SCHED] Scheduler disabled, doing single run_cycle() then serving Flask.", flush=True)
        run_cycle()
        return

    print(f"[SCHED] loop enabled, every {RUN_EVERY_MIN} min", flush=True)
    while True:
        start_ts = time.time()
        run_cycle()
        elapsed = time.time() - start_ts
        sleep_sec = max(1.0, (RUN_EVERY_MIN * 60) - elapsed)
        time.sleep(sleep_sec)


# ========================= MAIN =========================

if __name__ == "__main__":
    # Build India SKU index once at boot so US sync has something to read,
    # and so you immediately see 🗂️ IN INDEX_BUILT in logs.
    try:
        if IN_DOMAIN and IN_TOKEN and IN_COLLECTIONS:
            build_in_sku_index()
    except Exception as e:
        print(f"[BOOT] build_in_sku_index error: {e}", flush=True)

    # Start background scheduler thread
    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()

    # Serve Flask with dev server (Render is currently calling python directly)
    app.run(host="0.0.0.0", port=PORT)
