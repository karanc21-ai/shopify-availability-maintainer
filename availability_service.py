#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Unified Shopify app: Dual-site availability sync + Counters + Daily CSV (IST)

What it combines (superset of both apps)
----------------------------------------
A) Dual-site sync (India + USA)
   - India scan: availability → badges/delivery/status, sales inference on drops,
     clamp negatives to 0 (optional), temp discount (apply/revert), sales_total & sales_dates.
   - USA scan: mirror decreases to India by exact SKU index; clamp negatives to 0 (optional);
     temp discount; copy India's first-variant price → US metafield custom.priceinindia.
   - Index: exact SKU → {product_id, inventory_item_id(first variant), variant_id(first variant), title, status}
   - Persistence: in_last_seen/us_last_seen, in_sku_index, discount_state
   - Logs: CSV + JSONL. HTTP: /health, /diag, /run, /rebuild_index
   - Scheduler loop (optional) to run every RUN_EVERY_MIN

B) Counters + Daily CSV (IST midnight)
   - Pixel endpoints:
       POST /track/product  (views_total)
       POST /track/atc      (added_to_cart_total)
     (scoped to ALLOWED_PIXEL_HOSTS env)
   - Availability-based sales proxy (background polling) → sales_total, sales_dates
   - DOB & AGE helpers:
       /dob/set, /dob/bulk, /dob/backfill_created_at, /dob/set_all_today, /age/recompute, /age/seed_all
   - Daily CSV ledger (12:00 AM IST): date_ist, product_id, views_today, atc_today, sales_today, age_in_days_today
   - Background flusher pushes counters to metafields every FLUSH_INTERVAL_SEC

Notes
-----
- Uses Shopify Admin GraphQL for reads/writes (metafields, listings) and REST for inventory adjust and variant pricing.
- First Dual-site cycle is read-only (learn baselines + build index), then deltas apply.
- Timezone: IST for CSV rollovers and "today" stamps, UTC for event ts normalization.

Portability
-----------
- Tested for Python 3.10+ (Render default stacks). If you must run 3.8, swap typing `|` with Optional/Set and add backports.zoneinfo.

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
IN_LOCATION_ID = os.getenv("IN_LOCATION_ID", "").strip()
IN_COLLECTIONS = [x.strip() for x in os.getenv("IN_COLLECTION_HANDLES", "").split(",") if x.strip()]
IN_INCLUDE_UNTRACKED = os.getenv("IN_INCLUDE_UNTRACKED", "0") == "1"
IN_CHANGE_STATUS = os.getenv("IN_CHANGE_STATUS", "0") == "1"
SPECIAL_STATUS_HANDLE = os.getenv("SPECIAL_STATUS_HANDLE", "").strip()

# ---- USA shop (Dual-site) ----
US_DOMAIN = os.getenv("US_DOMAIN", "").strip()
US_TOKEN = os.getenv("US_TOKEN", "").strip()
US_LOCATION_ID = os.getenv("US_LOCATION_ID", "").strip()
US_COLLECTIONS = [x.strip() for x in os.getenv("US_COLLECTION_HANDLES", "").split(",") if x.strip()]

# ---- Metafields & keys (shared) ----
MF_NAMESPACE = os.getenv("MF_NAMESPACE", "custom").strip()
MF_BADGES_KEY = os.getenv("MF_BADGES_KEY", "badges").strip()
MF_DELIVERY_KEY = os.getenv("MF_DELIVERY_KEY", "delivery_time").strip()
KEY_SALES = os.getenv("KEY_SALES", "sales_total").strip()
KEY_DATES = os.getenv("KEY_DATES", "sales_dates").strip()      # list.date recommended
TEMP_DISC_KEY = os.getenv("TEMP_DISC_KEY", "temp_discount_active").strip()
MF_PRICEIN_KEY = os.getenv("MF_PRICEIN_KEY", "priceinindia").strip()  # on US product
KEY_VIEWS = os.getenv("KEY_VIEWS", "views_total").strip()
KEY_ATC = os.getenv("KEY_ATC", "added_to_cart_total").strip()
KEY_AGE = os.getenv("KEY_AGE", "age_in_days").strip()
KEY_DOB = os.getenv("KEY_DOB", "dob").strip()

BADGE_READY = os.getenv("BADGE_READY", "Ready To Ship")
DELIVERY_READY = os.getenv("DELIVERY_READY", "2-5 Days Across India")
DELIVERY_MTO = os.getenv("DELIVERY_MTO", "12-15 Days Across India")

# ---- Behavior toggles (Dual-site) ----
USE_PRODUCT_CUSTOM_SKU = os.getenv("USE_PRODUCT_CUSTOM_SKU", "1") == "1"
ONLY_ACTIVE_FOR_MAPPING = os.getenv("ONLY_ACTIVE_FOR_MAPPING", "1") == "1"
MIRROR_US_INCREASES = os.getenv("MIRROR_US_INCREASES", "0") == "1"
CLAMP_AVAIL_TO_ZERO = os.getenv("CLAMP_AVAIL_TO_ZERO", "1") == "1"

# ---- Discount rounding steps ----
ROUND_STEP_INR = int(os.getenv("DISCOUNT_ROUND_STEP_INR", "5"))
ROUND_STEP_USD = int(os.getenv("DISCOUNT_ROUND_STEP_USD", "5"))

# ---- Counters app: Admin host/token (single-host counters target) ----
ADMIN_HOST = os.getenv("ADMIN_HOST", IN_DOMAIN or "silver-rudradhan.myshopify.com")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", IN_TOKEN)
if not ADMIN_TOKEN:
    raise SystemExit("ADMIN_TOKEN (or IN_TOKEN) required for counters")
ADMIN_TOKENS: Dict[str, str] = {ADMIN_HOST: ADMIN_TOKEN}

SHOPIFY_WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET", "").strip()
if not SHOPIFY_WEBHOOK_SECRET:
    print("[WARN] SHOPIFY_WEBHOOK_SECRET not set; /webhook/products_create verification will fail.")

# ---- Counters app: polling + pixel hosts ----
INVENTORY_POLL_SEC = int(os.getenv("INVENTORY_POLL_SEC", "0"))  # 0 disables availability polling
AVAIL_POLL_PRODUCT_IDS: Set[str] = set(s.strip() for s in (os.getenv("AVAIL_POLL_PRODUCT_IDS","") or "").split(",") if s.strip())

ALLOWED_PIXEL_HOSTS = set(h.strip().lower() for h in (os.getenv("ALLOWED_PIXEL_HOSTS","rudradhan.com").split(",")))
# IP ignore list for pixels (comma-separated single IPs or CIDRs)
IGNORE_IPS_ENV = [s.strip() for s in os.getenv("IGNORE_IPS", "").split(",")] if os.getenv("IGNORE_IPS") else []

# ---- Server & persistence ----
DATA_DIR = os.getenv("DATA_DIR", ".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)
PORT = int(os.getenv("PORT", "10000"))
FLUSH_INTERVAL_SEC = int(os.getenv("FLUSH_INTERVAL_SEC", "5"))
LOG_TO_STDOUT = os.getenv("LOG_TO_STDOUT", "1") == "1"

def p(*parts): return os.path.join(DATA_DIR, *parts)

# Dual-site persistence
IN_LAST_SEEN = p("in_last_seen.json")
US_LAST_SEEN = p("us_last_seen.json")
STATE_PATH = p("dual_state.json")
IN_SKU_INDEX = p("in_sku_index.json")
LOG_CSV = p("dual_sync_log.csv")
LOG_JSONL = p("dual_sync_log.jsonl")
DISC_STATE = p("discount_state.json")

# Counters persistence
EVENTS_CSV = p("events.csv")
ATC_CSV    = p("atc_events.csv")
VIEWS_JSON = p("view_counts.json")
ATC_JSON   = p("atc_counts.json")
SALES_JSON = p("sales_counts.json")
SALE_DATES_JSON = p("sale_dates.json")
AGE_JSON   = p("age_days.json")
DOB_CACHE_JSON = p("dob_cache.json")
PROCESSED_ORDERS = p("processed_orders.json")  # retained placeholder
AVAIL_BASELINE_JSON = p("availability_baseline.json")

DAILY_CSV = p("daily_metrics.csv")
TODAY_STATE_JSON = p("today_state.json")
DAILY_CSV_HEADER = ["date_ist","product_id","views_today","atc_today","sales_today","age_in_days_today"]

# =============================== HELPERS ===============================

def hdr(token: str) -> Dict[str, str]:
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json", "Accept": "application/json"}

def _backoff_delay(attempt: int, base: float = 0.35, mx: float = 10.0) -> float:
    return min(mx, base * (2 ** (attempt - 1))) + random.uniform(0, 0.25)

def gql(domain: str, token: str, query: str, variables: dict = None) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    last_err = None
    for attempt in range(1, 8):
        try:
            r = requests.post(url, headers=hdr(token), json={"query": query, "variables": variables or {}}, timeout=60)
            if r.status_code in (429,) or r.status_code >= 500:
                time.sleep(_backoff_delay(attempt, base=0.4))
                continue
            if r.status_code != 200:
                raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")
            data = r.json()
            if data.get("errors"):
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
    payload = {"inventory_item_id": int(inventory_item_id), "location_id": int(location_id), "available_adjustment": int(delta)}
    last_err = None
    for attempt in range(1, 6):
        r = requests.post(url, headers=hdr(token), json=payload, timeout=30)
        if r.status_code in (429,) or r.status_code >= 500:
            time.sleep(_backoff_delay(attempt, base=0.3, mx=8.0))
            continue
        if r.status_code >= 400:
            last_err = RuntimeError(f"REST adjust {r.status_code}: {r.text}")
            time.sleep(_backoff_delay(attempt, base=0.2))
            continue
        return
    if last_err: raise last_err

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
    if last_err: raise last_err

def now_ist():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

def now_ist_str():
    return now_ist().strftime("%Y-%m-%d %H:%M:%S %z")

def today_ist_str():
    return now_ist().date().isoformat()

def sleep_ms(ms: int): time.sleep(max(0, ms)/1000.0)
def gid_num(gid: str) -> str: return (gid or "").split("/")[-1]

def _as_int_or_none(v):
    try:
        if v is None: return None
        if isinstance(v, (int, float)): return int(v)
        s = str(v).strip()
        return None if s == "" or s == "-" else int(s)
    except:
        return None

def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return default

def save_json(path: str, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)

# -------- Logging (Dual-site) --------
def ensure_log_header():
    need = (not os.path.exists(LOG_CSV)) or (os.path.getsize(LOG_CSV) == 0)
    if need:
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["ts", "phase", "shop", "note", "product_id", "variant_id", "sku", "delta", "message"])

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
        csv.writer(f).writerow([ts, emoji_phase, shop, note, product_id, variant_id, sku, delta, message])
    row = {
        "ts": ts, "phase": emoji_phase, "shop": shop, "note": note,
        "product_id": product_id, "variant_id": variant_id, "sku": sku,
        "delta": str(delta), "message": message, "title": title,
        "before": str(before), "after": str(after), "collections": collections,
    }
    if extra: row.update(extra)
    with open(LOG_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    if LOG_TO_STDOUT:
        human = f"{emoji_phase} {shop} {note} [SKU {sku}] pid={product_id} vid={variant_id} {before}→{after} Δ{delta} “{title}” — {message}"
        print(human, flush=True)

# -------- IP tools (pixel) --------
def _parse_networks(items: List[str]):
    nets = []
    for s in items:
        s = (s or "").strip()
        if not s: continue
        try:
            if "/" in s:
                nets.append(ipaddress.ip_network(s, strict=False))
            else:
                nets.append(ipaddress.ip_network(s + ("/32" if ":" not in s else "/128"), strict=False))
        except Exception:
            print(f"[IP] invalid ignore entry: {s}")
    return nets

IGNORE_NETS = _parse_networks(IGNORE_IPS_ENV)

def get_client_ip(req) -> str:
    h = req.headers
    ip = (h.get("CF-Connecting-IP")
          or h.get("True-Client-IP")
          or (h.get("X-Forwarded-For") or "").split(",")[0].strip()
          or req.remote_addr
          or "")
    return ip

def ip_is_ignored(ip: str) -> bool:
    if not ip: return False
    try:
        ip_obj = ipaddress.ip_address(ip)
    except Exception:
        return False
    return any(ip_obj in net for net in IGNORE_NETS)

def normalize_host(h: str) -> str:
    h = (h or "").lower().strip()
    h = h.replace("https://","").replace("http://","")
    return h[4:] if h.startswith("www.") else h

# ========================= GRAPHQL FRAGMENTS =========================

QUERY_COLLECTION_PAGE_IN = f"""
query ($handle:String!, $cursor:String) {{
  collectionByHandle(handle:$handle){{
    products(first: 60, after:$cursor){{
      pageInfo{{ hasNextPage endCursor }}
      nodes{{
        id
        title
        status
        metafield(namespace:"{MF_NAMESPACE}", key:"sku"){{ value }}
        tdisc: metafield(namespace:"{MF_NAMESPACE}", key:"{TEMP_DISC_KEY}"){{ id value type }}
        variants(first: 100){{
          nodes{{
            id
            title
            sku
            inventoryQuantity
            inventoryItem{{ id tracked }}
            inventoryPolicy
          }}
        }}
        badges: metafield(namespace:"{MF_NAMESPACE}", key:"{MF_BADGES_KEY}"){{ id value type }}
        dtime:  metafield(namespace:"{MF_NAMESPACE}", key:"{MF_DELIVERY_KEY}"){{ id value type }}
        salesTotal: metafield(namespace:"{MF_NAMESPACE}", key:"{KEY_SALES}"){{ id value type }}
        salesDates: metafield(namespace:"{MF_NAMESPACE}", key:"{KEY_DATES}"){{ id value type }}
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
        tdisc: metafield(namespace:"{MF_NAMESPACE}", key:"{TEMP_DISC_KEY}"){{ id value type }}
        pricein: metafield(namespace:"{MF_NAMESPACE}", key:"{MF_PRICEIN_KEY}"){{ id value type }}
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

# Metafield definition cache
_MF_DEF_CACHE: Dict[Tuple[str, str, str], str] = {}

def get_mf_def_type(domain: str, token: str, owner_type: str, namespace: str, key: str) -> str:
    ck = (owner_type, namespace, key)
    if ck in _MF_DEF_CACHE: return _MF_DEF_CACHE[ck]
    q = """
    query($ownerType:MetafieldOwnerType!, $ns:String!, $key:String!){
      metafieldDefinitions(first:1, ownerType:$ownerType, namespace:$ns, key:$key){
        edges{ node{ id type{ name } } }
      }
    }"""
    data = gql(domain, token, q, {"ownerType": owner_type, "ns": namespace, "key": key})
    edges = (((data.get("metafieldDefinitions") or {}).get("edges")) or [])
    tname = ((((edges[0] if edges else {}).get("node") or {}).get("type") or {}).get("name")) or ""
    _MF_DEF_CACHE[ck] = tname or "single_line_text_field"
    return _MF_DEF_CACHE[ck]

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
    return total if counted else 0

def desired_state(avail: int) -> Tuple[str, str, str]:
    if avail > 0:
        return ("ACTIVE", BADGE_READY, DELIVERY_READY)
    return ("DRAFT", "", DELIVERY_MTO)

MUTATION_PRODUCT_UPDATE = """
mutation ProductUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id status }
    userErrors { field message }
  }
}
"""

def update_product_status(domain: str, token: str, product_gid: str, target_status: str) -> bool:
    data = gql(domain, token, MUTATION_PRODUCT_UPDATE, {"input": {"id": product_gid, "status": target_status}})
    errs = ((data.get("productUpdate") or {}).get("userErrors") or [])
    if errs:
        log_row("⚠️", "IN", "WARN", product_id=gid_num(product_gid), message=str(errs))
        return False
    time.sleep(MUTATION_SLEEP_SEC)
    return True

def set_product_metafields(domain: str, token: str, product_gid: str, badges_node: dict, dtime_node: dict, target_badge: str, target_delivery: str):
    badges_type = (badges_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_BADGES_KEY)
    delivery_type = (dtime_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_DELIVERY_KEY)
    mf_inputs = []
    if target_badge:
        if str(badges_type).startswith("list."):
            mf_inputs.append({"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_BADGES_KEY, "type": badges_type, "value": json.dumps([target_badge])})
        else:
            mf_inputs.append({"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_BADGES_KEY, "type": badges_type, "value": target_badge})
    else:
        if (badges_node or {}).get("id"):
            delm = "mutation($id:ID!){ metafieldDelete(input:{id:$id}){ userErrors{message} } }"
            gql(domain, token, delm, {"id": badges_node["id"]})
    mf_inputs.append({"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_DELIVERY_KEY, "type": delivery_type, "value": target_delivery})
    if mf_inputs:
        mutation = "mutation($mfs:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$mfs){ userErrors{ field message } } }"
        data = gql(domain, token, mutation, {"mfs": mf_inputs})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_row("⚠️", "IN", "WARN", product_id=gid_num(product_gid), message=f"metafieldsSet errors: {errs}")

def bump_sales_in(domain: str, token: str, product_gid: str, sales_total_node: dict, sales_dates_node: dict, sold: int, today: str):
    st_type = (sales_total_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, KEY_SALES) or "number_integer"
    sd_type = (sales_dates_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, KEY_DATES) or "date"
    try:
        current_total = int((sales_total_node or {}).get("value") or "0")
    except:
        current_total = 0
    new_total = current_total + int(sold)
    if sd_type.startswith("list."):
        existing = []
        raw = (sales_dates_node or {}).get("value")
        try:
            if isinstance(raw, str) and raw.strip().startswith("["):
                existing = json.loads(raw)
        except:
            existing = []
        if today not in existing:
            existing.append(today)
        sd_payload = {"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": KEY_DATES, "type": "list.date", "value": json.dumps(sorted(set(existing))[-365:])}
    elif sd_type == "date":
        sd_payload = {"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": KEY_DATES, "type": "date", "value": today}
    else:
        sd_payload = {"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": KEY_DATES, "type": sd_type, "value": today}
    mutation = "mutation($mfs:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$mfs){ userErrors{ field message } } }"
    mfs = [
        {"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": KEY_SALES, "type": st_type, "value": str(new_total)},
        sd_payload,
    ]
    data = gql(domain, token, mutation, {"mfs": mfs})
    errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
    if errs:
        log_row("⚠️", "IN", "WARN", product_id=gid_num(product_gid), message=f"sales metafieldsSet errors: {errs}")

# --- temp discount helpers ---
def ceil_to_step(value: float, step: int) -> float:
    if step <= 1: return math.ceil(value * 100) / 100.0
    return float(int(math.ceil(value / step) * step))

def discount_round_step_for_domain(domain: str) -> int:
    return ROUND_STEP_INR if domain == IN_DOMAIN else ROUND_STEP_USD

def load_disc_state() -> Dict[str, Any]: return load_json(DISC_STATE, {})
def save_disc_state(state: Dict[str, Any]) -> None: save_json(DISC_STATE, state)
def disc_key(shop_tag: str, pid: str, vid: str) -> str: return f"{shop_tag}:{pid}:{vid}"

def parse_percent(node: dict) -> int:
    v = ((node or {}).get("value") or "").strip()
    try:
        return max(0, int(float(v)))
    except:
        return 0

def set_temp_discount_percent(domain: str, token: str, product_gid: str, node: dict, percent: int):
    mtype = (node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, TEMP_DISC_KEY) or "number_integer"
    mutation = "mutation($mfs:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$mfs){ userErrors{ field message } } }"
    mfs = [{"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": TEMP_DISC_KEY, "type": mtype, "value": str(int(percent))}]
    data = gql(domain, token, mutation, {"mfs": mfs})
    errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
    if errs:
        log_row("⚠️", "IN", "WARN", product_id=gid_num(product_gid), message=f"temp_discount_active set errors: {errs}")

def maybe_apply_temp_discount_for_product(shop_tag: str, domain: str, token: str, product_node: dict, availability: int):
    tdisc_node = product_node.get("tdisc") or {}
    percent = parse_percent(tdisc_node)
    if percent <= 0 or availability <= 0:
        return
    step = discount_round_step_for_domain(domain)
    pid = gid_num(product_node["id"])
    title = product_node.get("title") or ""
    sku = (((product_node.get("metafield") or {}).get("value")) or "").strip()
    disc_state = load_disc_state()
    changed_any = False
    for v in ((product_node.get("variants") or {}).get("nodes") or []):
        vid = gid_num(v.get("id"))
        key = disc_key(shop_tag, pid, vid)
        try:
            var = rest_get_variant(domain, token, int(vid))
            cur_price = float(var.get("price") or 0.0)
            if cur_price <= 0: continue
            entry = disc_state.get(key)
            if not entry:
                entry = {"original_price": str(cur_price), "applied_percent": 0, "ts": now_ist_str()}
                disc_state[key] = entry
            original = float(entry.get("original_price") or cur_price)
            last_applied = int(entry.get("applied_percent") or 0)
            if last_applied != percent:
                new_price = ceil_to_step(original * (1.0 - (percent / 100.0)), step)
                if new_price > 0 and abs(new_price - cur_price) >= 0.01:
                    rest_update_variant_price(domain, token, int(vid), str(int(new_price)))
                    changed_any = True
                    log_row("🏷️", "DISC", "APPLIED", product_id=pid, variant_id=vid, sku=sku, delta=f"-{percent}%", title=title, message=f"Price {cur_price} → {new_price} (step {step})")
                    time.sleep(MUTATION_SLEEP_SEC)
                entry["applied_percent"] = percent
        except Exception as e:
            log_row("⚠️", "DISC", "WARN", product_id=pid, variant_id=vid, sku=sku, message=f"Apply discount error: {e}")
    if changed_any or percent > 0:
        save_disc_state(disc_state)

def revert_temp_discount_for_product(shop_tag: str, domain: str, token: str, product_node: dict):
    pid = gid_num(product_node["id"])
    title = product_node.get("title") or ""
    sku = (((product_node.get("metafield") or {}).get("value")) or "").strip()
    disc_state = load_disc_state()
    changed_any = False
    for v in ((product_node.get("variants") or {}).get("nodes") or []):
        vid = gid_num(v.get("id"))
        key = disc_key(shop_tag, pid, vid)
        orig = disc_state.get(key)
        if not orig: continue
        try:
            original_price = float(orig.get("original_price"))
            rest_update_variant_price(domain, token, int(vid), str(int(original_price)))
            changed_any = True
            log_row("↩️", "DISC", "REVERTED", product_id=pid, variant_id=vid, sku=sku, title=title, message=f"Restored price to {original_price}")
            time.sleep(MUTATION_SLEEP_SEC)
            disc_state.pop(key, None)
        except Exception as e:
            log_row("⚠️", "DISC", "WARN", product_id=pid, sku=sku, message=f"Revert discount error: {e}")
    if changed_any:
        save_disc_state(disc_state)
    set_temp_discount_percent(domain, token, product_node["id"], product_node.get("tdisc") or {}, 0)

# --- priceinindia helpers (US) ---
def normalize_price_for_meta(value_rupees: float, meta_type: str) -> str:
    try:
        v_int = int(round(float(value_rupees)))
    except:
        v_int = 0
    return str(v_int)  # use integer string regardless of metafield type

def sync_priceinindia_for_us_product(us_product_node: dict, idx: dict):
    if not idx: return
    pid_us = gid_num(us_product_node["id"])
    title = us_product_node.get("title") or ""
    sku = (((us_product_node.get("metafield") or {}).get("value")) or "").strip()
    in_variant_id = idx.get("variant_id")
    if not in_variant_id: return
    try:
        var = rest_get_variant(IN_DOMAIN, IN_TOKEN, int(in_variant_id))
        in_price = float(var.get("price") or 0.0)
    except Exception as e:
        log_row("⚠️", "US", "PRICEINDIA_WARN", product_id=pid_us, sku=sku, message=f"read IN price error: {e}")
        return
    pin_node = us_product_node.get("pricein") or {}
    mf_type = pin_node.get("type") or get_mf_def_type(US_DOMAIN, US_TOKEN, "PRODUCT", MF_NAMESPACE, MF_PRICEIN_KEY) or "single_line_text_field"
    desired = normalize_price_for_meta(in_price, mf_type)
    current = (pin_node.get("value") or "").strip()
    if current == desired: return
    mutation = "mutation($mfs:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$mfs){ userErrors{ field message } } }"
    mfs = [{"ownerId": us_product_node["id"], "namespace": MF_NAMESPACE, "key": MF_PRICEIN_KEY, "type": mf_type, "value": desired}]
    try:
        data = gql(US_DOMAIN, US_TOKEN, mutation, {"mfs": mfs})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_row("⚠️", "US", "PRICEINDIA_WARN", product_id=pid_us, sku=sku, message=f"sync error: {errs}")
        else:
            log_row("🏷️", "US", "PRICEINDIA_SET", product_id=pid_us, sku=sku, delta=desired, title=title, message=f"Set US {MF_NAMESPACE}.{MF_PRICEIN_KEY} = {desired}")
    except Exception as e:
        log_row("⚠️", "US", "PRICEINDIA_WARN", product_id=pid_us, sku=sku, message=f"sync error: {e}")

# ========================= INDEX BUILDER (IN) =========================

def build_in_sku_index():
    index: Dict[str, Dict[str, Any]] = {}
    for handle in IN_COLLECTIONS:
        cursor = None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll: break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                status = (p.get("status") or "").upper()
                if ONLY_ACTIVE_FOR_MAPPING and status != "ACTIVE":
                    continue
                sku = (((p.get("metafield") or {}).get("value")) or "").strip()
                if not sku: continue
                variants = ((p.get("variants") or {}).get("nodes") or [])
                if not variants: continue
                v0 = variants[0]
                inv_item_gid = (((v0.get("inventoryItem") or {}).get("id")) or "")
                if not inv_item_gid: continue
                inv_item_id = int(gid_num(inv_item_gid) or "0")
                variant_id = int(gid_num(v0.get("id") or "") or "0")
                index[sku] = {"product_id": gid_num(p["id"]), "inventory_item_id": inv_item_id, "variant_id": variant_id, "title": p.get("title") or "", "status": status}
                sleep_ms(5)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break
    save_json(IN_SKU_INDEX, index)
    log_row("🗂️", "IN", "INDEX_BUILT", message=f"entries={len(index)}")

# ========================= DUAL-SITE SCANS =========================

def scan_india_and_update(read_only: bool = False):
    last_seen: Dict[str, int] = load_json(IN_LAST_SEEN, {})
    today = today_ist_str()
    for handle in IN_COLLECTIONS:
        cursor = None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                log_row("⚠️", "IN", "WARN", message=f"Collection not found: {handle}")
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                pid = gid_num(p["id"])
                title = p.get("title") or ""
                status = (p.get("status") or "").upper()
                sku = (((p.get("metafield") or {}).get("value")) or "").strip()
                variants = ((p.get("variants") or {}).get("nodes") or [])
                avail = compute_product_availability(variants, IN_INCLUDE_UNTRACKED)
                if not read_only:
                    try: maybe_apply_temp_discount_for_product("IN", IN_DOMAIN, IN_TOKEN, p, avail)
                    except Exception as e: log_row("⚠️", "DISC", "WARN", product_id=pid, sku=sku, message=f"Apply discount pass error: {e}")
                prev = _as_int_or_none(last_seen.get(pid))
                if prev is None: prev = 0
                if not read_only and avail < prev:
                    sold = prev - avail
                    bump_sales_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, sold, today)
                    log_row("🧾➖", "IN", "SALES_BUMP", product_id=pid, sku=sku, delta=f"-{sold}", message=f"avail {prev}->{avail} (sold={sold})", title=title, before=str(prev), after=str(avail))
                    try: revert_temp_discount_for_product("IN", IN_DOMAIN, IN_TOKEN, p)
                    except Exception as e: log_row("⚠️", "DISC", "WARN", product_id=pid, sku=sku, message=f"Revert on sale error: {e}")
                if not read_only and CLAMP_AVAIL_TO_ZERO and avail < 0 and variants:
                    inv_item_gid = (((variants[0].get("inventoryItem") or {}).get("id")) or "")
                    inv_item_id = int(gid_num(inv_item_gid) or "0")
                    try:
                        rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id, int(IN_LOCATION_ID), -avail)
                        log_row("🧰0️⃣", "IN", "CLAMP_TO_ZERO", product_id=pid, sku=sku, delta=f"+{-avail}", message=f"Raised availability to 0 on inventory_item_id={inv_item_id}", title=title, before=str(avail), after="0")
                        avail = 0
                    except Exception as e:
                        log_row("⚠️", "IN", "WARN", product_id=pid, sku=sku, message=f"Clamp error: {e}")
                _, target_badge, target_delivery = desired_state(avail)
                if not read_only:
                    set_product_metafields(IN_DOMAIN, IN_TOKEN, p["id"], p.get("badges") or {}, p.get("dtime") or {}, target_badge, target_delivery)
                if (not read_only) and IN_CHANGE_STATUS and SPECIAL_STATUS_HANDLE and (handle == SPECIAL_STATUS_HANDLE):
                    if avail < 1 and status == "ACTIVE":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "DRAFT")
                        log_row("🛑", "IN", "STATUS_TO_DRAFT" if ok else "STATUS_TO_DRAFT_FAILED", product_id=pid, sku=sku, delta=str(avail), title=title, message=f"handle={handle}")
                    elif avail >= 1 and status == "DRAFT":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "ACTIVE")
                        log_row("✅", "IN", "STATUS_TO_ACTIVE" if ok else "STATUS_TO_ACTIVE_FAILED", product_id=pid, sku=sku, delta=str(avail), title=title, message=f"handle={handle}")
                last_seen[pid] = max(0, int(avail))
                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
            save_json(IN_LAST_SEEN, last_seen)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

def scan_usa_and_mirror_to_india(read_only: bool = False):
    last_seen: Dict[str, int] = load_json(US_LAST_SEEN, {})
    in_index: Dict[str, Any] = load_json(IN_SKU_INDEX, {})
    for handle in US_COLLECTIONS:
        cursor = None
        while True:
            data = gql(US_DOMAIN, US_TOKEN, QUERY_COLLECTION_PAGE_US, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll: break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                p_sku = (((p.get("metafield") or {}).get("value")) or "").strip()
                title = p.get("title") or ""
                us_avail = 0
                for v0 in ((p.get("variants") or {}).get("nodes") or []):
                    us_avail += int(v0.get("inventoryQuantity") or 0)
                if not read_only:
                    try: maybe_apply_temp_discount_for_product("US", US_DOMAIN, US_TOKEN, p, us_avail)
                    except Exception as e: log_row("⚠️", "DISC", "WARN", product_id=gid_num(p["id"]), message=f"US apply discount pass error: {e}")
                if not read_only and p_sku and p_sku in in_index:
                    try: sync_priceinindia_for_us_product(p, in_index.get(p_sku))
                    except Exception as e: log_row("⚠️", "US", "PRICEINDIA_WARN", product_id=gid_num(p["id"]), sku=p_sku, message=f"sync error: {e}")
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
                    if delta > 0:
                        if not MIRROR_US_INCREASES:
                            log_row("🙅‍♂️➕", "US→IN", "IGNORED_INCREASE", variant_id=vid, sku=sku_exact, delta=str(delta), title=title, message="US qty increase; mirroring disabled", before=str(prev), after=str(qty))
                            last_seen[vid] = qty
                            continue
                    if not read_only and delta < 0 and sku_exact:
                        idx = in_index.get(sku_exact)
                        if not idx:
                            log_row("⚠️", "US→IN", "WARN_SKU_NOT_FOUND", variant_id=vid, sku=sku_exact, delta=str(delta), title=title, message="No matching SKU in India index")
                        else:
                            in_inv_item_id = int(idx.get("inventory_item_id") or 0)
                            try:
                                rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, in_inv_item_id, int(IN_LOCATION_ID), delta)
                                log_row("🔁", "US→IN", "APPLIED_DELTA", variant_id=vid, sku=sku_exact, delta=str(delta), title=title, message=f"Adjusted IN inventory_item_id={in_inv_item_id} by {delta} (via EXACT index)")
                                time.sleep(MUTATION_SLEEP_SEC)
                            except Exception as e:
                                log_row("⚠️", "US→IN", "ERROR_APPLYING_DELTA", variant_id=vid, sku=sku_exact, delta=str(delta), title=title, message=str(e))
                    if not read_only and qty < 0:
                        try:
                            inv_item_gid = (((v.get("inventoryItem") or {}).get("id")) or "")
                            if inv_item_gid:
                                rest_adjust_inventory(US_DOMAIN, US_TOKEN, int(gid_num(inv_item_gid)), int(US_LOCATION_ID), -qty)
                                log_row("🧰0️⃣", "US", "CLAMP_TO_ZERO_US", variant_id=vid, sku=sku_exact, delta=f"+{-qty}", title=title, message=f"Raised US availability to 0 on inventory_item_id={gid_num(inv_item_gid)}", before=str(qty), after="0")
                                qty = 0
                        except Exception as e:
                            log_row("⚠️", "US→IN", "WARN", variant_id=vid, sku=sku_exact, title=title, message=f"US clamp error: {e}")
                    last_seen[vid] = qty
                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
            save_json(US_LAST_SEEN, last_seen)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

# ========================= COUNTERS & DAILY CSV =========================

# Global lock for all state
lock = threading.Lock()

# Lifetime state (in-memory)
from collections import defaultdict
view_counts  = defaultdict(int)
atc_counts   = defaultdict(int)
sales_counts = defaultdict(int)
sale_dates   = defaultdict(set)
age_days     = defaultdict(int)
dob_cache    = {}
processed_orders = set()
last_avail   = {}  # product_id -> last seen availability (int)

dirty_views: Set[Tuple[str, str]] = set()
dirty_atc:   Set[Tuple[str, str]] = set()
dirty_sales: Set[Tuple[str, str]] = set()
dirty_age:   Set[Tuple[str, str]] = set()

# Per-day state (IST)
_today_ist_date_str: Optional[str] = None
views_today  = defaultdict(int)
atc_today    = defaultdict(int)
sales_today  = defaultdict(int)

def _load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return default

def _save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)

def _persist_all():
    with lock:
        _save_json(VIEWS_JSON, {k:int(v) for k,v in view_counts.items()})
        _save_json(ATC_JSON,  {k:int(v) for k,v in atc_counts.items()})
        _save_json(SALES_JSON,{k:int(v) for k,v in sales_counts.items()})
        _save_json(SALE_DATES_JSON, {k:sorted(list(v)) for k,v in sale_dates.items()})
        _save_json(AGE_JSON, {k:int(v) for k,v in age_days.items()})
        _save_json(DOB_CACHE_JSON, dob_cache)
        _save_json(PROCESSED_ORDERS, sorted(list(processed_orders)))
        _save_json(AVAIL_BASELINE_JSON, {k:int(v) for k,v in last_avail.items()})

def _ist_today_date_str():
    return today_ist_str()

def _load_today_state():
    global _today_ist_date_str
    st = _load_json(TODAY_STATE_JSON, {})
    _today_ist_date_str = st.get("ist_date_str") or _ist_today_date_str()
    for k, v in (st.get("views_today") or {}).items(): views_today[str(k)] = int(v)
    for k, v in (st.get("atc_today") or {}).items():   atc_today[str(k)]   = int(v)
    for k, v in (st.get("sales_today") or {}).items(): sales_today[str(k)] = int(v)

def _save_today_state():
    _save_json(TODAY_STATE_JSON, {
        "ist_date_str": _today_ist_date_str,
        "views_today":  {k:int(v) for k,v in views_today.items()},
        "atc_today":    {k:int(v) for k,v in atc_today.items()},
        "sales_today":  {k:int(v) for k,v in sales_today.items()},
    })

def _ensure_daily_csv_header():
    if not Path(DAILY_CSV).exists():
        with open(DAILY_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(DAILY_CSV_HEADER)

def _write_daily_csv_for_date(ist_date: str):
    _ensure_daily_csv_header()
    with lock:
        pids = set(views_today.keys()) | set(atc_today.keys()) | set(sales_today.keys())
        rows = []
        for pid in sorted(pids):
            rows.append([ist_date, pid, int(views_today.get(pid, 0)), int(atc_today.get(pid, 0)), int(sales_today.get(pid, 0)), int(age_days.get(pid, 0))])
    if rows:
        with open(DAILY_CSV, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)

def _reset_today_counters(new_ist_date: str):
    global _today_ist_date_str
    with lock:
        views_today.clear()
        atc_today.clear()
        sales_today.clear()
        _today_ist_date_str = new_ist_date
    _save_today_state()

def _rollover_if_needed():
    global _today_ist_date_str
    current = _ist_today_date_str()
    with lock:
        prev = _today_ist_date_str
    if prev is None:
        _reset_today_counters(current); return
    if current == prev: return
    _write_daily_csv_for_date(prev)
    _reset_today_counters(current)

def daily_csv_worker():
    try:
        _rollover_if_needed()
    except Exception as e:
        print("[DAILY CSV] initial rollover error:", e)
    while True:
        try:
            # sleep until next midnight IST
            now = now_ist()
            next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            time.sleep(max(1, int((next_midnight - now).total_seconds())))
            _rollover_if_needed()
        except Exception as e:
            print("[DAILY CSV] rollover error:", e)
            time.sleep(60)

def to_iso8601(ts):
    if ts is None:
        return datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
    try:
        val = float(ts)
        if val > 1e12:   val = val / 1e6
        elif val > 1e10: val = val / 1e3
        return datetime.utcfromtimestamp(val).isoformat() + 'Z'
    except Exception:
        pass
    s = str(ts)
    try:
        dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        return dt.astimezone(timezone.utc).isoformat().replace('+00:00','Z')
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace('+00:00','Z')

# ========================= DOB / AGE & LISTING =========================

def metafields_set(shop_host, token, metafields):
    url = f"https://{shop_host}/admin/api/{API_VERSION}/graphql.json"
    query = """
      mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) {
          metafields { id key namespace ownerType }
          userErrors { field message code }
        }
      }
    """
    for attempt in range(1, 6):
        try:
            r = requests.post(url, headers={"Content-Type": "application/json", "X-Shopify-Access-Token": token}, json={"query": query, "variables": {"metafields": metafields}}, timeout=25)
            ok_json = True
            try:
                j = r.json()
            except Exception:
                ok_json = False
                print(f"[GRAPHQL] non-JSON response {r.status_code}: {r.text[:500]}")
                time.sleep(_backoff_delay(attempt, base=0.3)); continue
            errs = (j.get("data", {}) or {}).get("metafieldsSet", {}).get("userErrors", [])
            if r.status_code in (429,) or r.status_code >= 500 or errs:
                if errs:
                    print(f"[GRAPHQL] userErrors ({shop_host}): {errs}")
                time.sleep(_backoff_delay(attempt, base=0.4)); continue
            return True
        except Exception as e:
            print(f"[{shop_host}] push error:", e)
            time.sleep(_backoff_delay(attempt, base=0.3))
    return False

def fetch_product_dob(pid: str) -> str:
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    gid = f"gid://shopify/Product/{pid}"
    query = """
      query($id: ID!) {
        node(id: $id) {
          ... on Product {
            dob: metafield(namespace: "%s", key: "%s") { value }
          }
        }
      }""" % (MF_NAMESPACE, KEY_DOB)
    try:
        r = requests.post(f"https://{admin_host}/admin/api/{API_VERSION}/graphql.json", headers={"Content-Type": "application/json", "X-Shopify-Access-Token": token}, json={"query": query, "variables": {"id": gid}}, timeout=25)
        j = r.json()
        node = (j.get("data") or {}).get("node") or {}
        dob_val = ((node.get("dob") or {}) or {}).get("value")
        return dob_val or ""
    except Exception as e:
        print(f"[DOB] fetch error for {pid}: {e}")
    return ""

def _compute_age_for_pid(pid: str, today_date):
    dob = dob_cache.get(pid)
    if not dob:
        dob = fetch_product_dob(pid)
        if dob: dob_cache[pid] = dob
    if not dob: return False
    try:
        if "T" in dob:
            dob_date = datetime.fromisoformat(dob.replace("Z","+00:00")).date()
        else:
            from datetime import datetime as dtmod
            dob_date = dtmod.strptime(dob, "%Y-%m-%d").date()
    except Exception:
        return False
    new_age = max((today_date - dob_date).days, 0)
    old_age = age_days.get(pid, -1)
    if new_age != old_age:
        age_days[pid] = new_age
        dirty_age.add((ADMIN_HOST, pid))
        return True
    return False

def _recompute_age_for_known_pids():
    today = datetime.now(timezone.utc).date()
    with lock:
        pids = set(view_counts.keys()) | set(atc_counts.keys()) | set(sales_counts.keys())
    changed = 0
    for pid in pids:
        try:
            if _compute_age_for_pid(pid, today):
                changed += 1
        except Exception:
            pass
    if changed:
        _persist_all()
    return changed

def _list_all_product_nodes():
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    query = """
      query($after: String) {
        products(first: 200, after: $after) {
          edges {
            cursor
            node {
              id
              createdAt
              dob: metafield(namespace: "%s", key: "%s") { value }
            }
          }
          pageInfo { hasNextPage }
        }
      }""" % (MF_NAMESPACE, KEY_DOB)
    nodes, after = [], None
    while True:
        r = requests.post(f"https://{admin_host}/admin/api/{API_VERSION}/graphql.json", headers={"Content-Type":"application/json","X-Shopify-Access-Token":token}, json={"query": query, "variables": {"after": after}}, timeout=30)
        j = r.json()
        data = (j.get("data") or {}).get("products") or {}
        edges = data.get("edges") or []
        for e in edges:
            n = e.get("node") or {}
            gid = n.get("id") or ""
            pid = gid.split("/")[-1] if gid else ""
            nodes.append({"pid": pid, "createdAt": n.get("createdAt"), "dob": ((n.get("dob") or {}) or {}).get("value")})
        if not data.get("pageInfo", {}).get("hasNextPage"): break
        after = edges[-1]["cursor"] if edges else None
        if not after: break
    return nodes

# ========================= AVAILABILITY POLLER (COUNTERS) =========================

def _compute_availability_from_variants(variants: list) -> int:
    total, any_tracked = 0, False
    for v in variants or []:
        inv = (v.get("inventoryItem") or {})
        if bool(inv.get("tracked")):
            any_tracked = True
            qty = int(v.get("inventoryQuantity") or 0)
            total += max(qty, 0)
    return total if any_tracked else 0

def _list_products_availability(limit_to_pids: Optional[Set[str]] = None) -> Dict[str, int]:
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    query = """
      query($after: String) {
        products(first: 200, after: $after) {
          edges {
            cursor
            node {
              id
              variants(first: 100) {
                nodes {
                  inventoryQuantity
                  inventoryItem { tracked }
                }
              }
            }
          }
          pageInfo { hasNextPage }
        }
      }
    """
    out, after = {}, None
    while True:
        r = requests.post(f"https://{admin_host}/admin/api/{API_VERSION}/graphql.json", headers={"Content-Type":"application/json","X-Shopify-Access-Token":token}, json={"query": query, "variables": {"after": after}}, timeout=30)
        j = r.json()
        data = (j.get("data") or {}).get("products") or {}
        edges = data.get("edges") or []
        for e in edges:
            n = e.get("node") or {}
            gid = n.get("id") or ""
            pid = gid.split("/")[-1] if gid else ""
            if not pid: continue
            if limit_to_pids and pid not in limit_to_pids: continue
            total = _compute_availability_from_variants(((n.get("variants") or {}).get("nodes")) or [])
            out[pid] = int(total)
        if not data.get("pageInfo", {}).get("hasNextPage"): break
        after = edges[-1]["cursor"] if edges else None
        if not after: break
    return out

def _availability_seed_all():
    current = _list_products_availability(AVAIL_POLL_PRODUCT_IDS if AVAIL_POLL_PRODUCT_IDS else None)
    with lock:
        last_avail.clear()
        last_avail.update({pid:int(av) for pid,av in current.items()})
    _persist_all()
    return len(current)

def _availability_poll_once():
    today_date = datetime.now(timezone.utc).date()
    date_str = today_date.isoformat()
    current = _list_products_availability(AVAIL_POLL_PRODUCT_IDS if AVAIL_POLL_PRODUCT_IDS else None)
    changed_sales = 0
    with lock:
        for pid, new_av in current.items():
            old_av = int(last_avail.get(pid, new_av))
            if new_av < old_av:
                delta = old_av - new_av  # units sold
                sales_counts[pid] = int(sales_counts.get(pid, 0)) + delta
                sales_today[pid]  = int(sales_today.get(pid, 0)) + delta
                sd = sale_dates[pid]
                if date_str not in sd:
                    sd.add(date_str)
                    if len(sd) > 365:
                        newest_sorted = sorted(sd)[-365:]
                        sale_dates[pid] = set(newest_sorted)
                dirty_sales.add((ADMIN_HOST, pid))
                changed_sales += delta
            last_avail[pid] = int(new_av)
    if changed_sales:
        _persist_all()
    return changed_sales

def availability_poller():
    if INVENTORY_POLL_SEC <= 0:
        return
    if not last_avail:
        try:
            seeded = _availability_seed_all()
            print(f"[AVAIL] seeded baseline for {seeded} product(s)")
        except Exception as e:
            print("[AVAIL] seed error:", e)
    while True:
        try:
            sold = _availability_poll_once()
            if sold:
                print(f"[AVAIL] detected {sold} unit(s) sold across products (via availability drop)")
        except Exception as e:
            print("[AVAIL] poll error:", e)
        time.sleep(max(10, INVENTORY_POLL_SEC))

# ========================= FLASK APP & ENDPOINTS =========================

app = Flask(__name__)

def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Shopify-Topic, X-Shopify-Hmac-SHA256, X-Shopify-Shop-Domain, X-Forwarded-For, CF-Connecting-IP, True-Client-IP"
    return resp

@app.route("/health", methods=["GET"])
def health():
    return "ok", 200

@app.route("/diag", methods=["GET"])
def diag():
    info = {
        "api_version": API_VERSION,
        "data_dir": DATA_DIR,
        "in_domain": IN_DOMAIN, "us_domain": US_DOMAIN,
        "in_collections": IN_COLLECTIONS, "us_collections": US_COLLECTIONS,
        "run_every_min": RUN_EVERY_MIN, "scheduler_enabled": ENABLE_SCHEDULER,
        "in_last_seen_count": len(load_json(IN_LAST_SEEN, {})),
        "us_last_seen_count": len(load_json(US_LAST_SEEN, {})),
        "index_entries": len(load_json(IN_SKU_INDEX, {})),
        "use_product_custom_sku": USE_PRODUCT_CUSTOM_SKU,
        "only_active_for_mapping": ONLY_ACTIVE_FOR_MAPPING,
        "mirror_us_increases": MIRROR_US_INCREASES,
        "clamp_avail_to_zero": CLAMP_AVAIL_TO_ZERO,
        "round_step_inr": ROUND_STEP_INR, "round_step_usd": ROUND_STEP_USD,
        "temp_discount_key": f"{MF_NAMESPACE}.{TEMP_DISC_KEY}",
        "priceinindia_key": f"{MF_NAMESPACE}.{MF_PRICEIN_KEY}",
        "initialized": load_json(STATE_PATH, {}).get("initialized", False),

        # counters
        "admin_host": ADMIN_HOST,
        "inventory_poll_sec": INVENTORY_POLL_SEC,
        "allowed_pixel_hosts": sorted(list(ALLOWED_PIXEL_HOSTS)),
        "daily_csv": DAILY_CSV,
    }
    return _cors(jsonify(info)), 200

@app.route("/rebuild_index", methods=["POST"])
def rebuild_index():
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))
    build_in_sku_index()
    return _cors(jsonify({"status": "ok", "entries": len(load_json(IN_SKU_INDEX, {}))})), 200

@app.route("/run", methods=["POST"])
def run_now():
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))
    status = run_cycle()
    return _cors(jsonify({"status": status})), 200 if status == "ok" else 409

# ---- DOB / AGE endpoints ----
@app.route("/dob/set", methods=["POST"])
def dob_set_single():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return "forbidden", 403
    data = request.get_json(silent=True) or {}
    pid = str(data.get("productId") or "").strip()
    dob = str(data.get("dob") or "").strip()
    if not pid or not dob: return "bad request", 400
    try: datetime.strptime(dob, "%Y-%m-%d")
    except Exception: return "invalid date format", 400
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    mfs = [{"ownerId": f"gid://shopify/Product/{pid}", "namespace": MF_NAMESPACE, "key": KEY_DOB, "type": "date", "value": dob}]
    ok = metafields_set(admin_host, token, mfs)
    if ok:
        with lock:
            dob_cache[pid] = dob
        try: _compute_age_for_pid(pid, datetime.now(timezone.utc).date())
        except Exception: pass
    return ("ok", 200) if ok else ("failed", 500)

@app.route("/dob/bulk", methods=["POST"])
def dob_bulk():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return "forbidden", 403
    items = request.get_json(silent=True) or []
    to_write = []
    for it in items:
        pid = str(it.get("productId") or "").strip()
        dob = str(it.get("dob") or "").strip()
        try: datetime.strptime(dob, "%Y-%m-%d")
        except Exception: continue
        if pid and dob:
            to_write.append({"ownerId": f"gid://shopify/Product/{pid}", "namespace": MF_NAMESPACE, "key": KEY_DOB, "type": "date", "value": dob})
    if not to_write: return "no valid rows", 400
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    CHUNK = 25; ok_all = True
    for i in range(0, len(to_write), CHUNK):
        chunk = to_write[i:i+CHUNK]
        ok = metafields_set(admin_host, token, chunk)
        ok_all = ok_all and ok
        time.sleep(0.3)
    today = datetime.now(timezone.utc).date()
    for it in items:
        pid = str(it.get("productId") or "").strip()
        dob = str(it.get("dob") or "").strip()
        if pid and dob:
            dob_cache[pid] = dob
            try: _compute_age_for_pid(pid, today)
            except Exception: pass
    return ("ok", 200) if ok_all else ("partial/fail", 207)

@app.route("/dob/backfill_created_at", methods=["POST","GET"])
def dob_backfill_created_at():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return "forbidden", 403
    nodes = _list_all_product_nodes()
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    to_write = []; today = datetime.now(timezone.utc).date()
    for n in nodes:
        if n.get("dob"): continue
        pid = n.get("pid"); createdAt = n.get("createdAt")
        if not pid or not createdAt: continue
        try: dt = datetime.fromisoformat(createdAt.replace("Z","+00:00")).date()
        except Exception: continue
        dob_str = dt.isoformat()
        to_write.append({"ownerId": f"gid://shopify/Product/{pid}", "namespace": MF_NAMESPACE, "key": KEY_DOB, "type": "date", "value": dob_str})
        dob_cache[pid] = dob_str
        try: _compute_age_for_pid(pid, today)
        except Exception: pass
    if not to_write: return "nothing to backfill", 200
    CHUNK = 25; wrote = 0
    for i in range(0, len(to_write), CHUNK):
        chunk = to_write[i:i+CHUNK]
        ok = metafields_set(admin_host, token, chunk)
        if ok: wrote += len(chunk)
        time.sleep(0.3)
    _persist_all()
    return f"backfilled dob for {wrote} product(s)", 200

@app.route("/dob/set_all_today", methods=["POST","GET"])
def dob_set_all_today():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return "forbidden", 403
    nodes = _list_all_product_nodes()
    today = datetime.now(timezone.utc).date().isoformat()
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    to_write, touched = [], 0
    for n in nodes:
        pid = n.get("pid")
        if not pid: continue
        to_write.append({"ownerId": f"gid://shopify/Product/{pid}", "namespace": MF_NAMESPACE, "key": KEY_DOB, "type": "date", "value": today})
        dob_cache[pid] = today
        age_days[pid] = 0
        dirty_age.add((admin_host, pid))
        touched += 1
    if not to_write: return "no products found", 200
    CHUNK = 25; wrote = 0
    for i in range(0, len(to_write), CHUNK):
        chunk = to_write[i:i+CHUNK]
        ok = metafields_set(admin_host, token, chunk)
        if ok: wrote += len(chunk)
        time.sleep(0.3)
    _persist_all()
    return f"set dob=today for {wrote}/{touched} product(s)", 200

@app.route("/age/recompute", methods=["POST","GET"])
def age_recompute():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return "forbidden", 403
    changed = _recompute_age_for_known_pids()
    return f"recomputed age for {changed} product(s)", 200

@app.route("/age/seed_all", methods=["POST","GET"])
def age_seed_all():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return "forbidden", 403
    nodes = _list_all_product_nodes()
    today = datetime.now(timezone.utc).date()
    seen = 0
    for n in nodes:
        pid = n.get("pid"); dob = n.get("dob")
        if not pid or not dob: continue
        dob_cache[pid] = dob
        try:
            _compute_age_for_pid(pid, today)
            seen += 1
        except Exception:
            pass
    _persist_all()
    return f"seeded age for {seen} product(s) with dob", 200

@app.route("/availability/seed_all", methods=["GET","POST"])
def availability_seed_all():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return "forbidden", 403
    n = _availability_seed_all()
    return f"seeded baseline for {n} product(s)", 200

@app.route("/availability/force_poll", methods=["GET","POST"])
def availability_force_poll():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return "forbidden", 403
    n = _availability_poll_once()
    return f"detected {n} unit(s) sold via availability drop", 200

# ---- Pixel endpoints ----
@app.route("/track/product", methods=["OPTIONS"])
def track_options_product(): return _cors(make_response("", 204))

@app.route("/track/product", methods=["POST"])
def track_product():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return _cors(make_response(("forbidden", 403)))
    cip = get_client_ip(request)
    if ip_is_ignored(cip): return _cors(make_response(("ok", 200)))
    data = request.get_json(silent=True) or {}
    product_id = str(data.get("productId") or "").strip()
    handle     = (data.get("handle") or "").strip()
    session_id = (data.get("sessionId") or "").strip()
    user_agent = (data.get("userAgent") or "").strip()
    shop_host  = normalize_host(data.get("shop"))
    if shop_host not in ALLOWED_PIXEL_HOSTS: return _cors(make_response(("ok", 200)))
    if not product_id: return _cors(make_response(("bad request", 400)))
    ts_iso = to_iso8601(data.get("ts"))
    if not os.path.exists(EVENTS_CSV):
        with open(EVENTS_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["ts_iso","shop","productId","handle","sessionId","userAgent"])
    with open(EVENTS_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([ts_iso, shop_host, product_id, handle, session_id, user_agent])
    with lock:
        view_counts[product_id] = int(view_counts.get(product_id, 0)) + 1
        views_today[product_id] = int(views_today.get(product_id, 0)) + 1
        dirty_views.add((shop_host, product_id))
    _save_today_state()
    try: _compute_age_for_pid(product_id, datetime.now(timezone.utc).date())
    except Exception: pass
    return _cors(make_response(("ok", 200)))

@app.route("/track/atc", methods=["OPTIONS"])
def track_options_atc(): return _cors(make_response("", 204))

@app.route("/track/atc", methods=["POST"])
def track_atc():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return _cors(make_response(("forbidden", 403)))
    cip = get_client_ip(request)
    if ip_is_ignored(cip): return _cors(make_response(("ok", 200)))
    data = request.get_json(silent=True) or {}
    product_id = str(data.get("productId") or "").strip()
    qty        = int(data.get("qty") or 1)
    handle     = (data.get("handle") or "").strip()
    session_id = (data.get("sessionId") or "").strip()
    user_agent = (data.get("userAgent") or "").strip()
    shop_host  = normalize_host(data.get("shop"))
    if shop_host not in ALLOWED_PIXEL_HOSTS: return _cors(make_response(("ok", 200)))
    if not product_id: return _cors(make_response(("bad request", 400)))
    if qty < 1: qty = 1
    ts_iso = to_iso8601(data.get("ts"))
    if not os.path.exists(ATC_CSV):
        with open(ATC_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["ts_iso","shop","productId","qty","handle","sessionId","userAgent","ip"])
    with open(ATC_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([ts_iso, shop_host, product_id, qty, handle, session_id, user_agent, cip])
    with lock:
        atc_counts[product_id] = int(atc_counts.get(product_id, 0)) + qty
        atc_today[product_id]  = int(atc_today.get(product_id, 0)) + qty
        dirty_atc.add((shop_host, product_id))
    _save_today_state()
    try: _compute_age_for_pid(product_id, datetime.now(timezone.utc).date())
    except Exception: pass
    return _cors(make_response(("ok", 200)))

# ---- Daily CSV helpers ----
@app.route("/daily/download", methods=["GET"])
def daily_download():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return "forbidden", 403
    if not os.path.exists(DAILY_CSV): return "not found", 404
    return send_file(DAILY_CSV, as_attachment=True, download_name="daily_metrics.csv")

@app.route("/daily/preview", methods=["GET"])
def daily_preview():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return "forbidden", 403
    n = int(request.args.get("n", 100))
    if n < 1: n = 1
    if not os.path.exists(DAILY_CSV): return "not found", 404
    with open(DAILY_CSV, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
    header = lines[:1]; body = lines[-n:] if len(lines) > 1 else []
    out = (header + body) if header else body
    return ("\n".join(out) + "\n", 200, {"Content-Type": "text/plain; charset=utf-8"})

# ---- Webhook: products_create (optional) ----
def verify_hmac(req_body: bytes, header_hmac: str) -> bool:
    try:
        digest = hmac.new(key=SHOPIFY_WEBHOOK_SECRET.encode("utf-8"), msg=req_body, digestmod="sha256").digest()
        expected = base64.b64encode(digest).decode("utf-8")
        return hmac.compare_digest(expected, header_hmac or "")
    except Exception:
        return False

@app.route("/webhook/products_create", methods=["POST"])
def products_create():
    h = request.headers.get("X-Shopify-Hmac-SHA256", "")
    raw = request.get_data()
    if not verify_hmac(raw, h): return "unauthorized", 401
    shop_host_hdr = request.headers.get("X-Shopify-Shop-Domain", "").strip()
    shop_host = normalize_host(shop_host_hdr)
    if shop_host not in ADMIN_TOKENS: return "ok", 200
    payload = request.get_json(silent=True) or {}
    pid = str(payload.get("id") or "")
    created_at = payload.get("created_at")
    if not pid or not created_at: return "ok", 200
    try: dob_date = datetime.fromisoformat(created_at.replace("Z","+00:00")).date()
    except Exception: dob_date = datetime.now(timezone.utc).date()
    dob_str = dob_date.isoformat()
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    ok = metafields_set(admin_host, token, [{"ownerId": f"gid://shopify/Product/{pid}","namespace": MF_NAMESPACE,"key": KEY_DOB,"type": "date","value": dob_str}])
    if ok:
        with lock:
            dob_cache[pid] = dob_str
            age_days[pid] = 0
            dirty_age.add((shop_host, pid))
    return "ok", 200

# ========================= FLUSHER (push counters) =========================

def flusher():
    ADMIN_HOST_LOCAL = list(ADMIN_TOKENS.keys())[0]
    TOKEN_LOCAL = ADMIN_TOKENS[ADMIN_HOST_LOCAL]
    while True:
        time.sleep(FLUSH_INTERVAL_SEC)
        with lock:
            if not (dirty_views or dirty_atc or dirty_sales or dirty_age):
                continue
            to_push = {}
            for (_shop, pid) in dirty_views: to_push.setdefault(pid, set()).add("views")
            for (_shop, pid) in dirty_atc:   to_push.setdefault(pid, set()).add("atc")
            for (_shop, pid) in dirty_sales: to_push.setdefault(pid, set()).add("sales")
            for (_shop, pid) in dirty_age:   to_push.setdefault(pid, set()).add("age")
            dirty_views.clear(); dirty_atc.clear(); dirty_sales.clear(); dirty_age.clear()
        mfs = []
        for pid, kinds in to_push.items():
            if "views" in kinds:
                mfs.append({"ownerId": f"gid://shopify/Product/{pid}", "namespace": MF_NAMESPACE, "key": KEY_VIEWS, "type": "number_integer", "value": str(int(view_counts.get(pid, 0)))})
            if "atc" in kinds:
                mfs.append({"ownerId": f"gid://shopify/Product/{pid}", "namespace": MF_NAMESPACE, "key": KEY_ATC, "type": "number_integer", "value": str(int(atc_counts.get(pid, 0)))})
            if "sales" in kinds:
                mfs.append({"ownerId": f"gid://shopify/Product/{pid}", "namespace": MF_NAMESPACE, "key": KEY_SALES, "type": "number_integer", "value": str(int(sales_counts.get(pid, 0)))})
                dates = sorted(list(sale_dates.get(pid, set())))
                mfs.append({"ownerId": f"gid://shopify/Product/{pid}", "namespace": MF_NAMESPACE, "key": KEY_DATES, "type": "list.date", "value": json.dumps(dates)})
            if "age" in kinds:
                mfs.append({"ownerId": f"gid://shopify/Product/{pid}", "namespace": MF_NAMESPACE, "key": KEY_AGE, "type": "number_integer", "value": str(int(age_days.get(pid, 0)))})
        CHUNK = 25
        items = list(mfs)
        if items:
            try:
                print(f"[PUSH] preview ownerId/key/type -> {items[0].get('ownerId')}, {items[0].get('key')}, {items[0].get('type')}")
            except Exception:
                pass
        for i in range(0, len(items), CHUNK):
            chunk = items[i:i+CHUNK]
            ok = metafields_set(ADMIN_HOST_LOCAL, TOKEN_LOCAL, chunk)
            print(f"[PUSH] {ADMIN_HOST_LOCAL}: {len(chunk)} -> {'OK' if ok else 'ERR'}")
            time.sleep(0.3)
        _persist_all()

# ========================= SCHEDULER / RUNNER =========================

from threading import Thread, Lock
run_lock = Lock()
is_running = False

def load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"initialized": False}

def save_state(obj):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, STATE_PATH)

def run_cycle():
    global is_running
    with run_lock:
        if is_running:
            return "busy"
        is_running = True
    try:
        state = load_state()
        in_seen = load_json(IN_LAST_SEEN, {})
        us_seen = load_json(US_LAST_SEEN, {})
        first_cycle = (not state.get("initialized", False)) and (len(in_seen) == 0 and len(us_seen) == 0)

        # Always (re)build index before scans
        try: build_in_sku_index()
        except Exception as e: log_row("⚠️", "SCHED", "WARN", message=str(e))

        # India scan
        scan_india_and_update(read_only=first_cycle)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

        # USA scan
        if US_DOMAIN and US_TOKEN and US_COLLECTIONS:
            scan_usa_and_mirror_to_india(read_only=first_cycle)

        if first_cycle:
            state["initialized"] = True
            save_state(state)
            log_row("🟢", "SCHED", "FIRST_CYCLE_DONE", message="Baselines learned; future cycles will apply deltas")
        return "ok"
    finally:
        with run_lock:
            is_running = False

def scheduler_loop():
    if RUN_EVERY_MIN <= 0:
        return
    while True:
        try:
            run_cycle()
        except Exception as e:
            log_row("⚠️", "SCHED", "WARN", message=str(e))
        time.sleep(max(1, RUN_EVERY_MIN) * 60)

# ========================= BOOTSTRAP =========================

# Create pixel CSVs with headers if missing
if not os.path.exists(EVENTS_CSV):
    with open(EVENTS_CSV, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["ts_iso","shop","productId","handle","sessionId","userAgent"])
if not os.path.exists(ATC_CSV):
    with open(ATC_CSV, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["ts_iso","shop","productId","qty","handle","sessionId","userAgent","ip"])

# Load counters lifetime state
_view_disk = _load_json(VIEWS_JSON, {})
for k, v in _view_disk.items(): view_counts[str(k)] = int(v)
_atc_disk = _load_json(ATC_JSON, {})
for k, v in _atc_disk.items(): atc_counts[str(k)] = int(v)
_sales_disk = _load_json(SALES_JSON, {})
for k, v in _sales_disk.items(): sales_counts[str(k)] = int(v)
_dates_disk = _load_json(SALE_DATES_JSON, {})
for k, lst in _dates_disk.items(): sale_dates[str(k)] = set(lst)
_age_disk = _load_json(AGE_JSON, {})
for k, v in _age_disk.items(): age_days[str(k)] = int(v)
dob_cache.update(_load_json(DOB_CACHE_JSON, {}))
processed_orders = set(_load_json(PROCESSED_ORDERS, []))
last_avail.update(_load_json(AVAIL_BASELINE_JSON, {}))

# Load per-day state (IST) & rollover if missed
_load_today_state()
try: _rollover_if_needed()
except Exception as e: print("[BOOT] rollover check error:", e)

# Start background workers
Thread(target=flusher, daemon=True).start()
Thread(target=daily_csv_worker, daemon=True).start()
Thread(target=availability_poller, daemon=True).start()
if ENABLE_SCHEDULER:
    Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    print(f"[BOOT] Unified app on port {PORT} | API {API_VERSION}")
    print(f"[CFG] IN={IN_DOMAIN} (loc {IN_LOCATION_ID}) | US={US_DOMAIN} (loc {US_LOCATION_ID}) | ADMIN_HOST={ADMIN_HOST} | DATA_DIR={DATA_DIR}")
    from werkzeug.serving import run_simple
    run_simple("0.0.0.0", PORT, app)
