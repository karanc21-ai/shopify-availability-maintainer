#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Unified Shopify app: Dual-site availability sync + Counters + Daily CSV (IST)
(With manufacturing notifier wired in on availability drops)
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

BADGE_READY = os.getenv("BADGE_READY", "Ready To Ship").strip()
DELIVERY_READY = os.getenv("DELIVERY_READY", "2-5 Days Across India").strip()
DELIVERY_MTO = os.getenv("DELIVERY_MTO", "12-15 Days Across India").strip()

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

ALLOWED_PIXEL_HOSTS = set(h.strip().lower() for h in (os.getenv("ALLOWED_PIXEL_HOSTS","silver-rudradhan.myshopify.com").split(",")))
# IP ignore list for pixels (comma-separated single IPs or CIDRs)
IGNORE_IPS_ENV = [s.strip() for s in os.getenv("IGNORE_IPS", "").split(",")] if os.getenv("IGNORE_IPS") else []

# ---- App A -> App B Manufacturing notifier (NEW) ----
ENABLE_MFG_NOTIFY = os.getenv("ENABLE_MFG_NOTIFY", "1") == "1"
B_ENDPOINT_URL = os.getenv("B_ENDPOINT_URL", "").strip()  # e.g. https://manufacturing-handler.onrender.com/manufacturing/start
A_TO_B_SHARED_SECRET = os.getenv("A_TO_B_SHARED_SECRET", "").strip()
SOURCE_SITE = os.getenv("SOURCE_SITE", "IN").strip()

# ---- Server & persistence ----
DATA_DIR = os.getenv("DATA_DIR", ".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)
PORT = int(os.getenv("PORT", "10000"))
FLUSH_INTERVAL_SEC = int(os.getenv("FLUSH_INTERVAL_SEC", "5"))
LOG_TO_STDOUT = os.getenv("LOG_TO_STDOUT", "1") == "1"
# quiet toggle for push logs
QUIET_PUSH = os.getenv("QUIET_PUSH", "0") == "1"

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
        sd_payload = {"ownerId": product_gid, "namespace": MF_NAMESPACE, "ke_
