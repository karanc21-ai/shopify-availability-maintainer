#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dual-site availability sync — EXACT product.custom.sku mapping + Snapshots to disk

What this app does
- India scan:
  * Recompute availability (sum of tracked variants by default)
  * Bump sales_total & sales_dates on availability drops
  * Update badges/delivery based on availability
  * SPECIAL_STATUS_HANDLE: auto ACTIVE<->DRAFT + strike_rate (sales_total / active_hours_total)
  * Rebuild exact index: /data/in_sku_index.json  (trim+lower product.custom.sku -> product + first tracked inv_item_id)
- USA scan:
  * Detect per-variant qty deltas
  * Read **US product** custom.sku once
  * Mirror delta to India using ONLY the local exact index (no Shopify search)
- Snapshots (CSV + JSONL to /data):
  * /export_csv?key=...   → in_products.csv/jsonl & us_products.csv/jsonl

First cycle is learn-only (no writes). Thereafter, deltas & metafield changes apply.
"""

import os, sys, time, json, csv, random
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import requests
from flask import Flask, request, jsonify, make_response
from threading import Thread, Lock

# ---------- Config & pacing ----------
API_VERSION = os.getenv("API_VERSION", "2024-10").strip()
PIXEL_SHARED_SECRET = os.getenv("PIXEL_SHARED_SECRET", "").strip()
if not PIXEL_SHARED_SECRET:
    raise SystemExit("PIXEL_SHARED_SECRET required")

RUN_EVERY_MIN = int(os.getenv("RUN_EVERY_MIN", "5"))
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "0") == "1"

SLEEP_BETWEEN_PRODUCTS_MS = int(os.getenv("SLEEP_BETWEEN_PRODUCTS_MS", "120"))   # ms
SLEEP_BETWEEN_PAGES_MS = int(os.getenv("SLEEP_BETWEEN_PAGES_MS", "400"))         # ms
SLEEP_BETWEEN_SHOPS_MS = int(os.getenv("SLEEP_BETWEEN_SHOPS_MS", "800"))         # ms
MUTATION_SLEEP_SEC = float(os.getenv("MUTATION_SLEEP_SEC", "0.35"))              # sec

# India (metafields + receives inventory deltas)
IN_DOMAIN = os.getenv("IN_DOMAIN", "").strip()
IN_TOKEN  = os.getenv("IN_TOKEN", "").strip()
IN_LOCATION_ID = os.getenv("IN_LOCATION_ID", "").strip()
IN_COLLECTIONS = [x.strip() for x in os.getenv("IN_COLLECTION_HANDLES","").split(",") if x.strip()]
IN_INCLUDE_UNTRACKED = os.getenv("IN_INCLUDE_UNTRACKED", "0") == "1"
IN_CHANGE_STATUS = os.getenv("IN_CHANGE_STATUS", "0") == "1"  # must be 1 to flip status

# USA (source of variant-level deltas; mapping via product.custom.sku)
US_DOMAIN = os.getenv("US_DOMAIN", "").strip()
US_TOKEN  = os.getenv("US_TOKEN", "").strip()
US_LOCATION_ID = os.getenv("US_LOCATION_ID", "").strip()
US_COLLECTIONS = [x.strip() for x in os.getenv("US_COLLECTION_HANDLES","").split(",") if x.strip()]

# Metafields (India)
MF_NAMESPACE = os.getenv("MF_NAMESPACE", "custom")
MF_BADGES_KEY = os.getenv("MF_BADGES_KEY", "badges")
MF_DELIVERY_KEY = os.getenv("MF_DELIVERY_KEY", "delivery_time")
KEY_SALES = os.getenv("KEY_SALES", "sales_total")
KEY_DATES = os.getenv("KEY_DATES", "sales_dates")

BADGE_READY    = "Ready To Ship"
DELIVERY_READY = "2-5 Days Across India"
DELIVERY_MTO   = "12-15 Days Across India"

# Special collection (status + strike rate)
SPECIAL_STATUS_HANDLE = os.getenv(
    "SPECIAL_STATUS_HANDLE",
    "budget-gold-plated-jewelry-premium-jadau-ornaments-on-mix-metal-base"
).strip()

# Product-metafield bridge
CUSTOM_SKU_NAMESPACE = os.getenv("CUSTOM_SKU_NAMESPACE", "custom").strip()
CUSTOM_SKU_KEY = os.getenv("CUSTOM_SKU_KEY", "sku").strip()
USE_PRODUCT_CUSTOM_SKU = os.getenv("USE_PRODUCT_CUSTOM_SKU", "1") == "1"
ONLY_ACTIVE_FOR_MAPPING = os.getenv("ONLY_ACTIVE_FOR_MAPPING", "1") == "1"  # index includes ACTIVE only if True

# Strike-rate keys & optional type forcing
STRIKE_RATE_KEY = os.getenv("STRIKE_RATE_KEY", "strike_rate")
ACTIVE_HOURS_KEY = os.getenv("ACTIVE_HOURS_KEY", "active_hours_total")
ACTIVE_STARTED_AT_KEY = os.getenv("ACTIVE_STARTED_AT_KEY", "active_started_at")
STRIKE_RATE_FORCE_TYPE = os.getenv("STRIKE_RATE_FORCE_TYPE")            # e.g., number_decimal
ACTIVE_HOURS_FORCE_TYPE = os.getenv("ACTIVE_HOURS_FORCE_TYPE")          # e.g., number_decimal
ACTIVE_STARTED_AT_FORCE_TYPE = os.getenv("ACTIVE_STARTED_AT_FORCE_TYPE")# e.g., date_time

# Badges/delivery optional type forcing
BADGES_FORCE_TYPE = os.getenv("BADGES_FORCE_TYPE")                      # e.g., list.single_line_text_field
DELIVERY_FORCE_TYPE = os.getenv("DELIVERY_FORCE_TYPE")                  # e.g., single_line_text_field
SALES_DATES_FORCE_TYPE = os.getenv("SALES_DATES_FORCE_TYPE")            # e.g., date or list.date

# Render / persistence
DATA_DIR = os.getenv("DATA_DIR", ".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_TO_STDOUT = os.getenv("LOG_TO_STDOUT", "1") == "1"

def p(*parts):  # path helper within DATA_DIR
    return os.path.join(DATA_DIR, *parts)

PORT = int(os.getenv("PORT", os.getenv("PORT", "10000")))

# Last-seen persistence
IN_LAST_SEEN   = p("in_last_seen.json")    # product_id(num) -> last_seen_availability
US_LAST_SEEN   = p("us_last_seen.json")    # variant_id(num)  -> last_seen_qty
STATE_PATH     = p("dual_state.json")      # {"initialized": bool}
LOG_CSV        = p("dual_sync_log.csv")    # CSV + stdout
IN_SKU_INDEX   = p("in_sku_index.json")    # EXACT index: trim+lower custom.sku -> info

# Snapshots
IN_JSONL = p("in_products.jsonl")
IN_CSV   = p("in_products.csv")
US_JSONL = p("us_products.jsonl")
US_CSV   = p("us_products.csv")

# ---------- HTTP helpers ----------
def hdr(token: str) -> Dict[str, str]:
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json", "Accept": "application/json"}

def _backoff_sleep(attempt: int) -> None:
    time.sleep(min(10.0, 0.4 * (2 ** (attempt-1))) + random.uniform(0, 0.25))

def gql(domain: str, token: str, query: str, variables: dict = None) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    for attempt in range(1, 9):
        r = requests.post(url, headers=hdr(token), json={"query": query, "variables": variables or {}}, timeout=60)
        if r.status_code in (429, 500, 502, 503, 504):
            _backoff_sleep(attempt); continue
        if r.status_code != 200:
            raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")
        data = r.json()
        if data.get("errors"):
            if any(((e.get("extensions") or {}).get("code","").upper() == "THROTTLED") for e in data["errors"]):
                _backoff_sleep(attempt); continue
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]
    raise RuntimeError("GraphQL throttled/5xx repeatedly")

def rest_adjust_inventory(domain: str, token: str, inventory_item_id: int, location_id: int, delta: int) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/adjust.json"
    payload = {"inventory_item_id": int(inventory_item_id), "location_id": int(location_id), "available_adjustment": int(delta)}
    for attempt in range(1, 6):
        r = requests.post(url, headers=hdr(token), json=payload, timeout=30)
        if r.status_code in (429, 500, 502, 503, 504):
            _backoff_sleep(attempt); continue
        if r.status_code >= 400:
            raise RuntimeError(f"REST adjust {r.status_code}: {r.text}")
        return

# ---------- Utils ----------
def now_ist():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

def now_ist_str():
    return now_ist().strftime("%Y-%m-%d %H:%M:%S %z")

def today_ist_str():
    return now_ist().date().isoformat()

def sleep_ms(ms: int):
    time.sleep(max(0, ms) / 1000.0)

def gid_num(gid: str) -> str:
    return (gid or "").split("/")[-1]

def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)

def ensure_log_header():
    need = (not os.path.exists(LOG_CSV)) or (os.path.getsize(LOG_CSV) == 0)
    if need:
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ts","phase","shop","note","product_id","variant_id","sku","delta","message"])

def log_row(phase: str, shop: str, note: str, product_id="", variant_id="", sku="", delta="", message=""):
    ensure_log_header()
    row = [now_ist_str(), phase, shop, note, product_id, variant_id, sku, delta, message]
    if LOG_TO_STDOUT:
        print("|".join(str(x) for x in row), flush=True)
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)

def _as_int_or_none(v):
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if s == "" or s == "-":
            return None
        return int(s)
    except Exception:
        return None

def _norm_exact(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = s.strip().lower()
    return s2 if s2 else None

def summarize_product_skus(variants: List[dict], include_untracked: bool) -> str:
    seen: List[str] = []
    for v in variants or []:
        tracked = bool(((v.get("inventoryItem") or {}).get("tracked")))
        if include_untracked or tracked:
            raw = (v.get("sku") or "").strip()
            if raw and raw not in seen:
                seen.append(raw)
    if not seen:
        return ""
    return ";".join(seen[:5]) + (f";…(+{len(seen)-5})" if len(seen) > 5 else "")

# ---------- State ----------
def load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"initialized": False}

def save_state(obj):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, STATE_PATH)

# ---------- GraphQL ----------
QUERY_COLLECTION_PAGE_IN = """
query ($handle:String!, $cursor:String) {
  collectionByHandle(handle:$handle){
    products(first: 40, after:$cursor){
      pageInfo{ hasNextPage endCursor }
      nodes{
        id title status
        collections(first: 10){ nodes{ handle } }
        variants(first: 100){
          nodes{
            id title sku inventoryQuantity
            inventoryItem{ id tracked }
            inventoryPolicy
          }
        }
        # India product-level bridge metafield:
        skuMeta: metafield(namespace: "%(NS_SKU)s", key: "%(KEY_SKU)s"){ value }
        badges:      metafield(namespace: "%(NS)s", key: "%(BK)s"){ id value type }
        dtime:       metafield(namespace: "%(NS)s", key: "%(DK)s"){ id value type }
        salesTotal:  metafield(namespace: "%(NS)s", key: "%(KS)s"){ id value type }
        salesDates:  metafield(namespace: "%(NS)s", key: "%(KD)s"){ id value type }
        strikeRate:  metafield(namespace: "%(NS)s", key: "%(KR)s"){ id value type }
        actHours:    metafield(namespace: "%(NS)s", key: "%(KH)s"){ id value type }
        actStarted:  metafield(namespace: "%(NS)s", key: "%(KA)s"){ id value type }
      }
    }
  }
}
""".replace("%(NS)s", MF_NAMESPACE).replace("%(BK)s", MF_BADGES_KEY)\
   .replace("%(DK)s", MF_DELIVERY_KEY).replace("%(KS)s", KEY_SALES).replace("%(KD)s", KEY_DATES) \
   .replace("%(KR)s", STRIKE_RATE_KEY).replace("%(KH)s", ACTIVE_HOURS_KEY).replace("%(KA)s", ACTIVE_STARTED_AT_KEY) \
   .replace("%(NS_SKU)s", CUSTOM_SKU_NAMESPACE).replace("%(KEY_SKU)s", CUSTOM_SKU_KEY)

QUERY_COLLECTION_PAGE_US = """
query ($handle:String!, $cursor:String) {
  collectionByHandle(handle:$handle){
    products(first: 40, after:$cursor){
      pageInfo{ hasNextPage endCursor }
      nodes{
        id title
        collections(first: 10){ nodes{ handle } }
        variants(first: 100){
          nodes{
            id sku inventoryQuantity
            inventoryItem{ id }
          }
        }
        # US product-level bridge metafield for mapping:
        skuMeta: metafield(namespace: "%(NS_SKU)s", key: "%(KEY_SKU)s"){ value }
      }
    }
  }
}
""".replace("%(NS_SKU)s", CUSTOM_SKU_NAMESPACE).replace("%(KEY_SKU)s", CUSTOM_SKU_KEY)

# ----- India behavior -----
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
    else:
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
        log_row("IN", "IN", "WARN_STATUS", product_id=gid_num(product_gid), sku="", message=str(errs))
        return False
    time.sleep(MUTATION_SLEEP_SEC)
    return True

def set_product_metafields_in(domain:str, token:str, product_gid:str, badges_node:dict, dtime_node:dict,
                              target_badge:str, target_delivery:str) -> None:
    btype = BADGES_FORCE_TYPE or (badges_node or {}).get("type") or "list.single_line_text_field"
    dtype = DELIVERY_FORCE_TYPE or (dtime_node  or {}).get("type")  or "single_line_text_field"

    mutation = """
    mutation($metafields:[MetafieldsSetInput!]!){
      metafieldsSet(metafields:$metafields){
        userErrors{ field message }
      }
    }"""
    mf = []
    if target_badge:
        if btype.startswith("list."):
            mf.append({"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":MF_BADGES_KEY,"type":btype,"value":json.dumps([target_badge])})
        else:
            mf.append({"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":MF_BADGES_KEY,"type":btype,"value":target_badge})
    else:
        if (badges_node or {}).get("id"):
            delm = "mutation($id:ID!){ metafieldDelete(input:{id:$id}){ userErrors{message} } }"
            gql(domain, token, delm, {"id": badges_node["id"]})
    mf.append({"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":MF_DELIVERY_KEY,"type":dtype,"value":target_delivery})
    if mf:
        data = gql(domain, token, mutation, {"metafields": mf})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_row("IN", "IN", "WARN", product_id=gid_num(product_gid), sku="", message=f"metafieldsSet errors: {errs}")

def bump_sales_in(domain:str, token:str, product_gid:str, sales_total_node:dict, sales_dates_node:dict, sold:int, today:str):
    st_type = (sales_total_node or {}).get("type") or "number_integer"
    sd_type = SALES_DATES_FORCE_TYPE or (sales_dates_node or {}).get("type") or "date"

    try:
        current = int((sales_total_node or {}).get("value") or "0")
    except Exception:
        current = 0
    new_total = current + int(sold)

    if sd_type == "list.date":
        existing = []
        raw = (sales_dates_node or {}).get("value")
        try:
            existing = json.loads(raw) if isinstance(raw,str) and raw.startswith("[") else []
        except Exception:
            existing = []
        if today not in existing:
            existing.append(today)
        dates_val = json.dumps(sorted(set(existing))[-365:])
        sd_payload = {"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":KEY_DATES,"type":"list.date","value":dates_val}
    else:
        sd_payload = {"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":KEY_DATES,"type":"date","value":today}

    m = "mutation($mfs:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$mfs){ userErrors{message field} } }"
    mf_inputs = [
        {"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":KEY_SALES,"type":st_type,"value":str(new_total)},
        sd_payload
    ]
    data = gql(domain, token, m, {"mfs": mf_inputs})
    errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
    if errs:
        log_row("IN", "IN", "WARN", product_id=gid_num(product_gid), sku="", message=f"sales metafieldsSet errors: {errs}")

# ----- Strike rate support -----
def _parse_dt_or_none(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None

def _hours_between(start: datetime, end: datetime) -> float:
    return max(0.0, (end - start).total_seconds() / 3600.0)

def update_active_time_and_strike_rate_in(product_node: dict, now_dt: datetime):
    pid_gid = product_node["id"]
    pid = gid_num(pid_gid)
    status = (product_node.get("status") or "").upper()

    sales_total_node = product_node.get("salesTotal") or {}
    strike_node      = product_node.get("strikeRate") or {}
    act_hours_node   = product_node.get("actHours") or {}
    act_started_node = product_node.get("actStarted") or {}

    try:
        sales_total = int((sales_total_node.get("value") or "0").strip())
    except Exception:
        sales_total = 0

    try:
        hours_total = float((act_hours_node.get("value") or "0").strip())
    except Exception:
        hours_total = 0.0

    started_at = _parse_dt_or_none((act_started_node.get("value") or "").strip())

    if status == "ACTIVE":
        if started_at is None:
            started_at = now_dt
        else:
            hours_total += _hours_between(started_at, now_dt)
            started_at = now_dt
    else:
        if started_at is not None:
            hours_total += _hours_between(started_at, now_dt)
            started_at = None

    denom = hours_total if hours_total > 0 else 0.01
    strike_rate = float(sales_total) / denom

    sr_type = STRIKE_RATE_FORCE_TYPE or (strike_node.get("type") or "number_decimal")
    ah_type = ACTIVE_HOURS_FORCE_TYPE or (act_hours_node.get("type") or "number_decimal")
    as_type = ACTIVE_STARTED_AT_FORCE_TYPE or (act_started_node.get("type") or "date_time")

    mutation = """
    mutation($mfs:[MetafieldsSetInput!]!){
      metafieldsSet(metafields:$mfs){
        userErrors{ field message }
      }
    }"""

    mfs = [
        { "ownerId": pid_gid, "namespace": MF_NAMESPACE,
          "key": ACTIVE_HOURS_KEY, "type": ah_type, "value": f"{hours_total:.6f}" },
        { "ownerId": pid_gid, "namespace": MF_NAMESPACE,
          "key": STRIKE_RATE_KEY, "type": sr_type, "value": f"{strike_rate:.6f}" },
    ]

    if started_at is None:
        if (act_started_node or {}).get("id"):
            delm = "mutation($id:ID!){ metafieldDelete(input:{id:$id}){ userErrors{message} } }"
            try:
                gql(IN_DOMAIN, IN_TOKEN, delm, {"id": act_started_node["id"]})
            except Exception as e:
                log_row("IN", "IN", "WARN", product_id=pid, message=f"actStarted delete error: {e}")
    else:
        mfs.append({
          "ownerId": pid_gid, "namespace": MF_NAMESPACE,
          "key": ACTIVE_STARTED_AT_KEY, "type": as_type, "value": started_at.isoformat()
        })

    try:
        data = gql(IN_DOMAIN, IN_TOKEN, mutation, {"mfs": mfs})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_row("IN", "IN", "WARN", product_id=pid, message=f"strike_rate writes: {errs}")
    except Exception as e:
        log_row("IN", "IN", "WARN", product_id=pid, message=f"strike_rate exception: {e}")

# ----- Index builder (India) -----
def build_and_persist_india_sku_index():
    """
    Create EXACT (trim+lower) custom.sku -> product & first tracked inventory_item_id map.
    Honors ONLY_ACTIVE_FOR_MAPPING (include only ACTIVE if True).
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
                raw_sku = (((p.get("skuMeta") or {}).get("value")) or "")
                norm = _norm_exact(raw_sku)
                if not norm:
                    continue

                # choose first tracked variant
                inv_id = 0
                for v in ((p.get("variants") or {}).get("nodes") or []):
                    inv = (v.get("inventoryItem") or {})
                    if bool(inv.get("tracked")):
                        inv_id = int(gid_num(inv.get("id") or "0") or "0")
                        if inv_id > 0:
                            break

                index[norm] = {
                    "product_gid": p.get("id"),
                    "product_id": gid_num(p.get("id")),
                    "inventory_item_id": inv_id,
                    "status": status,
                    "title": (p.get("title") or "")
                }
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break
    save_json(IN_SKU_INDEX, index)
    log_row("IN", "IN", "INDEX_BUILT", message=f"entries={len(index)}")

# ----- India scan (availability + metafields + strike + status) -----
def scan_india_and_update(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(IN_LAST_SEEN, {})
    today = today_ist_str()

    for handle in IN_COLLECTIONS:
        cursor = None
        visited_products = set()
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                log_row("IN", "IN", "WARN", sku="", message=f"Collection not found: {handle}")
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                pid = gid_num(p["id"])
                if pid in visited_products:
                    continue
                visited_products.add(pid)

                variants = ((p.get("variants") or {}).get("nodes") or [])
                avail = compute_product_availability(variants, IN_INCLUDE_UNTRACKED)
                sku_summary = summarize_product_skus(variants, IN_INCLUDE_UNTRACKED)

                prev = _as_int_or_none(last_seen.get(pid))
                if prev is None:
                    last_seen[pid] = int(avail)
                    log_row("IN", "IN", "FIRST_SEEN", product_id=pid, sku=sku_summary, delta="", message=f"avail={avail}")
                else:
                    diff = int(avail) - int(prev)
                    if not read_only and diff < 0:
                        bump_sales_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, -diff, today)
                        log_row("IN", "IN", "SALES_BUMP", product_id=pid, sku=sku_summary, delta=diff, message=f"avail {prev}->{avail} (sold={-diff})")
                    last_seen[pid] = int(avail)

                # badges/delivery each time (post first cycle)
                _, target_badge, target_delivery = desired_state(avail)
                if not read_only:
                    set_product_metafields_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("badges") or {}, p.get("dtime") or {}, target_badge, target_delivery)

                # Strike rate accrual for special collection
                if (not read_only) and (handle == SPECIAL_STATUS_HANDLE):
                    update_active_time_and_strike_rate_in(p, now_ist())

                # SPECIAL STATUS RULE (symmetric)
                if (not read_only) and IN_CHANGE_STATUS and (handle == SPECIAL_STATUS_HANDLE):
                    current_status = (p.get("status") or "").upper()
                    if avail < 1 and current_status == "ACTIVE":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "DRAFT")
                        log_row("IN", "IN", "STATUS_TO_DRAFT" if ok else "STATUS_TO_DRAFT_FAILED",
                                product_id=pid, sku=sku_summary, delta=avail, message=f"handle={handle}")
                    elif avail >= 1 and current_status == "DRAFT":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "ACTIVE")
                        log_row("IN", "IN", "STATUS_TO_ACTIVE" if ok else "STATUS_TO_ACTIVE_FAILED",
                                product_id=pid, sku=sku_summary, delta=avail, message=f"handle={handle}")

                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            save_json(IN_LAST_SEEN, {k:int(v) for k,v in last_seen.items()})
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

    # Rebuild EXACT index at the end of India scan
    try:
        build_and_persist_india_sku_index()
    except Exception as e:
        log_row("IN", "IN", "WARN", message=f"INDEX_BUILD_ERROR: {e}")

# ----- US → IN using EXACT index -----
def scan_usa_and_mirror_to_india(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(US_LAST_SEEN, {})
    sku_index: Dict[str, Dict[str, Any]] = load_json(IN_SKU_INDEX, {})

    for handle in US_COLLECTIONS:
        cursor = None
        visited_variants = set()
        while True:
            data = gql(US_DOMAIN, US_TOKEN, QUERY_COLLECTION_PAGE_US, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                log_row("US", "US", "WARN", sku="", message=f"Collection not found: {handle}")
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                raw_prod_sku = (((p.get("skuMeta") or {}).get("value")) or "")
                norm_prod_sku = _norm_exact(raw_prod_sku)

                for v in ((p.get("variants") or {}).get("nodes") or []):
                    vgid = v.get("id"); vid = gid_num(vgid)
                    if vid in visited_variants:
                        continue
                    visited_variants.add(vid)

                    qty = int(v.get("inventoryQuantity") or 0)
                    prev = _as_int_or_none(last_seen.get(vid))
                    if prev is None:
                        last_seen[vid] = qty
                        log_row("US", "US", "FIRST_SEEN", variant_id=vid, sku=(raw_prod_sku or ""), delta="", message=f"qty={qty}")
                    else:
                        delta = qty - int(prev)
                        if delta != 0:
                            if read_only:
                                log_row("US→IN", "IN", "FIRST_CYCLE_SKIP",
                                        variant_id=vid, sku=(raw_prod_sku or ""), delta=delta,
                                        message="Global first cycle is read-only; no adjust applied")
                            else:
                                if not USE_PRODUCT_CUSTOM_SKU:
                                    log_row("US→IN", "US", "SKIPPED", variant_id=vid, delta=delta,
                                            message="USE_PRODUCT_CUSTOM_SKU=0; no mirroring performed")
                                else:
                                    if not norm_prod_sku:
                                        log_row("US→IN", "US", "**WARN_NO_PRODUCT_CUSTOM_SKU_US**",
                                                variant_id=vid, sku=(raw_prod_sku or ""), delta=delta,
                                                message="US product has no custom.sku; cannot mirror to India")
                                    else:
                                        target = sku_index.get(norm_prod_sku)
                                        if not target:
                                            log_row("US→IN", "IN", "**SKU_NOT_IN_INDEX**",
                                                    variant_id=vid, sku=(raw_prod_sku or ""), delta=delta,
                                                    message="No exact match in India index (wait for next IN scan)")
                                        else:
                                            inv_item_id = int(target.get("inventory_item_id") or 0)
                                            if inv_item_id <= 0:
                                                log_row("US→IN", "IN", "**WARN_NO_TRACKED_VARIANT_IN_INDIA**",
                                                        variant_id=vid, sku=(raw_prod_sku or ""), delta=delta,
                                                        message=f"India product has no tracked variant | pid={target.get('product_id')}")
                                            else:
                                                try:
                                                    rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id, int(IN_LOCATION_ID), delta)
                                                    log_row("US→IN", "IN", "APPLIED_DELTA",
                                                            variant_id=vid, sku=(raw_prod_sku or ""), delta=delta,
                                                            message=f"Adjusted IN inventory_item_id={inv_item_id} by {delta} (via EXACT index)")
                                                    time.sleep(MUTATION_SLEEP_SEC)
                                                except Exception as e:
                                                    log_row("US→IN", "IN", "ERROR_APPLYING_DELTA",
                                                            variant_id=vid, sku=(raw_prod_sku or ""), delta=delta, message=str(e))
                            last_seen[vid] = qty

                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
            save_json(US_LAST_SEEN, {k:int(v) for k,v in last_seen.items()})
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

# ---------- Snapshots (CSV + JSONL) ----------
def _flatten_collections(node: dict) -> str:
    cols = (((node.get("collections") or {}).get("nodes")) or [])
    return ",".join(sorted({(c.get("handle") or "").strip() for c in cols if (c.get("handle") or "").strip()}))

def export_india_snapshot() -> Dict[str, Any]:
    """
    Write /data/in_products.jsonl and /data/in_products.csv
    Columns: product_id, title, status, custom.sku, availability, first_tracked_inventory_item_id,
             sales_total, sales_dates, badges, delivery_time, collections, variant_sku_sample
    """
    jsonl_path, csv_path = IN_JSONL, IN_CSV
    count = 0
    with open(jsonl_path, "w", encoding="utf-8") as jf, open(csv_path, "w", newline="", encoding="utf-8") as cf:
        w = csv.writer(cf)
        w.writerow(["product_id","title","status","custom.sku","availability","first_tracked_inventory_item_id",
                    "sales_total","sales_dates","badges","delivery_time","collections","variant_sku_sample"])
        for handle in IN_COLLECTIONS:
            cursor = None
            while True:
                data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN, {"handle": handle, "cursor": cursor})
                coll = data.get("collectionByHandle")
                if not coll: break
                prods = ((coll.get("products") or {}).get("nodes") or [])
                pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
                for pnode in prods:
                    pid = gid_num(pnode["id"])
                    status = (pnode.get("status") or "").upper()
                    raw_sku = (((pnode.get("skuMeta") or {}).get("value")) or "").strip()
                    variants = ((pnode.get("variants") or {}).get("nodes") or [])
                    avail = compute_product_availability(variants, IN_INCLUDE_UNTRACKED)

                    inv_id = 0
                    sku_sample = []
                    for v in variants:
                        inv = (v.get("inventoryItem") or {})
                        if inv and inv.get("tracked"):
                            if inv_id == 0:
                                inv_id = int(gid_num(inv.get("id") or "0") or "0")
                        rsku = (v.get("sku") or "").strip()
                        if rsku and len(sku_sample) < 5 and rsku not in sku_sample:
                            sku_sample.append(rsku)

                    sales_total = (pnode.get("salesTotal") or {}).get("value") or ""
                    sales_dates = (pnode.get("salesDates") or {}).get("value") or ""
                    badges = (pnode.get("badges") or {}).get("value") or ""
                    delivery = (pnode.get("dtime") or {}).get("value") or ""
                    collections = _flatten_collections(pnode)

                    row = {
                        "product_id": pid,
                        "title": pnode.get("title") or "",
                        "status": status,
                        "custom.sku": raw_sku,
                        "availability": avail,
                        "first_tracked_inventory_item_id": inv_id,
                        "sales_total": sales_total,
                        "sales_dates": sales_dates,
                        "badges": badges,
                        "delivery_time": delivery,
                        "collections": collections,
                        "variant_sku_sample": ";".join(sku_sample)
                    }
                    jf.write(json.dumps(row, ensure_ascii=False) + "\n")
                    w.writerow([row["product_id"], row["title"], row["status"], row["custom.sku"], row["availability"],
                                row["first_tracked_inventory_item_id"], row["sales_total"], row["sales_dates"],
                                row["badges"], row["delivery_time"], row["collections"], row["variant_sku_sample"]])
                    count += 1
                sleep_ms(SLEEP_BETWEEN_PAGES_MS)
                if pageInfo.get("hasNextPage"):
                    cursor = pageInfo.get("endCursor")
                else:
                    break
    return {"count": count, "jsonl": jsonl_path, "csv": csv_path}

def export_usa_snapshot() -> Dict[str, Any]:
    """
    Write /data/us_products.jsonl and /data/us_products.csv
    Columns: product_id, title, custom.sku, variant_id, variant_sku, variant_qty, collections
    """
    jsonl_path, csv_path = US_JSONL, US_CSV
    count = 0
    with open(jsonl_path, "w", encoding="utf-8") as jf, open(csv_path, "w", newline="", encoding="utf-8") as cf:
        w = csv.writer(cf)
        w.writerow(["product_id","title","custom.sku","variant_id","variant_sku","variant_qty","collections"])
        for handle in US_COLLECTIONS:
            cursor = None
            while True:
                data = gql(US_DOMAIN, US_TOKEN, QUERY_COLLECTION_PAGE_US, {"handle": handle, "cursor": cursor})
                coll = data.get("collectionByHandle")
                if not coll: break
                prods = ((coll.get("products") or {}).get("nodes") or [])
                pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
                for pnode in prods:
                    pid = gid_num(pnode["id"])
                    raw_sku = (((pnode.get("skuMeta") or {}).get("value")) or "").strip()
                    collections = _flatten_collections(pnode)
                    for v in ((pnode.get("variants") or {}).get("nodes") or []):
                        vid = gid_num(v.get("id") or "")
                        w.writerow([pid, pnode.get("title") or "", raw_sku, vid, (v.get("sku") or "").strip(), int(v.get("inventoryQuantity") or 0), collections])
                        row = {
                            "product_id": pid, "title": pnode.get("title") or "",
                            "custom.sku": raw_sku, "variant_id": vid,
                            "variant_sku": (v.get("sku") or "").strip(),
                            "variant_qty": int(v.get("inventoryQuantity") or 0),
                            "collections": collections
                        }
                        jf.write(json.dumps(row, ensure_ascii=False) + "\n")
                        count += 1
                sleep_ms(SLEEP_BETWEEN_PAGES_MS)
                if pageInfo.get("hasNextPage"):
                    cursor = pageInfo.get("endCursor")
                else:
                    break
    return {"count": count, "jsonl": jsonl_path, "csv": csv_path}

# ---------- Scheduler / HTTP ----------
run_lock = Lock()
is_running = False

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

        # India first (also rebuilds the index at end)
        scan_india_and_update(read_only=first_cycle)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

        # US second (consumes the most recent index)
        scan_usa_and_mirror_to_india(read_only=first_cycle)

        if first_cycle:
            state["initialized"] = True
            save_state(state)
            log_row("SCHED", "BOTH", "FIRST_CYCLE_DONE", sku="", message="Baselines learned; future cycles will apply deltas")

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
            log_row("SCHED", "BOTH", "ERROR", sku="", message=str(e))
        time.sleep(max(1, RUN_EVERY_MIN) * 60)

app = Flask(__name__)

def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp

@app.route("/", methods=["GET"])
def root():
    return "ok", 200

@app.route("/health", methods=["GET"])
def health():
    return "ok", 200

@app.route("/diag", methods=["GET"])
def diag():
    info = {
        "api_version": API_VERSION,
        "in_domain": IN_DOMAIN,
        "us_domain": US_DOMAIN,
        "in_collections": IN_COLLECTIONS,
        "us_collections": US_COLLECTIONS,
        "data_dir": DATA_DIR,
        "scheduler_enabled": ENABLE_SCHEDULER,
        "run_every_min": RUN_EVERY_MIN,
        "use_product_custom_sku": USE_PRODUCT_CUSTOM_SKU,
        "only_active_for_mapping": ONLY_ACTIVE_FOR_MAPPING,
        "in_last_seen_count": len(load_json(IN_LAST_SEEN, {})),
        "us_last_seen_count": len(load_json(US_LAST_SEEN, {})),
        "index_entries": len(load_json(IN_SKU_INDEX, {}))
    }
    return _cors(jsonify(info)), 200

@app.route("/diag_index", methods=["GET"])
def diag_index():
    idx = load_json(IN_SKU_INDEX, {})
    sample = list(idx.keys())[:10]
    return _cors(jsonify({"entries": len(idx), "sample_keys": sample})), 200

@app.route("/who_maps", methods=["GET"])
def who_maps():
    sku = (request.args.get("sku") or "").strip()
    idx = load_json(IN_SKU_INDEX, {})
    hit = idx.get(_norm_exact(sku or ""))
    return _cors(jsonify({"sku": sku, "found": bool(hit), "target": hit})), 200

@app.route("/rebuild_index", methods=["GET"])
def rebuild_index():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))
    try:
        build_and_persist_india_sku_index()
        idx = load_json(IN_SKU_INDEX, {})
        return _cors(jsonify({"ok": True, "entries": len(idx)})), 200
    except Exception as e:
        return _cors(jsonify({"ok": False, "error": str(e)})), 500

@app.route("/export_csv", methods=["GET"])
def export_csv():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))
    try:
        in_res = export_india_snapshot()
        us_res = export_usa_snapshot()
        return _cors(jsonify({"ok": True, "india": in_res, "usa": us_res})), 200
    except Exception as e:
        return _cors(jsonify({"ok": False, "error": str(e)})), 500

@app.route("/run", methods=["GET","POST"])
def run_now():
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))
    Thread(target=run_cycle, daemon=True).start()
    return _cors(jsonify({"status": "started"})), 202

if ENABLE_SCHEDULER:
    Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    print(f"[BOOT] Dual sync on port {PORT} | API {API_VERSION}")
    print(f"[CFG] IN={IN_DOMAIN} (loc {IN_LOCATION_ID}) | US={US_DOMAIN} (loc {US_LOCATION_ID}) | DATA_DIR={DATA_DIR}")
    run_simple("0.0.0.0", PORT, app)
