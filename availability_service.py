#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dual-site availability sync (Render-friendly)
- India (IN):
    * Scans products in configured collections
    * Computes product availability (sum of tracked variant qty, or include untracked via flag)
    * On availability drop vs last_seen: bump sales_total & sales_dates
    * Sets badges & delivery_time based on availability
    * SPECIAL_STATUS_HANDLE: ACTIVE<->DRAFT based on availability >=1 or <1
    * If CLAMP_AVAIL_TO_ZERO=1 and availability < 0: clamp back to 0 via REST adjust
      - **Baseline reset on clamp**: last_seen[pid] := 0 (prevents repeat clamp without bump)
      - Optional CLAMP_IMPLIES_SALE=1 to add a safety sale if a negative is seen without a diff bump in the same pass
- USA (US):
    * Reads product variants; gets product.custom.sku from the parent product
    * If a variant qty decreases: mirror Δ to India using IN index (sku -> inventory_item_id)
    * If variant qty increases: ignore (unless MIRROR_US_INCREASES=1)

State & logs:
- Uses DATA_DIR (e.g., /data) for persistence across restarts: last_seen, index, logs
- CSV: dual_sync_log.csv  | JSONL: dual_sync_log.jsonl
- /diag endpoint to check configuration & counts
- /run endpoint (POST ?key=PIXEL_SHARED_SECRET) to trigger a cycle
- Optional scheduler loop (ENABLE_SCHEDULER=1) for background worker

Safe defaults and throttling-friendly sleeps.
"""

import os, sys, time, json, csv, random
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple

import requests
from flask import Flask, request, jsonify, make_response

# -----------------------------
# Config / Env
# -----------------------------
API_VERSION = os.getenv("API_VERSION", "2024-10").strip()

PIXEL_SHARED_SECRET = os.getenv("PIXEL_SHARED_SECRET", "").strip()
if not PIXEL_SHARED_SECRET:
    raise SystemExit("PIXEL_SHARED_SECRET required")

# Shared pacing (ms) and schedule
RUN_EVERY_MIN = int(os.getenv("RUN_EVERY_MIN", "5"))
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "0") == "1"

SLEEP_BETWEEN_PRODUCTS_MS = int(os.getenv("SLEEP_BETWEEN_PRODUCTS_MS", "300"))
SLEEP_BETWEEN_PAGES_MS = int(os.getenv("SLEEP_BETWEEN_PAGES_MS", "600"))
SLEEP_BETWEEN_SHOPS_MS = int(os.getenv("SLEEP_BETWEEN_SHOPS_MS", "1200"))
MUTATION_SLEEP_SEC = float(os.getenv("MUTATION_SLEEP_SEC", "0.35"))

# India (metafields + receives inventory deltas)
IN_DOMAIN = os.getenv("IN_DOMAIN", "").strip()
IN_TOKEN  = os.getenv("IN_TOKEN", "").strip()
IN_LOCATION_ID = os.getenv("IN_LOCATION_ID", "").strip()
IN_COLLECTIONS = [x.strip() for x in os.getenv("IN_COLLECTION_HANDLES", "").split(",") if x.strip()]
IN_INCLUDE_UNTRACKED = os.getenv("IN_INCLUDE_UNTRACKED", "0") == "1"
IN_CHANGE_STATUS = os.getenv("IN_CHANGE_STATUS", "0") == "1"  # allow status changes for special handle

# USA (source of variant-level deltas)
US_DOMAIN = os.getenv("US_DOMAIN", "").strip()
US_TOKEN  = os.getenv("US_TOKEN", "").strip()
US_LOCATION_ID = os.getenv("US_LOCATION_ID", "").strip()
US_COLLECTIONS = [x.strip() for x in os.getenv("US_COLLECTION_HANDLES", "").split(",") if x.strip()]

# Metafields & logic
MF_NAMESPACE = os.getenv("MF_NAMESPACE", "custom")
MF_BADGES_KEY = os.getenv("MF_BADGES_KEY", "badges")
MF_DELIVERY_KEY = os.getenv("MF_DELIVERY_KEY", "delivery_time")
KEY_SALES = os.getenv("KEY_SALES", "sales_total")
KEY_DATES = os.getenv("KEY_DATES", "sales_dates")

BADGE_READY    = "Ready To Ship"
DELIVERY_READY = "2-5 Days Across India"
DELIVERY_MTO   = "12-15 Days Across India"

SPECIAL_STATUS_HANDLE = os.getenv(
    "SPECIAL_STATUS_HANDLE",
    "budget-gold-plated-jewelry-premium-jadau-ornaments-on-mix-metal-base"
).strip()

# product.custom.sku usage
CUSTOM_SKU_NAMESPACE = os.getenv("CUSTOM_SKU_NAMESPACE", "custom").strip()
CUSTOM_SKU_KEY       = os.getenv("CUSTOM_SKU_KEY", "sku").strip()
USE_PRODUCT_CUSTOM_SKU = os.getenv("USE_PRODUCT_CUSTOM_SKU", "1") == "1"

# India index & mapping behavior
ONLY_ACTIVE_FOR_MAPPING = os.getenv("ONLY_ACTIVE_FOR_MAPPING", "1") == "1"

# Mirroring policy for US increases
MIRROR_US_INCREASES = os.getenv("MIRROR_US_INCREASES", "0") == "1"

# Clamp behavior
CLAMP_AVAIL_TO_ZERO = os.getenv("CLAMP_AVAIL_TO_ZERO", "1") == "1"
CLAMP_IMPLIES_SALE  = os.getenv("CLAMP_IMPLIES_SALE", "0") == "1"  # record a sale if we see negative and no bump in the pass
INSTANT_BUMP_AFTER_US_DELTA = os.getenv("INSTANT_BUMP_AFTER_US_DELTA", "0") == "1"

# Force metafield types (optional if definitions mismatch)
BADGES_FORCE_TYPE = os.getenv("BADGES_FORCE_TYPE", "").strip()                # e.g., "list.single_line_text_field"
DELIVERY_FORCE_TYPE = os.getenv("DELIVERY_FORCE_TYPE", "").strip()
SALES_DATES_FORCE_TYPE = os.getenv("SALES_DATES_FORCE_TYPE", "").strip()      # "list.date" or "date"
STRIKE_RATE_FORCE_TYPE = os.getenv("STRIKE_RATE_FORCE_TYPE", "").strip()
ACTIVE_HOURS_FORCE_TYPE = os.getenv("ACTIVE_HOURS_FORCE_TYPE", "").strip()
ACTIVE_STARTED_AT_FORCE_TYPE = os.getenv("ACTIVE_STARTED_AT_FORCE_TYPE", "").strip()

# Render / persistence
DATA_DIR = os.getenv("DATA_DIR", ".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)

# Logging detail
LOG_JSONL = os.getenv("LOG_JSONL", "1") == "1"
LOG_EMOJI = os.getenv("LOG_EMOJI", "1") == "1"
LOG_TITLES = os.getenv("LOG_TITLES", "1") == "1"

def p(*parts): return os.path.join(DATA_DIR, *parts)

PORT = int(os.getenv("PORT", os.getenv("PORT", "10000")))

# Files
IN_LAST_SEEN  = p("in_last_seen.json")     # product_id(num) -> last_seen_availability (int)
US_LAST_SEEN  = p("us_last_seen.json")     # variant_id(num) -> last_seen_qty (int)
IN_SKU_INDEX  = p("in_sku_index.json")     # sku(str) -> { "item_id": int, "product_id": str }
STATE_PATH    = p("dual_state.json")       # {"initialized": bool}
LOG_CSV       = p("dual_sync_log.csv")     # CSV logs
LOG_JSONL_PATH = p("dual_sync_log.jsonl")  # JSONL logs

# -----------------------------
# HTTP helpers (Shopify Admin)
# -----------------------------
def hdr(token: str) -> Dict[str,str]:
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json", "Accept": "application/json"}

def gql(domain: str, token: str, query: str, variables: dict = None) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    # robust retry including 429 and 5xx
    for attempt in range(1, 9):
        r = requests.post(url, headers=hdr(token), json={"query": query, "variables": variables or {}}, timeout=60)
        if r.status_code in (429, 502, 503, 504):
            time.sleep(min(12.0, 0.5 * (2 ** (attempt - 1))) + random.uniform(0, 0.35))
            continue
        if r.status_code != 200:
            raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")
        data = r.json()
        if data.get("errors"):
            throttled = any(((e.get("extensions") or {}).get("code", "").upper() == "THROTTLED") for e in data["errors"])
            if throttled:
                time.sleep(min(12.0, 0.5 * (2 ** (attempt - 1))) + random.uniform(0, 0.35))
                continue
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]
    raise RuntimeError("GraphQL throttled/5xx repeatedly")

def rest_adjust_inventory(domain: str, token: str, inventory_item_id: int, location_id: int, delta: int) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/adjust.json"
    payload = {"inventory_item_id": int(inventory_item_id), "location_id": int(location_id), "available_adjustment": int(delta)}
    for attempt in range(1, 7):
        r = requests.post(url, headers=hdr(token), json=payload, timeout=40)
        if r.status_code in (429, 502, 503, 504):
            time.sleep(min(10.0, 0.4 * (2 ** (attempt - 1))) + random.uniform(0, 0.3))
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"REST adjust {r.status_code}: {r.text}")
        return

# -----------------------------
# Utils / Persistence / Logging
# -----------------------------
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

def normalize_sku(s: str) -> str:
    # For strict matching we only trim and lower; NO hyphen/space normalization (per your requirement)
    return (s or "").strip().lower()

def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return default

def save_json(path: str, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(obj, f, indent=2)
    os.replace(tmp, path)

def ensure_log_headers():
    if not os.path.exists(LOG_CSV) or os.path.getsize(LOG_CSV) == 0:
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["ts","phase","shop","note","product_id","variant_id","sku","delta","message","title","before","after","collections"])
    if LOG_JSONL and (not os.path.exists(LOG_JSONL_PATH) or os.path.getsize(LOG_JSONL_PATH) == 0):
        with open(LOG_JSONL_PATH, "a", encoding="utf-8") as _:
            pass

def log_row(note: str, phase: str, shop: str,
            product_id: str="", variant_id: str="", sku: str="", delta: str="",
            message: str="", title: str="", before: str="", after: str="", collections: str=""):
    ensure_log_headers()
    # Emojis
    em = ""
    if LOG_EMOJI:
        em = {
            "INDEX_BUILT": "🗂️",
            "APPLIED_DELTA": "🔁",
            "IGNORED_INCREASE": "🙅‍♂️➕",
            "SALES_BUMP": "🧾➖",
            "CLAMP_TO_ZERO": "🧰0️⃣",
            "RESTOCK": "📦➕",
            "WARN": "⚠️",
            "STATUS_TO_DRAFT": "🛑",
            "STATUS_TO_ACTIVE": "✅",
        }.get(note, "")
    # CSV
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([now_ist_str(), phase, shop, note, product_id, variant_id, sku, delta, message, title if LOG_TITLES else "", before, after, collections])
    # STDOUT for Render log stream
    print(f"{em} {phase} {note} [SKU {sku}] pid={product_id} {before}→{after} Δ{delta} “{title}” — {message}".strip(), flush=True)
    # JSONL
    if LOG_JSONL:
        rec = {
            "ts": now_ist_str(), "phase": phase, "shop": shop, "note": note,
            "product_id": product_id, "variant_id": variant_id, "sku": sku,
            "delta": delta, "message": message, "title": title if LOG_TITLES else "",
            "before": before, "after": after, "collections": collections
        }
        with open(LOG_JSONL_PATH, "a", encoding="utf-8") as jf:
            jf.write(json.dumps(rec, ensure_ascii=False) + "\n")

def _as_int_or_none(v):
    try:
        if v is None: return None
        if isinstance(v, (int, float)): return int(v)
        s = str(v).strip()
        if s == "" or s == "-": return None
        return int(s)
    except Exception:
        return None

# -----------------------------
# GraphQL Queries
# -----------------------------
# India: full product page, including metafields (badges/delivery/sales), variants, custom.sku
QUERY_COLLECTION_PAGE_IN = """
query ($handle:String!, $cursor:String) {
  collectionByHandle(handle:$handle){
    products(first: 60, after:$cursor){
      pageInfo{ hasNextPage endCursor }
      nodes{
        id title status
        variants(first: 100){
          nodes{
            id title sku inventoryQuantity
            inventoryItem{ id tracked }
            inventoryPolicy
          }
        }
        customSku: metafield(namespace: "%(NSC)s", key: "%(KC)s"){ value }
        badges: metafield(namespace: "%(NS)s", key: "%(BK)s"){ id value type }
        dtime:  metafield(namespace: "%(NS)s", key: "%(DK)s"){ id value type }
        salesTotal: metafield(namespace: "%(NS)s", key: "%(KS)s"){ id value type }
        salesDates: metafield(namespace: "%(NS)s", key: "%(KD)s"){ id value type }
      }
    }
  }
}
""".replace("%(NSC)s", CUSTOM_SKU_NAMESPACE)\
   .replace("%(KC)s", CUSTOM_SKU_KEY)\
   .replace("%(NS)s", MF_NAMESPACE).replace("%(BK)s", MF_BADGES_KEY)\
   .replace("%(DK)s", MF_DELIVERY_KEY).replace("%(KS)s", KEY_SALES)\
   .replace("%(KD)s", KEY_DATES)

# USA: product variants with parent product custom.sku
QUERY_COLLECTION_PAGE_US = """
query ($handle:String!, $cursor:String) {
  collectionByHandle(handle:$handle){
    products(first: 60, after:$cursor){
      pageInfo{ hasNextPage endCursor }
      nodes{
        id title
        variants(first: 100){
          nodes{
            id sku inventoryQuantity
            inventoryItem{ id }
            product{
              id title
              customSku: metafield(namespace: "%(NSC)s", key: "%(KC)s"){ value }
            }
          }
        }
      }
    }
  }
}
""".replace("%(NSC)s", CUSTOM_SKU_NAMESPACE).replace("%(KC)s", CUSTOM_SKU_KEY)

# India: product metafields verify query
QUERY_PRODUCT_MF = """
query ($id: ID!){
  product(id:$id){
    badges: metafield(namespace: "%(NS)s", key: "%(BK)s"){ id value type }
    dtime:  metafield(namespace: "%(NS)s", key: "%(DK)s"){ id value type }
    salesTotal: metafield(namespace: "%(NS)s", key: "%(KS)s"){ id value type }
    salesDates: metafield(namespace: "%(NS)s", key: "%(KD)s"){ id value type }
  }
}
""".replace("%(NS)s", MF_NAMESPACE).replace("%(BK)s", MF_BADGES_KEY)\
   .replace("%(DK)s", MF_DELIVERY_KEY).replace("%(KS)s", KEY_SALES).replace("%(KD)s", KEY_DATES)

MUTATION_PRODUCT_UPDATE = """
mutation ProductUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id status }
    userErrors { field message }
  }
}
"""

# -----------------------------
# Core helpers
# -----------------------------
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

def update_product_status(domain: str, token: str, product_gid: str, target_status: str) -> bool:
    data = gql(domain, token, MUTATION_PRODUCT_UPDATE, {"input": {"id": product_gid, "status": target_status}})
    errs = ((data.get("productUpdate") or {}).get("userErrors") or [])
    if errs:
        log_row("WARN", "IN", "IN", product_id=gid_num(product_gid), message=f"status errors: {errs}")
        return False
    time.sleep(MUTATION_SLEEP_SEC)
    return True

def set_product_metafields_in(domain:str, token:str, product_gid:str,
                              badges_node:dict, dtime_node:dict,
                              target_badge:str, target_delivery:str):
    btype = BADGES_FORCE_TYPE or (badges_node or {}).get("type") or "single_line_text_field"
    dtype = DELIVERY_FORCE_TYPE or (dtime_node  or {}).get("type") or "single_line_text_field"
    # If allowed type is list.* and we have a single item, wrap accordingly
    m = "mutation($metafields:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$metafields){ userErrors{ field message } } }"
    mfs = []
    if target_badge:
        if btype.startswith("list."):
            mfs.append({"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_BADGES_KEY, "type": btype, "value": json.dumps([target_badge])})
        else:
            mfs.append({"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_BADGES_KEY, "type": btype, "value": target_badge})
    else:
        # badge empty → delete if exists
        if (badges_node or {}).get("id"):
            delm = "mutation($id:ID!){ metafieldDelete(input:{id:$id}){ userErrors{message} } }"
            gql(domain, token, delm, {"id": badges_node["id"]})
    mfs.append({"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_DELIVERY_KEY, "type": dtype,
                "value": json.dumps([target_delivery]) if dtype.startswith("list.") else target_delivery})
    if mfs:
        data = gql(domain, token, m, {"metafields": mfs})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_row("WARN", "IN", "IN", product_id=gid_num(product_gid), message=f"metafieldsSet errors: {errs}")

def bump_sales_in(domain:str, token:str, product_gid:str, sales_total_node:dict, sales_dates_node:dict, sold:int, today:str):
    # sales_total
    try:
        current = int((sales_total_node or {}).get("value") or "0")
    except Exception:
        current = 0
    new_total = current + int(sold)
    st_type = (sales_total_node or {}).get("type") or "number_integer"

    # sales_dates
    sd_node_type = SALES_DATES_FORCE_TYPE or (sales_dates_node or {}).get("type") or "list.date"
    if sd_node_type == "list.date":
        existing = []
        raw = (sales_dates_node or {}).get("value")
        try:
            existing = json.loads(raw) if isinstance(raw, str) and raw.startswith("[") else []
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
        log_row("WARN", "IN", "IN", product_id=gid_num(product_gid), message=f"sales metafieldsSet errors: {errs}")

# -----------------------------
# India: Build SKU index (strict)
# -----------------------------
def build_india_index() -> Dict[str, Dict[str, Any]]:
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
                pid = gid_num(p["id"])
                status = (p.get("status") or "").upper()
                if ONLY_ACTIVE_FOR_MAPPING and status != "ACTIVE":
                    continue
                custom_sku = (((p.get("customSku") or {}).get("value")) or "").strip()
                if not custom_sku:
                    continue
                sku_key = normalize_sku(custom_sku)
                # choose a representative inventory_item_id for adjustments (first variant that is tracked or fallback to first)
                inv_id = None
                for v in ((p.get("variants") or {}).get("nodes") or []):
                    inv_item_gid = ((v.get("inventoryItem") or {}).get("id") or "")
                    if inv_item_gid:
                        inv_id = int(gid_num(inv_item_gid))
                        break
                if inv_id:
                    index[sku_key] = {"item_id": inv_id, "product_id": pid}
                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
                sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            else:
                break
    save_json(IN_SKU_INDEX, index)
    log_row("INDEX_BUILT", "IN", "IN", message=f"entries={len(index)}")
    return index

# -----------------------------
# India: Scan & update
# -----------------------------
def scan_india_and_update(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(IN_LAST_SEEN, {})
    index = load_json(IN_SKU_INDEX, {})
    today = today_ist_str()

    for handle in IN_COLLECTIONS:
        cursor = None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll: break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})

            for p in prods:
                pid = gid_num(p["id"])
                ptitle = p.get("title") or ""
                variants = ((p.get("variants") or {}).get("nodes") or [])
                avail = compute_product_availability(variants, IN_INCLUDE_UNTRACKED)
                prev = _as_int_or_none(last_seen.get(pid))
                custom_sku = (((p.get("customSku") or {}).get("value")) or "").strip()

                # Decide badge/delivery based on availability
                _, target_badge, target_delivery = desired_state(avail)

                # Sales bump on drop
                sale_bumped = False
                if prev is None:
                    last_seen[pid] = int(avail)
                else:
                    diff = int(avail) - int(prev)
                    if not read_only and diff < 0:
                        bump_sales_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, -diff, today)
                        log_row("SALES_BUMP", "IN", "IN", product_id=pid, sku=custom_sku, delta=str(diff),
                                message=f"avail {prev}->{avail} (sold={-diff})", title=ptitle, before=str(prev), after=str(avail))
                        sale_bumped = True
                    last_seen[pid] = int(avail)

                # CLAMP negative to zero (and reset baseline to 0)
                if not read_only and CLAMP_AVAIL_TO_ZERO and avail < 0:
                    # optional safety bump if no diff-bump happened in this pass
                    if CLAMP_IMPLIES_SALE and not sale_bumped:
                        bump_sales_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, 1, today)
                        log_row("SALES_BUMP", "IN", "IN", product_id=pid, sku=custom_sku, delta="-1",
                                message="SAFETY_BUMP_ON_CLAMP", title=ptitle, before=str(avail), after=str(avail))

                    raise_by = -avail  # e.g., -2 → +2
                    # pick first variant's inventory_item_id
                    inv_item_id = None
                    for v in variants:
                        inv_gid = ((v.get("inventoryItem") or {}).get("id") or "")
                        if inv_gid:
                            inv_item_id = int(gid_num(inv_gid))
                            break
                    if inv_item_id:
                        try:
                            rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id, int(IN_LOCATION_ID), int(raise_by))
                            log_row("CLAMP_TO_ZERO", "IN", "IN", product_id=pid, sku=custom_sku, delta=f"+{raise_by}",
                                    message=f"Raised availability to 0 on inventory_item_id={inv_item_id}", title=ptitle, before=str(avail), after="0")
                            # **Baseline reset on clamp**
                            last_seen[pid] = 0
                            save_json(IN_LAST_SEEN, {k:int(v) for k,v in last_seen.items()})
                            avail = 0  # local state for below metafields/status
                        except Exception as e:
                            log_row("WARN", "IN", "IN", product_id=pid, sku=custom_sku, message=f"CLAMP failed: {e}", title=ptitle, before=str(avail), after=str(avail))

                # Metafields: badges/delivery (skip in read_only)
                if not read_only:
                    set_product_metafields_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("badges") or {}, p.get("dtime") or {}, target_badge, target_delivery)

                # SPECIAL status rule (only for the special handle)
                if (not read_only) and IN_CHANGE_STATUS and (handle == SPECIAL_STATUS_HANDLE):
                    current_status = (p.get("status") or "").upper()
                    if avail < 1 and current_status == "ACTIVE":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "DRAFT")
                        log_row("STATUS_TO_DRAFT" if ok else "WARN", "IN", "IN", product_id=pid, sku=custom_sku, delta=str(avail),
                                message=f"handle={handle}", title=ptitle, before="ACTIVE", after="DRAFT" if ok else current_status)
                    elif avail >= 1 and current_status == "DRAFT":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "ACTIVE")
                        log_row("STATUS_TO_ACTIVE" if ok else "WARN", "IN", "IN", product_id=pid, sku=custom_sku, delta=str(avail),
                                message=f"handle={handle}", title=ptitle, before="DRAFT", after="ACTIVE" if ok else current_status)

                # RESTOCK info log (when we saw negative before and now 0)
                if prev is not None and prev < 0 and avail == 0:
                    log_row("RESTOCK", "IN", "IN", product_id=pid, sku=custom_sku, delta=str(+(-prev)),
                            message=f"avail {prev}->{avail} (+{(-prev)})", title=ptitle, before=str(prev), after=str(avail))

                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            save_json(IN_LAST_SEEN, {k:int(v) for k,v in last_seen.items()})
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
                sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            else:
                break

# -----------------------------
# USA: Scan & mirror to India
# -----------------------------
def scan_usa_and_mirror_to_india(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(US_LAST_SEEN, {})
    index = load_json(IN_SKU_INDEX, {})

    for handle in US_COLLECTIONS:
        cursor = None
        while True:
            data = gql(US_DOMAIN, US_TOKEN, QUERY_COLLECTION_PAGE_US, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll: break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})

            for p in prods:
                for v in ((p.get("variants") or {}).get("nodes") or []):
                    vgid = v.get("id") or ""
                    vid = gid_num(vgid)
                    qty = int(v.get("inventoryQuantity") or 0)
                    title = (p.get("title") or "")  # parent product title
                    # From parent product metafield
                    raw_custom_sku = (((v.get("product") or {}).get("customSku") or {}).get("value") or "").strip()
                    sku_key = normalize_sku(raw_custom_sku)

                    prev = _as_int_or_none(last_seen.get(vid))
                    if prev is None:
                        last_seen[vid] = qty
                    else:
                        delta = qty - int(prev)
                        if delta != 0:
                            # Increases ignored unless explicitly allowed
                            if delta > 0 and not MIRROR_US_INCREASES:
                                log_row("IGNORED_INCREASE", "US→IN", "US", variant_id=vid, sku=raw_custom_sku,
                                        delta=str(delta), message="US qty increase; mirroring disabled", title=title, before=str(prev), after=str(qty))
                            else:
                                # delta < 0 (typical), mirror to India
                                if not read_only and sku_key and sku_key in index:
                                    in_item_id = int(index[sku_key]["item_id"])
                                    try:
                                        rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, in_item_id, int(IN_LOCATION_ID), int(delta))
                                        log_row("APPLIED_DELTA", "US→IN", "IN", variant_id=vid, sku=raw_custom_sku, delta=str(delta),
                                                message=f"Adjusted IN inventory_item_id={in_item_id} by {delta} (via EXACT index)",
                                                title=title, before=str(prev), after=str(qty))
                                        time.sleep(MUTATION_SLEEP_SEC)
                                        # Optional immediate bump+clamp
                                        if INSTANT_BUMP_AFTER_US_DELTA:
                                            # synthesize a local bump+clamp without waiting for IN pass
                                            # (we cannot cheaply recompute product availability exactly here without another GQL fetch,
                                            # so we leave it to IN pass for accuracy unless you want an extra query here)
                                            pass
                                    except Exception as e:
                                        log_row("WARN", "US→IN", "IN", variant_id=vid, sku=raw_custom_sku, delta=str(delta),
                                                message=f"ERROR_APPLYING_DELTA: {e}", title=title)
                                else:
                                    note = "WARN_NO_SKU" if not sku_key else "WARN_SKU_NOT_IN_INDEX"
                                    log_row(note, "US→IN", "US", variant_id=vid, sku=raw_custom_sku, delta=str(delta),
                                            message="Cannot mirror to India", title=title, before=str(prev), after=str(qty))
                            last_seen[vid] = qty

                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            save_json(US_LAST_SEEN, {k:int(v) for k,v in last_seen.items()})
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
                sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            else:
                break

# -----------------------------
# Orchestration
# -----------------------------
from threading import Thread, Lock
run_lock = Lock()
is_running = False

def run_cycle():
    global is_running
    with run_lock:
        if is_running:
            return "busy"
        is_running = True
    try:
        # Always (re)build India index first
        build_india_index()
        # Decide first-cycle
        state = load_json(STATE_PATH, {"initialized": False})
        in_seen = load_json(IN_LAST_SEEN, {})
        us_seen = load_json(US_LAST_SEEN, {})
        first_cycle = (not state.get("initialized", False)) and (len(in_seen) == 0 and len(us_seen) == 0)

        # India pass
        scan_india_and_update(read_only=first_cycle)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

        # USA pass
        scan_usa_and_mirror_to_india(read_only=first_cycle)

        if first_cycle:
            state["initialized"] = True
            save_json(STATE_PATH, state)
            log_row("FIRST_CYCLE_DONE", "SCHED", "BOTH", message="Baselines learned; future cycles will apply deltas")

        return "ok"
    finally:
        with run_lock:
            is_running = False

# -----------------------------
# Flask app
# -----------------------------
app = Flask(__name__)

def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp

@app.route("/health", methods=["GET"])
def health():
    return "ok", 200

@app.route("/diag", methods=["GET"])
def diag():
    try:
        in_seen = load_json(IN_LAST_SEEN, {})
        us_seen = load_json(US_LAST_SEEN, {})
        index = load_json(IN_SKU_INDEX, {})
        payload = {
            "api_version": API_VERSION,
            "data_dir": DATA_DIR,
            "scheduler_enabled": ENABLE_SCHEDULER,
            "run_every_min": RUN_EVERY_MIN,
            "in_domain": IN_DOMAIN,
            "us_domain": US_DOMAIN,
            "in_collections": IN_COLLECTIONS,
            "us_collections": US_COLLECTIONS,
            "in_last_seen_count": len(in_seen),
            "us_last_seen_count": len(us_seen),
            "index_entries": len(index),
            "use_product_custom_sku": USE_PRODUCT_CUSTOM_SKU,
            "only_active_for_mapping": ONLY_ACTIVE_FOR_MAPPING,
            "mirror_us_increases": MIRROR_US_INCREASES,
            "clamp_avail_to_zero": CLAMP_AVAIL_TO_ZERO,
        }
        return _cors(jsonify(payload)), 200
    except Exception as e:
        return _cors(make_response((f"diag error: {e}", 500)))

@app.route("/run", methods=["POST"])
def run_now():
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))
    status = run_cycle()
    return _cors(jsonify({"status": status})), 200 if status == "ok" else 409

def scheduler_loop():
    if RUN_EVERY_MIN <= 0:
        return
    while True:
        try:
            run_cycle()
        except Exception as e:
            log_row("WARN", "SCHED", "BOTH", message=str(e))
        time.sleep(max(1, RUN_EVERY_MIN) * 60)

if ENABLE_SCHEDULER:
    Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    print(f"[BOOT] Dual sync on port {PORT} | API {API_VERSION}")
    print(f"[CFG] IN={IN_DOMAIN} (loc {IN_LOCATION_ID}) | US={US_DOMAIN} (loc {US_LOCATION_ID}) | DATA_DIR={DATA_DIR}")
    run_simple("0.0.0.0", PORT, app)
