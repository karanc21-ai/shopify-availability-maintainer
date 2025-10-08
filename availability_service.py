#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dual-site availability sync — exact product.custom.sku mapping, clamp-to-zero, robust metafields.

What it does (high level):
- First-ever full cycle is READ-ONLY (learn last_seen; no writes).
- India scan (per collection handle):
    * Compute product availability = sum(variant.inventoryQuantity of counted variants).
    * If availability < 0 → count sales (delta vs last_seen), then clamp to 0 (adjust inventory up),
      set last_seen = 0.
    * If availability dropped (but still >=0) → bump sales_total by the drop.
    * If availability increased → log RESTOCK and set last_seen = new.
    * Update badges/delivery metafields each cycle.
    * SPECIAL collection: if avail < 1 → DRAFT; if avail >= 1 → ACTIVE.
- USA scan (per collection handle):
    * Look at variant qty deltas. For delta<0 → find INDIA product by exact product.custom.sku,
      adjust India inventory by that negative delta (mirror sale). For delta>0 → IGNORED unless MIRROR_US_INCREASES=1.
- India SKU Index:
    * Exact map of normalized custom.sku -> {inventory_item_id (first counted variant), product info}.
      Rebuilt each India scan to stay fresh. Used by US→IN mirroring & clamp-to-zero.

Notes:
- Sleeps are in ms (except MUTATION_SLEEP_SEC).
- Types for metafields are resolved from definitions; tolerant fallback avoids crashes.
"""

import os, sys, time, json, csv, random
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import requests
from flask import Flask, request, jsonify, make_response

# =========================
# Config & pacing
# =========================
API_VERSION = os.getenv("API_VERSION", "2024-10").strip()
PIXEL_SHARED_SECRET = os.getenv("PIXEL_SHARED_SECRET", "").strip()
if not PIXEL_SHARED_SECRET:
    raise SystemExit("PIXEL_SHARED_SECRET required")

RUN_EVERY_MIN = int(os.getenv("RUN_EVERY_MIN", "5"))
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "0") == "1"

SLEEP_BETWEEN_PRODUCTS_MS = int(os.getenv("SLEEP_BETWEEN_PRODUCTS_MS", "80"))
SLEEP_BETWEEN_PAGES_MS    = int(os.getenv("SLEEP_BETWEEN_PAGES_MS", "250"))
SLEEP_BETWEEN_SHOPS_MS    = int(os.getenv("SLEEP_BETWEEN_SHOPS_MS", "500"))
MUTATION_SLEEP_SEC        = float(os.getenv("MUTATION_SLEEP_SEC", "0.25"))

# India (metafields + receives inventory deltas)
IN_DOMAIN = os.getenv("IN_DOMAIN", "").strip()
IN_TOKEN  = os.getenv("IN_TOKEN", "").strip()
IN_LOCATION_ID = os.getenv("IN_LOCATION_ID", "").strip()
IN_COLLECTIONS = [x.strip() for x in os.getenv("IN_COLLECTION_HANDLES","").split(",") if x.strip()]
IN_INCLUDE_UNTRACKED = os.getenv("IN_INCLUDE_UNTRACKED", "0") == "1"
IN_CHANGE_STATUS = os.getenv("IN_CHANGE_STATUS", "1") == "1"  # allow status flip for special collection

# USA (source of deltas)
US_DOMAIN = os.getenv("US_DOMAIN", "").strip()
US_TOKEN  = os.getenv("US_TOKEN", "").strip()
US_LOCATION_ID = os.getenv("US_LOCATION_ID", "").strip()  # not directly used, kept for symmetry
US_COLLECTIONS = [x.strip() for x in os.getenv("US_COLLECTION_HANDLES","").split(",") if x.strip()]

# Behavior flags
USE_PRODUCT_CUSTOM_SKU = os.getenv("USE_PRODUCT_CUSTOM_SKU", "1") == "1"  # exact mapping by product metafield custom.sku
ONLY_ACTIVE_FOR_MAPPING = os.getenv("ONLY_ACTIVE_FOR_MAPPING", "1") == "1" # when building index, only active products count
MIRROR_US_INCREASES = os.getenv("MIRROR_US_INCREASES", "0") == "1"        # default: ignore +ve changes on US
CLAMP_AVAILABILITY_TO_ZERO = os.getenv("CLAMP_AVAILABILITY_TO_ZERO", "1") == "1"

# Metafields (India)
MF_NAMESPACE = os.getenv("MF_NAMESPACE", "custom")
MF_BADGES_KEY = os.getenv("MF_BADGES_KEY", "badges")
MF_DELIVERY_KEY = os.getenv("MF_DELIVERY_KEY", "delivery_time")
KEY_SALES = os.getenv("KEY_SALES", "sales_total")
KEY_DATES = os.getenv("KEY_DATES", "sales_dates")

# Badges/Delivery labels
BADGE_READY    = os.getenv("BADGE_READY", "Ready To Ship")
DELIVERY_READY = os.getenv("DELIVERY_READY", "2-5 Days Across India")
DELIVERY_MTO   = os.getenv("DELIVERY_MTO", "12-15 Days Across India")

# Special collection (status flip)
SPECIAL_STATUS_HANDLE = os.getenv(
    "SPECIAL_STATUS_HANDLE",
    "budget-gold-plated-jewelry-premium-jadau-ornaments-on-mix-metal-base"
).strip()

# Render / persistence
DATA_DIR = os.getenv("DATA_DIR", ".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_TO_STDOUT = os.getenv("LOG_TO_STDOUT", "1") == "1"

def p(*parts):  # path helper
    return os.path.join(DATA_DIR, *parts)

PORT = int(os.getenv("PORT", os.getenv("PORT", "10000")))

# State
IN_LAST_SEEN  = p("in_last_seen.json")   # product_id(num) -> last_seen_availability (clamped >=0)
US_LAST_SEEN  = p("us_last_seen.json")   # variant_id(num)  -> last_seen_qty
STATE_PATH    = p("dual_state.json")     # {"initialized": bool}
LOG_CSV       = p("dual_sync_log.csv")
LOG_JSONL     = p("dual_sync_log.jsonl")
IN_SKU_INDEX  = p("in_sku_index.json")   # sku_norm -> {inv_item_id, pid_num, product_gid, title}

# =========================
# HTTP helpers
# =========================
def hdr(token: str) -> Dict[str, str]:
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json", "Accept": "application/json"}

_last_query_payload: Optional[dict] = None

def gql(domain: str, token: str, query: str, variables: dict = None) -> dict:
    global _last_query_payload
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    payload = {"query": query, "variables": (variables or {})}
    _last_query_payload = payload
    for attempt in range(1, 8):
        try:
            r = requests.post(url, headers=hdr(token), json=payload, timeout=60)
        except Exception as e:
            time.sleep(min(10.0, 0.4 * (2 ** (attempt-1))) + random.uniform(0, 0.25))
            if attempt >= 7: raise
            continue
        if r.status_code in (502, 503, 504, 520, 522, 524, 429):
            time.sleep(min(10.0, 0.4 * (2 ** (attempt-1))) + random.uniform(0, 0.25))
            continue
        if r.status_code != 200:
            raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")
        data = r.json()
        if data.get("errors"):
            # throttle/5xx or syntax → backoff; otherwise raise
            if any(((e.get("extensions") or {}).get("code","").upper() == "THROTTLED") for e in data["errors"]):
                time.sleep(min(10.0, 0.4 * (2 ** (attempt-1))) + random.uniform(0, 0.25))
                continue
            # surface query & vars for debugging
            log_row("⚠️","GQL","ERR","",message=json.dumps(data["errors"]))
            log_row("⚙️","GQL","LAST_QUERY","",message=json.dumps(payload))
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]
    raise RuntimeError("GraphQL throttled/5xx repeatedly")

def rest_adjust_inventory(domain: str, token: str, inventory_item_id: int, location_id: int, delta: int) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/adjust.json"
    payload = {"inventory_item_id": int(inventory_item_id), "location_id": int(location_id), "available_adjustment": int(delta)}
    for attempt in range(1, 6):
        r = requests.post(url, headers=hdr(token), json=payload, timeout=30)
        if r.status_code in (429, 502, 503, 504, 520, 522, 524):
            time.sleep(min(8.0, 0.3 * (2 ** (attempt-1))) + random.uniform(0, 0.25))
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"REST adjust {r.status_code}: {r.text}")
        return

# =========================
# Utils & logging
# =========================
def now_ist():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

def now_ist_str():
    return now_ist().strftime("%Y-%m-%d %H:%M:%S %z")

def today_ist_str():
    return now_ist().date().isoformat()

def sleep_ms(ms: int):
    time.sleep(max(0, ms) / 1000.0)

def normalize_sku(s: str) -> str:
    return (s or "").strip().lower()

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

def log_row(phase: str, shop: str, note: str, product_id="", variant_id="", sku="", delta="", message="", title=""):
    ensure_log_header()
    row = [now_ist_str(), phase, shop, note, product_id, variant_id, sku, delta, message]
    if LOG_TO_STDOUT:
        # emoji prefix for readability (phase often contains an emoji already)
        pretty = f"{' '}{phase} {shop} {note} [SKU {sku}] pid={product_id} vid={variant_id} {delta} “{title}” — {message}".strip()
        print(pretty, flush=True)
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)
    with open(LOG_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "ts": row[0], "phase": phase, "shop": shop, "note": note,
            "product_id": str(product_id or ""), "variant_id": str(variant_id or ""),
            "sku": str(sku or ""), "delta": str(delta or ""), "message": str(message or ""),
            "title": title
        })+"\n")

def _as_int_or_none(v):
    try:
        if v is None or v == "":
            return None
        if isinstance(v, (int, float)):
            return int(v)
        return int(str(v).strip())
    except Exception:
        return None

# =========================
# GQL queries (NO 'query:' argument on products)
# =========================
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
        skuMeta: metafield(namespace: "custom", key: "sku"){ value }
        badges:  metafield(namespace: "custom", key: "badges"){ id value type }
        dtime:   metafield(namespace: "custom", key: "delivery_time"){ id value type }
        salesTotal:  metafield(namespace: "custom", key: "sales_total"){ id value type }
        salesDates:  metafield(namespace: "custom", key: "sales_dates"){ id value type }
      }
    }
  }
}
"""

QUERY_COLLECTION_PAGE_US = """
query ($handle:String!, $cursor:String) {
  collectionByHandle(handle:$handle){
    products(first: 60, after:$cursor){
      pageInfo{ hasNextPage endCursor }
      nodes{
        id title
        skuMeta: metafield(namespace: "custom", key: "sku"){ value }
        variants(first: 100){
          nodes{
            id sku inventoryQuantity
            inventoryItem{ id }
          }
        }
      }
    }
  }
}
"""

# =========================
# Metafield definition resolver (tolerant wrapper)
# =========================
QUERY_MF_DEFINITION = """
query ($ownerType: MetafieldOwnerType!, $namespace: String!, $key: String!) {
  metafieldDefinitions(first: 1, ownerType: $ownerType, namespace: $namespace, key: $key) {
    edges { node { id name type { name } } }
  }
}
"""

_MF_DEF_CACHE: Dict[Tuple[str,str,str], str] = {}

def _get_mf_def_type(domain: str, token: str, owner_type: str, ns: str, key: str) -> str:
    ck = (owner_type, ns, key)
    if ck in _MF_DEF_CACHE:
        return _MF_DEF_CACHE[ck]
    data = gql(domain, token, QUERY_MF_DEFINITION, {
        "ownerType": owner_type, "namespace": ns, "key": key
    })
    edges = (((data.get("metafieldDefinitions") or {}).get("edges")) or [])
    tname = ((((edges[0] or {}).get("node") or {}).get("type") or {}).get("name")) if edges else None
    tname = (tname or "single_line_text_field").strip()
    _MF_DEF_CACHE[ck] = tname
    return tname

def get_mf_def_type(*args):
    """
    Preferred: get_mf_def_type(domain, token, owner_type, namespace, key)
    If called without key (legacy), default to badges key in our namespace & log a warning.
    """
    if len(args) == 5:
        return _get_mf_def_type(*args)
    elif len(args) == 4:
        domain, token, owner_type, ns = args
        assumed_key = MF_BADGES_KEY if ns == MF_NAMESPACE else MF_BADGES_KEY
        log_row("⚠️","SCHED","BOTH","WARN",message=f"get_mf_def_type called without key; default ns={ns} key={assumed_key}")
        return _get_mf_def_type(domain, token, owner_type, ns, assumed_key)
    else:
        log_row("⚠️","SCHED","BOTH","WARN",message=f"get_mf_def_type bad arity={len(args)}; returning single_line_text_field")
        return "single_line_text_field"

# =========================
# Helpers: availability, metafields, status
# =========================
def compute_product_availability(variants: List[dict], include_untracked: bool) -> int:
    total = 0
    counted = False
    first_counted_inv_item_id = None
    for v in variants:
        tracked = bool(((v.get("inventoryItem") or {}).get("tracked")))
        qty = int(v.get("inventoryQuantity") or 0)
        if include_untracked or tracked:
            counted = True
            total += qty
            if first_counted_inv_item_id is None:
                inv_item_gid = ((v.get("inventoryItem") or {}).get("id") or "")
                try:
                    first_counted_inv_item_id = int(gid_num(inv_item_gid))
                except Exception:
                    first_counted_inv_item_id = None
    return (total if counted else 0), first_counted_inv_item_id

def effective_labels(avail_nonneg: int) -> Tuple[str,str,str]:
    if avail_nonneg > 0:
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
        log_row("⚠️","IN","WARN",product_id=gid_num(product_gid),message=str(errs))
        return False
    time.sleep(MUTATION_SLEEP_SEC)
    return True

def set_product_metafields(domain:str, token:str, product_gid:str,
                           badges_node:dict, dtime_node:dict,
                           target_badge:str, target_delivery:str):
    # Resolve types from definitions (tolerant)
    badges_type = (badges_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_BADGES_KEY)
    delivery_type= (dtime_node  or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_DELIVERY_KEY)

    # Force scalar if store expects scalar (your store has badges as single_line_text_field)
    mf_inputs = []
    if target_badge:
        mf_inputs.append({"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":MF_BADGES_KEY,"type":badges_type,"value":target_badge})
    else:
        # delete badge if present
        if (badges_node or {}).get("id"):
            delm = "mutation($id:ID!){ metafieldDelete(input:{id:$id}){ userErrors{message} } }"
            gql(domain, token, delm, {"id": badges_node["id"]})

    mf_inputs.append({"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":MF_DELIVERY_KEY,"type":delivery_type,"value":target_delivery})

    mutation = "mutation($mfs:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$mfs){ userErrors{ field message } } }"
    data = gql(domain, token, mutation, {"mfs": mf_inputs})
    errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
    if errs:
        log_row("⚠️","IN","WARN",product_id=gid_num(product_gid),message=f"metafieldsSet errors: {errs}")

def bump_sales(domain:str, token:str, product_gid:str, sales_total_node:dict, sales_dates_node:dict, sold:int, today:str):
    st_type = (sales_total_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, KEY_SALES) or "number_integer"
    sd_type = (sales_dates_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, KEY_DATES) or "list.date"
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
        log_row("⚠️","IN","WARN",product_id=gid_num(product_gid),message=f"sales metafieldsSet errors: {errs}")

# =========================
# India: Build SKU index (exact product.custom.sku) & Scan
# =========================
def build_india_sku_index() -> Dict[str, dict]:
    index: Dict[str, dict] = {}
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
                sku_val = ((p.get("skuMeta") or {}).get("value") or "").strip()
                if not sku_val:
                    continue
                sku_norm = normalize_sku(sku_val)
                variants = ((p.get("variants") or {}).get("nodes") or [])
                avail, first_inv_item_id = compute_product_availability(variants, IN_INCLUDE_UNTRACKED)
                if first_inv_item_id:
                    index[sku_norm] = {
                        "inv_item_id": first_inv_item_id,
                        "product_gid": p["id"],
                        "pid_num": gid_num(p["id"]),
                        "title": p.get("title",""),
                        "handle": handle,
                        "avail": avail
                    }
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break
    save_json(IN_SKU_INDEX, index)
    log_row("🗂️","IN","INDEX_BUILT", message=f"entries={len(index)}")
    return index

def scan_india_and_update(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(IN_LAST_SEEN, {})
    today = today_ist_str()
    index = build_india_sku_index()  # fresh each pass

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
                pid = gid_num(p["id"])
                title = p.get("title","")
                variants = ((p.get("variants") or {}).get("nodes") or [])
                avail_raw, any_inv_item_id = compute_product_availability(variants, IN_INCLUDE_UNTRACKED)
                sku_val = ((p.get("skuMeta") or {}).get("value") or "").strip()
                sku_norm = normalize_sku(sku_val)

                prev = _as_int_or_none(last_seen.get(pid))
                if prev is None:
                    # first time → learn non-negative last_seen baseline
                    last_seen[pid] = max(0, int(avail_raw))
                    log_row("📝","IN","FIRST_SEEN", product_id=pid, sku=sku_val, message=f"avail={avail_raw}", title=title)
                    # still proceed to write metafields (unless global first cycle)
                else:
                    # compare vs last_seen (which we keep non-negative only)
                    new_avail = int(avail_raw)
                    if new_avail < 0:
                        # sales happened beyond on-hand
                        sold = (-new_avail) + prev if prev == 0 else (prev - new_avail)
                        # normalize: if prev>=0 and new<0: sold = prev - new_avail (e.g., 0 -> -3 => 3)
                        sold = prev - new_avail
                        if not read_only and sold > 0:
                            bump_sales(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, sold, today)
                            log_row("🧾➖","IN","SALES_BUMP", product_id=pid, sku=sku_val, delta=f"-{sold}", message=f"avail {prev}->{new_avail} (sold={sold})", title=title)
                        # clamp to zero in Shopify (raise stock up by +abs(new))
                        if CLAMP_AVAILABILITY_TO_ZERO and not read_only and any_inv_item_id:
                            try:
                                rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, any_inv_item_id, int(IN_LOCATION_ID), abs(new_avail))
                                log_row("🧰0️⃣","IN","CLAMP_TO_ZERO", product_id=pid, sku=sku_val, delta=f"+{abs(new_avail)}",
                                        message=f"Raised availability to 0 on inventory_item_id={any_inv_item_id}", title=title)
                            except Exception as e:
                                log_row("⚠️","IN","WARN", product_id=pid, sku=sku_val, message=str(e), title=title)
                        # last_seen becomes 0
                        last_seen[pid] = 0
                        # for metafields/status decisions treat as 0 now
                        new_avail = 0
                    elif new_avail < prev:
                        sold = prev - new_avail
                        if not read_only and sold > 0:
                            bump_sales(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, sold, today)
                            log_row("🧾➖","IN","SALES_BUMP", product_id=pid, sku=sku_val, delta=f"-{sold}", message=f"avail {prev}->{new_avail} (sold={sold})", title=title)
                        last_seen[pid] = max(0, new_avail)
                    elif new_avail > prev:
                        # RESTOCK
                        last_seen[pid] = new_avail
                        log_row("📦➕","IN","RESTOCK", product_id=pid, sku=sku_val, delta=f"{new_avail - prev}", message=f"avail {prev}->{new_avail} (+{new_avail - prev})", title=title)
                    else:
                        # unchanged
                        pass

                # Compute effective non-negative availability for labels/status
                effective_avail = max(0, int(avail_raw))
                target_status, target_badge, target_delivery = effective_labels(effective_avail)

                if not read_only:
                    # metafields
                    set_product_metafields(IN_DOMAIN, IN_TOKEN, p["id"], p.get("badges") or {}, p.get("dtime") or {}, target_badge, target_delivery)

                    # SPECIAL STATUS RULE
                    if IN_CHANGE_STATUS and (handle == SPECIAL_STATUS_HANDLE):
                        current_status = (p.get("status") or "").upper()
                        if effective_avail < 1 and current_status == "ACTIVE":
                            ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "DRAFT")
                            log_row("🛑","IN","STATUS_TO_DRAFT" if ok else "STATUS_TO_DRAFT_FAILED",
                                    product_id=pid, sku=sku_val, delta=str(effective_avail), message=f"handle={handle}", title=title)
                        elif effective_avail >= 1 and current_status == "DRAFT":
                            ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "ACTIVE")
                            log_row("✅","IN","STATUS_TO_ACTIVE" if ok else "STATUS_TO_ACTIVE_FAILED",
                                    product_id=pid, sku=sku_val, delta=str(effective_avail), message=f"handle={handle}", title=title)

                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            save_json(IN_LAST_SEEN, {k:int(v) for k,v in last_seen.items()})
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

# =========================
# USA scan → mirror negative deltas to India by EXACT index
# =========================
def scan_usa_and_mirror_to_india(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(US_LAST_SEEN, {})
    index: Dict[str, dict] = load_json(IN_SKU_INDEX, {})  # use built index

    for handle in US_COLLECTIONS:
        cursor = None
        while True:
            data = gql(US_DOMAIN, US_TOKEN, QUERY_COLLECTION_PAGE_US, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                title = p.get("title","")
                sku_val = ((p.get("skuMeta") or {}).get("value") or "").strip()
                sku_norm = normalize_sku(sku_val)
                for v in ((p.get("variants") or {}).get("nodes") or []):
                    vgid = v.get("id"); vid = gid_num(vgid)
                    qty = int(v.get("inventoryQuantity") or 0)

                    prev = _as_int_or_none(last_seen.get(vid))
                    if prev is None:
                        last_seen[vid] = qty
                        log_row("📝","US","FIRST_SEEN", variant_id=vid, sku=sku_val, message=f"qty={qty}", title=title)
                    else:
                        delta = qty - int(prev)
                        if delta == 0:
                            pass
                        elif delta > 0:
                            # Qty increase on US
                            if MIRROR_US_INCREASES and not read_only and sku_norm in index:
                                inv_item_id = index[sku_norm]["inv_item_id"]
                                try:
                                    rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id, int(IN_LOCATION_ID), delta)
                                    log_row("🔁","US→IN","APPLIED_DELTA", variant_id=vid, sku=sku_val, delta=str(delta),
                                            message=f"Adjusted IN inventory_item_id={inv_item_id} by {delta} (via EXACT index)", title=title)
                                except Exception as e:
                                    log_row("⚠️","US→IN","ERROR_APPLYING_DELTA", variant_id=vid, sku=sku_val, delta=str(delta), message=str(e), title=title)
                            else:
                                log_row("🙅‍♂️➕","US→IN","IGNORED_INCREASE", variant_id=vid, sku=sku_val, delta=str(delta),
                                        message="US qty increase; mirroring disabled", title=title)
                        else:
                            # delta < 0 → mirror to India if exact SKU found
                            if not read_only and sku_norm in index:
                                inv_item_id = index[sku_norm]["inv_item_id"]
                                try:
                                    rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id, int(IN_LOCATION_ID), delta)
                                    log_row("🔁","US→IN","APPLIED_DELTA", variant_id=vid, sku=sku_val, delta=str(delta),
                                            message=f"Adjusted IN inventory_item_id={inv_item_id} by {delta} (via EXACT index)", title=title)
                                except Exception as e:
                                    log_row("⚠️","US→IN","ERROR_APPLYING_DELTA", variant_id=vid, sku=sku_val, delta=str(delta), message=str(e), title=title)
                            else:
                                log_row("⚠️","US→IN","WARN_SKU_NOT_FOUND_IN_INDIA", variant_id=vid, sku=sku_val, delta=str(delta),
                                        message="US change; no matching product.custom.sku in India index", title=title)
                        last_seen[vid] = qty
                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            save_json(US_LAST_SEEN, {k:int(v) for k,v in last_seen.items()})
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

# =========================
# First-cycle guard & scheduler
# =========================
from threading import Thread, Lock
run_lock = Lock()
is_running = False

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

def run_cycle():
    global is_running
    with run_lock:
        if is_running:
            return "busy"
        is_running = True
    try:
        # first-ever cycle detection
        state = load_state()
        in_seen = load_json(IN_LAST_SEEN, {})
        us_seen = load_json(US_LAST_SEEN, {})
        first_cycle = (not state.get("initialized", False)) and (len(in_seen) == 0 and len(us_seen) == 0)

        # India first
        scan_india_and_update(read_only=first_cycle)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

        # USA second
        scan_usa_and_mirror_to_india(read_only=first_cycle)

        if first_cycle:
            state["initialized"] = True
            save_state(state)
            log_row("🧭","SCHED","FIRST_CYCLE_DONE", message="Baselines learned; future cycles will apply deltas")

        return "ok"
    except Exception as e:
        log_row("⚠️","SCHED","WARN", message=str(e))
        return "error"
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
            log_row("⚠️","SCHED","ERROR", message=str(e))
        time.sleep(max(1, RUN_EVERY_MIN) * 60)

# =========================
# Flask app
# =========================
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
    return jsonify({
        "api_version": API_VERSION,
        "data_dir": DATA_DIR,
        "in_domain": IN_DOMAIN,
        "us_domain": US_DOMAIN,
        "in_collections": IN_COLLECTIONS,
        "us_collections": US_COLLECTIONS,
        "run_every_min": RUN_EVERY_MIN,
        "scheduler_enabled": ENABLE_SCHEDULER,
        "only_active_for_mapping": ONLY_ACTIVE_FOR_MAPPING,
        "use_product_custom_sku": USE_PRODUCT_CUSTOM_SKU,
        "mirror_us_increases": MIRROR_US_INCREASES,
        "clamp_avail_to_zero": CLAMP_AVAILABILITY_TO_ZERO,
        "in_last_seen_count": len(load_json(IN_LAST_SEEN, {})),
        "us_last_seen_count": len(load_json(US_LAST_SEEN, {})),
        "index_entries": len(load_json(IN_SKU_INDEX, {})),
    }), 200

@app.route("/run", methods=["POST"])
def run_now():
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))
    status = run_cycle()
    return _cors(jsonify({"status": status})), 200 if status == "ok" else 409

# Start scheduler thread if enabled (do NOT use in Render Web Service; use Worker)
if ENABLE_SCHEDULER:
    Thread(target=scheduler_loop, daemon=True).start()

# WSGI entrypoint
if __name__ == "__main__":
    print(f"[BOOT] Dual sync on port {PORT} | API {API_VERSION}")
    print(f"[CFG] IN={IN_DOMAIN} (loc {IN_LOCATION_ID}) | US={US_DOMAIN} (loc {US_LOCATION_ID}) | DATA_DIR={DATA_DIR}")
    from werkzeug.serving import run_simple
    run_simple("0.0.0.0", PORT, app)
