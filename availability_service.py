#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dual-site availability sync (Render-ready)
- India (IN):
  * Compute product availability from variants (tracked variants by default).
  * If new_avail < last_seen: bump sales_total & sales_dates.
  * If new_avail < 0 and CLAMP_AVAIL_TO_ZERO=1: raise stock to 0 and set last_seen=0 immediately.
  * Always set badges/delivery based on availability (>0 => Ready To Ship; else MTO).
  * SPECIAL_STATUS_HANDLE: ACTIVE<->DRAFT symmetric flips by availability threshold (<1 => DRAFT, >=1 => ACTIVE).
  * last_seen is NEVER negative (we clamp; first cycle is read-only so no sales counted).

- USA (US):
  * Track per-variant quantity deltas.
  * On delta < 0: mirror to India by EXACT product.custom.sku using a prebuilt IN index (sku -> inventory_item_id).
  * On delta > 0: do nothing (log IGNORED_INCREASE) unless MIRROR_US_INCREASES=1.

- Metafields:
  * Auto-detect real definition types (badges can be list.single_line_text_field etc.).
  * Write values in the correct shape (JSON array for list.*).
  * sales_total (int) and sales_dates (date or list.date) handled safely.

- Persistence:
  * DATA_DIR holds: in_last_seen.json, us_last_seen.json, in_sku_index.json, dual_sync_log.csv, dual_sync_log.jsonl, dual_state.json
  * Attach a Persistent Disk at /data and set DATA_DIR=/data in BOTH services (web + worker).

- Logs:
  * Human line (with emoji) + CSV + JSONL per event.

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

SLEEP_BETWEEN_PRODUCTS_MS = int(os.getenv("SLEEP_BETWEEN_PRODUCTS_MS", "100"))
SLEEP_BETWEEN_PAGES_MS = int(os.getenv("SLEEP_BETWEEN_PAGES_MS", "250"))
SLEEP_BETWEEN_SHOPS_MS = int(os.getenv("SLEEP_BETWEEN_SHOPS_MS", "500"))
MUTATION_SLEEP_SEC = float(os.getenv("MUTATION_SLEEP_SEC", "0.25"))

# India (metafields + receives inventory deltas)
IN_DOMAIN = os.getenv("IN_DOMAIN", "").strip()
IN_TOKEN  = os.getenv("IN_TOKEN", "").strip()
IN_LOCATION_ID = os.getenv("IN_LOCATION_ID", "").strip()
IN_COLLECTIONS = [x.strip() for x in os.getenv("IN_COLLECTION_HANDLES","").split(",") if x.strip()]
IN_INCLUDE_UNTRACKED = os.getenv("IN_INCLUDE_UNTRACKED", "0") == "1"
IN_CHANGE_STATUS = os.getenv("IN_CHANGE_STATUS", "0") == "1"  # must be 1 to flip status

# USA (source of variant-level deltas)
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
MF_PRODUCT_SKU_KEY = os.getenv("MF_PRODUCT_SKU_KEY", "sku")  # product-level unique SKU

BADGE_READY    = os.getenv("BADGE_READY", "Ready To Ship")
DELIVERY_READY = os.getenv("DELIVERY_READY", "2-5 Days Across India")
DELIVERY_MTO   = os.getenv("DELIVERY_MTO", "12-15 Days Across India")

# Special collection where availability controls status (symmetric rule)
SPECIAL_STATUS_HANDLE = os.getenv(
    "SPECIAL_STATUS_HANDLE",
    "budget-gold-plated-jewelry-premium-jadau-ornaments-on-mix-metal-base"
).strip()

# Behavior toggles
USE_PRODUCT_CUSTOM_SKU = os.getenv("USE_PRODUCT_CUSTOM_SKU", "1") == "1"
ONLY_ACTIVE_FOR_MAPPING = os.getenv("ONLY_ACTIVE_FOR_MAPPING", "1") == "1"
MIRROR_US_INCREASES = os.getenv("MIRROR_US_INCREASES", "0") == "1"
CLAMP_AVAIL_TO_ZERO = os.getenv("CLAMP_AVAIL_TO_ZERO", "1") == "1"

# Render / persistence
DATA_DIR = os.getenv("DATA_DIR", ".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_TO_STDOUT = os.getenv("LOG_TO_STDOUT", "1") == "1"

def p(*parts):  # path helper within DATA_DIR
    return os.path.join(DATA_DIR, *parts)

PORT = int(os.getenv("PORT", os.getenv("PORT", "10000")))  # Render injects PORT

# Last-seen persistence (first cycle read-only)
IN_LAST_SEEN  = p("in_last_seen.json")   # product_id(num) -> last_seen_availability (NEVER negative)
US_LAST_SEEN  = p("us_last_seen.json")   # variant_id(num)  -> last_seen_qty
IN_SKU_INDEX  = p("in_sku_index.json")   # sku -> {inventory_item_id, product_id, title}
STATE_PATH    = p("dual_state.json")     # {"initialized": bool}
LOG_CSV       = p("dual_sync_log.csv")
LOG_JSONL     = p("dual_sync_log.jsonl")

# =========================
# HTTP helpers
# =========================
def hdr(token: str) -> Dict[str, str]:
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json", "Accept": "application/json"}

def gql(domain: str, token: str, query: str, variables: dict = None) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    log_queries = os.getenv("LOG_GRAPHQL","0") == "1"
    last = {"query": query, "variables": variables or {}}

    for attempt in range(1, 8):
        r = requests.post(url, headers=hdr(token), json={"query": query, "variables": variables or {}}, timeout=60)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(10.0, 0.4 * (2 ** (attempt-1))) + random.uniform(0,0.25))
            continue
        try:
            data = r.json()
        except Exception:
            # Non-JSON body – log and raise
            if log_queries:
                print(f"⚠️ GQL RAW_ERROR body={r.text[:400]}", flush=True)
                print(f"⚙️ GQL LAST_QUERY {json.dumps(last)[:800]}", flush=True)
            raise

        if r.status_code != 200:
            if log_queries:
                print(f"⚠️ GQL HTTP{r.status_code} body={r.text[:400]}", flush=True)
                print(f"⚙️ GQL LAST_QUERY {json.dumps(last)[:800]}", flush=True)
            raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")

        if data.get("errors"):
            if log_queries:
                print(f"⚠️ GQL ERRORS {json.dumps(data['errors'])[:400]}", flush=True)
                print(f"⚙️ GQL LAST_QUERY {json.dumps(last)[:800]}", flush=True)
            # retry on throttled
            if any(((e.get('extensions') or {}).get('code','').upper() == 'THROTTLED') for e in data["errors"]):
                time.sleep(min(10.0, 0.4 * (2 ** (attempt-1))) + random.uniform(0,0.25))
                continue
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]
    raise RuntimeError("GraphQL throttled/5xx repeatedly")


def rest_adjust_inventory(domain: str, token: str, inventory_item_id: int, location_id: int, delta: int) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/adjust.json"
    payload = {"inventory_item_id": int(inventory_item_id), "location_id": int(location_id), "available_adjustment": int(delta)}
    backoffs = [0.2, 0.5, 1.0, 2.0, 3.0]
    for attempt, b in enumerate(backoffs, 1):
        r = requests.post(url, headers=hdr(token), json=payload, timeout=45)
        if r.status_code in (429, 502, 503, 504, 520, 522, 524):
            time.sleep(b + random.uniform(0, 0.25))
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"REST adjust {r.status_code}: {r.text}")
        return

# =========================
# Utils
# =========================
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

def log_event(emoji: str, phase: str, shop: str, note: str,
              product_id:str="", variant_id:str="", sku:str="", delta:str="", message:str="",
              title:str="", before:str="", after:str="", collections:str=""):
    ensure_log_header()
    ts = now_ist_str()
    human = f"{emoji} {phase} {note} [SKU {sku}] pid={product_id} vid={variant_id} {before}→{after} Δ{delta} “{title}” — {message}"
    if LOG_TO_STDOUT:
        print(human, flush=True)
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([ts, phase, shop, note, product_id, variant_id, sku, delta, message])
    try:
        with open(LOG_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "ts": ts, "phase": phase, "shop": shop, "note": note,
                "product_id": product_id, "variant_id": variant_id, "sku": sku,
                "delta": delta, "message": message, "title": title, "before": before, "after": after,
                "collections": collections
            }, ensure_ascii=False) + "\n")
    except Exception:
        pass

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

# =========================
# State (first-cycle flag)
# =========================
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

# =========================
# GraphQL queries
# =========================
QUERY_COLLECTION_PAGE_IN = """
query ($handle:String!, $cursor:String, $onlyActive:Boolean!) {
  collectionByHandle(handle:$handle){
    products(first: 60, after:$cursor, query: $onlyActive ? "status:active" : null){
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
        skuMeta: metafield(namespace: "%(NS)s", key: "%(SKU)s"){ value }
        badges: metafield(namespace: "%(NS)s", key: "%(BK)s"){ id value type }
        dtime:  metafield(namespace: "%(NS)s", key: "%(DK)s"){ id value type }
        salesTotal: metafield(namespace: "%(NS)s", key: "%(KS)s"){ id value type }
        salesDates: metafield(namespace: "%(NS)s", key: "%(KD)s"){ id value type }
      }
    }
  }
}
""".replace("%(NS)s", MF_NAMESPACE)\
   .replace("%(SKU)s", MF_PRODUCT_SKU_KEY)\
   .replace("%(BK)s", MF_BADGES_KEY)\
   .replace("%(DK)s", MF_DELIVERY_KEY)\
   .replace("%(KS)s", KEY_SALES)\
   .replace("%(KD)s", KEY_DATES)

QUERY_COLLECTION_PAGE_US = """
query ($handle:String!, $cursor:String) {
  collectionByHandle(handle:$handle){
    products(first: 60, after:$cursor){
      pageInfo{ hasNextPage endCursor }
      nodes{
        id title
        skuMeta: metafield(namespace: "%(NS)s", key: "%(SKU)s"){ value }
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
""".replace("%(NS)s", MF_NAMESPACE).replace("%(SKU)s", MF_PRODUCT_SKU_KEY)

QUERY_FIND_IN_VARIANT_BY_SKU = """
query ($q:String!){
  productVariants(first: 10, query: $q){
    nodes{
      id sku inventoryQuantity inventoryItem{ id }
      product{ id title }
    }
  }
}
"""

# =========================
# Metafield definitions + formatting
# =========================
_MF_DEF_CACHE = {}  # {(domain,namespace,key): type_name}

QUERY_MF_DEFINITION = """
query MFDef($ownerType: MetafieldOwnerType!, $namespace: String!, $key: String!) {
  metafieldDefinitions(first: 1, ownerType: $ownerType, namespace: $namespace, key: $key) {
    edges { node { id type { name } } }
  }
}
"""

def get_definition_type(domain: str, token: str, namespace: str, key: str, owner_type: str = "PRODUCT") -> str:
    ck = (domain, namespace, key)
    if ck in _MF_DEF_CACHE:
        return _MF_DEF_CACHE[ck]
    data = gql(domain, token, QUERY_MF_DEFINITION, {
        "ownerType": owner_type, "namespace": namespace, "key": key
    })
    edges = (((data.get("metafieldDefinitions") or {}).get("edges")) or [])
    tname = ((((edges or [{}])[0].get("node") or {}).get("type") or {}).get("name")) or ""
    tname = tname.strip() or "single_line_text_field"
    _MF_DEF_CACHE[ck] = tname
    return tname

def resolve_type(domain: str, token: str, namespace: str, key: str, node: dict, fallback: str = "single_line_text_field") -> str:
    ntype = (node or {}).get("type")
    if isinstance(ntype, str) and ntype.strip():
        return ntype.strip()
    return get_definition_type(domain, token, namespace, key) or fallback

def format_value_for_type(val: str, tname: str) -> str:
    if tname.startswith("list."):
        if not val:
            return "[]"
        return json.dumps([val])
    return val or ""

# =========================
# Availability helpers
# =========================
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

# =========================
# Mutations
# =========================
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
        log_event("⚠️", "IN", "IN", "WARN_STATUS", product_id=gid_num(product_gid), message=str(errs))
        return False
    time.sleep(MUTATION_SLEEP_SEC)
    return True

def set_product_metafields_in(domain:str, token:str, product_gid:str, badges_node:dict, dtime_node:dict,
                              target_badge:str, target_delivery:str) -> None:
    btype = resolve_type(domain, token, MF_NAMESPACE, MF_BADGES_KEY, badges_node, "single_line_text_field")
    dtype = resolve_type(domain, token, MF_NAMESPACE, MF_DELIVERY_KEY, dtime_node, "single_line_text_field")

    mf_inputs = []

    # badges
    if target_badge:
        mf_inputs.append({
            "ownerId": product_gid,
            "namespace": MF_NAMESPACE,
            "key": MF_BADGES_KEY,
            "type": btype,
            "value": format_value_for_type(target_badge, btype),
        })
    else:
        if (badges_node or {}).get("id"):
            delm = "mutation($id:ID!){ metafieldDelete(input:{id:$id}){ userErrors{message} } }"
            try:
                gql(domain, token, delm, {"id": badges_node["id"]})
                time.sleep(MUTATION_SLEEP_SEC)
            except Exception as e:
                log_event("⚠️", "IN", "IN", "WARN", message=f"metafieldDelete badges failed: {e}")

    # delivery_time (always set)
    mf_inputs.append({
        "ownerId": product_gid,
        "namespace": MF_NAMESPACE,
        "key": MF_DELIVERY_KEY,
        "type": dtype,
        "value": format_value_for_type(target_delivery, dtype),
    })

    if mf_inputs:
        mutation = "mutation($metafields:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$metafields){ userErrors{ field message } } }"
        data = gql(domain, token, mutation, {"metafields": mf_inputs})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_event("⚠️", "IN", "IN", "WARN", product_id=gid_num(product_gid), message=f"metafieldsSet errors: {errs}")
        time.sleep(MUTATION_SLEEP_SEC)

def bump_sales_in(domain:str, token:str, product_gid:str, sales_total_node:dict, sales_dates_node:dict, sold:int, today:str):
    st_type = resolve_type(domain, token, MF_NAMESPACE, KEY_SALES, sales_total_node, "number_integer")
    sd_type = resolve_type(domain, token, MF_NAMESPACE, KEY_DATES, sales_dates_node, "list.date")

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
        log_event("⚠️", "IN", "IN", "WARN", product_id=gid_num(product_gid), message=f"sales metafieldsSet errors: {errs}")
    time.sleep(MUTATION_SLEEP_SEC)

# =========================
# Index building (India): sku -> inventory_item_id, product_id, title
# =========================
def build_india_sku_index():
    index: Dict[str, Dict[str, Any]] = {}
    only_active = ONLY_ACTIVE_FOR_MAPPING
    for handle in IN_COLLECTIONS:
        cursor = None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN, {"handle": handle, "cursor": cursor, "onlyActive": only_active})
            coll = data.get("collectionByHandle")
            if not coll:
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                pid = gid_num(p["id"])
                title = p.get("title","")
                sku_val = ((p.get("skuMeta") or {}).get("value") or "").strip()
                if not sku_val:
                    continue
                # pick first variant's inventoryItem.id as representative (we adjust any variant; clamping/sales use product level)
                best_inv_item_id = None
                for v in ((p.get("variants") or {}).get("nodes") or []):
                    inv_gid = ((v.get("inventoryItem") or {}).get("id") or "")
                    if inv_gid:
                        best_inv_item_id = int(gid_num(inv_gid))
                        break
                if best_inv_item_id:
                    index[sku_val] = {
                        "inventory_item_id": best_inv_item_id,
                        "product_id": pid,
                        "title": title
                    }
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break
    save_json(IN_SKU_INDEX, index)
    log_event("🗂️", "IN", "IN", "INDEX_BUILT", message=f"entries={len(index)}")

# =========================
# India scan
# =========================
def set_status_and_metafields_for_product(p: dict, handle: str, read_only: bool):
    pid = gid_num(p["id"])
    title = p.get("title","")
    variants = ((p.get("variants") or {}).get("nodes") or [])
    avail = compute_product_availability(variants, IN_INCLUDE_UNTRACKED)

    # last_seen is NEVER negative
    last_seen = load_json(IN_LAST_SEEN, {})
    ls_prev = _as_int_or_none(last_seen.get(pid))
    if ls_prev is None:
        # first time: learn baseline (read-only cycle already handled)
        last_seen[pid] = max(0, int(avail))
        save_json(IN_LAST_SEEN, last_seen)
        return

    # Compute diff vs last_seen (non-negative)
    diff = int(avail) - int(ls_prev)

    # Sales bump for drops
    if not read_only and diff < 0:
        bump_sales_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, -diff, today_ist_str())
        log_event("🧾➖", "IN", "IN", "SALES_BUMP",
                  product_id=pid, sku=((p.get("skuMeta") or {}).get("value") or ""),
                  delta=str(diff), message=f"avail {ls_prev}->{avail} (sold={-diff})", title=title, before=str(ls_prev), after=str(avail))

    # Clamp negatives to zero (and immediately set last_seen to 0)
    if not read_only and CLAMP_AVAIL_TO_ZERO and avail < 0:
        # bring to zero via adjust +|avail|
        rep_inv_id = None
        for v in variants:
            inv_gid = ((v.get("inventoryItem") or {}).get("id") or "")
            if inv_gid:
                rep_inv_id = int(gid_num(inv_gid))
                break
        if rep_inv_id:
            try:
                rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, rep_inv_id, int(IN_LOCATION_ID), +abs(int(avail)))
                log_event("🧰0️⃣", "IN", "IN", "CLAMP_TO_ZERO",
                          product_id=pid, sku=((p.get("skuMeta") or {}).get("value") or ""),
                          delta=f"+{abs(int(avail))}",
                          message=f"Raised availability to 0 on inventory_item_id={rep_inv_id}",
                          title=title, before=str(avail), after="0")
                time.sleep(MUTATION_SLEEP_SEC)
                avail = 0  # reflect new stock
            except Exception as e:
                log_event("⚠️", "IN", "IN", "WARN", product_id=pid, message=f"clamp_to_zero failed: {e}", title=title)

    # Restock informational log (ls_prev<=0 -> avail>0)
    if ls_prev <= 0 and avail > 0:
        log_event("📦➕", "IN", "IN", "RESTOCK",
                  product_id=pid, sku=((p.get("skuMeta") or {}).get("value") or ""),
                  delta=str(avail - ls_prev), message=f"avail {ls_prev}->{avail} (+{avail - ls_prev})",
                  title=title, before=str(ls_prev), after=str(avail))

    # Metafields every non-read-only cycle
    if not read_only:
        _, target_badge, target_delivery = desired_state(avail)
        set_product_metafields_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("badges") or {}, p.get("dtime") or {}, target_badge, target_delivery)

    # SPECIAL STATUS RULE
    if (not read_only) and IN_CHANGE_STATUS and (handle == SPECIAL_STATUS_HANDLE):
        current_status = (p.get("status") or "").upper()
        if avail < 1 and current_status == "ACTIVE":
            ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "DRAFT")
            log_event("🛑", "IN", "IN", "STATUS_TO_DRAFT" if ok else "STATUS_TO_DRAFT_FAILED",
                      product_id=pid, delta=str(avail), message=f"handle={handle}", title=title)
        elif avail >= 1 and current_status == "DRAFT":
            ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "ACTIVE")
            log_event("✅", "IN", "IN", "STATUS_TO_ACTIVE" if ok else "STATUS_TO_ACTIVE_FAILED",
                      product_id=pid, delta=str(avail), message=f"handle={handle}", title=title)

    # Update last_seen (never negative)
    last_seen[pid] = max(0, int(avail))
    save_json(IN_LAST_SEEN, last_seen)

def scan_india_and_update(read_only: bool = False):
    for handle in IN_COLLECTIONS:
        cursor = None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN, {"handle": handle, "cursor": cursor, "onlyActive": False})
            coll = data.get("collectionByHandle")
            if not coll:
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                set_status_and_metafields_for_product(p, handle, read_only=read_only)
                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

# =========================
# USA scan → mirror to India (delta<0 only)
# =========================
def scan_usa_and_mirror_to_india(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(US_LAST_SEEN, {})
    in_index = load_json(IN_SKU_INDEX, {})

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
                sku_meta = ((p.get("skuMeta") or {}).get("value") or "").strip()
                for v in ((p.get("variants") or {}).get("nodes") or []):
                    vgid = v.get("id")
                    vid = gid_num(vgid)
                    qty = int(v.get("inventoryQuantity") or 0)

                    prev = _as_int_or_none(last_seen.get(vid))
                    if prev is None:
                        last_seen[vid] = qty
                        continue
                    delta = qty - int(prev)
                    if delta != 0:
                        if delta > 0 and not MIRROR_US_INCREASES:
                            log_event("🙅‍♂️➕", "US→IN", "US", "IGNORED_INCREASE",
                                      variant_id=vid, sku=sku_meta, delta=str(delta),
                                      message="US qty increase; mirroring disabled", title=title, before=str(prev), after=str(qty))
                        elif not read_only and delta < 0:
                            # need exact sku match in IN index
                            if not sku_meta or sku_meta not in in_index:
                                log_event("⚠️", "US→IN", "US", "WARN_SKU_NOT_FOUND_IN_INDIA",
                                          variant_id=vid, sku=sku_meta, delta=str(delta),
                                          message=f"US change {delta}; no matching product.custom.sku in India",
                                          title=title, before=str(prev), after=str(qty))
                            else:
                                inv_id = int(in_index[sku_meta]["inventory_item_id"])
                                try:
                                    rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_id, int(IN_LOCATION_ID), delta)
                                    log_event("🔁", "US→IN", "IN", "APPLIED_DELTA",
                                              variant_id=vid, sku=sku_meta, delta=str(delta),
                                              message=f"Adjusted IN inventory_item_id={inv_id} by {delta} (via EXACT index)",
                                              title=title, before=str(prev), after=str(qty))
                                    time.sleep(MUTATION_SLEEP_SEC)
                                except Exception as e:
                                    log_event("⚠️", "US→IN", "IN", "ERROR_APPLYING_DELTA",
                                              variant_id=vid, sku=sku_meta, delta=str(delta),
                                              message=str(e), title=title, before=str(prev), after=str(qty))
                        last_seen[vid] = qty
                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
            save_json(US_LAST_SEEN, {k:int(v) for k,v in last_seen.items()})
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

# =========================
# Scheduler loop
# =========================
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
        # Build/refresh IN index every cycle (fast; uses paging)
        build_india_sku_index()

        # First global cycle is read-only when both last-seen are empty
        state = load_state()
        in_seen = load_json(IN_LAST_SEEN, {})
        us_seen = load_json(US_LAST_SEEN, {})
        first_cycle = (not state.get("initialized", False)) and (len(in_seen) == 0 and len(us_seen) == 0)

        # India first (metafields + sales + clamp) — skip writes if first cycle
        scan_india_and_update(read_only=first_cycle)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

        # USA second (mirror variant deltas → IN) — skip adjust if first cycle
        scan_usa_and_mirror_to_india(read_only=first_cycle)

        # Mark initialized after first full cycle completes
        if first_cycle:
            state["initialized"] = True
            save_state(state)
            log_event("🗓️", "SCHED", "BOTH", "FIRST_CYCLE_DONE", message="Baselines learned; future cycles will apply deltas")

        return "ok"
    except Exception as e:
        log_event("⚠️", "SCHED", "BOTH", "WARN", message=str(e))
        return "error"
    finally:
        with run_lock:
            is_running = False

# =========================
# Flask API
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

@app.route("/run", methods=["POST"])
def run_now():
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))
    status = run_cycle()
    return _cors(jsonify({"status": status})), 200 if status == "ok" else 409

@app.route("/diag", methods=["GET"])
def diag():
    return _cors(jsonify({
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
        "clamp_avail_to_zero": CLAMP_AVAIL_TO_ZERO,
        "in_last_seen_count": len(load_json(IN_LAST_SEEN, {})),
        "us_last_seen_count": len(load_json(US_LAST_SEEN, {})),
        "index_entries": len(load_json(IN_SKU_INDEX, {})),
    })), 200

def scheduler_loop():
    if RUN_EVERY_MIN <= 0:
        return
    while True:
        try:
            run_cycle()
        except Exception as e:
            log_event("⚠️", "SCHED", "BOTH", "ERROR", message=str(e))
        time.sleep(max(1, RUN_EVERY_MIN) * 60)

if ENABLE_SCHEDULER:
    Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    print(f"[BOOT] Dual sync on port {PORT} | API {API_VERSION}")
    print(f"[CFG] IN={IN_DOMAIN} (loc {IN_LOCATION_ID}) | US={US_DOMAIN} (loc {US_LOCATION_ID}) | DATA_DIR={DATA_DIR}")
    run_simple("0.0.0.0", PORT, app)
