#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dual-site availability sync — exact SKU index, list/single metafield safe,
always-on temp discount enforcement, and clamp-to-zero for both shops.

This file supersedes your last working version; all prior behavior preserved.

ENV VARS (set on Web Service and Worker, except where noted):

Core
- API_VERSION=2024-10
- PIXEL_SHARED_SECRET=********             # required for /run
- DATA_DIR=/data                           # strongly recommended (Persistent Disk)
- RUN_EVERY_MIN=5
- ENABLE_SCHEDULER=0 (web) / 1 (worker)

India shop (receives deltas + metafields)
- IN_DOMAIN=silver-rudradhan.myshopify.com
- IN_TOKEN=shpat_*************************
- IN_LOCATION_ID=57791840446
- IN_COLLECTION_HANDLES=gold-plated,another-collection
- IN_INCLUDE_UNTRACKED=0|1                # default 0
- IN_CHANGE_STATUS=1                      # to flip ACTIVE<->DRAFT for special handle
- SPECIAL_STATUS_HANDLE=budget-gold-plated-jewelry-premium-jadau-ornaments-on-mix-metal-base

USA shop (source of deltas)
- US_DOMAIN=897265-11.myshopify.com
- US_TOKEN=shpat_*************************
- US_LOCATION_ID=74401939693
- US_COLLECTION_HANDLES=gold-plated-jewellery

Metafields (India)
- MF_NAMESPACE=custom
- MF_BADGES_KEY=badges
- MF_DELIVERY_KEY=delivery_time
- KEY_SALES=sales_total
- KEY_DATES=sales_dates

Behavior toggles
- USE_PRODUCT_CUSTOM_SKU=1                # index via product metafield custom.sku (exact)
- ONLY_ACTIVE_FOR_MAPPING=1               # index only ACTIVE products for SKU→inventory_item_id
- MIRROR_US_INCREASES=0                   # ignore US increases
- CLAMP_AVAIL_TO_ZERO=1                   # clamp negative availability to zero after counting sales

Temp discount (always-on; per product)
- TEMP_DISCOUNT_KEY=temp_discount_active   # product metafield (numeric percent, e.g., 5/10/15/20/25)
- DISCOUNT_STEP_IN=5                      # ₹ step to round UP to (e.g., 5 → 23444 → 23445)
- DISCOUNT_STEP_US=5                      # $ step to round UP to
- DISCOUNT_STATE_FILE will be stored at {DATA_DIR}/discount_state.json automatically

Pacing
- MUTATION_SLEEP_SEC=0.35
- SLEEP_BETWEEN_PRODUCTS_MS=120
- SLEEP_BETWEEN_PAGES_MS=400
- SLEEP_BETWEEN_SHOPS_MS=800

Logging
- LOG_TO_STDOUT=1
- CSV:   {DATA_DIR}/dual_sync_log.csv
- JSONL: {DATA_DIR}/dual_sync_log.jsonl

Notes
- First global cycle is read-only (learn baselines, build index). Discounts still ENFORCED
  (read-only is only for availability writes and sales bump; price changes are allowed every pass).
- last_seen is guaranteed non-negative (never store <0).
"""

import os, sys, time, json, csv, random
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import requests
from flask import Flask, request, jsonify, make_response
from decimal import Decimal, ROUND_CEILING, getcontext

getcontext().prec = 28

# ---------------- Config ----------------
API_VERSION = os.getenv("API_VERSION", "2024-10").strip()
PIXEL_SHARED_SECRET = os.getenv("PIXEL_SHARED_SECRET", "").strip()
if not PIXEL_SHARED_SECRET:
    raise SystemExit("PIXEL_SHARED_SECRET required")

RUN_EVERY_MIN = int(os.getenv("RUN_EVERY_MIN", "5"))
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "0") == "1"

SLEEP_BETWEEN_PRODUCTS_MS = int(os.getenv("SLEEP_BETWEEN_PRODUCTS_MS", "120"))
SLEEP_BETWEEN_PAGES_MS = int(os.getenv("SLEEP_BETWEEN_PAGES_MS", "400"))
SLEEP_BETWEEN_SHOPS_MS = int(os.getenv("SLEEP_BETWEEN_SHOPS_MS", "800"))
MUTATION_SLEEP_SEC = float(os.getenv("MUTATION_SLEEP_SEC", "0.35"))

# India
IN_DOMAIN = os.getenv("IN_DOMAIN", "").strip()
IN_TOKEN  = os.getenv("IN_TOKEN", "").strip()
IN_LOCATION_ID = os.getenv("IN_LOCATION_ID", "").strip()
IN_COLLECTIONS = [x.strip() for x in os.getenv("IN_COLLECTION_HANDLES","").split(",") if x.strip()]
IN_INCLUDE_UNTRACKED = os.getenv("IN_INCLUDE_UNTRACKED","0") == "1"
IN_CHANGE_STATUS = os.getenv("IN_CHANGE_STATUS","0") == "1"
SPECIAL_STATUS_HANDLE = os.getenv("SPECIAL_STATUS_HANDLE", "").strip()

# USA
US_DOMAIN = os.getenv("US_DOMAIN", "").strip()
US_TOKEN  = os.getenv("US_TOKEN", "").strip()
US_LOCATION_ID = os.getenv("US_LOCATION_ID", "").strip()
US_COLLECTIONS = [x.strip() for x in os.getenv("US_COLLECTION_HANDLES","").split(",") if x.strip()]

# Metafields
MF_NAMESPACE = os.getenv("MF_NAMESPACE", "custom")
MF_BADGES_KEY = os.getenv("MF_BADGES_KEY", "badges")
MF_DELIVERY_KEY = os.getenv("MF_DELIVERY_KEY", "delivery_time")
KEY_SALES = os.getenv("KEY_SALES", "sales_total")
KEY_DATES = os.getenv("KEY_DATES", "sales_dates")

# Temp discount config
TEMP_DISCOUNT_KEY = os.getenv("TEMP_DISCOUNT_KEY", "temp_discount_active")
DISCOUNT_STEP_IN = Decimal(str(os.getenv("DISCOUNT_STEP_IN", "5")))
DISCOUNT_STEP_US = Decimal(str(os.getenv("DISCOUNT_STEP_US", "5")))

BADGE_READY    = "Ready To Ship"
DELIVERY_READY = "2-5 Days Across India"
DELIVERY_MTO   = "12-15 Days Across India"

# Behavior toggles
USE_PRODUCT_CUSTOM_SKU = os.getenv("USE_PRODUCT_CUSTOM_SKU","1") == "1"
ONLY_ACTIVE_FOR_MAPPING = os.getenv("ONLY_ACTIVE_FOR_MAPPING","1") == "1"
MIRROR_US_INCREASES = os.getenv("MIRROR_US_INCREASES","0") == "1"
CLAMP_AVAIL_TO_ZERO = os.getenv("CLAMP_AVAIL_TO_ZERO","1") == "1"

# Persistence
DATA_DIR = os.getenv("DATA_DIR",".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)
PORT = int(os.getenv("PORT", "10000"))

# State files
def p(*parts): return os.path.join(DATA_DIR, *parts)
IN_LAST_SEEN   = p("in_last_seen.json")     # product_id(num) -> last_seen_availability (>=0)
US_LAST_SEEN   = p("us_last_seen.json")     # variant_id(num) -> last_seen_qty
STATE_PATH     = p("dual_state.json")       # {"initialized": bool}
IN_SKU_INDEX   = p("in_sku_index.json")     # { "NS 202": {"product_id":..., "inventory_item_id":..., "title":...}, ... }
LOG_CSV        = p("dual_sync_log.csv")
LOG_JSONL      = p("dual_sync_log.jsonl")
DISCOUNT_STATE = p("discount_state.json")   # { "IN": {pid:{vid:orig}}, "US":{...} }
LOG_TO_STDOUT  = os.getenv("LOG_TO_STDOUT","1") == "1"

# --------------- HTTP helpers ---------------
def hdr(token: str) -> Dict[str, str]:
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json", "Accept": "application/json"}

def _backoff(attempt:int, base:float=0.35, mx:float=10.0)->float:
    return min(mx, base * (2 ** (attempt-1))) + random.uniform(0, 0.25)

def gql(domain: str, token: str, query: str, variables: dict = None) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    for attempt in range(1, 8):
        r = requests.post(url, headers=hdr(token), json={"query": query, "variables": variables or {}}, timeout=60)
        if r.status_code >= 500 or r.status_code == 429:
            time.sleep(_backoff(attempt, base=0.4))
            continue
        if r.status_code != 200:
            raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")
        data = r.json()
        if data.get("errors"):
            # throttle or others
            if any(((e.get("extensions") or {}).get("code","").upper()=="THROTTLED") for e in data["errors"]):
                time.sleep(_backoff(attempt, base=0.4))
                continue
            log_row("⚠️","GQL","ERRORS", message=json.dumps(data["errors"]), extra={"LAST_QUERY":query[:800]})
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]
    raise RuntimeError("GraphQL throttled/5xx repeatedly")

def rest_adjust_inventory(domain: str, token: str, inventory_item_id: int, location_id: int, delta: int) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/adjust.json"
    payload = {"inventory_item_id": int(inventory_item_id), "location_id": int(location_id), "available_adjustment": int(delta)}
    for attempt in range(1, 6):
        r = requests.post(url, headers=hdr(token), json=payload, timeout=30)
        if r.status_code in (422,):  # level not set at location? try set, then retry
            # get current, then set explicitly to allow adjust
            try:
                rest_set_inventory_to(domain, token, inventory_item_id, location_id, None)  # ensure record exists
            except Exception:
                pass
            time.sleep(_backoff(attempt, base=0.2, mx=3.0))
            continue
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(_backoff(attempt, base=0.3, mx=8.0))
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"REST adjust {r.status_code}: {r.text}")
        return

def rest_set_inventory_to(domain: str, token: str, inventory_item_id: int, location_id: int, new_level: Optional[int]):
    """
    Create/ensure the inventory level exists, and optionally set to an exact quantity.
    If new_level is None, we simply ensure the level is associated by connect.
    """
    # connect if missing
    url_conn = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/connect.json"
    requests.post(url_conn, headers=hdr(token), json={
        "inventory_item_id": int(inventory_item_id),
        "location_id": int(location_id),
        "relocate_if_necessary": True
    }, timeout=30)
    # optionally set
    if new_level is not None:
        url_set = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/set.json"
        r = requests.post(url_set, headers=hdr(token), json={
            "inventory_item_id": int(inventory_item_id),
            "location_id": int(location_id),
            "available": int(new_level)
        }, timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"REST set {r.status_code}: {r.text}")

# REST price helpers
def rest_get_product(domain:str, token:str, product_id:int) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/products/{int(product_id)}.json"
    r = requests.get(url, headers=hdr(token), timeout=60)
    r.raise_for_status()
    return r.json().get("product") or {}

def rest_get_variant(domain:str, token:str, variant_id:int) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/variants/{int(variant_id)}.json"
    r = requests.get(url, headers=hdr(token), timeout=30)
    r.raise_for_status()
    return r.json().get("variant") or {}

def rest_update_variant_price(domain:str, token:str, variant_id:int, new_price:str):
    url = f"https://{domain}/admin/api/{API_VERSION}/variants/{int(variant_id)}.json"
    payload = {"variant": {"id": int(variant_id), "price": str(new_price)}}
    r = requests.put(url, headers=hdr(token), json=payload, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"REST price update {r.status_code}: {r.text}")
    time.sleep(MUTATION_SLEEP_SEC)

# --------------- Utils ---------------
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
        if isinstance(v,(int,float)): return int(v)
        s=str(v).strip()
        return None if s=="" or s=="-" else int(s)
    except: return None
def load_json(path: str, default): 
    try:
        with open(path,"r",encoding="utf-8") as f: return json.load(f)
    except: return default
def save_json(path: str, obj):
    tmp=path+".tmp"
    with open(tmp,"w",encoding="utf-8") as f: json.dump(obj,f,indent=2)
    os.replace(tmp,path)

def normalize_sku(s: str) -> str:
    return (s or "").strip()

# logging
def ensure_log_header():
    need = (not os.path.exists(LOG_CSV)) or (os.path.getsize(LOG_CSV) == 0)
    if need:
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["ts","phase","shop","note","product_id","variant_id","sku","delta","message"])
def log_row(emoji_phase:str, shop:str, note:str, product_id:str="", variant_id:str="", sku:str="", delta:str="", message:str="", extra:dict=None, title:str="", before:str="", after:str="", collections:str=""):
    ensure_log_header()
    ts = now_ist_str()
    # CSV
    with open(LOG_CSV,"a",newline="",encoding="utf-8") as f:
        csv.writer(f).writerow([ts, shop, shop, note, product_id, variant_id, sku, delta, message])
    # JSONL (rich)
    row = {"ts":ts,"phase":emoji_phase,"shop":shop,"note":note,"product_id":product_id,"variant_id":variant_id,"sku":sku,"delta":str(delta),"message":message,"title":title,"before":str(before),"after":str(after),"collections":collections}
    if extra: row.update(extra)
    with open(LOG_JSONL,"a",encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False)+"\n")
    if LOG_TO_STDOUT:
        human = f"{emoji_phase} {shop} {note} [SKU {sku}] pid={product_id} vid={variant_id} {before}→{after} Δ{delta} “{title}” — {message}"
        print(human, flush=True)

# --------------- GraphQL queries ---------------
# Include variant price so we can discount without extra REST GETs.
QUERY_COLLECTION_PAGE_IN = """
query ($handle:String!, $cursor:String) {
  collectionByHandle(handle:$handle){
    products(first: 60, after:$cursor){
      pageInfo{ hasNextPage endCursor }
      nodes{
        id title status
        skuMeta: metafield(namespace:"custom", key:"sku"){ value }
        tempDisc: metafield(namespace:"%NS%", key:"%TDK%"){ value }
        variants(first: 100){
          nodes{
            id title sku inventoryQuantity price compareAtPrice
            inventoryItem{ id tracked }
            inventoryPolicy
          }
        }
        badges:  metafield(namespace:"%NS%", key:"%BK%"){ id value type }
        dtime:   metafield(namespace:"%NS%", key:"%DK%"){ id value type }
        salesTotal: metafield(namespace:"%NS%", key:"%KS%"){ id value type }
        salesDates: metafield(namespace:"%NS%", key:"%KD%"){ id value type }
      }
    }
  }
}
""".replace("%NS%", MF_NAMESPACE)\
   .replace("%BK%", MF_BADGES_KEY)\
   .replace("%DK%", MF_DELIVERY_KEY)\
   .replace("%KS%", KEY_SALES)\
   .replace("%KD%", KEY_DATES)\
   .replace("%TDK%", TEMP_DISCOUNT_KEY)

QUERY_COLLECTION_PAGE_US = """
query ($handle:String!, $cursor:String) {
  collectionByHandle(handle:$handle){
    products(first: 60, after:$cursor){
      pageInfo{ hasNextPage endCursor }
      nodes{
        id title
        skuMeta: metafield(namespace:"custom", key:"sku"){ value }
        tempDisc: metafield(namespace:"%NS%", key:"%TDK%"){ value }
        variants(first: 100){
          nodes{
            id sku inventoryQuantity price compareAtPrice
            inventoryItem{ id }
          }
        }
      }
    }
  }
}
""".replace("%NS%", MF_NAMESPACE)\
   .replace("%TDK%", TEMP_DISCOUNT_KEY)

# definition lookup cache
_MF_DEF_CACHE: Dict[Tuple[str,str,str], str] = {}  # (ownerType, namespace, key)->typeName
def get_mf_def_type(domain:str, token:str, owner_type:str, namespace:str, key:str) -> str:
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

# --------------- Availability logic ---------------
def compute_product_availability(variants: List[dict], include_untracked: bool) -> int:
    total = 0; counted=False
    for v in variants:
        tracked = bool(((v.get("inventoryItem") or {}).get("tracked")))
        qty = int(v.get("inventoryQuantity") or 0)
        if include_untracked or tracked:
            counted=True; total += qty
    return total if counted else 0

def desired_state(avail: int) -> Tuple[str, str, str]:
    if avail > 0: return ("ACTIVE", BADGE_READY, DELIVERY_READY)
    return ("DRAFT", "", DELIVERY_MTO)

# --------------- Metafields & status ---------------
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
    time.sleep(MUTATION_SLEEP_SEC); return True

def set_product_metafields(domain:str, token:str, product_gid:str,
                           badges_node:dict, dtime_node:dict,
                           target_badge:str, target_delivery:str):
    # Resolve types, preferring on-node type, else definition, else safe default
    badges_type  = (badges_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_BADGES_KEY)
    delivery_type= (dtime_node  or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_DELIVERY_KEY)

    mf_inputs = []

    # badges: handle list vs single safely
    if target_badge:
        if str(badges_type).startswith("list."):
            mf_inputs.append({
                "ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_BADGES_KEY,
                "type": badges_type, "value": json.dumps([target_badge])
            })
        else:
            mf_inputs.append({
                "ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_BADGES_KEY,
                "type": badges_type, "value": target_badge
            })
    else:
        # clear if exists
        if (badges_node or {}).get("id"):
            delm = "mutation($id:ID!){ metafieldDelete(input:{id:$id}){ userErrors{message} } }"
            gql(domain, token, delm, {"id": badges_node["id"]})

    # delivery time
    mf_inputs.append({
        "ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_DELIVERY_KEY,
        "type": delivery_type, "value": target_delivery
    })

    if mf_inputs:
        mutation = "mutation($mfs:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$mfs){ userErrors{ field message } } }"
        data = gql(domain, token, mutation, {"mfs": mf_inputs})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_row("⚠️","IN","WARN",product_id=gid_num(product_gid),message=f"metafieldsSet errors: {errs}")

def bump_sales_in(domain:str, token:str, product_gid:str,
                  sales_total_node:dict, sales_dates_node:dict,
                  sold:int, today:str):
    # Resolve types
    st_type = (sales_total_node or {}).get("type") or \
              get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, KEY_SALES) or \
              "number_integer"
    sd_type = (sales_dates_node or {}).get("type") or \
              get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, KEY_DATES) or \
              "date"

    try:
        current_total = int((sales_total_node or {}).get("value") or "0")
    except Exception:
        current_total = 0
    new_total = current_total + int(sold)

    if sd_type.startswith("list."):
        existing = []
        raw = (sales_dates_node or {}).get("value")
        try:
            if isinstance(raw, str) and raw.strip().startswith("["):
                existing = json.loads(raw)
        except Exception:
            existing = []
        if today not in existing:
            existing.append(today)
        sd_payload = {"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":KEY_DATES,"type":"list.date","value":json.dumps(sorted(set(existing))[-365:])}
    elif sd_type == "date":
        sd_payload = {"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":KEY_DATES,"type":"date","value":today}
    else:
        sd_payload = {"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":KEY_DATES,"type":sd_type,"value":today}

    m = "mutation($mfs:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$mfs){ userErrors{message field} } }"
    mf_inputs = [
        {"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":KEY_SALES,"type":st_type,"value":str(new_total)},
        sd_payload
    ]
    data = gql(domain, token, m, {"mfs": mf_inputs})
    errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
    if errs:
        log_row("⚠️","IN","WARN", product_id=gid_num(product_gid), message=f"sales metafieldsSet errors: {errs}")

# --------------- Index builder (India) ---------------
def build_in_sku_index():
    index: Dict[str, Dict[str,Any]] = {}
    for handle in IN_COLLECTIONS:
        cursor=None
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
                sku = ((((p.get("skuMeta") or {})).get("value")) or "").strip()
                if not sku: continue
                sku_key = sku.strip()
                variants = ((p.get("variants") or {}).get("nodes") or [])
                if not variants: continue
                inv_item_gid = (((variants[0].get("inventoryItem") or {}).get("id")) or "")
                if not inv_item_gid: continue
                inv_item_id = int(gid_num(inv_item_gid) or "0")
                index[sku_key] = {
                    "product_id": gid_num(p["id"]),
                    "inventory_item_id": inv_item_id,
                    "title": p.get("title") or "",
                    "status": status
                }
                sleep_ms(5)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break
    save_json(IN_SKU_INDEX, index)
    log_row("🗂️","IN","INDEX_BUILT", message=f"entries={len(index)}")

# ---------- Discount helpers ----------
def load_discount_state() -> Dict[str, Dict[str, Dict[str,str]]]:
    return load_json(DISCOUNT_STATE, {"IN":{}, "US":{}})

def save_discount_state(st: Dict[str, Dict[str, Dict[str,str]]]):
    save_json(DISCOUNT_STATE, st)

def parse_percent_from_metafield_value(v: Any) -> Decimal:
    try:
        if v is None: return Decimal(0)
        s = str(v).strip()
        if s == "": return Decimal(0)
        return Decimal(s)
    except Exception:
        return Decimal(0)

def round_up_to_step(amount: Decimal, step: Decimal) -> Decimal:
    if step <= 0: return amount
    # ceil(amount/step)*step
    return ( (amount / step).to_integral_exact(rounding=ROUND_CEILING) * step )

def product_availability_from_nodes(variants: List[dict], include_untracked: bool) -> int:
    # Same as compute_product_availability but without tracked info for US where tracked may be missing
    total = 0; counted=False
    for v in variants:
        qty = int(v.get("inventoryQuantity") or 0)
        tracked = True
        inv = v.get("inventoryItem") or {}
        if "tracked" in inv:
            tracked = bool(inv.get("tracked"))
        if include_untracked or tracked:
            counted=True; total += qty
    return total if counted else 0

def ensure_discount_apply_or_revert(shop_key:str, domain:str, token:str,
                                    product_node:dict, availability:int, step:Decimal):
    """
    Enforce discount/revert for a product based on product metafield custom.temp_discount_active (numeric %).
    Saves originals on first apply; reverts when percent<=0 or availability==0.
    """
    st = load_discount_state()
    st_shop = st.get(shop_key) or {}
    pid = gid_num(product_node["id"])
    title = product_node.get("title") or ""
    temp_disc_val = ((((product_node.get("tempDisc") or {})).get("value")) or "").strip()
    pct = parse_percent_from_metafield_value(temp_disc_val)
    variants = ((product_node.get("variants") or {}).get("nodes") or [])

    def _ensure_shop(): 
        if shop_key not in st: st[shop_key]={}
        if pid not in st[shop_key]: st[shop_key][pid]={}

    def _apply():
        _ensure_shop()
        changed = 0
        for v in variants:
            vid = int(gid_num(v["id"]))
            cur_price_str = (v.get("price") or "").strip()
            if not cur_price_str:
                # fallback to REST GET
                vdata = rest_get_variant(domain, token, vid)
                cur_price_str = str(vdata.get("price") or "").strip()
            if not cur_price_str:
                continue
            cur = Decimal(cur_price_str)
            # record original if not present
            if str(vid) not in st[shop_key][pid]:
                st[shop_key][pid][str(vid)] = str(cur)
            target = round_up_to_step( (cur * (Decimal(100) - pct) / Decimal(100)), step )
            if target <= 0:
                target = step  # safety
            if target != cur:
                try:
                    rest_update_variant_price(domain, token, vid, str(target))
                    changed += 1
                except Exception as e:
                    log_row("⚠️", shop_key, "DISCOUNT_APPLY_ERROR", product_id=pid, variant_id=str(vid),
                            message=str(e), title=title, sku="", before=str(cur), after=str(target))
                    continue
        if changed:
            save_discount_state(st)
            log_row("🏷️", shop_key, "DISCOUNT_APPLIED", product_id=pid, title=title,
                    message=f"pct={pct} step={step} variants_changed={changed}")

    def _revert():
        saved = st_shop.get(pid) or {}
        if not saved:
            return
        changed = 0
        # Prefer current product variants (ids may be same)
        saved_items = list(saved.items())
        for vid_str, orig_str in saved_items:
            vid = int(vid_str)
            try:
                rest_update_variant_price(domain, token, vid, str(orig_str))
                changed += 1
            except Exception as e:
                log_row("⚠️", shop_key, "DISCOUNT_REVERT_ERROR", product_id=pid, variant_id=str(vid),
                        message=str(e), title=title)
        if changed:
            # clear saved originals for this product
            st[shop_key].pop(pid, None)
            save_discount_state(st)
            log_row("♻️", shop_key, "DISCOUNT_REVERTED", product_id=pid, title=title,
                    message=f"variants_restored={changed}")

    if pct > 0 and availability > 0:
        _apply()
    else:
        _revert()

def revert_discount_immediately_for_india_by_sku(sku: str):
    """Used after US→IN sale mirroring: try to revert India prices for that product immediately."""
    if not sku: return
    idx = load_json(IN_SKU_INDEX, {})
    rec = idx.get(sku)
    if not rec: return
    pid = rec.get("product_id")
    if not pid: return
    try:
        prod = rest_get_product(IN_DOMAIN, IN_TOKEN, int(pid))
    except Exception:
        return
    # shape minimal product_node for ensure_discount_apply_or_revert
    variants_nodes = [{"id": f"gid://shopify/ProductVariant/{v['id']}", "price": str(v.get("price") or "")} for v in (prod.get("variants") or [])]
    product_node = {
        "id": f"gid://shopify/Product/{pid}",
        "title": prod.get("title") or "",
        "tempDisc": {"value": ""},  # treat as disabled on immediate revert
        "variants": {"nodes": variants_nodes}
    }
    ensure_discount_apply_or_revert("IN", IN_DOMAIN, IN_TOKEN, product_node, availability=0, step=DISCOUNT_STEP_IN)

# --------------- India scan ---------------
def scan_india_and_update(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(IN_LAST_SEEN, {})
    today = today_ist_str()

    for handle in IN_COLLECTIONS:
        cursor=None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                log_row("⚠️","IN","WARN", message=f"Collection not found: {handle}")
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                pid = gid_num(p["id"])
                title = p.get("title") or ""
                status = (p.get("status") or "").upper()
                sku = ((((p.get("skuMeta") or {})).get("value")) or "").strip()
                variants = ((p.get("variants") or {}).get("nodes") or [])
                avail = compute_product_availability(variants, IN_INCLUDE_UNTRACKED)

                # last_seen is never negative
                prev = _as_int_or_none(last_seen.get(pid))
                if prev is None: prev = 0

                # discounts: enforce every pass (idempotent)
                try:
                    ensure_discount_apply_or_revert("IN", IN_DOMAIN, IN_TOKEN, p, availability=avail, step=DISCOUNT_STEP_IN)
                except Exception as e:
                    log_row("⚠️","IN","DISCOUNT_ENFORCE_ERROR", product_id=pid, title=title, message=str(e))

                # sales detection & clamping
                if not read_only and avail < prev:
                    sold = prev - avail
                    bump_sales_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, sold, today)
                    log_row("🧾➖","IN","SALES_BUMP", product_id=pid, sku=sku, delta=f"-{sold}", message=f"avail {prev}->{avail} (sold={sold})", title=title, before=str(prev), after=str(avail))

                # clamp <0 → 0 (after counting sales)
                if not read_only and CLAMP_AVAIL_TO_ZERO and avail < 0 and variants:
                    inv_item_gid = (((variants[0].get("inventoryItem") or {}).get("id")) or "")
                    inv_item_id = int(gid_num(inv_item_gid) or "0")
                    try:
                        rest_set_inventory_to(IN_DOMAIN, IN_TOKEN, inv_item_id, int(IN_LOCATION_ID), 0)
                        log_row("🧰0️⃣","IN","CLAMP_TO_ZERO", product_id=pid, sku=sku, delta=f"+{-avail}", message=f"Raised availability to 0 on inventory_item_id={inv_item_id}", title=title, before=str(avail), after="0")
                        avail = 0
                    except Exception as e:
                        log_row("⚠️","IN","WARN", product_id=pid, sku=sku, message=f"Clamp error: {e}")

                # desired badges/delivery & optional status flip
                _, target_badge, target_delivery = desired_state(avail)
                if not read_only:
                    set_product_metafields(IN_DOMAIN, IN_TOKEN, p["id"], p.get("badges") or {}, p.get("dtime") or {}, target_badge, target_delivery)

                if (not read_only) and IN_CHANGE_STATUS and SPECIAL_STATUS_HANDLE and (handle == SPECIAL_STATUS_HANDLE):
                    if avail < 1 and status == "ACTIVE":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "DRAFT")
                        log_row("🛑","IN","STATUS_TO_DRAFT" if ok else "STATUS_TO_DRAFT_FAILED", product_id=pid, sku=sku, delta=str(avail), title=title, message=f"handle={handle}")
                    elif avail >= 1 and status == "DRAFT":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "ACTIVE")
                        log_row("✅","IN","STATUS_TO_ACTIVE" if ok else "STATUS_TO_ACTIVE_FAILED", product_id=pid, sku=sku, delta=str(avail), title=title, message=f"handle={handle}")

                # update last_seen (never negative)
                last_seen[pid] = max(0, int(avail))

                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            save_json(IN_LAST_SEEN, last_seen)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

# --------------- USA scan → mirror to India + discount enforce + clamp ---------------
def scan_usa_and_mirror_to_india(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(US_LAST_SEEN, {})
    in_index: Dict[str,Any] = load_json(IN_SKU_INDEX, {})

    for handle in US_COLLECTIONS:
        cursor=None
        while True:
            data = gql(US_DOMAIN, US_TOKEN, QUERY_COLLECTION_PAGE_US, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll: break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                p_sku = ((((p.get("skuMeta") or {})).get("value")) or "").strip()
                title = p.get("title") or ""
                vnodes = ((p.get("variants") or {}).get("nodes") or [])

                # Enforce discount on US product based on its availability
                try:
                    us_avail = product_availability_from_nodes(vnodes, include_untracked=True)
                    ensure_discount_apply_or_revert("US", US_DOMAIN, US_TOKEN, p, availability=us_avail, step=DISCOUNT_STEP_US)
                except Exception as e:
                    log_row("⚠️","US","DISCOUNT_ENFORCE_ERROR", title=title, message=str(e))

                for v in vnodes:
                    vid = gid_num(v.get("id"))
                    raw_sku = v.get("sku") or p_sku
                    sku_exact = normalize_sku(raw_sku)
                    qty = int(v.get("inventoryQuantity") or 0)

                    prev = _as_int_or_none(last_seen.get(vid))
                    if prev is None:
                        last_seen[vid] = qty
                        continue

                    delta = qty - int(prev)
                    if delta == 0:
                        continue

                    # increases
                    if delta > 0:
                        if not MIRROR_US_INCREASES:
                            log_row("🙅‍♂️➕","US→IN","IGNORED_INCREASE", variant_id=vid, sku=sku_exact, delta=str(delta), title=title, message="US qty increase; mirroring disabled", before=str(prev), after=str(qty))
                            last_seen[vid] = qty
                            continue
                        # else: if enabled, could mirror increase (not typical for you)

                    # decreases (delta < 0) → mirror to India
                    if not read_only and delta < 0 and sku_exact:
                        idx = in_index.get(sku_exact)
                        if not idx:
                            log_row("⚠️","US→IN","WARN_SKU_NOT_FOUND", variant_id=vid, sku=sku_exact, delta=str(delta), title=title, message="No matching SKU in India index")
                        else:
                            in_inv_item_id = int(idx.get("inventory_item_id") or 0)
                            try:
                                rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, in_inv_item_id, int(IN_LOCATION_ID), delta)
                                log_row("🔁","US→IN","APPLIED_DELTA", variant_id=vid, sku=sku_exact, delta=str(delta), title=title, message=f"Adjusted IN inventory_item_id={in_inv_item_id} by {delta} (via EXACT index)")
                                time.sleep(MUTATION_SLEEP_SEC)
                                # Immediate revert of India discount for this SKU (so next buyer sees full price)
                                revert_discount_immediately_for_india_by_sku(sku_exact)
                            except Exception as e:
                                log_row("⚠️","US→IN","ERROR_APPLYING_DELTA", variant_id=vid, sku=sku_exact, delta=str(delta), title=title, message=str(e))

                    last_seen[vid] = qty
                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

                    # CLAMP US negatives to zero (after delta handling)
                    if not read_only and CLAMP_AVAIL_TO_ZERO and qty < 0:
                        inv_item_gid = ((v.get("inventoryItem") or {}).get("id")) or ""
                        if inv_item_gid:
                            inv_item_id = int(gid_num(inv_item_gid) or "0")
                            try:
                                rest_set_inventory_to(US_DOMAIN, US_TOKEN, inv_item_id, int(US_LOCATION_ID), 0)
                                log_row("🧰0️⃣","US","CLAMP_TO_ZERO_US", variant_id=vid, sku=sku_exact, delta=f"+{-qty}", title=title, message=f"Raised US availability to 0 on inventory_item_id={inv_item_id}", before=str(qty), after="0")
                                qty = 0
                            except Exception as e:
                                log_row("⚠️","US→IN","WARN", variant_id=vid, sku=sku_exact, title=title, message=f"US clamp error: {e}")

            save_json(US_LAST_SEEN, last_seen)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

# --------------- Scheduler / Runner ---------------
from threading import Thread, Lock
run_lock = Lock()
is_running = False

def load_state():
    try:
        with open(STATE_PATH,"r",encoding="utf-8") as f: return json.load(f)
    except: return {"initialized": False}
def save_state(obj):
    tmp=STATE_PATH+".tmp"
    with open(tmp,"w",encoding="utf-8") as f: json.dump(obj,f,indent=2)
    os.replace(tmp,STATE_PATH)

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
        first_cycle = (not state.get("initialized", False)) and (len(in_seen)==0 and len(us_seen)==0)

        # Always (re)build index before scans
        try:
            build_in_sku_index()
        except Exception as e:
            log_row("⚠️","SCHED","WARN", message=str(e))

        # India scan (metafields/sales/clamp + discount)
        scan_india_and_update(read_only=first_cycle)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

        # USA scan (mirror decreases only + discount + clamp)
        scan_usa_and_mirror_to_india(read_only=first_cycle)

        if first_cycle:
            state["initialized"] = True
            save_state(state)
            log_row("🟢","SCHED","FIRST_CYCLE_DONE", message="Baselines learned; future cycles will apply deltas")
        return "ok"
    finally:
        with run_lock:
            is_running = False

# --------------- Flask app ---------------
app = Flask(__name__)
def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"]="*"
    resp.headers["Access-Control-Allow-Methods"]="GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"]="Content-Type"
    return resp

@app.route("/health", methods=["GET"])
def health(): return "ok", 200

@app.route("/diag", methods=["GET"])
def diag():
    info = {
        "api_version": API_VERSION,
        "data_dir": DATA_DIR,
        "in_domain": IN_DOMAIN,
        "us_domain": US_DOMAIN,
        "in_collections": IN_COLLECTIONS,
        "us_collections": US_COLLECTIONS,
        "run_every_min": RUN_EVERY_MIN,
        "scheduler_enabled": ENABLE_SCHEDULER,
        "in_last_seen_count": len(load_json(IN_LAST_SEEN, {})),
        "us_last_seen_count": len(load_json(US_LAST_SEEN, {})),
        "index_entries": len(load_json(IN_SKU_INDEX, {})),
        "use_product_custom_sku": USE_PRODUCT_CUSTOM_SKU,
        "only_active_for_mapping": ONLY_ACTIVE_FOR_MAPPING,
        "mirror_us_increases": MIRROR_US_INCREASES,
        "clamp_avail_to_zero": CLAMP_AVAIL_TO_ZERO,
        "discount_step_in": str(DISCOUNT_STEP_IN),
        "discount_step_us": str(DISCOUNT_STEP_US),
        "temp_discount_key": TEMP_DISCOUNT_KEY
    }
    return _cors(jsonify(info)), 200

@app.route("/rebuild_index", methods=["POST"])
def rebuild_index():
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))
    build_in_sku_index()
    return _cors(jsonify({"status":"ok","entries":len(load_json(IN_SKU_INDEX, {}))})), 200

@app.route("/run", methods=["POST"])
def run_now():
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))
    status = run_cycle()
    return _cors(jsonify({"status": status})), 200 if status=="ok" else 409

def scheduler_loop():
    if RUN_EVERY_MIN <= 0: return
    while True:
        try:
            run_cycle()
        except Exception as e:
            log_row("⚠️","SCHED","WARN", message=str(e))
        time.sleep(max(1, RUN_EVERY_MIN)*60)

if ENABLE_SCHEDULER:
    Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    print(f"[BOOT] Dual sync on port {PORT} | API {API_VERSION}")
    print(f"[CFG] IN={IN_DOMAIN} (loc {IN_LOCATION_ID}) | US={US_DOMAIN} (loc {US_LOCATION_ID}) | DATA_DIR={DATA_DIR}")
    from werkzeug.serving import run_simple
    run_simple("0.0.0.0", PORT, app)
