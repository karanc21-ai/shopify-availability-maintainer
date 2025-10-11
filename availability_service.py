#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dual-site availability sync — temp discount (apply-once + revert-on-sale) + periodic US priceinindia sync.

What’s new vs your last working file:
1) Temp discount is **apply-once**:
   - Reads product metafield custom.temp_discount_active (percent; 0=off).
   - If percent>0 and availability>0 and not previously applied → discount ALL variants once (rounded UP to nearest 5).
   - Persist originals in /data/discount_state.json; DO NOT re-apply on later passes.
   - On INDIA sale detection (new<prev), immediately restore original prices and set custom.temp_discount_active=0.

2) Periodic US priceinindia sync:
   - New envs:
       PRICE_SYNC_HOURS=6              # run the check every N hours
       MF_PRICE_IN_INDIA_KEY=priceinindia
   - On each run, if at least PRICE_SYNC_HOURS elapsed since last sync, we:
       * build/read India SKU index (which now stores India base variant_id)
       * for each US product with SKU that exists in India index:
           - fetch India base variant price
           - write that value into US product metafield custom.priceinindia (type-safe)
     (No price conversion or rounding; we write India’s number as-is.)

Everything else (index build, clamping, badges/delivery, sales_total, US→IN mirror) remains unchanged.
"""

import os, sys, time, json, csv, random, math
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import requests
from flask import Flask, request, jsonify, make_response

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

# Metafields / keys
MF_NAMESPACE = os.getenv("MF_NAMESPACE", "custom")
MF_BADGES_KEY = os.getenv("MF_BADGES_KEY", "badges")
MF_DELIVERY_KEY = os.getenv("MF_DELIVERY_KEY", "delivery_time")
KEY_SALES = os.getenv("KEY_SALES", "sales_total")
KEY_DATES = os.getenv("KEY_DATES", "sales_dates")
TEMP_DISC_KEY = os.getenv("TEMP_DISC_KEY", "temp_discount_active")
MF_PRICE_IN_INDIA_KEY = os.getenv("MF_PRICE_IN_INDIA_KEY", "priceinindia")

BADGE_READY    = "Ready To Ship"
DELIVERY_READY = "2-5 Days Across India"
DELIVERY_MTO   = "12-15 Days Across India"

# Behavior toggles
USE_PRODUCT_CUSTOM_SKU = os.getenv("USE_PRODUCT_CUSTOM_SKU","1") == "1"
ONLY_ACTIVE_FOR_MAPPING = os.getenv("ONLY_ACTIVE_FOR_MAPPING","1") == "1"
MIRROR_US_INCREASES = os.getenv("MIRROR_US_INCREASES","0") == "1"
CLAMP_AVAIL_TO_ZERO = os.getenv("CLAMP_AVAIL_TO_ZERO","1") == "1"

# Discount rounding steps
ROUND_STEP_INR = int(os.getenv("DISCOUNT_ROUND_STEP_INR", "5"))
ROUND_STEP_USD = int(os.getenv("DISCOUNT_ROUND_STEP_USD", "5"))

# Price sync cadence
PRICE_SYNC_HOURS = int(os.getenv("PRICE_SYNC_HOURS", "6"))

# Persistence
DATA_DIR = os.getenv("DATA_DIR",".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)
PORT = int(os.getenv("PORT", "10000"))

def p(*parts): return os.path.join(DATA_DIR, *parts)
IN_LAST_SEEN   = p("in_last_seen.json")
US_LAST_SEEN   = p("us_last_seen.json")
STATE_PATH     = p("dual_state.json")
IN_SKU_INDEX   = p("in_sku_index.json")     # now includes india_base_variant_id
LOG_CSV        = p("dual_sync_log.csv")
LOG_JSONL      = p("dual_sync_log.jsonl")
DISC_STATE     = p("discount_state.json")   # per-variant originals
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
            raise RuntimeError(f"GQL errors: {data['errors']}")
        return data["data"]
    raise RuntimeError("GraphQL throttled/5xx repeatedly")

def rest_adjust_inventory(domain: str, token: str, inventory_item_id: int, location_id: int, delta: int) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/adjust.json"
    payload = {"inventory_item_id": int(inventory_item_id), "location_id": int(location_id), "available_adjustment": int(delta)}
    for attempt in range(1, 6):
        r = requests.post(url, headers=hdr(token), json=payload, timeout=30)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(_backoff(attempt, base=0.3, mx=8.0))
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"REST adjust {r.status_code}: {r.text}")
        return

# REST helpers for pricing
def rest_get_variant(domain: str, token: str, variant_id_num: int) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/variants/{variant_id_num}.json"
    r = requests.get(url, headers=hdr(token), timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"REST get variant {variant_id_num} {r.status_code}: {r.text}")
    return r.json().get("variant") or {}

def rest_update_variant_price(domain: str, token: str, variant_id_num: int, price: str) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/variants/{variant_id_num}.json"
    payload = {"variant": {"id": variant_id_num, "price": str(price)}}
    for attempt in range(1, 6):
        r = requests.put(url, headers=hdr(token), json=payload, timeout=30)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(_backoff(attempt, base=0.3, mx=8.0)); continue
        if r.status_code >= 400:
            raise RuntimeError(f"REST update price {r.status_code}: {r.text}")
        return

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

# logging
def ensure_log_header():
    need = (not os.path.exists(LOG_CSV)) or (os.path.getsize(LOG_CSV) == 0)
    if need:
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["ts","phase","shop","note","product_id","variant_id","sku","delta","message"])
def log_row(emoji_phase:str, shop:str, note:str, product_id:str="", variant_id:str="", sku:str="", delta:str="", message:str="", extra:dict=None, title:str="", before:str="", after:str="", collections:str=""):
    ensure_log_header()
    ts = now_ist_str()
    with open(LOG_CSV,"a",newline="",encoding="utf-8") as f:
        csv.writer(f).writerow([ts, shop, shop, note, product_id, variant_id, sku, delta, message])
    row = {"ts":ts,"phase":emoji_phase,"shop":shop,"note":note,"product_id":product_id,"variant_id":variant_id,"sku":sku,"delta":str(delta),"message":message,"title":title,"before":str(before),"after":str(after),"collections":collections}
    if extra: row.update(extra)
    with open(LOG_JSONL,"a",encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False)+"\n")
    if LOG_TO_STDOUT:
        human = f"{emoji_phase} {shop} {note} [SKU {sku}] pid={product_id} vid={variant_id} {before}→{after} Δ{delta} “{title}” — {message}"
        print(human, flush=True)

# --------------- GraphQL queries ---------------
QUERY_COLLECTION_PAGE_IN = f"""
query ($handle:String!, $cursor:String) {{
  collectionByHandle(handle:$handle){{
    products(first: 60, after:$cursor){{
      pageInfo{{ hasNextPage endCursor }}
      nodes{{
        id title status
        metafield(namespace:"{MF_NAMESPACE}", key:"sku"){{ value }}
        tdisc: metafield(namespace:"{MF_NAMESPACE}", key:"{TEMP_DISC_KEY}"){{ id value type }}
        variants(first: 100){{
          nodes{{
            id title sku inventoryQuantity
            inventoryItem{{ id tracked }}
            inventoryPolicy
          }}
        }}
        badges:  metafield(namespace:"{MF_NAMESPACE}", key:"{MF_BADGES_KEY}"){{ id value type }}
        dtime:   metafield(namespace:"{MF_NAMESPACE}", key:"{MF_DELIVERY_KEY}"){{ id value type }}
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
        id title
        metafield(namespace:"{MF_NAMESPACE}", key:"sku"){{ value }}
        tdisc: metafield(namespace:"{MF_NAMESPACE}", key:"{TEMP_DISC_KEY}"){{ id value type }}
        pIn:  metafield(namespace:"{MF_NAMESPACE}", key:"{MF_PRICE_IN_INDIA_KEY}"){{ id value type }}
        variants(first: 100){{
          nodes{{
            id sku inventoryQuantity
            inventoryItem{{ id }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# definition lookup cache
_MF_DEF_CACHE: Dict[Tuple[str,str,str], str] = {}
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
    badges_type  = (badges_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_BADGES_KEY)
    delivery_type= (dtime_node  or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_DELIVERY_KEY)

    mf_inputs = []
    if target_badge:
        if str(badges_type).startswith("list."):
            mf_inputs.append({"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":MF_BADGES_KEY,"type":badges_type,"value":json.dumps([target_badge])})
        else:
            mf_inputs.append({"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":MF_BADGES_KEY,"type":badges_type,"value":target_badge})
    else:
        if (badges_node or {}).get("id"):
            delm = "mutation($id:ID!){ metafieldDelete(input:{id:$id}){ userErrors{message} } }"
            gql(domain, token, delm, {"id": badges_node["id"]})

    mf_inputs.append({"ownerId": product_gid,"namespace":MF_NAMESPACE,"key":MF_DELIVERY_KEY,"type":delivery_type,"value":target_delivery})

    if mf_inputs:
        mutation = "mutation($mfs:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$mfs){ userErrors{ field message } } }"
        data = gql(domain, token, mutation, {"mfs": mf_inputs})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_row("⚠️","IN","WARN",product_id=gid_num(product_gid),message=f"metafieldsSet errors: {errs}")

def bump_sales_in(domain:str, token:str, product_gid:str,
                  sales_total_node:dict, sales_dates_node:dict,
                  sold:int, today:str):
    st_type = (sales_total_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, KEY_SALES) or "number_integer"
    sd_type = (sales_dates_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, KEY_DATES) or "date"
    try: current_total = int((sales_total_node or {}).get("value") or "0")
    except: current_total = 0
    new_total = current_total + int(sold)

    if sd_type.startswith("list."):
        existing = []
        raw = (sales_dates_node or {}).get("value")
        try:
            if isinstance(raw, str) and raw.strip().startswith("["):
                existing = json.loads(raw)
        except: existing = []
        if today not in existing: existing.append(today)
        sd_payload = {"ownerId":product_gid,"namespace":MF_NAMESPACE,"key":KEY_DATES,"type":"list.date","value":json.dumps(sorted(set(existing))[-365:])}
    elif sd_type == "date":
        sd_payload = {"ownerId":product_gid,"namespace":MF_NAMESPACE,"key":KEY_DATES,"type":"date","value":today}
    else:
        sd_payload = {"ownerId":product_gid,"namespace":MF_NAMESPACE,"key":KEY_DATES,"type":sd_type,"value":today}

    mutation = """
    mutation($mfs:[MetafieldsSetInput!]!){
      metafieldsSet(metafields:$mfs){ userErrors{ field message } }
    }"""
    mfs = [
        {"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": KEY_SALES, "type": st_type, "value": str(new_total)},
        sd_payload
    ]
    data = gql(domain, token, mutation, {"mfs": mfs})
    errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
    if errs:
        log_row("⚠️","IN","WARN", product_id=gid_num(product_gid), message=f"sales metafieldsSet errors: {errs}")

# --- temp discount helpers (apply-once + revert-on-sale) ---
def ceil_to_step(value: float, step: int) -> float:
    if step <= 1: return math.ceil(value * 100) / 100.0
    return float(int(math.ceil(value / step) * step))

def discount_round_step_for_domain(domain: str) -> int:
    return ROUND_STEP_INR if domain == IN_DOMAIN else ROUND_STEP_USD

def load_disc_state() -> Dict[str, Any]:
    return load_json(DISC_STATE, {})

def save_disc_state(state: Dict[str, Any]) -> None:
    save_json(DISC_STATE, state)

def disc_key(shop_tag: str, pid: str, vid: str) -> str:
    return f"{shop_tag}:{pid}:{vid}"

def parse_percent(node: dict) -> int:
    v = ((node or {}).get("value") or "").strip()
    try: return max(0, int(float(v)))
    except: return 0

def set_temp_discount_percent(domain:str, token:str, product_gid:str, node:dict, percent:int):
    mtype = (node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, TEMP_DISC_KEY) or "number_integer"
    mutation = "mutation($mfs:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$mfs){ userErrors{ field message } } }"
    mfs = [{"ownerId":product_gid,"namespace":MF_NAMESPACE,"key":TEMP_DISC_KEY,"type":mtype,"value":str(int(percent))}]
    data = gql(domain, token, mutation, {"mfs": mfs})
    errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
    if errs:
        log_row("⚠️","IN","WARN", product_id=gid_num(product_gid), message=f"temp_discount_active set errors: {errs}")

def maybe_apply_temp_discount_for_product(shop_tag:str, domain:str, token:str, product_node:dict, availability:int):
    """
    Apply ONCE if temp_discount_active > 0 and availability > 0, and not already applied per variant.
    """
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
        if key in disc_state:
            continue  # already discounted earlier → don't re-apply

        try:
            var = rest_get_variant(domain, token, int(vid))
            cur_price = float(var.get("price") or 0.0)
            if cur_price <= 0:
                continue

            new_price = ceil_to_step(cur_price * (1.0 - (percent/100.0)), step)
            if new_price <= 0:
                continue
            if abs(new_price - cur_price) >= 0.01:
                # store original then update
                disc_state[key] = {"original_price": str(cur_price), "ts": now_ist_str()}
                rest_update_variant_price(domain, token, int(vid), str(int(new_price)))
                changed_any = True
                log_row("🏷️","DISC","APPLIED", product_id=pid, variant_id=vid, sku=sku, delta=f"-{percent}%",
                        title=title, message=f"Price {cur_price} → {new_price} (step {step})")
                time.sleep(MUTATION_SLEEP_SEC)
        except Exception as e:
            log_row("⚠️","DISC","WARN", product_id=pid, variant_id=vid, sku=sku, message=f"Apply discount error: {e}")

    if changed_any:
        save_disc_state(disc_state)

def revert_temp_discount_for_product(shop_tag:str, domain:str, token:str, product_node:dict):
    """
    If we had discounted this product earlier, restore all variants and set temp_discount_active=0.
    """
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
            rest_update_variant_price(domain, token, int(vid), str(int(original_price)))
            changed_any = True
            log_row("↩️","DISC","REVERTED", product_id=pid, variant_id=vid, sku=sku, title=title,
                    message=f"Restored price to {original_price}")
            time.sleep(MUTATION_SLEEP_SEC)
            disc_state.pop(key, None)
        except Exception as e:
            log_row("⚠️","DISC","WARN", product_id=pid, variant_id=vid, sku=sku, message=f"Revert discount error: {e}")

    if changed_any:
        save_disc_state(disc_state)
        set_temp_discount_percent(domain, token, product_node["id"], product_node.get("tdisc") or {}, 0)

# --------------- Index builder (India) ---------------
def build_in_sku_index():
    """
    Build exact SKU index for India and store: product_id, inventory_item_id, **india_base_variant_id**, title, status.
    """
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
                sku = (((p.get("metafield") or {}).get("value")) or "").strip()
                if not sku: continue
                variants = ((p.get("variants") or {}).get("nodes") or [])
                if not variants: continue
                inv_item_gid = (((variants[0].get("inventoryItem") or {}).get("id")) or "")
                if not inv_item_gid: continue
                inv_item_id = int(gid_num(inv_item_gid) or "0")
                base_vid = gid_num(variants[0].get("id"))
                index[sku] = {
                    "product_id": gid_num(p["id"]),
                    "inventory_item_id": inv_item_id,
                    "india_base_variant_id": base_vid,   # NEW
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
                sku = (((p.get("metafield") or {}).get("value")) or "").strip()
                variants = ((p.get("variants") or {}).get("nodes") or [])
                avail = compute_product_availability(variants, IN_INCLUDE_UNTRACKED)

                # Try applying discount once (percent >0) if we have stock
                if not read_only:
                    try:
                        maybe_apply_temp_discount_for_product("IN", IN_DOMAIN, IN_TOKEN, p, avail)
                    except Exception as e:
                        log_row("⚠️","DISC","WARN", product_id=pid, sku=sku, message=f"Apply discount pass error: {e}")

                # last_seen is never negative
                prev = _as_int_or_none(last_seen.get(pid))
                if prev is None: prev = 0

                # SALES (new < prev)
                if not read_only and avail < prev:
                    sold = prev - avail
                    bump_sales_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, sold, today)
                    log_row("🧾➖","IN","SALES_BUMP", product_id=pid, sku=sku, delta=f"-{sold}", message=f"avail {prev}->{avail} (sold={sold})", title=title, before=str(prev), after=str(avail))
                    # Immediately revert any temp discount and flip metafield to 0
                    try:
                        revert_temp_discount_for_product("IN", IN_DOMAIN, IN_TOKEN, p)
                    except Exception as e:
                        log_row("⚠️","DISC","WARN", product_id=pid, sku=sku, message=f"Revert on sale error: {e}")

                # clamp <0 → 0
                if not read_only and CLAMP_AVAIL_TO_ZERO and avail < 0 and variants:
                    inv_item_gid = (((variants[0].get("inventoryItem") or {}).get("id")) or "")
                    inv_item_id = int(gid_num(inv_item_gid) or "0")
                    try:
                        rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id, int(IN_LOCATION_ID), -avail)
                        log_row("🧰0️⃣","IN","CLAMP_TO_ZERO", product_id=pid, sku=sku, delta=f"+{-avail}", message=f"Raised availability to 0 on inventory_item_id={inv_item_id}", title=title, before=str(avail), after="0")
                        avail = 0
                    except Exception as e:
                        log_row("⚠️","IN","WARN", product_id=pid, sku=sku, message=f"Clamp error: {e}")

                # badges/delivery & optional status flip
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

                last_seen[pid] = max(0, int(avail))
                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            save_json(IN_LAST_SEEN, last_seen)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

# --------------- USA scan → mirror to India ---------------
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
                p_sku = (((p.get("metafield") or {}).get("value")) or "").strip()
                title = p.get("title") or ""

                # apply discount on US too if percent>0 and availability>0
                us_avail = sum(int(v0.get("inventoryQuantity") or 0) for v0 in ((p.get("variants") or {}).get("nodes") or []))
                if not read_only:
                    try:
                        maybe_apply_temp_discount_for_product("US", US_DOMAIN, US_TOKEN, p, us_avail)
                    except Exception as e:
                        log_row("⚠️","DISC","WARN", product_id=gid_num(p["id"]), message=f"US apply discount pass error: {e}")

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

                    # increases
                    if delta > 0:
                        if not MIRROR_US_INCREASES:
                            log_row("🙅‍♂️➕","US→IN","IGNORED_INCREASE", variant_id=vid, sku=sku_exact, delta=str(delta), title=title, message="US qty increase; mirroring disabled", before=str(prev), after=str(qty))
                            last_seen[vid] = qty
                            continue

                    # decreases → mirror to India
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
                            except Exception as e:
                                log_row("⚠️","US→IN","ERROR_APPLYING_DELTA", variant_id=vid, sku=sku_exact, delta=str(delta), title=title, message=str(e))

                    last_seen[vid] = qty
                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            save_json(US_LAST_SEEN, last_seen)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

# --------------- US priceinindia sync ---------------
def set_us_priceinindia(domain:str, token:str, product_gid:str, node:dict, value: str):
    """Type-safe set of custom.priceinindia at the PRODUCT level."""
    mtype = (node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_PRICE_IN_INDIA_KEY) or "single_line_text_field"
    mutation = "mutation($mfs:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$mfs){ userErrors{ field message } } }"
    mfs = [{"ownerId":product_gid,"namespace":MF_NAMESPACE,"key":MF_PRICE_IN_INDIA_KEY,"type":mtype,"value":str(value)}]
    data = gql(domain, token, mutation, {"mfs": mfs})
    errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
    if errs:
        log_row("⚠️","US→IN","WARN", product_id=gid_num(product_gid), message=f"priceinindia set errors: {errs}")

def maybe_sync_us_priceinindia():
    """
    Every PRICE_SYNC_HOURS, update US product.metafield custom.priceinindia
    to equal India base variant price for the same SKU (if available).
    """
    try:
        state = load_json(STATE_PATH, {})
        last = state.get("last_price_sync_ts")
        due = True
        if last:
            try:
                last_dt = datetime.fromisoformat(last.replace(" +0530",""))
                due = (now_ist() - last_dt) >= timedelta(hours=PRICE_SYNC_HOURS)
            except:
                due = True
        if not due:
            return

        in_index = load_json(IN_SKU_INDEX, {})
        # iterate all US collections/products
        for handle in US_COLLECTIONS:
            cursor=None
            while True:
                data = gql(US_DOMAIN, US_TOKEN, QUERY_COLLECTION_PAGE_US, {"handle": handle, "cursor": cursor})
                coll = data.get("collectionByHandle")
                if not coll: break
                prods = ((coll.get("products") or {}).get("nodes") or [])
                pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
                for p in prods:
                    p_sku = (((p.get("metafield") or {}).get("value")) or "").strip()
                    if not p_sku: continue
                    idx = in_index.get(p_sku)
                    if not idx: continue
                    base_vid = idx.get("india_base_variant_id")
                    if not base_vid: continue
                    try:
                        v = rest_get_variant(IN_DOMAIN, IN_TOKEN, int(base_vid))
                        in_price = v.get("price")
                        if in_price is None: continue
                        current_pi = ((p.get("pIn") or {}).get("value")) or ""
                        if str(current_pi).strip() != str(in_price).strip():
                            set_us_priceinindia(US_DOMAIN, US_TOKEN, p["id"], p.get("pIn") or {}, str(in_price))
                            log_row("🏷️","US","PRICEINDIA_SET", product_id=gid_num(p["id"]), sku=p_sku, delta=str(in_price),
                                    message=f"Set US {MF_NAMESPACE}.{MF_PRICE_IN_INDIA_KEY} = {in_price}", title=p.get("title") or "")
                            time.sleep(MUTATION_SLEEP_SEC)
                    except Exception as e:
                        log_row("⚠️","US","PRICEINDIA_WARN", product_id=gid_num(p["id"]), sku=p_sku, message=str(e))
                sleep_ms(SLEEP_BETWEEN_PAGES_MS)
                if pageInfo.get("hasNextPage"):
                    cursor = pageInfo.get("endCursor")
                else:
                    break

        state["last_price_sync_ts"] = now_ist_str()
        save_json(STATE_PATH, state)
    except Exception as e:
        log_row("⚠️","US","PRICEINDIA_WARN", message=f"sync error: {e}")

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
        index   = load_json(IN_SKU_INDEX, {})
        first_cycle = (not state.get("initialized", False)) and (len(in_seen)==0 and len(us_seen)==0)

        # Always (re)build index before scans (now includes india_base_variant_id)
        try:
            build_in_sku_index()
        except Exception as e:
            log_row("⚠️","SCHED","WARN", message=str(e))

        # India scan
        scan_india_and_update(read_only=first_cycle)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

        # USA scan
        scan_usa_and_mirror_to_india(read_only=first_cycle)

        # Periodic US priceinindia sync
        maybe_sync_us_priceinindia()

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
        "round_step_inr": ROUND_STEP_INR,
        "round_step_usd": ROUND_STEP_USD,
        "temp_discount_key": f"{MF_NAMESPACE}.{TEMP_DISC_KEY}",
        "price_sync_hours": PRICE_SYNC_HOURS,
        "priceinindia_key": f"{MF_NAMESPACE}.{MF_PRICE_IN_INDIA_KEY}"
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
