#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dual-site availability sync — Render-friendly (no collection-level query arg)

Fixes:
- Removed GraphQL `query:` argument under collection products (Shopify doesn't allow it).
- Filter ACTIVE products client-side when ONLY_ACTIVE_FOR_MAPPING=1.

Behaviors
- First full cycle is READ-ONLY (learn baselines).
- IN scan:
  * Compute availability (tracked-only by default).
  * On availability drop: bump sales_total & sales_dates.
  * Set badges/delivery each run after first cycle.
  * SPECIAL collection status flip ACTIVE<->DRAFT by availability.
  * If availability negative, CLAMP to 0 and set last_seen=0.
- US scan:
  * Mirror ONLY NEGATIVE deltas to IN via exact product.custom.sku.
  * Increases ignored unless MIRROR_US_INCREASES=1.

Logging: CSV + JSONL in DATA_DIR. Toggle GraphQL debug with LOG_GRAPHQL=1.
"""

import os, sys, time, json, csv, random
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import requests
from flask import Flask, request, jsonify, make_response

# ---------- Config ----------
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
IN_INCLUDE_UNTRACKED = os.getenv("IN_INCLUDE_UNTRACKED", "0") == "1"
IN_CHANGE_STATUS = os.getenv("IN_CHANGE_STATUS", "0") == "1"

# USA
US_DOMAIN = os.getenv("US_DOMAIN", "").strip()
US_TOKEN  = os.getenv("US_TOKEN", "").strip()
US_LOCATION_ID = os.getenv("US_LOCATION_ID", "").strip()
US_COLLECTIONS = [x.strip() for x in os.getenv("US_COLLECTION_HANDLES","").split(",") if x.strip()]

# Mapping/filtering
USE_PRODUCT_CUSTOM_SKU = os.getenv("USE_PRODUCT_CUSTOM_SKU", "1") == "1"
ONLY_ACTIVE_FOR_MAPPING = os.getenv("ONLY_ACTIVE_FOR_MAPPING", "1") == "1"

# US→IN increases mirroring
MIRROR_US_INCREASES = os.getenv("MIRROR_US_INCREASES", "0") == "1"

# Clamp negatives to zero
CLAMP_AVAIL_TO_ZERO = os.getenv("CLAMP_AVAIL_TO_ZERO", "1") == "1"

# Metafields (India)
MF_NAMESPACE = os.getenv("MF_NAMESPACE", "custom")
MF_BADGES_KEY = os.getenv("MF_BADGES_KEY", "badges")
MF_DELIVERY_KEY = os.getenv("MF_DELIVERY_KEY", "delivery_time")
KEY_SALES = os.getenv("KEY_SALES", "sales_total")
KEY_DATES = os.getenv("KEY_DATES", "sales_dates")
KEY_SKU   = os.getenv("KEY_SKU", "sku")

BADGE_READY    = "Ready To Ship"
DELIVERY_READY = "2-5 Days Across India"
DELIVERY_MTO   = "12-15 Days Across India"

# Special collection
SPECIAL_STATUS_HANDLE = os.getenv(
    "SPECIAL_STATUS_HANDLE",
    "budget-gold-plated-jewelry-premium-jadau-ornaments-on-mix-metal-base"
).strip()

# Persistence
DATA_DIR = os.getenv("DATA_DIR", ".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_TO_STDOUT = os.getenv("LOG_TO_STDOUT", "1") == "1"
LOG_GRAPHQL = os.getenv("LOG_GRAPHQL", "0") == "1"

def p(*parts): return os.path.join(DATA_DIR, *parts)
PORT = int(os.getenv("PORT", os.getenv("PORT", "10000")))

IN_LAST_SEEN  = p("in_last_seen.json")
US_LAST_SEEN  = p("us_last_seen.json")
STATE_PATH    = p("dual_state.json")
LOG_CSV       = p("dual_sync_log.csv")
LOG_JSONL     = p("dual_sync_log.jsonl")
IN_SKU_INDEX  = p("in_sku_index.json")

# ---------- HTTP helpers ----------
def hdr(token: str) -> Dict[str, str]:
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json", "Accept": "application/json"}

def gql(domain: str, token: str, query: str, variables: dict = None) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    last = {"query": query, "variables": variables or {}}
    for attempt in range(1, 8):
        r = requests.post(url, headers=hdr(token), json={"query": query, "variables": variables or {}}, timeout=60)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(10.0, 0.4 * (2 ** (attempt-1))) + random.uniform(0,0.25)); continue
        try:
            data = r.json()
        except Exception:
            if LOG_GRAPHQL:
                print(f"⚠️ GQL RAW_ERROR body={r.text[:400]}", flush=True)
                print(f"⚙️ GQL LAST_QUERY {json.dumps(last)[:800]}", flush=True)
            raise
        if r.status_code != 200:
            if LOG_GRAPHQL:
                print(f"⚠️ GQL HTTP{r.status_code} body={r.text[:400]}", flush=True)
                print(f"⚙️ GQL LAST_QUERY {json.dumps(last)[:800]}", flush=True)
            raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")
        if data.get("errors"):
            if LOG_GRAPHQL:
                print(f"⚠️ GQL ERRORS {json.dumps(data['errors'])[:400]}", flush=True)
                print(f"⚙️ GQL LAST_QUERY {json.dumps(last)[:800]}", flush=True)
            if any(((e.get('extensions') or {}).get('code','').upper() == 'THROTTLED') for e in data["errors"]):
                time.sleep(min(10.0, 0.4 * (2 ** (attempt-1))) + random.uniform(0,0.25)); continue
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]
    raise RuntimeError("GraphQL throttled/5xx repeatedly")

def rest_adjust_inventory(domain: str, token: str, inventory_item_id: int, location_id: int, delta: int) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/adjust.json"
    payload = {"inventory_item_id": int(inventory_item_id), "location_id": int(location_id), "available_adjustment": int(delta)}
    for attempt in range(1, 6):
        r = requests.post(url, headers=hdr(token), json=payload, timeout=30)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(8.0, 0.3 * (2 ** (attempt-1))) + random.uniform(0,0.25)); continue
        if r.status_code >= 400: raise RuntimeError(f"REST adjust {r.status_code}: {r.text}")
        return

# ---------- Utils ----------
def now_ist(): return datetime.now(timezone(timedelta(hours=5, minutes=30)))
def now_ist_str(): return now_ist().strftime("%Y-%m-%d %H:%M:%S %z")
def today_ist_str(): return now_ist().date().isoformat()
def sleep_ms(ms: int): time.sleep(max(0, ms) / 1000.0)
def normalize_sku(s: str) -> str: return (s or "").strip().lower()
def gid_num(gid: str) -> str: return (gid or "").split("/")[-1]

def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return default

def save_json(path: str, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(obj, f, indent=2)
    os.replace(tmp, path)

def ensure_log_header():
    need_csv = (not os.path.exists(LOG_CSV)) or (os.path.getsize(LOG_CSV) == 0)
    if need_csv:
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["ts","phase","shop","note","product_id","variant_id","sku","delta","message"])

def log_row(phase:str, shop:str, note:str, product_id="", variant_id="", sku="", delta="", message="", title="", before="", after="", collections=""):
    ensure_log_header()
    ts = now_ist_str()
    if LOG_TO_STDOUT:
        emoji = {
            "INDEX_BUILT": "🗂️", "APPLIED_DELTA": "🔁", "IGNORED_INCREASE": "🙅‍♂️➕",
            "SALES_BUMP": "🧾➖", "RESTOCK": "📦➕", "CLAMP_TO_ZERO": "🧰0️⃣",
            "WARN": "⚠️", "STATUS_TO_DRAFT": "🟥", "STATUS_TO_ACTIVE": "🟩",
        }.get(note, "•")
        print(f"{emoji} {phase} {note} [SKU {sku}] pid={product_id} vid={variant_id} {before}→{after} Δ{delta} “{title}” — {message}", flush=True)
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([ts,phase,shop,note,product_id,variant_id,sku,delta,message])
    try:
        with open(LOG_JSONL, "a", encoding="utf-8") as jf:
            jf.write(json.dumps({
                "ts": ts, "phase": phase, "shop": shop, "note": note,
                "product_id": str(product_id or ""), "variant_id": str(variant_id or ""),
                "sku": str(sku or ""), "delta": str(delta or ""), "message": str(message or ""),
                "title": str(title or ""), "before": str(before or ""), "after": str(after or ""),
                "collections": str(collections or "")
            }) + "\n")
    except Exception:
        pass

def _as_int_or_none(v):
    try:
        if v is None: return None
        if isinstance(v,(int,float)): return int(v)
        s = str(v).strip()
        if s in ("","-"): return None
        return int(s)
    except Exception:
        return None

# ---------- State ----------
def load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return {"initialized": False}

def save_state(obj):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(obj, f, indent=2)
    os.replace(tmp, STATE_PATH)

# ---------- GraphQL (no collection-level query arg) ----------
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
        id title status
        variants(first: 100){
          nodes{
            id sku inventoryQuantity
            inventoryItem{ id }
          }
        }
        skuMeta: metafield(namespace: "custom", key: "sku"){ value }
      }
    }
  }
}
"""

QUERY_FIND_IN_PRODUCT_BY_SKU = """
query ($q:String!){
  products(first: 10, query: $q){
    nodes{
      id title
      variants(first: 1){ nodes{ inventoryItem{ id } } }
    }
  }
}
"""

# ---------- IN helpers ----------
def compute_product_availability(variants: List[dict], include_untracked: bool) -> int:
    total = 0; counted = False
    for v in variants:
        tracked = bool(((v.get("inventoryItem") or {}).get("tracked")))
        qty = int(v.get("inventoryQuantity") or 0)
        if include_untracked or tracked:
            counted = True; total += qty
    return total if counted else 0

def desired_state(avail: int) -> Tuple[str, str, str]:
    return ("ACTIVE", BADGE_READY, DELIVERY_READY) if avail > 0 else ("DRAFT", "", DELIVERY_MTO)

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
        log_row("IN", "IN", "WARN", product_id=gid_num(product_gid), message=str(errs)); return False
    time.sleep(MUTATION_SLEEP_SEC); return True

def set_product_metafields_in(domain:str, token:str, product_gid:str, badges_node:dict, dtime_node:dict,
                              target_badge:str, target_delivery:str, title:str="", sku:str="", pid:str=""):
    btype = (badges_node or {}).get("type") or "single_line_text_field"
    dtype = (dtime_node or {}).get("type") or "single_line_text_field"
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
            log_row("IN", "IN", "WARN", product_id=gid_num(product_gid), sku=sku, title=title,
                    message=f"metafieldsSet errors: {errs}")

def bump_sales_in(domain:str, token:str, product_gid:str, sales_total_node:dict, sales_dates_node:dict, sold:int, today:str,
                  sku:str="", pid:str="", title:str=""):
    st_type = (sales_total_node or {}).get("type") or "number_integer"
    sd_type = (sales_dates_node or {}).get("type") or "list.date"
    try: current = int((sales_total_node or {}).get("value") or "0")
    except Exception: current = 0
    new_total = current + int(sold)
    if sd_type == "list.date":
        existing = []
        raw = (sales_dates_node or {}).get("value")
        try:
            existing = json.loads(raw) if isinstance(raw,str) and raw.startswith("[") else []
        except Exception:
            existing = []
        if today not in existing: existing.append(today)
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
        log_row("IN", "IN", "WARN", product_id=gid_num(product_gid), sku=sku, title=title,
                message=f"sales metafieldsSet errors: {errs}")

def compute_availability_and_title(p: dict, include_untracked: bool) -> Tuple[int, str]:
    variants = ((p.get("variants") or {}).get("nodes") or [])
    avail = compute_product_availability(variants, include_untracked)
    title = p.get("title") or ""
    return avail, title

def clamp_to_zero_if_needed(domain: str, token: str, product_node: dict, pid_num: str, current_avail: int, sku: str, title: str):
    if not CLAMP_AVAIL_TO_ZERO or current_avail >= 0: return current_avail, False
    variants = ((product_node.get("variants") or {}).get("nodes") or [])
    if not variants: return current_avail, False
    need = -current_avail
    inv_item_gid = ((variants[0].get("inventoryItem") or {}).get("id") or "")
    inv_item_id = int(gid_num(inv_item_gid) or "0")
    if inv_item_id > 0:
        try:
            rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id, int(IN_LOCATION_ID), need)
            log_row("IN", "IN", "CLAMP_TO_ZERO",
                    product_id=pid_num, sku=sku, delta=f"+{need}",
                    message=f"Raised availability to 0 on inventory_item_id={inv_item_id}",
                    title=title, before=str(current_avail), after="0")
            time.sleep(MUTATION_SLEEP_SEC)
            return 0, True
        except Exception as e:
            log_row("IN", "IN", "WARN", product_id=pid_num, sku=sku, title=title, message=f"Clamp to zero failed: {e}")
    return current_avail, False

# ---------- IN scan ----------
def scan_india_and_update(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(IN_LAST_SEEN, {})
    today = today_ist_str()

    # Build/refresh IN index (exact custom.sku)
    idx: Dict[str, Dict[str, Any]] = {}
    for handle in IN_COLLECTIONS:
        cursor = None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                log_row("IN", "IN", "WARN", message=f"Collection not found: {handle}"); break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                if ONLY_ACTIVE_FOR_MAPPING and (p.get("status","").upper()!="ACTIVE"):  # client-side filter
                    continue
                pid = gid_num(p["id"]); title = p.get("title") or ""
                sku_val = (((p.get("skuMeta") or {}).get("value")) or "").strip()
                if sku_val:
                    sku_norm = normalize_sku(sku_val)
                    variants = ((p.get("variants") or {}).get("nodes") or [])
                    inv_item_id = 0
                    if variants:
                        inv_item_id = int(gid_num(((variants[0].get("inventoryItem") or {}).get("id") or "")) or "0")
                    idx[sku_norm] = {"inventory_item_id": inv_item_id, "product_id": pid, "title": title}
                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor"); sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            else: break
    save_json(IN_SKU_INDEX, idx)
    log_row("IN", "IN", "INDEX_BUILT", message=f"entries={len(idx)}")

    # Apply sales/metafields/status/clamp
    for handle in IN_COLLECTIONS:
        cursor = None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll: break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                if ONLY_ACTIVE_FOR_MAPPING and (p.get("status","").upper()!="ACTIVE"):
                    continue
                pid = gid_num(p["id"])
                sku_val = (((p.get("skuMeta") or {}).get("value")) or "").strip()
                avail, title = compute_availability_and_title(p, IN_INCLUDE_UNTRACKED)

                # Clamp negatives (and set last_seen=0)
                if not read_only and avail < 0 and CLAMP_AVAIL_TO_ZERO:
                    avail, did = clamp_to_zero_if_needed(IN_DOMAIN, IN_TOKEN, p, pid, avail, sku_val, title)
                    if did: last_seen[pid] = 0

                prev = _as_int_or_none(last_seen.get(pid))
                if prev is None:
                    last_seen[pid] = max(0, int(avail))
                else:
                    diff = int(avail) - int(prev)
                    if not read_only and diff < 0:
                        bump_sales_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, -diff, today,
                                      sku=sku_val, pid=pid, title=title)
                        log_row("IN", "IN", "SALES_BUMP",
                                product_id=pid, sku=sku_val, delta=diff,
                                message=f"avail {prev}->{avail} (sold={-diff})", title=title, before=str(prev), after=str(avail))
                    last_seen[pid] = max(0, int(avail))

                # badges/delivery
                _, target_badge, target_delivery = desired_state(avail)
                if not read_only:
                    set_product_metafields_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("badges") or {}, p.get("dtime") or {},
                                              target_badge, target_delivery, title=title, sku=sku_val, pid=pid)

                # SPECIAL STATUS RULE
                if (not read_only) and IN_CHANGE_STATUS and (handle == SPECIAL_STATUS_HANDLE):
                    current_status = (p.get("status") or "").upper()
                    if avail < 1 and current_status == "ACTIVE":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "DRAFT")
                        log_row("IN", "IN", "STATUS_TO_DRAFT" if ok else "WARN",
                                product_id=pid, sku=sku_val, delta=str(avail), message=f"handle={handle}", title=title)
                    elif avail >= 1 and current_status == "DRAFT":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "ACTIVE")
                        log_row("IN", "IN", "STATUS_TO_ACTIVE" if ok else "WARN",
                                product_id=pid, sku=sku_val, delta=str(avail), message=f"handle={handle}", title=title)

                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            save_json(IN_LAST_SEEN, {k:int(v) for k,v in last_seen.items()})
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor"); sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            else: break

# ---------- US scan (mirror decreases only) ----------
def find_in_index_by_sku_exact(sku_val: str) -> Optional[Dict[str, Any]]:
    idx = load_json(IN_SKU_INDEX, {})
    return idx.get(normalize_sku(sku_val))

def scan_usa_and_mirror_to_india(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(US_LAST_SEEN, {})

    for handle in US_COLLECTIONS:
        cursor = None
        while True:
            data = gql(US_DOMAIN, US_TOKEN, QUERY_COLLECTION_PAGE_US, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                log_row("US", "US", "WARN", message=f"Collection not found: {handle}"); break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                if ONLY_ACTIVE_FOR_MAPPING and (p.get("status","").upper()!="ACTIVE"):
                    continue
                title = p.get("title") or ""
                prod_sku = ((((p.get("skuMeta") or {}).get("value")) or "").strip())
                for v in ((p.get("variants") or {}).get("nodes") or []):
                    vgid = v.get("id"); vid = gid_num(vgid)
                    raw_sku = v.get("sku") or prod_sku or ""
                    qty = int(v.get("inventoryQuantity") or 0)

                    prev = _as_int_or_none(last_seen.get(vid))
                    if prev is None:
                        last_seen[vid] = qty
                    else:
                        delta = qty - int(prev)
                        if delta != 0:
                            if delta > 0 and not MIRROR_US_INCREASES:
                                log_row("US→IN", "US", "IGNORED_INCREASE",
                                        variant_id=vid, sku=raw_sku, delta=delta,
                                        message="US qty increase; mirroring disabled", title=title, before=str(prev), after=str(qty))
                            else:
                                if read_only:
                                    log_row("US→IN", "IN", "FIRST_CYCLE_SKIP",
                                            variant_id=vid, sku=raw_sku, delta=delta,
                                            message="Global first cycle is read-only; no adjust applied", title=title, before=str(prev), after=str(qty))
                                else:
                                    entry = find_in_index_by_sku_exact(raw_sku)
                                    if not entry or not int(entry.get("inventory_item_id") or 0):
                                        log_row("US→IN", "US", "WARN",
                                                variant_id=vid, sku=raw_sku, delta=delta,
                                                message="No exact match in IN index for product.custom.sku", title=title, before=str(prev), after=str(qty))
                                    else:
                                        try:
                                            rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, int(entry["inventory_item_id"]), int(IN_LOCATION_ID), delta)
                                            log_row("US→IN", "IN", "APPLIED_DELTA",
                                                    variant_id=vid, sku=raw_sku, delta=delta,
                                                    message=f"Adjusted IN inventory_item_id={entry['inventory_item_id']} by {delta} (via EXACT index)",
                                                    title=entry.get("title",""), before=str(prev), after=str(qty))
                                            time.sleep(MUTATION_SLEEP_SEC)
                                        except Exception as e:
                                            log_row("US→IN", "IN", "WARN",
                                                    variant_id=vid, sku=raw_sku, delta=delta,
                                                    message=f"Error applying delta to IN: {e}", title=title, before=str(prev), after=str(qty))
                            last_seen[vid] = qty

                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
            save_json(US_LAST_SEEN, {k:int(v) for k,v in last_seen.items()})
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor"); sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            else: break

# ---------- Scheduler ----------
from threading import Thread, Lock
run_lock = Lock()
is_running = False

def run_cycle():
    global is_running
    with run_lock:
        if is_running: return "busy"
        is_running = True
    try:
        state = load_state()
        in_seen = load_json(IN_LAST_SEEN, {})
        us_seen = load_json(US_LAST_SEEN, {})
        first_cycle = (not state.get("initialized", False)) and (len(in_seen)==0 and len(us_seen)==0)

        scan_india_and_update(read_only=first_cycle)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)
        scan_usa_and_mirror_to_india(read_only=first_cycle)

        if first_cycle:
            state["initialized"] = True
            save_state(state)
            log_row("SCHED", "BOTH", "FIRST_CYCLE_DONE", message="Baselines learned; future cycles will apply deltas")
        return "ok"
    except Exception as e:
        log_row("SCHED", "BOTH", "WARN", message=str(e)); return "error"
    finally:
        with run_lock: is_running = False

# ---------- Flask ----------
app = Flask(__name__)

def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
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
        "scheduler_enabled": ENABLE_SCHEDULER,
        "run_every_min": RUN_EVERY_MIN,
        "use_product_custom_sku": USE_PRODUCT_CUSTOM_SKU,
        "only_active_for_mapping": ONLY_ACTIVE_FOR_MAPPING,
        "mirror_us_increases": MIRROR_US_INCREASES,
        "clamp_avail_to_zero": CLAMP_AVAIL_TO_ZERO,
        "in_last_seen_count": len(load_json(IN_LAST_SEEN, {})),
        "us_last_seen_count": len(load_json(US_LAST_SEEN, {})),
        "index_entries": len(load_json(IN_SKU_INDEX, {})),
    }
    return _cors(jsonify(info)), 200

@app.route("/run", methods=["POST"])
def run_now():
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET: return _cors(make_response(("forbidden", 403)))
    status = run_cycle()
    return _cors(jsonify({"status": status})), 200 if status == "ok" else 409

def scheduler_loop():
    if RUN_EVERY_MIN <= 0: return
    while True:
        try: run_cycle()
        except Exception as e: log_row("SCHED", "BOTH", "WARN", message=str(e))
        time.sleep(max(1, RUN_EVERY_MIN) * 60)

if ENABLE_SCHEDULER:
    Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    print(f"[BOOT] Dual sync on port {PORT} | API {API_VERSION}")
    print(f"[CFG] IN={IN_DOMAIN} (loc {IN_LOCATION_ID}) | US={US_DOMAIN} (loc {US_LOCATION_ID}) | DATA_DIR={DATA_DIR}")
    run_simple("0.0.0.0", PORT, app)
