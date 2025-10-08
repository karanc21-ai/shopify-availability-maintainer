#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dual-site availability sync (IN ↔ US) — Render-friendly

Key behaviors
-------------
• First cycle = bootstrap (learn baselines, but we STILL clamp negatives to 0).
• India scan:
   - Compute availability (sum of counted variants).
   - If avail < 0 → clamp to 0 immediately (raise inventory by +abs(avail)).
   - If avail decreased vs last_seen → SALES_BUMP (sales_total += delta, record date).
   - Enforce metafields every scan:
       * avail > 0  → badges="Ready To Ship", delivery="2-5 Days Across India"
       * avail == 0 → badges="",           delivery="12-15 Days Across India"
   - Build India SKU index (custom.sku → {pid, title, inv_item_id}) and log INDEX_BUILT.

• US scan:
   - If qty < 0 → clamp to 0 on US shop too (raise by +abs(qty)).
   - If qty dropped vs last_seen → mirror delta to India by EXACT SKU using India index.
   - If qty increased → IGNORE (do nothing on India).
   - Log a symmetric "US INDEX_BUILT" (a light snapshot) for visibility.

• Last-seen persistence never stores negatives (store max(reading, 0)).

HTTP
----
/health    → 200 ok
/run?key=… → run one full cycle (IN then US) with lock
/diag      → prints current config & counters

Environment (minimal set)
-------------------------
PIXEL_SHARED_SECRET=...          # shared key to trigger /run
IN_DOMAIN=...                    # e.g. silver-rudradhan.myshopify.com
IN_TOKEN=...                     # Admin access token
IN_LOCATION_ID=...               # numeric location id for IN
IN_COLLECTION_HANDLES=gold-plated,another-collection
US_DOMAIN=...                    # e.g. 897265-11.myshopify.com
US_TOKEN=...
US_LOCATION_ID=...
US_COLLECTION_HANDLES=gold-plated-jewellery

# Optional:
API_VERSION=2024-10
DATA_DIR=/data                   # Render persistent disk mount point
RUN_EVERY_MIN=5
ENABLE_SCHEDULER=1               # background loop
ONLY_ACTIVE_FOR_MAPPING=1        # when building index, only record active products
MIRROR_US_INCREASES=0            # increases on US are ignored (default)
CLAMP_AVAIL_TO_ZERO=1            # clamp negatives on both shops (default)
IN_INCLUDE_UNTRACKED=0           # count only tracked variants by default
"""

import os, sys, time, json, csv, random
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import requests
from flask import Flask, request, jsonify, make_response
from threading import Thread, Lock

# ---------------- Config ----------------
API_VERSION = os.getenv("API_VERSION", "2024-10").strip()

PIXEL_SHARED_SECRET = os.getenv("PIXEL_SHARED_SECRET", "").strip()
if not PIXEL_SHARED_SECRET:
    raise SystemExit("PIXEL_SHARED_SECRET required")

IN_DOMAIN = os.getenv("IN_DOMAIN", "").strip()
IN_TOKEN  = os.getenv("IN_TOKEN", "").strip()
IN_LOCATION_ID = os.getenv("IN_LOCATION_ID", "").strip()
IN_COLLECTIONS = [x.strip() for x in os.getenv("IN_COLLECTION_HANDLES", "").split(",") if x.strip()]
IN_INCLUDE_UNTRACKED = os.getenv("IN_INCLUDE_UNTRACKED", "0") == "1"

US_DOMAIN = os.getenv("US_DOMAIN", "").strip()
US_TOKEN  = os.getenv("US_TOKEN", "").strip()
US_LOCATION_ID = os.getenv("US_LOCATION_ID", "").strip()
US_COLLECTIONS = [x.strip() for x in os.getenv("US_COLLECTION_HANDLES", "").split(",") if x.strip()]

RUN_EVERY_MIN = int(os.getenv("RUN_EVERY_MIN", "5"))
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "0") == "1"

MIRROR_US_INCREASES = os.getenv("MIRROR_US_INCREASES", "0") == "1"  # default: ignore increases
CLAMP_AVAIL_TO_ZERO = os.getenv("CLAMP_AVAIL_TO_ZERO", "1") == "1"
ONLY_ACTIVE_FOR_MAPPING = os.getenv("ONLY_ACTIVE_FOR_MAPPING", "1") == "1"  # only index active IN products

# pacing
SLEEP_BETWEEN_PRODUCTS_MS = int(os.getenv("SLEEP_BETWEEN_PRODUCTS_MS", "120"))
SLEEP_BETWEEN_PAGES_MS    = int(os.getenv("SLEEP_BETWEEN_PAGES_MS", "400"))
SLEEP_BETWEEN_SHOPS_MS    = int(os.getenv("SLEEP_BETWEEN_SHOPS_MS", "800"))
MUTATION_SLEEP_SEC        = float(os.getenv("MUTATION_SLEEP_SEC", "0.35"))

# metafields
MF_NAMESPACE = "custom"
MF_BADGES_KEY = "badges"
MF_DELIVERY_KEY = "delivery_time"
KEY_SALES = "sales_total"
KEY_DATES = "sales_dates"

BADGE_READY    = "Ready To Ship"
DELIVERY_READY = "2-5 Days Across India"
DELIVERY_MTO   = "12-15 Days Across India"

# storage
DATA_DIR = os.getenv("DATA_DIR", ".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)
def p(*parts): return os.path.join(DATA_DIR, *parts)
PORT = int(os.getenv("PORT", "10000"))

IN_LAST_SEEN   = p("in_last_seen.json")     # pid(num) -> last_seen_avail (>=0)
US_LAST_SEEN   = p("us_last_seen.json")     # vid(num) -> last_seen_qty  (>=0)
IN_SKU_INDEX   = p("in_sku_index.json")     # sku_norm -> { pid, title, inv_item_id }
LOG_CSV        = p("dual_sync_log.csv")
LOG_JSONL      = p("dual_sync_log.jsonl")
STATE_PATH     = p("dual_state.json")       # {"initialized": bool}

# -------------- HTTP helpers --------------
def hdr(token: str) -> Dict[str, str]:
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json", "Accept": "application/json"}

def gql(domain: str, token: str, query: str, variables: dict = None) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    last_err = None
    for attempt in range(1, 8):
        try:
            r = requests.post(url, headers=hdr(token), json={"query": query, "variables": variables or {}}, timeout=60)
            if r.status_code == 429:
                time.sleep(min(10.0, 0.4 * (2 ** (attempt-1))) + random.uniform(0, 0.25))
                continue
            if r.status_code >= 500:
                last_err = f"HTTP {r.status_code}: {r.text}"
                time.sleep(min(10.0, 0.4 * (2 ** (attempt-1))) + random.uniform(0, 0.25))
                continue
            if r.status_code != 200:
                raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")
            data = r.json()
            if data.get("errors"):
                # log last query if schema errors
                print(f"⚠️ GQL ERRORS {json.dumps(data['errors'])}", flush=True)
                print(f"⚙️ GQL LAST_QUERY {json.dumps({'query': query[:800]})}", flush=True)
                # Retry only if throttled
                if any(((e.get("extensions") or {}).get("code","").upper()=="THROTTLED") for e in data["errors"]):
                    time.sleep(min(10.0, 0.4*(2**(attempt-1))) + random.uniform(0,0.25))
                    continue
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data["data"]
        except Exception as e:
            last_err = str(e)
            time.sleep(min(10.0, 0.4 * (2 ** (attempt-1))) + random.uniform(0, 0.25))
    raise RuntimeError(f"GraphQL throttled/5xx repeatedly: {last_err}")

def rest_adjust_inventory(domain: str, token: str, inventory_item_id: int, location_id: int, delta: int) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/adjust.json"
    payload = {"inventory_item_id": int(inventory_item_id), "location_id": int(location_id), "available_adjustment": int(delta)}
    last_err = None
    for attempt in range(1, 6):
        r = requests.post(url, headers=hdr(token), json=payload, timeout=30)
        if r.status_code == 429 or r.status_code >= 500:
            last_err = f"REST {r.status_code}: {r.text}"
            time.sleep(min(8.0, 0.3 * (2 ** (attempt-1))) + random.uniform(0, 0.25))
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"REST adjust {r.status_code}: {r.text}")
        return
    raise RuntimeError(last_err or "REST adjust failed")

# -------------- Utils --------------
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

def ensure_log_headers():
    if not os.path.exists(LOG_CSV):
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["ts","phase","shop","note","product_id","variant_id","sku","delta","message","title","before","after","collections"])

def log_hr(phase, shop, note, product_id, variant_id, sku, delta, message, title="", before="", after="", collections=""):
    ensure_log_headers()
    row = [now_ist_str(), phase, shop, note, str(product_id or ""), str(variant_id or ""), str(sku or ""), str(delta or ""), str(message or ""), str(title or ""), str(before or ""), str(after or ""), str(collections or "")]
    # pretty to STDOUT
    emoji = {
        "INDEX_BUILT":"🗂️", "SALES_BUMP":"🧾➖", "APPLIED_DELTA":"🔁", "IGNORED_INCREASE":"🙅‍♂️➕",
        "CLAMP_TO_ZERO":"🧰0️⃣", "RESTOCK":"📦➕", "WARN":"⚠️", "ERROR":"💥"
    }.get(note.split(":")[0], "•")
    pretty = f"{emoji} {phase} {note} [SKU {sku}] pid={product_id} vid={variant_id} {before}→{after} Δ{delta} “{title}” — {message}"
    print(pretty, flush=True)
    # CSV
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)
    # JSONL
    try:
        with open(LOG_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "ts": row[0], "phase":phase, "shop":shop, "note":note,
                "product_id":product_id, "variant_id":variant_id, "sku":sku,
                "delta":delta, "message":message, "title":title,
                "before":before, "after":after, "collections":collections
            })+"\n")
    except Exception:
        pass

def normalize_sku(s: str) -> str:
    # Your SKUs are exact across shops except casing/whitespace.
    return (s or "").strip().lower()

def _as_int_or_none(v):
    try:
        if v is None: return None
        if isinstance(v, (int, float)): return int(v)
        s = str(v).strip()
        if s in ("", "-"): return None
        return int(s)
    except Exception:
        return None

# -------------- GraphQL --------------
QUERY_COLLECTION_PAGE_IN = f"""
query ($handle:String!, $cursor:String) {{
  collectionByHandle(handle:$handle){{
    products(first: 60, after:$cursor){{
      pageInfo{{ hasNextPage endCursor }}
      nodes{{
        id title status
        variants(first: 100){{
          nodes{{
            id title sku inventoryQuantity
            inventoryItem{{ id tracked }}
            inventoryPolicy
          }}
        }}
        skuMeta: metafield(namespace: "{MF_NAMESPACE}", key: "sku"){{ value }}
        badges:  metafield(namespace: "{MF_NAMESPACE}", key: "{MF_BADGES_KEY}"){{ id value type }}
        dtime:   metafield(namespace: "{MF_NAMESPACE}", key: "{MF_DELIVERY_KEY}"){{ id value type }}
        salesTotal:  metafield(namespace: "{MF_NAMESPACE}", key: "{KEY_SALES}"){{ id value type }}
        salesDates:  metafield(namespace: "{MF_NAMESPACE}", key: "{KEY_DATES}"){{ id value type }}
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
        variants(first: 100){{
          nodes{{
            id sku inventoryQuantity
            inventoryItem{{ id }}
          }}
        }}
        skuMeta: metafield(namespace: "{MF_NAMESPACE}", key: "sku"){{ value }}
      }}
    }}
  }}
}}
"""

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
        log_hr("IN", "IN", "WARN", gid_num(product_gid), "", "", "", f"status update errors: {errs}")
        return False
    time.sleep(MUTATION_SLEEP_SEC)
    return True

def compute_availability(variants: List[dict], include_untracked: bool) -> Tuple[int, Optional[int]]:
    """
    Returns (availability_sum, first_counted_inventory_item_id)
    """
    total = 0
    first_inv_item_id = None
    for v in variants:
        tracked = bool(((v.get("inventoryItem") or {}).get("tracked")))
        qty = int(v.get("inventoryQuantity") or 0)
        if include_untracked or tracked:
            if first_inv_item_id is None:
                inv_gid = ((v.get("inventoryItem") or {}).get("id") or "")
                try:
                    first_inv_item_id = int(gid_num(inv_gid))
                except Exception:
                    first_inv_item_id = None
            total += qty
    return total, first_inv_item_id

def set_product_metafields_in(domain:str, token:str, product_gid:str, badges_node:dict, dtime_node:dict,
                              target_badge:str, target_delivery:str):
    # badges single_line_text_field
    btype = (badges_node or {}).get("type") or "single_line_text_field"
    dtype = (dtime_node  or {}).get("type") or "single_line_text_field"

    m = """
    mutation($mfs:[MetafieldsSetInput!]!){
      metafieldsSet(metafields:$mfs){
        userErrors{ field message }
      }
    }"""
    mfs = [
        {"ownerId":product_gid,"namespace":MF_NAMESPACE,"key":MF_BADGES_KEY,"type":btype,"value":(target_badge or "")},
        {"ownerId":product_gid,"namespace":MF_NAMESPACE,"key":MF_DELIVERY_KEY,"type":dtype,"value":(target_delivery or "")},
    ]
    data = gql(domain, token, m, {"mfs": mfs})
    errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
    if errs:
        log_hr("IN","IN","WARN", gid_num(product_gid),"","", "", f"metafieldsSet errors: {errs}")

def bump_sales_in(domain:str, token:str, product_gid:str, sales_total_node:dict, sales_dates_node:dict, sold:int, today:str):
    st_type = (sales_total_node or {}).get("type") or "number_integer"
    sd_type = (sales_dates_node or {}).get("type") or "list.date"
    try:
        current = int((sales_total_node or {}).get("value") or "0")
    except Exception:
        current = 0
    new_total = current + int(sold)

    if sd_type == "list.date":
        existing = []
        raw = (sales_dates_node or {}).get("value")
        try:
            existing = json.loads(raw) if isinstance(raw, str) and raw.strip().startswith("[") else []
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
        log_hr("IN","IN","WARN", gid_num(product_gid),"","", "", f"sales metafieldsSet errors: {errs}")

def desired_meta(avail: int) -> Tuple[str, str]:
    if avail > 0:
        return (BADGE_READY, DELIVERY_READY)
    else:
        return ("", DELIVERY_MTO)

# -------------- IN scan --------------
def scan_india_and_update(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(IN_LAST_SEEN, {})
    sku_index: Dict[str, dict] = {}
    today = today_ist_str()

    for handle in IN_COLLECTIONS:
        cursor = None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_COLLECTION_PAGE_IN, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                log_hr("IN","IN","WARN","","","","","Collection not found: "+handle)
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})

            for p in prods:
                pid = gid_num(p["id"])
                status = (p.get("status") or "").upper()
                raw_sku = ((p.get("skuMeta") or {}).get("value") or "").strip()
                sku_norm = normalize_sku(raw_sku)

                variants = ((p.get("variants") or {}).get("nodes") or [])
                avail, first_inv_item_id = compute_availability(variants, IN_INCLUDE_UNTRACKED)

                # Index only ACTIVE if ONLY_ACTIVE_FOR_MAPPING=1
                if sku_norm and (not ONLY_ACTIVE_FOR_MAPPING or status == "ACTIVE"):
                    if first_inv_item_id:
                        sku_index[sku_norm] = {"pid": pid, "title": p.get("title") or "", "inv_item_id": int(first_inv_item_id)}

                # Clamp negatives to zero (even first cycle) if enabled
                if CLAMP_AVAIL_TO_ZERO and avail < 0 and first_inv_item_id and IN_LOCATION_ID and not read_only:
                    try:
                        rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, first_inv_item_id, int(IN_LOCATION_ID), -avail)
                        log_hr("IN","IN","CLAMP_TO_ZERO", pid, "", raw_sku, +(-avail),
                               f"Raised availability to 0 on inventory_item_id={first_inv_item_id}",
                               title=p.get("title",""), before=str(avail), after="0")
                        time.sleep(MUTATION_SLEEP_SEC)
                        avail = 0
                    except Exception as e:
                        log_hr("IN","IN","ERROR", pid, "", raw_sku, +(-avail), str(e), p.get("title",""))

                prev = _as_int_or_none(last_seen.get(pid))
                if prev is None:
                    last_seen[pid] = max(0, int(avail))
                else:
                    # sales inference
                    if avail < prev and not read_only:
                        delta = int(prev) - int(avail)
                        bump_sales_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, delta, today)
                        log_hr("IN","IN","SALES_BUMP", pid, "", raw_sku, -delta,
                               f"avail {prev}->{avail} (sold={delta})", p.get("title",""), str(prev), str(avail))
                    # persist new baseline (never negative)
                    last_seen[pid] = max(0, int(avail))

                # Enforce metafields every time (except pure first boot read-only)
                if not read_only:
                    badge, dtime = desired_meta(avail)
                    set_product_metafields_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("badges") or {}, p.get("dtime") or {}, badge, dtime)

                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            # Save per page
            save_json(IN_LAST_SEEN, last_seen)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)

            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

    # Save SKU index & log
    save_json(IN_SKU_INDEX, sku_index)
    log_hr("IN","IN","INDEX_BUILT","","","", "", f"entries={len(sku_index)}")

# -------------- US scan --------------
def scan_usa_and_mirror_to_india(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(US_LAST_SEEN, {})
    in_index: Dict[str,dict] = load_json(IN_SKU_INDEX, {})

    # lightweight US index just for visibility
    us_index_count = 0

    for handle in US_COLLECTIONS:
        cursor = None
        while True:
            data = gql(US_DOMAIN, US_TOKEN, QUERY_COLLECTION_PAGE_US, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                log_hr("US","US","WARN","","","","","Collection not found: "+handle)
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})

            for p in prods:
                title = p.get("title") or ""
                for v in ((p.get("variants") or {}).get("nodes") or []):
                    vid = gid_num(v.get("id"))
                    raw_sku = (v.get("sku") or (p.get("skuMeta") or {}).get("value") or "").strip()
                    sku_norm = normalize_sku(raw_sku)
                    qty = int(v.get("inventoryQuantity") or 0)
                    inv_item_gid = ((v.get("inventoryItem") or {}).get("id") or "")
                    invid_us = int(gid_num(inv_item_gid) or "0") if inv_item_gid else 0

                    if raw_sku:
                        us_index_count += 1

                    prev = _as_int_or_none(last_seen.get(vid))
                    if prev is None:
                        last_seen[vid] = max(0, qty)
                    else:
                        delta = qty - int(prev)
                        if delta < 0:
                            # mirror drop to IN by EXACT SKU using index
                            hit = in_index.get(sku_norm) or {}
                            inv_item_id_in = int(hit.get("inv_item_id") or 0)
                            if not read_only and inv_item_id_in > 0 and IN_LOCATION_ID:
                                try:
                                    rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id_in, int(IN_LOCATION_ID), delta)
                                    log_hr("US→IN","IN","APPLIED_DELTA","", vid, raw_sku, delta,
                                           f"Adjusted IN inventory_item_id={inv_item_id_in} by {delta} (via EXACT index)",
                                           title)
                                    time.sleep(MUTATION_SLEEP_SEC)
                                except Exception as e:
                                    log_hr("US→IN","IN","ERROR","", vid, raw_sku, delta, str(e), title)
                            last_seen[vid] = max(0, qty)
                        elif delta > 0:
                            # increases ignored unless MIRROR_US_INCREASES=1
                            if MIRROR_US_INCREASES:
                                hit = in_index.get(sku_norm) or {}
                                inv_item_id_in = int(hit.get("inv_item_id") or 0)
                                if not read_only and inv_item_id_in > 0 and IN_LOCATION_ID:
                                    try:
                                        rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id_in, int(IN_LOCATION_ID), delta)
                                        log_hr("US→IN","IN","APPLIED_DELTA_INC","", vid, raw_sku, delta,
                                               f"Raised IN inventory_item_id={inv_item_id_in} by +{delta} (mirrored increase)",
                                               title)
                                        time.sleep(MUTATION_SLEEP_SEC)
                                    except Exception as e:
                                        log_hr("US→IN","IN","ERROR_INC","", vid, raw_sku, delta, str(e), title)
                            else:
                                log_hr("US→IN","US","IGNORED_INCREASE","", vid, raw_sku, delta,
                                       "US qty increase; mirroring disabled", title)
                            last_seen[vid] = max(0, qty)
                        else:
                            # unchanged
                            last_seen[vid] = max(0, qty)

                    # Clamp negatives on US too (even first cycle), if enabled
                    if CLAMP_AVAIL_TO_ZERO and qty < 0 and invid_us > 0 and US_LOCATION_ID and not read_only:
                        try:
                            rest_adjust_inventory(US_DOMAIN, US_TOKEN, invid_us, int(US_LOCATION_ID), -qty)
                            log_hr("US","US","CLAMP_TO_ZERO","", vid, raw_sku, +(-qty),
                                   f"Raised US availability to 0 on inventory_item_id={invid_us}", title, str(qty), "0")
                            time.sleep(MUTATION_SLEEP_SEC)
                            qty = 0
                            last_seen[vid] = 0
                        except Exception as e:
                            log_hr("US","US","ERROR","", vid, raw_sku, +(-qty), str(e), title)

                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            # Save per page
            save_json(US_LAST_SEEN, last_seen)
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)

            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

    # Symmetric visibility: US "index built"
    log_hr("US","US","INDEX_BUILT","","","", "", f"entries={us_index_count}")

# -------------- State & Scheduler --------------
def load_state():
    return load_json(STATE_PATH, {"initialized": False})

def save_state(obj):
    save_json(STATE_PATH, obj)

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
        # bootstrap means "we haven't seen anything yet"
        first_cycle = (not state.get("initialized", False)) and (len(in_seen) == 0 and len(us_seen) == 0)

        # IN first (metafields + sales + index + clamp)
        scan_india_and_update(read_only=False if not first_cycle else False)  # still False to allow clamp/metas on 1st
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

        # US then (mirror to IN + clamp)
        scan_usa_and_mirror_to_india(read_only=False if not first_cycle else False)

        if first_cycle:
            state["initialized"] = True
            save_state(state)
        return "ok"
    except Exception as e:
        log_hr("SCHED","BOTH","WARN","","","","", str(e))
        return "error"
    finally:
        with run_lock:
            is_running = False

# -------------- Flask --------------
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
    in_last = load_json(IN_LAST_SEEN, {})
    us_last = load_json(US_LAST_SEEN, {})
    in_idx  = load_json(IN_SKU_INDEX, {})
    return jsonify({
        "api_version": API_VERSION,
        "data_dir": DATA_DIR,
        "in_domain": IN_DOMAIN,
        "us_domain": US_DOMAIN,
        "in_collections": IN_COLLECTIONS,
        "us_collections": US_COLLECTIONS,
        "in_last_seen_count": len(in_last),
        "us_last_seen_count": len(us_last),
        "index_entries": len(in_idx),
        "run_every_min": RUN_EVERY_MIN,
        "scheduler_enabled": ENABLE_SCHEDULER,
        "only_active_for_mapping": ONLY_ACTIVE_FOR_MAPPING,
        "mirror_us_increases": MIRROR_US_INCREASES,
        "clamp_avail_to_zero": CLAMP_AVAIL_TO_ZERO,
    })

def scheduler_loop():
    if RUN_EVERY_MIN <= 0:
        return
    while True:
        try:
            run_cycle()
        except Exception as e:
            log_hr("SCHED","BOTH","WARN","","","","", str(e))
        time.sleep(max(1, RUN_EVERY_MIN) * 60)

if __name__ == "__main__":
    print(f"[BOOT] Dual sync on port {PORT} | API {API_VERSION}", flush=True)
    print(f"[CFG] IN={IN_DOMAIN} (loc {IN_LOCATION_ID}) | US={US_DOMAIN} (loc {US_LOCATION_ID}) | DATA_DIR={DATA_DIR}", flush=True)
    if ENABLE_SCHEDULER:
        Thread(target=scheduler_loop, daemon=True).start()
    from werkzeug.serving import run_simple
    run_simple("0.0.0.0", PORT, app)
