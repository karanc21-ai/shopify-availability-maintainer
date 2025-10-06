#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dual-site availability sync — Render-friendly (no global seeding needed)

- Per-item first sighting is learn-only (no deltas or sales bumps for that specific item).
- India (IN) scan:
   * Product availability from variants (tracked only by default unless IN_INCLUDE_UNTRACKED=1).
   * On availability drop vs last_seen: bump sales_total & sales_dates.
   * Update metafields (badges/delivery) after first sighting.
   * SPECIAL status rule for one collection (symmetric):
       - avail < 1 & status=ACTIVE  → DRAFT
       - avail ≥ 1 & status=DRAFT   → ACTIVE
- USA (US) scan:
   * Track per-variant qty; mirror delta to India by SKU (inventory_levels/adjust).
   * Warn if SKU missing or not found in India (with robust match attempts).

Render tips:
- Use a Persistent Disk mounted at /data and set DATA_DIR=/data (otherwise you’ll re-learn on each restart).
- Prefer a Background Worker (HEADLESS=1, ENABLE_SCHEDULER=1). Keep a tiny Web Service for /run and /diag.
- Sleep envs are in milliseconds; MUTATION_SLEEP_SEC is seconds.
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

SLEEP_BETWEEN_PRODUCTS_MS = int(os.getenv("SLEEP_BETWEEN_PRODUCTS_MS", "120"))
SLEEP_BETWEEN_PAGES_MS = int(os.getenv("SLEEP_BETWEEN_PAGES_MS", "400"))
SLEEP_BETWEEN_SHOPS_MS = int(os.getenv("SLEEP_BETWEEN_SHOPS_MS", "800"))
MUTATION_SLEEP_SEC = float(os.getenv("MUTATION_SLEEP_SEC", "0.35"))

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

BADGE_READY    = "Ready To Ship"
DELIVERY_READY = "2-5 Days Across India"
DELIVERY_MTO   = "12-15 Days Across India"

# Special collection where availability controls status (symmetric rule)
SPECIAL_STATUS_HANDLE = os.getenv(
    "SPECIAL_STATUS_HANDLE",
    "budget-gold-plated-jewelry-premium-jadau-ornaments-on-mix-metal-base"
).strip()

# Render / persistence
DATA_DIR = os.getenv("DATA_DIR", ".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_TO_STDOUT = os.getenv("LOG_TO_STDOUT", "1") == "1"

# Headless worker mode and fire-and-forget run
HEADLESS = os.getenv("HEADLESS", "0") == "1"
FIRE_AND_FORGET_RUN = os.getenv("FIRE_AND_FORGET_RUN", "1") == "1"

def p(*parts):  # path helper within DATA_DIR
    return os.path.join(DATA_DIR, *parts)

PORT = int(os.getenv("PORT", os.getenv("PORT", "10000")))  # Render injects PORT

# Last-seen persistence (no global seeding)
IN_LAST_SEEN  = p("in_last_seen.json")   # product_id(num) -> last_seen_availability
US_LAST_SEEN  = p("us_last_seen.json")   # variant_id(num)  -> last_seen_qty
STATE_PATH    = p("dual_state.json")     # {"initialized": bool}
LOG_CSV       = p("dual_sync_log.csv")   # CSV + stdout

# ---------- HTTP helpers ----------
def hdr(token: str) -> Dict[str, str]:
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json", "Accept": "application/json"}

def _backoff_sleep(attempt: int) -> None:
    time.sleep(min(10.0, 0.4 * (2 ** (attempt-1))) + random.uniform(0, 0.25))

def gql(domain: str, token: str, query: str, variables: dict = None) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    for attempt in range(1, 9):
        r = requests.post(url, headers=hdr(token), json={"query": query, "variables": variables or {}}, timeout=60)
        # Retry on throttles and transient 5xx from Shopify/CDN
        if r.status_code in (429, 500, 502, 503, 504):
            _backoff_sleep(attempt)
            continue
        if r.status_code != 200:
            raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")
        data = r.json()
        if data.get("errors"):
            # Retries on explicit THROTTLED errors
            if any(((e.get("extensions") or {}).get("code","").upper() == "THROTTLED") for e in data["errors"]):
                _backoff_sleep(attempt)
                continue
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]
    raise RuntimeError("GraphQL throttled/5xx repeatedly")

def rest_adjust_inventory(domain: str, token: str, inventory_item_id: int, location_id: int, delta: int) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/adjust.json"
    payload = {"inventory_item_id": int(inventory_item_id), "location_id": int(location_id), "available_adjustment": int(delta)}
    for attempt in range(1, 6):
        r = requests.post(url, headers=hdr(token), json=payload, timeout=30)
        if r.status_code in (429, 500, 502, 503, 504):
            _backoff_sleep(attempt)
            continue
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

def normalize_sku(s: str) -> str:
    s = (s or "").strip().lower().replace("_","-").replace(" ","-")
    while "--" in s:
        s = s.replace("--","-")
    return s

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

def summarize_product_skus(variants: List[dict], include_untracked: bool) -> str:
    """
    Returns a compact, human-readable SKU summary for a product (India logs).
    Counts only variants that contribute to availability. Shows up to 5 distinct SKUs.
    """
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

# ---------- State (first-cycle flag) ----------
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

# ---------- GraphQL queries ----------
QUERY_COLLECTION_PAGE_IN = """
query ($handle:String!, $cursor:String) {
  collectionByHandle(handle:$handle){
    products(first: 40, after:$cursor){
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
        badges: metafield(namespace: "%(NS)s", key: "%(BK)s"){ id value type }
        dtime:  metafield(namespace: "%(NS)s", key: "%(DK)s"){ id value type }
        salesTotal: metafield(namespace: "%(NS)s", key: "%(KS)s"){ id value type }
        salesDates: metafield(namespace: "%(NS)s", key: "%(KD)s"){ id value type }
      }
    }
  }
}
""".replace("%(NS)s", MF_NAMESPACE).replace("%(BK)s", MF_BADGES_KEY)\
   .replace("%(DK)s", MF_DELIVERY_KEY).replace("%(KS)s", KEY_SALES).replace("%(KD)s", KEY_DATES)

QUERY_COLLECTION_PAGE_US = """
query ($handle:String!, $cursor:String) {
  collectionByHandle(handle:$handle){
    products(first: 40, after:$cursor){
      pageInfo{ hasNextPage endCursor }
      nodes{
        id title
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

# ----- India behavior (product-level availability + metafields/sales) -----
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
            log_row("IN", "IN", "WARN", product_id=gid_num(product_gid), sku="", message=f"metafieldsSet errors: {errs}")

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

def scan_india_and_update(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(IN_LAST_SEEN, {})
    today = today_ist_str()

    for handle in IN_COLLECTIONS:
        cursor = None
        visited_products = set()  # per-cycle de-dupe
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

                # badges/delivery every time UNLESS first global cycle (read_only)
                _, target_badge, target_delivery = desired_state(avail)
                if not read_only:
                    set_product_metafields_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("badges") or {}, p.get("dtime") or {}, target_badge, target_delivery)

                # SPECIAL STATUS RULE for the special collection
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

# ----- USA behavior (variant-level deltas → adjust India) -----
def find_india_inventory_item_id_by_sku(sku_norm: str, raw_sku: Optional[str] = None) -> Optional[int]:
    """
    Try multiple forms to locate the India inventory_item_id by SKU.
    We accept a match only if normalize_sku(candidate) == sku_norm, ensuring robust but precise mapping.
    """
    candidates: List[str] = []
    if raw_sku:
        candidates += [raw_sku.strip(), raw_sku.strip().lower()]
    s = sku_norm
    # try hyphen/space/compact forms
    candidates += [s, s.replace("-", " "), s.replace("-", ""), s.upper(), s.title()]
    tried = set()
    for c in candidates:
        c = (c or "").strip()
        if not c or c in tried:
            continue
        tried.add(c)
        for q in (f"sku:'{c}'", f"sku:{c}"):
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_FIND_IN_VARIANT_BY_SKU, {"q": q})
            nodes = ((data.get("productVariants") or {}).get("nodes") or [])
            for n in nodes:
                nsku = (n.get("sku") or "").strip()
                if normalize_sku(nsku) == sku_norm:
                    inv_item_gid = ((n.get("inventoryItem") or {}).get("id") or "")
                    inv_item_id = int(gid_num(inv_item_gid) or "0")
                    if inv_item_id > 0:
                        return inv_item_id
    return None

def scan_usa_and_mirror_to_india(read_only: bool = False):
    last_seen: Dict[str,int] = load_json(US_LAST_SEEN, {})

    for handle in US_COLLECTIONS:
        cursor = None
        visited_variants = set()  # per-cycle de-dupe
        while True:
            data = gql(US_DOMAIN, US_TOKEN, QUERY_COLLECTION_PAGE_US, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                log_row("US", "US", "WARN", sku="", message=f"Collection not found: {handle}")
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                for v in ((p.get("variants") or {}).get("nodes") or []):
                    vgid = v.get("id")
                    vid = gid_num(vgid)
                    if vid in visited_variants:
                        continue
                    visited_variants.add(vid)

                    raw_sku = (v.get("sku") or "").strip()
                    sku_norm = normalize_sku(raw_sku)
                    qty = int(v.get("inventoryQuantity") or 0)

                    prev = _as_int_or_none(last_seen.get(vid))
                    if prev is None:
                        # first time (or '-') → learn & move on
                        last_seen[vid] = qty
                        log_row("US", "US", "FIRST_SEEN", variant_id=vid, sku=raw_sku, delta="", message=f"qty={qty}")
                    else:
                        delta = qty - int(prev)
                        if delta != 0:
                            if not sku_norm:
                                log_row("US→IN", "US", "**WARN_NO_SKU_US**",
                                        variant_id=vid, sku=raw_sku, delta=delta,
                                        message="US variant changed qty but has no SKU; cannot mirror to India")
                            else:
                                if read_only:
                                    log_row("US→IN", "IN", "FIRST_CYCLE_SKIP",
                                            variant_id=vid, sku=raw_sku, delta=delta,
                                            message="Global first cycle is read-only; no adjust applied")
                                else:
                                    in_inv_item_id = find_india_inventory_item_id_by_sku(sku_norm, raw_sku)
                                    if in_inv_item_id is None:
                                        log_row("US→IN", "US", "**WARN_SKU_NOT_FOUND_IN_INDIA**",
                                                variant_id=vid, sku=raw_sku, delta=delta,
                                                message=f"US change {delta}; no matching SKU in India")
                                    else:
                                        try:
                                            rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, in_inv_item_id, int(IN_LOCATION_ID), delta)
                                            log_row("US→IN", "IN", "APPLIED_DELTA",
                                                    variant_id=vid, sku=raw_sku, delta=delta,
                                                    message=f"Adjusted IN inventory_item_id={in_inv_item_id} by {delta}")
                                            time.sleep(MUTATION_SLEEP_SEC)
                                        except Exception as e:
                                            log_row("US→IN", "IN", "ERROR_APPLYING_DELTA",
                                                    variant_id=vid, sku=raw_sku, delta=delta, message=str(e))
                            last_seen[vid] = qty

                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
            save_json(US_LAST_SEEN, {k:int(v) for k,v in last_seen.items()})
            sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
            else:
                break

# ---------- Scheduler loop & run orchestration ----------
run_lock = Lock()
is_running = False

def run_cycle():
    global is_running
    with run_lock:
        if is_running:
            return "busy"
        is_running = True
    try:
        # Decide if this is the very first global cycle (read-only)
        state = load_state()
        in_seen = load_json(IN_LAST_SEEN, {})
        us_seen = load_json(US_LAST_SEEN, {})
        first_cycle = (not state.get("initialized", False)) and (len(in_seen) == 0 and len(us_seen) == 0)

        # India first
        scan_india_and_update(read_only=first_cycle)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

        # USA second
        scan_usa_and_mirror_to_india(read_only=first_cycle)

        # Mark initialized after the first full cycle completes
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

# ---------- Flask ----------
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
        "in_last_seen_count": len(load_json(IN_LAST_SEEN, {})),
        "us_last_seen_count": len(load_json(US_LAST_SEEN, {})),
    }
    return _cors(jsonify(info)), 200

@app.route("/run", methods=["GET","POST"])
def run_now():
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))
    if FIRE_AND_FORGET_RUN:
        Thread(target=run_cycle, daemon=True).start()
        return _cors(jsonify({"status": "started"})), 202
    else:
        status = run_cycle()
        return _cors(jsonify({"status": status})), 200 if status == "ok" else 409

# Start internal scheduler if enabled
if ENABLE_SCHEDULER:
    Thread(target=scheduler_loop, daemon=True).start()

# ---------- Entrypoint ----------
if __name__ == "__main__":
    if HEADLESS:
        # Pure worker: no HTTP, just scheduler loop (or single run if RUN_EVERY_MIN<=0)
        print(f"[BOOT] Headless worker | API {API_VERSION}")
        print(f"[CFG] IN={IN_DOMAIN} (loc {IN_LOCATION_ID}) | US={US_DOMAIN} (loc {US_LOCATION_ID}) | DATA_DIR={DATA_DIR}")
        try:
            if ENABLE_SCHEDULER and RUN_EVERY_MIN > 0:
                while True:
                    try:
                        run_cycle()
                    except Exception as e:
                        log_row("SCHED", "BOTH", "ERROR", sku="", message=str(e))
                    time.sleep(max(1, RUN_EVERY_MIN) * 60)
            else:
                run_cycle()
        except KeyboardInterrupt:
            pass
    else:
        from werkzeug.serving import run_simple
        print(f"[BOOT] Dual sync on port {PORT} | API {API_VERSION}")
        print(f"[CFG] IN={IN_DOMAIN} (loc {IN_LOCATION_ID}) | US={US_DOMAIN} (loc {US_LOCATION_ID}) | DATA_DIR={DATA_DIR}")
        run_simple("0.0.0.0", PORT, app)
