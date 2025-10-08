#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dual-site availability sync (IN <— US decreases) with product-level SKU mapping
- Uses product metafield custom.sku as the ONLY mapping key (exact after trim+upper).
- India (IN) scan:
    * Compute availability per product (sum of variants; tracked-only unless told).
    * If availability drops vs last_seen → bump sales_total & sales_dates.
    * Update metafields: custom.badges + custom.delivery_time based on availability.
    * SPECIAL collection: auto flip ACTIVE<->DRAFT when avail <1 / >=1.
    * If avail < 0 and CLAMP_AVAIL_TO_ZERO=1 → raise variant inventories back to 0 (per-variant),
      then set last_seen to 0 (so next diffs are correct).
- USA (US) scan:
    * Track variant quantities; on DECREASE only, find matching IN product by product.custom.sku
      and adjust IN inventory_item_id by the same delta (negative). Increases are ignored.
- Metafields writes use DEFINITION types (no scalar/list mismatch).

Endpoints:
  GET /health         → "ok"
  GET /diag           → JSON with config and counters
  POST /run?key=...   → run one full cycle (IN then US)

Run modes:
  ENABLE_SCHEDULER=1 → background loop every RUN_EVERY_MIN minutes
  Otherwise trigger with POST /run?key=...
"""

import os, sys, time, json, csv, random
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import requests
from flask import Flask, request, jsonify, make_response
from threading import Thread, Lock

# ========= Config (env) =========
API_VERSION = os.getenv("API_VERSION", "2024-10").strip()
PIXEL_SHARED_SECRET = os.getenv("PIXEL_SHARED_SECRET", "").strip()
if not PIXEL_SHARED_SECRET:
    raise SystemExit("PIXEL_SHARED_SECRET required")

RUN_EVERY_MIN = int(os.getenv("RUN_EVERY_MIN", "5"))
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "0") == "1"

SLEEP_BETWEEN_PRODUCTS_MS = int(os.getenv("SLEEP_BETWEEN_PRODUCTS_MS", "120"))
SLEEP_BETWEEN_PAGES_MS    = int(os.getenv("SLEEP_BETWEEN_PAGES_MS", "400"))
SLEEP_BETWEEN_SHOPS_MS    = int(os.getenv("SLEEP_BETWEEN_SHOPS_MS", "800"))
MUTATION_SLEEP_SEC        = float(os.getenv("MUTATION_SLEEP_SEC", "0.35"))

# India (metafields + receives inventory deltas)
IN_DOMAIN = os.getenv("IN_DOMAIN", "").strip()
IN_TOKEN  = os.getenv("IN_TOKEN", "").strip()
IN_LOCATION_ID = int(os.getenv("IN_LOCATION_ID", "0"))
IN_COLLECTIONS = [x.strip() for x in os.getenv("IN_COLLECTION_HANDLES","").split(",") if x.strip()]
IN_INCLUDE_UNTRACKED = os.getenv("IN_INCLUDE_UNTRACKED", "0") == "1"
IN_CHANGE_STATUS = os.getenv("IN_CHANGE_STATUS", "1") == "1"  # allow special status flips

# USA (source of deltas)
US_DOMAIN = os.getenv("US_DOMAIN", "").strip()
US_TOKEN  = os.getenv("US_TOKEN", "").strip()
US_LOCATION_ID = int(os.getenv("US_LOCATION_ID", "0"))
US_COLLECTIONS = [x.strip() for x in os.getenv("US_COLLECTION_HANDLES","").split(",") if x.strip()]

# Mapping & behavior
USE_PRODUCT_CUSTOM_SKU = os.getenv("USE_PRODUCT_CUSTOM_SKU", "1") == "1"
ONLY_ACTIVE_FOR_MAPPING = os.getenv("ONLY_ACTIVE_FOR_MAPPING", "1") == "1"
MIRROR_US_INCREASES = os.getenv("MIRROR_US_INCREASES", "0") == "1"  # default off
CLAMP_AVAIL_TO_ZERO = os.getenv("CLAMP_AVAIL_TO_ZERO", "1") == "1"

# Special collection where availability controls status (symmetric rule)
SPECIAL_STATUS_HANDLE = os.getenv(
    "SPECIAL_STATUS_HANDLE",
    "budget-gold-plated-jewelry-premium-jadau-ornaments-on-mix-metal-base"
).strip()

# Metafields (India)
MF_NAMESPACE    = os.getenv("MF_NAMESPACE", "custom")
MF_BADGES_KEY   = os.getenv("MF_BADGES_KEY", "badges")
MF_DELIVERY_KEY = os.getenv("MF_DELIVERY_KEY", "delivery_time")
KEY_SALES       = os.getenv("KEY_SALES", "sales_total")
KEY_DATES       = os.getenv("KEY_DATES", "sales_dates")

BADGE_READY    = "Ready To Ship"
DELIVERY_READY = "2-5 Days Across India"
DELIVERY_MTO   = "12-15 Days Across India"

# Render / persistence
DATA_DIR = os.getenv("DATA_DIR", ".").rstrip("/") or "."
os.makedirs(DATA_DIR, exist_ok=True)
PORT = int(os.getenv("PORT", "10000"))

def p(*parts): return os.path.join(DATA_DIR, *parts)

IN_LAST_SEEN   = p("in_last_seen.json")   # product_id(num) -> non-negative int
US_LAST_SEEN   = p("us_last_seen.json")   # variant_id(num) -> int
IN_SKU_INDEX   = p("in_sku_index.json")   # SKU_KEY -> {product_id, inv_item_id, title, status}
LOG_CSV        = p("dual_sync_log.csv")
LOG_JSONL      = p("dual_sync_log.jsonl")
STATE_PATH     = p("dual_state.json")

# ========= HTTP helpers =========
def hdr(token: str) -> Dict[str, str]:
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json", "Accept": "application/json"}

def _sleep_backoff(attempt: int, base: float = 0.4, cap: float = 10.0):
    time.sleep(min(cap, base * (2 ** (attempt-1))) + random.uniform(0, 0.25))

def gql(domain: str, token: str, query: str, variables: dict = None) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    last_err = None
    for attempt in range(1, 8):
        r = None
        try:
            r = requests.post(url, headers=hdr(token),
                              json={"query": query, "variables": variables or {}}, timeout=60)
            if r.status_code in (502, 503, 504, 520, 521, 522, 523, 524, 525, 526, 527, 529, 430, 429):
                last_err = f"HTTP {r.status_code}"
                _sleep_backoff(attempt)
                continue
            if r.status_code != 200:
                raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")
            data = r.json()
            if data.get("errors"):
                # throttle retry
                if any(((e.get("extensions") or {}).get("code","").upper() == "THROTTLED") for e in data["errors"]):
                    last_err = "THROTTLED"
                    _sleep_backoff(attempt)
                    continue
                # surface other errors
                print(f"⚠️ GQL ERRORS {json.dumps(data['errors'])}", flush=True)
                print(f"⚙️ GQL LAST_QUERY {json.dumps({'query': query[:600]})}", flush=True)
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data["data"]
        except Exception as e:
            last_err = str(e)
            _sleep_backoff(attempt)
    raise RuntimeError(f"GraphQL throttled/5xx repeatedly: {last_err}")

def rest_adjust_inventory(domain: str, token: str, inventory_item_id: int, location_id: int, delta: int) -> None:
    url = f"https://{domain}/admin/api/{API_VERSION}/inventory_levels/adjust.json"
    payload = {"inventory_item_id": int(inventory_item_id),
               "location_id": int(location_id),
               "available_adjustment": int(delta)}
    last = None
    for attempt in range(1, 6):
        r = requests.post(url, headers=hdr(token), json=payload, timeout=30)
        if r.status_code in (429, 430, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            _sleep_backoff(attempt, base=0.3, cap=8.0)
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"REST adjust {r.status_code}: {r.text}")
        return
    raise RuntimeError(f"REST adjust failed: {last}")

# ========= Utils & logging =========
def now_ist(): return datetime.now(timezone(timedelta(hours=5, minutes=30)))
def now_ist_str(): return now_ist().strftime("%Y-%m-%d %H:%M:%S %z")
def today_ist_str(): return now_ist().date().isoformat()
def sleep_ms(ms: int): time.sleep(max(0, ms) / 1000.0)
def gid_num(gid: str) -> str: return (gid or "").split("/")[-1]

def sku_key(s: str) -> str:
    # exact semantics except case/leading/trailing spaces
    return (s or "").strip().upper()

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
    if (not os.path.exists(LOG_CSV)) or (os.path.getsize(LOG_CSV) == 0):
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["ts","phase","shop","note","product_id","variant_id","sku","delta","message"])
    if (not os.path.exists(LOG_JSONL)) or (os.path.getsize(LOG_JSONL) == 0):
        open(LOG_JSONL, "a", encoding="utf-8").close()

def log_row(note_emoji: str, phase: str, shop: str, note: str,
            product_id="", variant_id="", sku="", delta="", message="", title="",
            before="", after="", collections=""):
    ensure_log_headers()
    ts = now_ist_str()
    line_csv = [ts, phase, shop, note, product_id, variant_id, sku, delta, message]
    print(f"{note_emoji} {phase} {note} [SKU {sku}] pid={product_id} vid={variant_id} {before}→{after} Δ{delta} “{title}” — {message}",
          flush=True)
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(line_csv)
    with open(LOG_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "ts": ts, "phase": phase, "shop": shop, "note": note,
            "product_id": product_id, "variant_id": variant_id, "sku": sku, "delta": str(delta),
            "message": message, "title": title, "before": str(before), "after": str(after),
            "collections": collections
        }, ensure_ascii=False) + "\n")

def safe_int(v, default=0):
    try:
        if isinstance(v, (int, float)): return int(v)
        if v is None: return default
        s = str(v).strip()
        # allow "-5" or "+3"
        return int(s) if (s and (s.lstrip("+-").isdigit())) else default
    except Exception:
        return default

# ========= GQL: metafield definition (cache) =========
QUERY_MF_DEFINITION = """
query ($ownerType: MetafieldOwnerType!, $namespace: String!, $key: String!) {
  metafieldDefinitions(first: 1, ownerType: $ownerType, namespace: $namespace, key: $key) {
    edges { node { id name type { name } } }
  }
}
"""

_MF_DEF_CACHE: Dict[Tuple[str,str,str], str] = {}
def get_mf_def_type(domain: str, token: str, owner_type: str, ns: str, key: str) -> str:
    ck = (owner_type, ns, key)
    if ck in _MF_DEF_CACHE: return _MF_DEF_CACHE[ck]
    data = gql(domain, token, QUERY_MF_DEFINITION, {
        "ownerType": owner_type, "namespace": ns, "key": key
    })
    edges = (((data.get("metafieldDefinitions") or {}).get("edges")) or [])
    tname = ((((edges[0] or {}).get("node") or {}).get("type") or {}).get("name")) if edges else None
    tname = (tname or "single_line_text_field").strip()
    _MF_DEF_CACHE[ck] = tname
    return tname

def format_for_type(value: str, tname: str) -> str:
    if tname.startswith("list."):
        return json.dumps([value]) if value else "[]"
    return value or ""

# ========= Queries =========
# (No 'query:' arg on products inside collectionByHandle; we filter active in code)
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

# ========= Availability + metafields helpers =========
def compute_product_availability(variants: List[dict], include_untracked: bool) -> int:
    total = 0
    counted = False
    for v in variants:
        tracked = bool(((v.get("inventoryItem") or {}).get("tracked")))
        qty = safe_int(v.get("inventoryQuantity"), 0)
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
    MUTATION_PRODUCT_UPDATE = """
    mutation ProductUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product { id status }
        userErrors { field message }
      }
    }"""
    data = gql(domain, token, MUTATION_PRODUCT_UPDATE, {"input": {"id": product_gid, "status": target_status}})
    errs = ((data.get("productUpdate") or {}).get("userErrors") or [])
    return not bool(errs)

def set_product_metafields_in(domain:str, token:str, product_gid:str, badges_node:dict, dtime_node:dict,
                              target_badge:str, target_delivery:str) -> None:
    # Always use STORE DEFINITION types
    btype = get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_BADGES_KEY)
    dtype = get_mf_def_type(domain, token, "PRODUCT", MF_DELIVERY_KEY)
    mutation = """
    mutation($metafields:[MetafieldsSetInput!]!){
      metafieldsSet(metafields:$metafields){
        userErrors{ field message }
      }
    }"""
    mf = []
    # badges
    if target_badge:
        mf.append({
            "ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_BADGES_KEY,
            "type": btype, "value": format_for_type(target_badge, btype)
        })
    else:
        # delete when empty desired
        if (badges_node or {}).get("id"):
            delm = "mutation($id:ID!){ metafieldDelete(input:{id:$id}){ userErrors{message} } }"
            try: gql(domain, token, delm, {"id": badges_node["id"]})
            except Exception as e:
                log_row("⚠️", "IN", "IN", "WARN", message=f"badges delete error: {e}")
    # delivery
    mf.append({
        "ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_DELIVERY_KEY,
        "type": dtype, "value": format_for_type(target_delivery, dtype)
    })
    if mf:
        data = gql(domain, token, mutation, {"metafields": mf})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_row("⚠️", "IN", "IN", "WARN", message=f"metafieldsSet errors: {errs} (btype={btype}, dtype={dtype})")

def bump_sales_in(domain:str, token:str, product_gid:str, sales_total_node:dict, sales_dates_node:dict, sold:int, today:str):
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
            if isinstance(raw, str) and raw.startswith("["):
                existing = [str(x) for x in json.loads(raw)]
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
        log_row("⚠️", "IN", "IN", "WARN", message=f"sales metafieldsSet errors: {errs}")

# ========= Build IN SKU index =========
def build_in_sku_index() -> Dict[str, dict]:
    index: Dict[str, dict] = {}
    for handle in IN_COLLECTIONS:
        cursor = None
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
                # product-level SKU metafield
                sku_val = ((p.get("skuMeta") or {}).get("value") or "").strip()
                if not sku_val:
                    continue
                key = sku_key(sku_val)
                # Choose the FIRST variant’s inventory_item_id as representative for clamps/adjusts
                first_variant = (((p.get("variants") or {}).get("nodes")) or [{}])[0]
                inv_item_id = safe_int(gid_num(((first_variant.get("inventoryItem") or {}).get("id") or "0")), 0)
                index[key] = {
                    "product_id": gid_num(p["id"]),
                    "product_gid": p["id"],
                    "title": p.get("title",""),
                    "status": status,
                    "inv_item_id": inv_item_id
                }
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
                sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            else:
                break
    save_json(IN_SKU_INDEX, index)
    log_row("🗂️", "IN", "IN", "INDEX_BUILT", message=f"entries={len(index)}")
    return index

# ========= IN scan =========
def scan_india_and_update(read_only: bool, sku_index: Dict[str,dict]):
    last_seen: Dict[str,int] = load_json(IN_LAST_SEEN, {})
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
                title = p.get("title","")
                variants = ((p.get("variants") or {}).get("nodes") or [])
                status = (p.get("status") or "").upper()
                sku_val = ((p.get("skuMeta") or {}).get("value") or "").strip()
                sku = sku_val
                key = sku_key(sku_val)

                avail = compute_product_availability(variants, IN_INCLUDE_UNTRACKED)
                prev = safe_int(last_seen.get(pid), 0)  # never negative

                # Compute desired delivery/badge/state (based on RAW avail, not yet clamped)
                target_status, target_badge, target_delivery = desired_state(avail)

                # Diff & sales bump
                if not read_only:
                    diff = avail - prev
                    if diff < 0:
                        bump_sales_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, -diff, today)
                        log_row("🧾➖", "IN", "IN", "SALES_BUMP", product_id=pid, sku=sku, delta=diff,
                                message=f"avail {prev}->{avail} (sold={-diff})", title=title, before=str(prev), after=str(avail))

                # Metafields (badge/delivery) — always apply (uses definition types)
                if not read_only:
                    set_product_metafields_in(IN_DOMAIN, IN_TOKEN, p["id"], p.get("badges") or {}, p.get("dtime") or {},
                                              target_badge, target_delivery)

                # SPECIAL STATUS flip on the special collection only
                if (not read_only) and IN_CHANGE_STATUS and (handle == SPECIAL_STATUS_HANDLE):
                    if avail < 1 and status == "ACTIVE":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "DRAFT")
                        log_row("🛑", "IN", "IN", "STATUS_TO_DRAFT" if ok else "STATUS_TO_DRAFT_FAILED",
                                product_id=pid, sku=sku, delta=str(avail), message=f"handle={handle}", title=title)
                    elif avail >= 1 and status == "DRAFT":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "ACTIVE")
                        log_row("✅", "IN", "IN", "STATUS_TO_ACTIVE" if ok else "STATUS_TO_ACTIVE_FAILED",
                                product_id=pid, sku=sku, delta=str(avail), message=f"handle={handle}", title=title)

                # Clamp negatives → 0 (per-variant raise)
                if (not read_only) and CLAMP_AVAIL_TO_ZERO and avail < 0:
                    to_raise = -avail  # how much total to add to reach 0
                    # Distribute over variants that are counted; raise negatives first
                    counted = []
                    for v in variants:
                        tracked = bool(((v.get("inventoryItem") or {}).get("tracked")))
                        if (IN_INCLUDE_UNTRACKED or tracked):
                            counted.append(v)
                    # First, neutralize negative variants
                    for v in counted:
                        qty = safe_int(v.get("inventoryQuantity"), 0)
                        if qty < 0 and to_raise > 0:
                            need = min(-qty, to_raise)
                            inv_item_id = safe_int(gid_num(((v.get("inventoryItem") or {}).get("id") or "0")), 0)
                            try:
                                rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id, IN_LOCATION_ID, need)
                                to_raise -= need
                            except Exception as e:
                                log_row("⚠️", "IN", "IN", "WARN", product_id=pid, sku=sku, message=f"clamp raise error: {e}", title=title)
                            time.sleep(MUTATION_SLEEP_SEC)
                    # If still not zero, raise smallest-qty variants
                    if to_raise > 0 and counted:
                        for v in sorted(counted, key=lambda x: safe_int(x.get("inventoryQuantity"), 0)):
                            if to_raise <= 0: break
                            inv_item_id = safe_int(gid_num(((v.get("inventoryItem") or {}).get("id") or "0")), 0)
                            add = to_raise
                            try:
                                rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id, IN_LOCATION_ID, add)
                                to_raise -= add
                            except Exception as e:
                                log_row("⚠️", "IN", "IN", "WARN", product_id=pid, sku=sku, message=f"clamp raise residual error: {e}", title=title)
                            time.sleep(MUTATION_SLEEP_SEC)
                    log_row("🧰0️⃣", "IN", "IN", "CLAMP_TO_ZERO", product_id=pid, sku=sku, delta=f"+{-avail}",
                            message="Raised availability to 0 (per-variant)", title=title, before=str(avail), after="0")
                    avail = 0  # reflect clamped state

                # Update last_seen (NEVER store negative)
                last_seen[pid] = max(0, int(avail))

                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            save_json(IN_LAST_SEEN, {k: safe_int(v, 0) for k, v in last_seen.items()})
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
                sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            else:
                break

# ========= US scan (mirror decreases only) =========
def scan_usa_and_mirror_to_india(read_only: bool, sku_index: Dict[str,dict]):
    last_seen: Dict[str,int] = load_json(US_LAST_SEEN, {})

    for handle in US_COLLECTIONS:
        cursor = None
        while True:
            data = gql(US_DOMAIN, US_TOKEN, QUERY_COLLECTION_PAGE_US, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll: break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            pageInfo = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                ptitle = p.get("title","")
                for v in ((p.get("variants") or {}).get("nodes") or []):
                    vid = gid_num(v.get("id") or "")
                    raw_sku = (v.get("sku") or "").strip()
                    qty = safe_int(v.get("inventoryQuantity"), 0)

                    prev = safe_int(last_seen.get(vid), qty)
                    delta = qty - prev
                    # learn if not seen before
                    if vid not in last_seen:
                        last_seen[vid] = qty
                        log_row("🧭", "US", "US", "FIRST_SEEN", variant_id=vid, sku=raw_sku, delta="", message=f"qty={qty}", title=ptitle)
                        sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
                        continue

                    if delta == 0:
                        last_seen[vid] = qty
                        sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
                        continue

                    if delta > 0:
                        # US increase → usually ignore
                        if not MIRROR_US_INCREASES:
                            log_row("🙅‍♂️➕", "US→IN", "US", "IGNORED_INCREASE", variant_id=vid, sku=raw_sku, delta=str(delta),
                                    message="US qty increase; mirroring disabled", title=ptitle, before=str(prev), after=str(qty))
                            last_seen[vid] = qty
                            sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
                            continue

                    # delta < 0 (or mirroring increases on)
                    if read_only:
                        log_row("🛑", "US→IN", "IN", "FIRST_CYCLE_SKIP", variant_id=vid, sku=raw_sku, delta=str(delta),
                                message="First global cycle is read-only; no adjust", title=ptitle, before=str(prev), after=str(qty))
                        last_seen[vid] = qty
                        sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
                        continue

                    key = sku_key(raw_sku)
                    if not key or key not in sku_index:
                        log_row("⚠️", "US→IN", "US", "**WARN_SKU_NOT_FOUND_IN_INDIA**",
                                variant_id=vid, sku=raw_sku, delta=str(delta),
                                message=f"US change {delta}; no matching IN product.custom.sku", title=ptitle,
                                before=str(prev), after=str(qty))
                        last_seen[vid] = qty
                        sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
                        continue

                    inv_item_id = safe_int(sku_index[key].get("inv_item_id"), 0)
                    try:
                        rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id, IN_LOCATION_ID, delta)
                        log_row("🔁", "US→IN", "IN", "APPLIED_DELTA",
                                variant_id=vid, sku=raw_sku, delta=str(delta),
                                message=f"Adjusted IN inventory_item_id={inv_item_id} by {delta} (via EXACT index)",
                                title=ptitle, before=str(prev), after=str(qty))
                    except Exception as e:
                        log_row("⚠️", "US→IN", "IN", "ERROR_APPLYING_DELTA",
                                variant_id=vid, sku=raw_sku, delta=str(delta), message=str(e), title=ptitle)

                    last_seen[vid] = qty
                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            save_json(US_LAST_SEEN, {k: safe_int(v, 0) for k, v in last_seen.items()})
            if pageInfo.get("hasNextPage"):
                cursor = pageInfo.get("endCursor")
                sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            else:
                break

# ========= Scheduler loop & HTTP =========
run_lock = Lock()
is_running = False

def run_cycle():
    global is_running
    with run_lock:
        if is_running:
            return "busy"
        is_running = True
    try:
        # Build fresh IN SKU index (ACTIVE-only if configured)
        index = build_in_sku_index()
        # Decide first cycle read-only: if no last_seen files at all
        in_seen = load_json(IN_LAST_SEEN, {})
        us_seen = load_json(US_LAST_SEEN, {})
        first_cycle = (len(in_seen) == 0 and len(us_seen) == 0)

        # IN then US
        scan_india_and_update(read_only=first_cycle, sku_index=index)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)
        scan_usa_and_mirror_to_india(read_only=first_cycle, sku_index=index)

        if first_cycle:
            # mark a state file just for visibility; logic already relies on last_seen length
            save_json(STATE_PATH, {"initialized": True})
            log_row("🧭", "SCHED", "BOTH", "FIRST_CYCLE_DONE", message="Baselines learned; future cycles will apply deltas")
        return "ok"
    except Exception as e:
        log_row("⚠️", "SCHED", "BOTH", "WARN", message=str(e))
        return "error"
    finally:
        with run_lock:
            is_running = False

# Flask app
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
    d = {
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
        "clamp_avail_to_zero": CLAMP_AVAIL_TO_ZERO
    }
    return _cors(jsonify(d)), 200

@app.route("/run", methods=["POST"])
def run_now():
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))
    status = run_cycle()
    return _cors(jsonify({"status": status})), 200 if status == "ok" else 409

def scheduler_loop():
    if RUN_EVERY_MIN <= 0: return
    while True:
        try:
            run_cycle()
        except Exception as e:
            log_row("⚠️", "SCHED", "BOTH", "ERROR", message=str(e))
        time.sleep(max(1, RUN_EVERY_MIN) * 60)

if ENABLE_SCHEDULER:
    Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    print(f"[BOOT] Dual sync on port {PORT} | API {API_VERSION}", flush=True)
    print(f"[CFG] IN={IN_DOMAIN} (loc {IN_LOCATION_ID}) | US={US_DOMAIN} (loc {US_LOCATION_ID}) | DATA_DIR={DATA_DIR}", flush=True)
    run_simple("0.0.0.0", PORT, app)
