#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dual-site availability sync (Render-friendly)

Key behavior (India = source of truth for readiness):
- Live availability = sum of tracked variant inventory at the IN location (can be negative when orders/backorders hit).
- If availability > 0  → badges="Ready To Ship", delivery="2-5 Days Across India"
- If availability <= 0 → badges="",                delivery="12-15 Days Across India"
- SALES_BUMP when availability decreases vs last_seen (we never store negative last_seen)
- When availability is negative, we "CLAMP_TO_ZERO": attempt to REST-adjust up to 0,
  and persist last_seen.qty = 0 (never negative), marking the negative streak as counted.
- SPECIAL_STATUS_HANDLE (optional): if avail <= 0 → DRAFT; if avail > 0 → ACTIVE.

US→IN Mirror:
- For any US variant delta < 0, find that variant's product.custom.sku and mirror the delta to India using
  an exact index on India product custom.sku → inventory_item_id. Delta > 0 is ignored unless MIRROR_US_INCREASES=1.

Caching/State on Disk (DATA_DIR; attach Render persistent disk and set DATA_DIR=/data):
- in_last_seen.json: {"<pid>": {"qty": <nonneg_int>, "neg": <bool>}}
- us_last_seen.json: {"<vid>": <int_qty_last_seen>}
- in_sku_index.json: {"<EXACT custom.sku>": {"pid": "<product_id_num>", "title":"...", "inventory_item_id": <int>}}
- dual_sync_log.csv + dual_sync_log.jsonl: human-friendly and machine-readable logs
- dual_state.json: {"initialized": bool} (not strictly required anymore, we learn baseline on the fly)

Run styles:
- Web service (Render Web Service): expose /health, /diag (GET), /run (POST?key=PIXEL_SHARED_SECRET)
- Background worker (Render Background Worker): run periodic cycles via internal scheduler with ENABLE_SCHEDULER=1

"""

import os, sys, time, json, csv, random
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

import requests
from requests.exceptions import RequestException
from flask import Flask, request, jsonify, make_response

# =========================
# Config / Env
# =========================
API_VERSION = os.getenv("API_VERSION", "2024-10").strip()
PIXEL_SHARED_SECRET = os.getenv("PIXEL_SHARED_SECRET", "").strip()
if not PIXEL_SHARED_SECRET:
    raise SystemExit("PIXEL_SHARED_SECRET required")

# India (metafields + receives inventory deltas)
IN_DOMAIN = os.getenv("IN_DOMAIN", "").strip()
IN_TOKEN  = os.getenv("IN_TOKEN", "").strip()
IN_LOCATION_ID = os.getenv("IN_LOCATION_ID", "").strip()
IN_COLLECTIONS = [x.strip() for x in os.getenv("IN_COLLECTION_HANDLES", "").split(",") if x.strip()]
IN_INCLUDE_UNTRACKED = os.getenv("IN_INCLUDE_UNTRACKED", "0") == "1"
IN_CHANGE_STATUS = os.getenv("IN_CHANGE_STATUS", "1") == "1"  # flip ACTIVE/DRAFT for special handle

# USA (source of variant-level deltas)
US_DOMAIN = os.getenv("US_DOMAIN", "").strip()
US_TOKEN  = os.getenv("US_TOKEN", "").strip()
US_LOCATION_ID = os.getenv("US_LOCATION_ID", "").strip()
US_COLLECTIONS = [x.strip() for x in os.getenv("US_COLLECTION_HANDLES", "").split(",") if x.strip()]

# Metafields & constants
MF_NAMESPACE = os.getenv("MF_NAMESPACE", "custom")
MF_BADGES_KEY = os.getenv("MF_BADGES_KEY", "badges")
MF_DELIVERY_KEY = os.getenv("MF_DELIVERY_KEY", "delivery_time")
KEY_SALES = os.getenv("KEY_SALES", "sales_total")
KEY_DATES = os.getenv("KEY_DATES", "sales_dates")

BADGE_READY    = "Ready To Ship"
DELIVERY_READY = "2-5 Days Across India"
DELIVERY_MTO   = "12-15 Days Across India"

# Special collection handle (status flips)
SPECIAL_STATUS_HANDLE = os.getenv(
    "SPECIAL_STATUS_HANDLE",
    "budget-gold-plated-jewelry-premium-jadau-ornaments-on-mix-metal-base"
).strip()

# Exact SKU mapping (use product metafield custom.sku, not variant SKU)
USE_PRODUCT_CUSTOM_SKU = os.getenv("USE_PRODUCT_CUSTOM_SKU", "1") == "1"
ONLY_ACTIVE_FOR_MAPPING = os.getenv("ONLY_ACTIVE_FOR_MAPPING", "1") == "1"

# Mirroring behavior
MIRROR_US_INCREASES = os.getenv("MIRROR_US_INCREASES", "0") == "1"  # default: ignore increases
CLAMP_AVAIL_TO_ZERO = os.getenv("CLAMP_AVAIL_TO_ZERO", "1") == "1"  # default: clamp negatives to zero

# Pacing
RUN_EVERY_MIN = int(os.getenv("RUN_EVERY_MIN", "5"))
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "0") == "1"
SLEEP_BETWEEN_PRODUCTS_MS = int(os.getenv("SLEEP_BETWEEN_PRODUCTS_MS", "80"))
SLEEP_BETWEEN_PAGES_MS = int(os.getenv("SLEEP_BETWEEN_PAGES_MS", "200"))
SLEEP_BETWEEN_SHOPS_MS = int(os.getenv("SLEEP_BETWEEN_SHOPS_MS", "500"))
MUTATION_SLEEP_SEC = float(os.getenv("MUTATION_SLEEP_SEC", "0.25"))

# Storage
DATA_DIR = os.getenv("DATA_DIR", ".").rstrip("/")
os.makedirs(DATA_DIR, exist_ok=True)
def P(*parts): return os.path.join(DATA_DIR, *parts)
IN_LAST_SEEN_PATH = P("in_last_seen.json")
US_LAST_SEEN_PATH = P("us_last_seen.json")
IN_SKU_INDEX_PATH = P("in_sku_index.json")
STATE_PATH        = P("dual_state.json")
LOG_CSV_PATH      = P("dual_sync_log.csv")
LOG_JSONL_PATH    = P("dual_sync_log.jsonl")

LOG_TO_STDOUT = os.getenv("LOG_TO_STDOUT", "1") == "1"

# =========================
# Helpers
# =========================
def hdr(token: str) -> Dict[str, str]:
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json", "Accept": "application/json"}

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

def norm_sku(s: str) -> str:
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
        json.dump(obj, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)

# =========================
# Logging
# =========================
def ensure_log_headers():
    need_csv = (not os.path.exists(LOG_CSV_PATH)) or (os.path.getsize(LOG_CSV_PATH) == 0)
    if need_csv:
        with open(LOG_CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["ts","phase","shop","note","product_id","variant_id","sku","delta","message"])
    if not os.path.exists(LOG_JSONL_PATH):
        open(LOG_JSONL_PATH, "a", encoding="utf-8").close()

def log_event(emoji: str, phase: str, shop: str, note: str,
              product_id: str = "", variant_id: str = "", sku: str = "",
              delta: str = "", message: str = "", title: str = "",
              before: str = "", after: str = "", collections: str = ""):
    ensure_log_headers()
    ts = now_ist_str()
    # pretty console
    line = f"{emoji} {phase} {note} [SKU {sku}] pid={product_id} vid={variant_id} {before}→{after} Δ{delta} “{title}” — {message}"
    if LOG_TO_STDOUT:
        print(line, flush=True)
    # csv
    with open(LOG_CSV_PATH, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([ts, phase, shop, note, product_id, variant_id, sku, delta, message])
    # jsonl
    payload = {
        "ts": ts, "phase": phase, "shop": shop, "note": note,
        "product_id": product_id, "variant_id": variant_id, "sku": sku,
        "delta": str(delta), "message": message, "title": title,
        "before": str(before), "after": str(after), "collections": collections
    }
    with open(LOG_JSONL_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")

# =========================
# Shopify calls (with retries)
# =========================
def gql(domain: str, token: str, query: str, variables: dict = None) -> dict:
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    for attempt in range(1, 9):
        try:
            r = requests.post(url, headers=hdr(token), json={"query": query, "variables": variables or {}}, timeout=60)
        except RequestException:
            time.sleep(min(12.0, 0.5 * (2 ** (attempt - 1))) + random.uniform(0, 0.35))
            continue
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
        try:
            r = requests.post(url, headers=hdr(token), json=payload, timeout=40)
        except RequestException:
            time.sleep(min(10.0, 0.4 * (2 ** (attempt - 1))) + random.uniform(0, 0.3))
            continue
        if r.status_code in (429, 502, 503, 504):
            time.sleep(min(10.0, 0.4 * (2 ** (attempt - 1))) + random.uniform(0, 0.3))
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"REST adjust {r.status_code}: {r.text}")
        return

# =========================
# GraphQL bits
# =========================
QUERY_IN_COLLECTION_PAGE = """
query ($handle:String!, $cursor:String){
  collectionByHandle(handle:$handle){
    products(first: 60, after:$cursor){
      pageInfo{ hasNextPage endCursor }
      nodes{
        id title status
        metafield(namespace: "%(NS)s", key:"sku"){ value }
        variants(first: 100){
          nodes{
            id title sku inventoryQuantity
            inventoryItem{ id tracked }
            inventoryPolicy
          }
        }
        badges: metafield(namespace:"%(NS)s", key:"%(BK)s"){ id value type }
        dtime:  metafield(namespace:"%(NS)s", key:"%(DK)s"){ id value type }
        salesTotal: metafield(namespace:"%(NS)s", key:"%(KS)s"){ id value type }
        salesDates: metafield(namespace:"%(NS)s", key:"%(KD)s"){ id value type }
      }
    }
  }
}
""".replace("%(NS)s", MF_NAMESPACE).replace("%(BK)s", MF_BADGES_KEY)\
   .replace("%(DK)s", MF_DELIVERY_KEY).replace("%(KS)s", KEY_SALES).replace("%(KD)s", KEY_DATES)

QUERY_US_COLLECTION_PAGE = """
query ($handle:String!, $cursor:String){
  collectionByHandle(handle:$handle){
    products(first: 60, after:$cursor){
      pageInfo{ hasNextPage endCursor }
      nodes{
        id title
        metafield(namespace: "%(NS)s", key:"sku"){ value }
        variants(first: 100){
          nodes{
            id inventoryQuantity
            inventoryItem{ id }
          }
        }
      }
    }
  }
}
""".replace("%(NS)s", MF_NAMESPACE)

MUTATION_PRODUCT_UPDATE = """
mutation ProductUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id status }
    userErrors { field message }
  }
}
"""

MUTATION_METAFIELDS_SET = """
mutation MetafieldsSet($metafields:[MetafieldsSetInput!]!){
  metafieldsSet(metafields:$metafields){
    userErrors{ field message }
  }
}
"""

# =========================
# Computation helpers
# =========================
def compute_availability(variants: List[dict], include_untracked: bool) -> int:
    total = 0
    counted = False
    for v in variants:
        tracked = bool(((v.get("inventoryItem") or {}).get("tracked")))
        qty = int(v.get("inventoryQuantity") or 0)
        if include_untracked or tracked:
            counted = True
            total += qty
    return total if counted else 0

def desired_badge_delivery(avail: int) -> Tuple[str, str]:
    if avail > 0:
        return (BADGE_READY, DELIVERY_READY)
    else:
        return ("", DELIVERY_MTO)

def update_product_status(domain: str, token: str, product_gid: str, target_status: str) -> bool:
    data = gql(domain, token, MUTATION_PRODUCT_UPDATE, {"input": {"id": product_gid, "status": target_status}})
    errs = ((data.get("productUpdate") or {}).get("userErrors") or [])
    return not bool(errs)

def set_product_metafields(domain: str, token: str, product_gid: str,
                           badges_node: dict, dtime_node: dict,
                           target_badge: str, target_delivery: str) -> None:
    btype = (badges_node or {}).get("type") or "single_line_text_field"
    dtype = (dtime_node  or {}).get("type") or "single_line_text_field"
    mfs = []
    if target_badge:
        if btype.startswith("list."):
            mfs.append({"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_BADGES_KEY, "type": btype, "value": json.dumps([target_badge])})
        else:
            mfs.append({"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_BADGES_KEY, "type": btype, "value": target_badge})
    else:
        # clear if present
        if (badges_node or {}).get("id"):
            delm = "mutation($id:ID!){ metafieldDelete(input:{id:$id}){ userErrors{message} } }"
            gql(domain, token, delm, {"id": badges_node["id"]})
    # delivery
    mfs.append({"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_DELIVERY_KEY, "type": dtype, "value": target_delivery})
    if mfs:
        data = gql(domain, token, MUTATION_METAFIELDS_SET, {"metafields": mfs})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_event("⚠️", "IN", "IN", "WARN", product_id=gid_num(product_gid), message=f"metafieldsSet errors: {errs}")

def bump_sales(domain: str, token: str, product_gid: str, sales_total_node: dict, sales_dates_node: dict, sold: int, today: str):
    if sold <= 0: return
    st_type = (sales_total_node or {}).get("type") or "number_integer"
    sd_type = (sales_dates_node or {}).get("type") or "list.date"
    try:
        current = int((sales_total_node or {}).get("value") or "0")
    except Exception:
        current = 0
    new_total = current + int(sold)
    # dates
    if sd_type == "list.date":
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
        log_event("⚠️", "IN", "IN", "WARN", product_id=gid_num(product_gid), message=f"sales metafieldsSet errors: {errs}")

# =========================
# Index building (India custom.sku → inventory_item_id)
# =========================
def build_india_sku_index() -> Dict[str, Any]:
    index: Dict[str, Any] = {}
    for handle in IN_COLLECTIONS:
        cursor = None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_IN_COLLECTION_PAGE, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            page = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                status = (p.get("status") or "").upper()
                if ONLY_ACTIVE_FOR_MAPPING and status != "ACTIVE":
                    continue
                sku_val = (((p.get("metafield") or {}) or {}).get("value") or "").strip()
                if not sku_val:
                    continue
                sku_key = norm_sku(sku_val)
                variants = ((p.get("variants") or {}).get("nodes") or [])
                # pick the first counted variant's inventory_item_id
                inv_id = None
                for v in variants:
                    tracked = bool(((v.get("inventoryItem") or {}).get("tracked")))
                    if IN_INCLUDE_UNTRACKED or tracked:
                        inv_id = gid_num(((v.get("inventoryItem") or {}).get("id") or ""))
                        if inv_id:
                            inv_id = int(inv_id)
                            break
                if inv_id:
                    index[sku_key] = {
                        "pid": gid_num(p["id"]),
                        "title": p.get("title") or "",
                        "inventory_item_id": inv_id
                    }
                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)
            if page.get("hasNextPage"):
                cursor = page.get("endCursor")
                sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            else:
                break
    save_json(IN_SKU_INDEX_PATH, index)
    log_event("🗂️", "IN", "IN", "INDEX_BUILT", message=f"entries={len(index)}")
    return index

# =========================
# India scan (never store negative last_seen)
# =========================
def get_in_entry(state: dict, pid: str) -> dict:
    v = state.get(pid)
    if isinstance(v, dict):
        return {"qty": max(0, int(v.get("qty", 0))), "neg": bool(v.get("neg", False))}
    try:
        # backward-compat: plain int
        return {"qty": max(0, int(v or 0)), "neg": False}
    except Exception:
        return {"qty": 0, "neg": False}

def scan_india_and_update(index: Dict[str, Any]):
    state: Dict[str, Any] = load_json(IN_LAST_SEEN_PATH, {})
    today = today_ist_str()

    for handle in IN_COLLECTIONS:
        cursor = None
        while True:
            data = gql(IN_DOMAIN, IN_TOKEN, QUERY_IN_COLLECTION_PAGE, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            page = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                pid = gid_num(p["id"])
                title = p.get("title") or ""
                variants = ((p.get("variants") or {}).get("nodes") or [])
                avail_raw = compute_availability(variants, IN_INCLUDE_UNTRACKED)
                entry = get_in_entry(state, pid)
                prev_qty, prev_neg = entry["qty"], entry["neg"]

                # Compute diff/sales
                if avail_raw < 0:
                    # First time in negative streak?
                    if not prev_neg:
                        sold = prev_qty + abs(avail_raw)
                        if sold > 0:
                            bump_sales(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, sold, today)
                            log_event("🧾➖", "IN", "IN", "SALES_BUMP",
                                      product_id=pid, sku=((p.get("metafield") or {}).get("value") or ""),
                                      delta=f"-{sold}", message=f"avail {prev_qty}->{avail_raw} (sold={sold})",
                                      title=title, before=str(prev_qty), after=str(avail_raw))
                    # Clamp to zero (attempt REST adjust by +abs(avail_raw))
                    if CLAMP_AVAIL_TO_ZERO and abs(avail_raw) > 0:
                        # find a counted variant's inventory_item_id
                        inv_item_id = None
                        for v in variants:
                            tracked = bool(((v.get("inventoryItem") or {}).get("tracked")))
                            if IN_INCLUDE_UNTRACKED or tracked:
                                inv_item_id = int(gid_num(((v.get("inventoryItem") or {}).get("id") or "0")))
                                if inv_item_id:
                                    break
                        try:
                            if inv_item_id:
                                rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_item_id, int(IN_LOCATION_ID), abs(avail_raw))
                                log_event("🧰0️⃣", "IN", "IN", "CLAMP_TO_ZERO",
                                          product_id=pid, sku=((p.get("metafield") or {}).get("value") or ""),
                                          delta=f"+{abs(avail_raw)}", message=f"Raised availability to 0 on inventory_item_id={inv_item_id}",
                                          title=title, before=str(avail_raw), after="0")
                                time.sleep(MUTATION_SLEEP_SEC)
                        except Exception as e:
                            log_event("⚠️", "IN", "IN", "CLAMP_FAIL", product_id=pid,
                                      sku=((p.get("metafield") or {}).get("value") or ""),
                                      delta=f"+{abs(avail_raw)}", message=str(e), title=title,
                                      before=str(avail_raw), after="0")
                    # Persist non-negative last_seen and mark neg streak counted
                    state[pid] = {"qty": 0, "neg": True}
                    eff_avail = 0
                else:
                    # avail_raw >= 0
                    if prev_neg:
                        # negative streak ended
                        prev_neg = False
                    if avail_raw < prev_qty:
                        sold = prev_qty - avail_raw
                        if sold > 0:
                            bump_sales(IN_DOMAIN, IN_TOKEN, p["id"], p.get("salesTotal") or {}, p.get("salesDates") or {}, sold, today)
                            log_event("🧾➖", "IN", "IN", "SALES_BUMP",
                                      product_id=pid, sku=((p.get("metafield") or {}).get("value") or ""),
                                      delta=f"-{sold}", message=f"avail {prev_qty}->{avail_raw} (sold={sold})",
                                      title=title, before=str(prev_qty), after=str(avail_raw))
                    elif avail_raw > prev_qty:
                        log_event("📦➕", "IN", "IN", "RESTOCK",
                                  product_id=pid, sku=((p.get("metafield") or {}).get("value") or ""),
                                  delta=str(avail_raw - prev_qty), message=f"avail {prev_qty}->{avail_raw} (+{avail_raw - prev_qty})",
                                  title=title, before=str(prev_qty), after=str(avail_raw))
                    state[pid] = {"qty": int(avail_raw), "neg": prev_neg}
                    eff_avail = int(avail_raw)

                # Metafields and (optional) status flip use effective (non-negative) availability
                badge, dtime = desired_badge_delivery(eff_avail)
                set_product_metafields(IN_DOMAIN, IN_TOKEN, p["id"], p.get("badges") or {}, p.get("dtime") or {}, badge, dtime)

                if IN_CHANGE_STATUS and (handle == SPECIAL_STATUS_HANDLE):
                    current_status = (p.get("status") or "").upper()
                    if eff_avail < 1 and current_status == "ACTIVE":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "DRAFT")
                        log_event("🛑", "IN", "IN", "STATUS_TO_DRAFT" if ok else "STATUS_TO_DRAFT_FAILED",
                                  product_id=pid, sku=((p.get("metafield") or {}).get("value") or ""),
                                  title=title, before="ACTIVE", after="DRAFT")
                    elif eff_avail >= 1 and current_status == "DRAFT":
                        ok = update_product_status(IN_DOMAIN, IN_TOKEN, p["id"], "ACTIVE")
                        log_event("✅", "IN", "IN", "STATUS_TO_ACTIVE" if ok else "STATUS_TO_ACTIVE_FAILED",
                                  product_id=pid, sku=((p.get("metafield") or {}).get("value") or ""),
                                  title=title, before="DRAFT", after="ACTIVE")

                save_json(IN_LAST_SEEN_PATH, state)
                sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            if page.get("hasNextPage"):
                cursor = page.get("endCursor")
                sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            else:
                break

# =========================
# US scan (mirror decreases only)
# =========================
def scan_usa_and_mirror(index: Dict[str, Any]):
    last_seen: Dict[str, Any] = load_json(US_LAST_SEEN_PATH, {})

    for handle in US_COLLECTIONS:
        cursor = None
        while True:
            data = gql(US_DOMAIN, US_TOKEN, QUERY_US_COLLECTION_PAGE, {"handle": handle, "cursor": cursor})
            coll = data.get("collectionByHandle")
            if not coll:
                break
            prods = ((coll.get("products") or {}).get("nodes") or [])
            page = ((coll.get("products") or {}).get("pageInfo") or {})
            for p in prods:
                title = p.get("title") or ""
                sku_val = (((p.get("metafield") or {}) or {}).get("value") or "").strip()
                sku_key = norm_sku(sku_val) if sku_val else ""
                for v in ((p.get("variants") or {}).get("nodes") or []):
                    vgid = v.get("id")
                    vid = gid_num(vgid)
                    qty = int(v.get("inventoryQuantity") or 0)
                    prev = None
                    try:
                        prev = int(last_seen.get(vid)) if vid in last_seen else None
                    except Exception:
                        prev = None

                    if prev is None:
                        last_seen[vid] = qty
                        # no noise for first seen
                    else:
                        delta = qty - int(prev)
                        if delta < 0:
                            # mirror only if we have exact SKU mapping
                            if sku_key and sku_key in index:
                                inv_id = int(index[sku_key]["inventory_item_id"])
                                try:
                                    rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_id, int(IN_LOCATION_ID), delta)
                                    log_event("🔁", "US→IN", "IN", "APPLIED_DELTA",
                                              variant_id=vid, sku=sku_val, delta=str(delta),
                                              message=f"Adjusted IN inventory_item_id={inv_id} by {delta} (via EXACT index)",
                                              title=title, before=str(prev), after=str(qty))
                                    time.sleep(MUTATION_SLEEP_SEC)
                                except Exception as e:
                                    log_event("⚠️", "US→IN", "IN", "ERROR_APPLYING_DELTA",
                                              variant_id=vid, sku=sku_val, delta=str(delta), message=str(e), title=title,
                                              before=str(prev), after=str(qty))
                            else:
                                log_event("🙅‍♂️", "US→IN", "US", "**WARN_SKU_NOT_FOUND_IN_INDIA**",
                                          variant_id=vid, sku=sku_val, delta=str(delta),
                                          message="US change; no matching custom.sku in India exact index",
                                          title=title, before=str(prev), after=str(qty))
                        elif delta > 0:
                            if MIRROR_US_INCREASES and sku_key and sku_key in index:
                                inv_id = int(index[sku_key]["inventory_item_id"])
                                try:
                                    rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, inv_id, int(IN_LOCATION_ID), delta)
                                    log_event("🔁➕", "US→IN", "IN", "APPLIED_INCREASE",
                                              variant_id=vid, sku=sku_val, delta=str(delta),
                                              message=f"Adjusted IN inventory_item_id={inv_id} by +{delta} (via EXACT index)",
                                              title=title, before=str(prev), after=str(qty))
                                    time.sleep(MUTATION_SLEEP_SEC)
                                except Exception as e:
                                    log_event("⚠️", "US→IN", "IN", "ERROR_APPLYING_DELTA",
                                              variant_id=vid, sku=sku_val, delta=str(delta), message=str(e), title=title,
                                              before=str(prev), after=str(qty))
                            else:
                                log_event("🙅‍♂️➕", "US→IN", "US", "IGNORED_INCREASE",
                                          variant_id=vid, sku=sku_val, delta=str(delta),
                                          message="US qty increase; mirroring disabled",
                                          title=title, before=str(prev), after=str(qty))
                        last_seen[vid] = qty

                    sleep_ms(SLEEP_BETWEEN_PRODUCTS_MS)

            save_json(US_LAST_SEEN_PATH, last_seen)
            if page.get("hasNextPage"):
                cursor = page.get("endCursor")
                sleep_ms(SLEEP_BETWEEN_PAGES_MS)
            else:
                break

# =========================
# Scheduler / Runner
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
        # 1) (Re)build India exact SKU index
        index = build_india_sku_index()
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

        # 2) India scan/update (sales bumps, clamp to zero, metafields, optional status flip)
        scan_india_and_update(index)
        sleep_ms(SLEEP_BETWEEN_SHOPS_MS)

        # 3) US scan/mirror (decreases only by default)
        scan_usa_and_mirror(index)

        return "ok"
    finally:
        with run_lock:
            is_running = False

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
    in_state = load_json(IN_LAST_SEEN_PATH, {})
    us_state = load_json(US_LAST_SEEN_PATH, {})
    index = load_json(IN_SKU_INDEX_PATH, {})
    cfg = {
        "api_version": API_VERSION,
        "data_dir": DATA_DIR,
        "in_domain": IN_DOMAIN,
        "us_domain": US_DOMAIN,
        "in_collections": IN_COLLECTIONS,
        "us_collections": US_COLLECTIONS,
        "in_last_seen_count": len(in_state),
        "us_last_seen_count": len(us_state),
        "index_entries": len(index),
        "use_product_custom_sku": USE_PRODUCT_CUSTOM_SKU,
        "only_active_for_mapping": ONLY_ACTIVE_FOR_MAPPING,
        "mirror_us_increases": MIRROR_US_INCREASES,
        "clamp_avail_to_zero": CLAMP_AVAIL_TO_ZERO,
        "run_every_min": RUN_EVERY_MIN,
        "scheduler_enabled": ENABLE_SCHEDULER,
    }
    return _cors(jsonify(cfg)), 200

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
            log_event("⚠️", "SCHED", "BOTH", "ERROR", message=str(e))
        time.sleep(max(1, RUN_EVERY_MIN) * 60)

if ENABLE_SCHEDULER:
    Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    PORT = int(os.getenv("PORT", "10000"))
    print(f"[BOOT] Dual sync on port {PORT} | API {API_VERSION}", flush=True)
    print(f"[CFG] IN={IN_DOMAIN} (loc {IN_LOCATION_ID}) | US={US_DOMAIN} (loc {US_LOCATION_ID}) | DATA_DIR={DATA_DIR}", flush=True)
    from werkzeug.serving import run_simple
    run_simple("0.0.0.0", PORT, app)
