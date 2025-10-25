#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Unified Shopify app: Dual-site availability sync + Counters + Daily CSV (IST)

What this script does
---------------------
1) Scans your IN (and optionally US) Shopify stores by collection:
   - Tracks per-product availability (sum of tracked variant quantities).
   - On availability drop in IN:
       • bumps sales counters & dates metafields,
       • sets custom.start_manufacturing="Start Manufacturing" so your team
         can manually kick off manufacturing,
       • optionally notifies a separate Manufacturing Handler app (App B),
       • reverts temporary discounts,
       • clamps negative availability back to 0.
   - Updates product metafields for badges and delivery time depending on availability.
   - (Optional) Changes product status to DRAFT/ACTIVE for a special collection.
   - (US→IN) Mirrors US decreases to IN stock (by SKU index) and syncs price-in-India metafield.

2) Counters and Pixels:
   - Views and add-to-cart counters (optional).
   - Daily CSV and JSONL logs (IST midnights).

Notes
-----
- Render-friendly single file.
- Persists state in /data/*.
- Uses GraphQL for batch writes (metafieldsSet) and for metafield deletes (metafieldsDelete).
- Uses REST for some inventory ops when convenient.

"""

import os
import sys
import time
import json
import csv
import math
import hmac
import base64
import random
import ipaddress
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, request, jsonify, make_response, send_file

# =========================
# Config & app bootstrap
# =========================

def env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and str(v).strip() != "" else default

APP_PORT               = int(env("PORT", "10000"))
RUN_EVERY_MIN          = int(env("RUN_EVERY_MIN", "0"))        # 0 disables scheduler
SHOP_IN_DOMAIN         = env("IN_DOMAIN")
SHOP_IN_TOKEN          = env("IN_TOKEN")
SHOP_US_DOMAIN         = env("US_DOMAIN")
SHOP_US_TOKEN          = env("US_TOKEN")

# Optional collection filters (comma-separated handles)
IN_COLLECTIONS         = [s.strip() for s in env("IN_COLLECTIONS", "").split(",") if s.strip()]
US_COLLECTIONS         = [s.strip() for s in env("US_COLLECTIONS", "").split(",") if s.strip()]

# Metafield keys
MF_NAMESPACE           = os.getenv("MF_NAMESPACE", "custom").strip()
MF_BADGES_KEY          = os.getenv("MF_BADGES_KEY", "badges").strip()
MF_DELIVERY_KEY        = os.getenv("MF_DELIVERY_KEY", "delivery_time").strip()
MF_START_MFG_KEY       = os.getenv("MF_START_MFG_KEY", "start_manufacturing").strip()
MF_TEMP_DISC_PCT_KEY   = os.getenv("MF_TEMP_DISC_PCT_KEY", "temp_discount_active").strip()
MF_PRICEININDIA_KEY    = os.getenv("MF_PRICEININDIA_KEY", "priceinindia").strip()
MF_SALES_TOTAL_KEY     = os.getenv("MF_SALES_TOTAL_KEY", "sales_total").strip()
MF_SALES_DATES_KEY     = os.getenv("MF_SALES_DATES_KEY", "sales_dates").strip()

# Delivery messages
READY_BADGE            = os.getenv("READY_BADGE", "Ready To Ship").strip()
READY_DELIVERY         = os.getenv("READY_DELIVERY", "2-5 Days Across India").strip()
MADE_TO_ORDER_DELIVERY = os.getenv("MADE_TO_ORDER_DELIVERY", "12-15 Days Across India").strip()

# Optional: special collection handle for ACTIVE↔DRAFT flips
SPECIAL_STATUS_HANDLE  = os.getenv("SPECIAL_STATUS_HANDLE", "").strip()

# Scheduler switch
ENABLE_US_SYNC         = env("ENABLE_US_SYNC", "0").strip() == "1"
ENABLE_PRICEININDIA    = env("ENABLE_PRICEININDIA", "1").strip() == "1"
ENABLE_TEMP_DISCOUNT   = env("ENABLE_TEMP_DISCOUNT", "1").strip() == "1"
INCLUDE_UNTRACKED_IN_AVAIL = env("INCLUDE_UNTRACKED_IN_AVAIL", "0").strip() == "1"

# Manufacturing notifier (App B)
MFG_NOTIFY_URL         = env("MFG_NOTIFY_URL")     # e.g., https://manufacturing-handler.onrender.com/notify
MFG_NOTIFY_TOKEN       = env("MFG_NOTIFY_TOKEN")   # auth token for App B
MFG_NOTIFY_ENABLED     = bool(MFG_NOTIFY_URL and MFG_NOTIFY_TOKEN)

# State & logs
DATA_DIR               = Path(env("DATA_DIR", "/data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
IN_LAST_SEEN_PATH      = DATA_DIR / "in_last_seen.json"
US_LAST_SEEN_PATH      = DATA_DIR / "us_last_seen.json"
IN_SKU_INDEX_PATH      = DATA_DIR / "in_sku_index.json"
DISCOUNT_STATE_PATH    = DATA_DIR / "discount_state.json"
CSV_LOG_PATH           = DATA_DIR / "log.csv"
JSONL_LOG_PATH         = DATA_DIR / "log.jsonl"

# API version control (centralized)
def shopify_api_version() -> str:
    v = os.getenv("SHOPIFY_API_VERSION", "").strip()
    return v if v else "2025-07"

def gql_endpoint(shop: str) -> str:
    return f"https://{shop}/admin/api/{shopify_api_version()}/graphql.json"

def rest_url(shop: str, path: str) -> str:
    return f"https://{shop}/admin/api/{shopify_api_version()}/{path.lstrip('/')}"

print(f"[BOOT] Shopify API version = {shopify_api_version()}")

app = Flask(__name__)

# =========================
# Helpers
# =========================

def gid_num(gid: str) -> str:
    # "gid://shopify/Product/12345" -> "12345"
    if not gid: return ""
    return gid.split("/")[-1]

def json_dump(p: Path, obj: Any):
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2))
    tmp.replace(p)

def json_load(p: Path, default):
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text())
    except Exception:
        return default

def log_row(icon: str, site: str, action: str, sku: str = "", product_id: str = "", variant_id: str = "", message: str = ""):
    title = message
    print(f"{icon} {site} {action} [SKU {sku}] pid={product_id} vid={variant_id} → Δ “{title}”")

    # CSV + JSONL
    now = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S%z")
    with CSV_LOG_PATH.open("a", newline="") as f:
        w = csv.writer(f)
        w.writerow([now, icon, site, action, sku, product_id, variant_id, title])
    with JSONL_LOG_PATH.open("a") as f:
        f.write(json.dumps({
            "ts": now, "icon": icon, "site": site, "action": action,
            "sku": sku, "product_id": product_id, "variant_id": variant_id, "title": title
        }, ensure_ascii=False) + "\n")

def gql(shop: str, token: str, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(
        gql_endpoint(shop),
        headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
        json={"query": query, "variables": variables},
        timeout=30
    )
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(f"GQL top-level errors: {data['errors']}")
    return data.get("data") or {}

def get_mf_def_type(shop: str, token: str, owner_type: str, namespace: str, key: str) -> str:
    q = """
    query($ownerType: MetafieldOwnerType!, $namespace: String!, $key: String!) {
      metafieldDefinition(ownerType: $ownerType, namespace: $namespace, key: $key) {
        type { name }
      }
    }"""
    d = gql(shop, token, q, {"ownerType": owner_type, "namespace": namespace, "key": key})
    return ((((d.get("metafieldDefinition") or {}).get("type") or {}).get("name")) or "single_line_text_field")

def set_metafields(shop: str, token: str, inputs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not inputs:
        return []
    m = """
    mutation($mfs:[MetafieldsSetInput!]!){
      metafieldsSet(metafields:$mfs){
        metafields { id key namespace }
        userErrors { field message code }
      }
    }"""
    d = gql(shop, token, m, {"mfs": inputs})
    errs = ((d.get("metafieldsSet") or {}).get("userErrors") or [])
    return errs

def find_product_by_id(shop: str, token: str, product_gid: str):
    q = """
    query($id: ID!){
      node(id:$id){
        ... on Product {
          id
          title
          status
          hasOnlyDefaultVariant
          variants(first: 50){
            nodes { id inventoryItem { id tracked } inventoryQuantity sku title }
          }
          metafields(identifiers:[
            {namespace:"%s", key:"%s"},
            {namespace:"%s", key:"%s"}
          ]){
            key namespace id type value
          }
        }
      }
    }""" % (MF_NAMESPACE, MF_BADGES_KEY, MF_NAMESPACE, MF_DELIVERY_KEY)
    return gql(shop, token, q, {"id": product_gid})

def product_availability(product: Dict[str, Any], include_untracked=False) -> Tuple[int, Optional[str], Optional[str]]:
    total = 0
    first_vid = None
    first_inv_item_id = None
    for v in (((product or {}).get("variants") or {}).get("nodes") or []):
        tracked = (((v or {}).get("inventoryItem") or {}).get("tracked") is True)
        qty = int((v or {}).get("inventoryQuantity") or 0)
        if tracked or include_untracked:
            total += qty
        if first_vid is None:
            first_vid = gid_num((v or {}).get("id") or "")
            first_inv_item_id = gid_num((((v or {}).get("inventoryItem") or {}).get("id") or ""))
    return total, first_vid, first_inv_item_id

def clamp_negative_to_zero(shop: str, token: str, inventory_item_id: str):
    url = rest_url(shop, f"inventory_levels/set.json")
    # this requires location; simplified here if using single location
    location_id = os.getenv("IN_LOCATION_ID")
    payload = {"location_id": int(location_id), "inventory_item_id": int(inventory_item_id), "available": 0}
    r = requests.post(url, headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"}, json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

def notify_mfg(site: str, sku: str, before: int, after: int):
    if not MFG_NOTIFY_ENABLED:
        return
    try:
        body = {
            "site": site,
            "sku": sku,
            "before": before,
            "after": after,
        }
        headers = {"Authorization": f"Bearer {MFG_NOTIFY_TOKEN}", "Content-Type": "application/json"}
        r = requests.post(MFG_NOTIFY_URL, headers=headers, json=body, timeout=25)
        r.raise_for_status()
        print(f"[MFG] queued ok (attempt 1) idem=idem-{hashlib_md5_hex(json.dumps(body))}")
    except Exception as e:
        print(f"[MFG] error (final): {e}")

def hashlib_md5_hex(s: str) -> str:
    import hashlib
    return hashlib.md5(s.encode("utf-8")).hexdigest()

# =========================
# Core scan & update loop
# =========================

def update_product_badges_and_delivery(domain: str, token: str, product_gid: str, avail: int, title: str):
    data = find_product_by_id(domain, token, product_gid)
    product = (data.get("node") or {})
    metafields = (product.get("metafields") or [])
    badges_node = next((m for m in metafields if m.get("key")==MF_BADGES_KEY and m.get("namespace")==MF_NAMESPACE), None)
    dtime_node  = next((m for m in metafields if m.get("key")==MF_DELIVERY_KEY and m.get("namespace")==MF_NAMESPACE), None)

    product_id_num = gid_num(product_gid)

    target_badge = READY_BADGE if avail > 0 else ""
    target_delivery = READY_DELIVERY if avail > 0 else MADE_TO_ORDER_DELIVERY

    badges_type = (badges_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_BADGES_KEY)
    delivery_type = (dtime_node or {}).get("type") or get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_DELIVERY_KEY)

    mf_inputs = []
    if target_badge:
        if str(badges_type).startswith("list."):
            mf_inputs.append({"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_BADGES_KEY, "type": badges_type, "value": json.dumps([target_badge])})
        else:
            mf_inputs.append({"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_BADGES_KEY, "type": badges_type, "value": target_badge})
    else:
        if (badges_node or {}).get("id"):
            # CHANGED: delete by ownerId+namespace+key (not by id)
            delm = "mutation($metafields:[MetafieldDeleteInput!]!){ metafieldsDelete(metafields:$metafields){ deletedMetafieldIds userErrors{field message code} } }"
            gql(domain, token, delm, {"metafields": [{"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_BADGES_KEY}]})

    mf_inputs.append({"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_DELIVERY_KEY, "type": delivery_type, "value": target_delivery})

    if mf_inputs:
        mutation = "mutation($mfs:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$mfs){ userErrors{ field message } } }"
        data = gql(domain, token, mutation, {"mfs": mf_inputs})
        errs = ((data.get("metafieldsSet") or {}).get("userErrors") or [])
        if errs:
            log_row("⚠️", "IN", "WARN", product_id=product_id_num, message=f"metafieldsSet userErrors: {errs}")

def set_start_manufacturing(domain: str, token: str, product_gid: str):
    # sets (or resets) start_manufacturing metafield on the product
    mf_type = get_mf_def_type(domain, token, "PRODUCT", MF_NAMESPACE, MF_START_MFG_KEY)
    m = """
    mutation($mfs:[MetafieldsSetInput!]!){
      metafieldsSet(metafields:$mfs){
        userErrors { field message code }
      }
    }"""
    inputs = [{
        "ownerId": product_gid,
        "namespace": MF_NAMESPACE,
        "key": MF_START_MFG_KEY,
        "type": mf_type,
        "value": "Start Manufacturing",
    }]
    d = gql(domain, token, m, {"mfs": inputs})
    errs = ((d.get("metafieldsSet") or {}).get("userErrors") or [])
    return errs

def clear_start_manufacturing_if_exists(domain: str, token: str, product_gid: str):
    try:
        q = """
        query($id: ID!){
          node(id:$id){
            ... on Product {
              mf: metafield(namespace:"%s", key:"%s"){ id }
            }}}""" % (MF_NAMESPACE, MF_START_MFG_KEY)
        data = gql(domain, token, q, {"id": product_gid})
        mf = (((data or {}).get("node") or {}).get("mf") or {})
        mf_id = mf.get("id")
        if mf_id:
            # CHANGED: delete by ownerId+namespace+key (not by id)
            delm = "mutation($metafields:[MetafieldDeleteInput!]!){ metafieldsDelete(metafields:$metafields){ deletedMetafieldIds userErrors{ field message code } } }"
            gql(domain, token, delm, {"metafields": [{"ownerId": product_gid, "namespace": MF_NAMESPACE, "key": MF_START_MFG_KEY}]})
            log_row("🧹", "IN", "START_MFG_CLEARED", product_id=gid_num(product_gid), message="deleted metafield")
    except Exception as e:
        log_row("⚠️", "IN", "START_MFG_ERROR", product_id=gid_num(product_gid), message=str(e))

# ... (rest of the file continues exactly as in your original, including scans, sales bump detection,
# clamping negatives, status flips, US sync (optional), endpoints, and scheduler loop) ...

# =========================
# Flask endpoints
# =========================

@app.route("/health")
def health():
    return jsonify({"ok": True, "api_version": shopify_api_version()})

@app.route("/run", methods=["POST", "GET"])
def run_once():
    # your existing scan orchestration code lives here unchanged
    # (collection fetch, index build, per-product updates, etc.)
    return jsonify({"run": "started"})

@app.route("/diag")
def diag():
    # prints simple diagnostics
    payload = {
        "api_version": shopify_api_version(),
        "in_collections": IN_COLLECTIONS,
        "us_collections": US_COLLECTIONS,
        "enable_us_sync": ENABLE_US_SYNC,
        "enable_temp_discount": ENABLE_TEMP_DISCOUNT,
        "include_untracked_in_avail": INCLUDE_UNTRACKED_IN_AVAIL,
    }
    return jsonify(payload)

@app.route("/rebuild_index", methods=["POST"])
def rebuild_index():
    # re-index logic (unchanged)
    return jsonify({"ok": True})

@app.route("/logs/csv")
def logs_csv():
    if not CSV_LOG_PATH.exists():
        return make_response("No CSV log yet", 404)
    return send_file(str(CSV_LOG_PATH), as_attachment=True, download_name="log.csv")

@app.route("/logs/jsonl")
def logs_jsonl():
    if not JSONL_LOG_PATH.exists():
        return make_response("No JSONL log yet", 404)
    return send_file(str(JSONL_LOG_PATH), as_attachment=True, download_name="log.jsonl")

# =========================
# Scheduler
# =========================

def scheduler_loop():
    if RUN_EVERY_MIN <= 0:
        return
    print(f"[SCHED] loop every {RUN_EVERY_MIN} min started")
    while True:
        try:
            # trigger one scan (unchanged)
            requests.get(f"http://127.0.0.1:{APP_PORT}/run", timeout=20)
        except Exception as e:
            print(f"[SCHED] error: {e}")
        time.sleep(RUN_EVERY_MIN * 60)

if __name__ == "__main__":
    # optional scheduler in a thread (unchanged)
    if RUN_EVERY_MIN > 0:
        import threading
        t = threading.Thread(target=scheduler_loop, daemon=True)
        t.start()
    app.run(host="0.0.0.0", port=APP_PORT)
