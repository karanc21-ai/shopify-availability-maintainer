#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shopify availability → status + metafields updater (event-driven via webhook)
Admin API: 2024-10

Behavior:
- Listens to Shopify webhook: inventory_levels/update
- For each inventory_item_id in the webhook, finds the owning product
- Recomputes product availability = sum of ALL tracked variants' inventoryQuantity
- If availability == 0:
    - (optionally) status → DRAFT            (CHANGE_STATUS=True)
    - badges (custom.badges) → **deleted**   (mirrors UI "Clear all")
    - delivery_time (custom.delivery_time) → "12-15 Days Across India"
- If availability > 0:
    - (optionally) status → ACTIVE
    - badges → "Ready To Ship"
    - delivery_time → "2-5 Days Across India"
"""

import os
import time
import json
import hmac
import base64
import threading
from typing import Dict, Any, Optional, List, Tuple

import requests
from flask import Flask, request, jsonify

# --------------------
# CONFIG (via env)
# --------------------
ADMIN_HOST = os.environ.get("ADMIN_HOST", "silver-rudradhan.myshopify.com")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")                 # REQUIRED (Admin access token)
SHOPIFY_WEBHOOK_SECRET = os.environ.get("SHOPIFY_WEBHOOK_SECRET")  # REQUIRED (webhook signing key / app API secret)
API_VERSION = os.environ.get("ADMIN_API_VERSION", "2024-10")

# Behavior flags
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
CHANGE_STATUS = os.environ.get("CHANGE_STATUS", "false").lower() == "true"

# Metafields
MF_NAMESPACE = os.environ.get("MF_NAMESPACE", "custom")
MF_BADGES_KEY = os.environ.get("MF_BADGES_KEY", "badges")
MF_DELIVERY_KEY = os.environ.get("MF_DELIVERY_KEY", "delivery_time")

BADGE_READY    = os.environ.get("BADGE_READY", "Ready To Ship")
DELIVERY_READY = os.environ.get("DELIVERY_READY", "2-5 Days Across India")
DELIVERY_MTO   = os.environ.get("DELIVERY_MTO", "12-15 Days Across India")

# If your badges metafield is a LIST, keep this override; if scalar, change to "single_line_text_field"
TYPE_OVERRIDE: Dict[Tuple[str, str], str] = {
    (MF_NAMESPACE, MF_BADGES_KEY): os.environ.get("BADGES_TYPE", "list.single_line_text_field"),
    # You can force delivery_time type too if needed:
    # (MF_NAMESPACE, MF_DELIVERY_KEY): "single_line_text_field",
}

# Debounce same product within this window (seconds)
DEBOUNCE_SEC = int(os.environ.get("DEBOUNCE_SEC", "10"))

# --------------------
# Flask
# --------------------
app = Flask(__name__)
app.url_map.strict_slashes = False

HEADERS = {
    "X-Shopify-Access-Token": ADMIN_TOKEN or "",
    "Content-Type": "application/json",
}

if not ADMIN_TOKEN:
    raise RuntimeError("ADMIN_TOKEN env var is required")
if not SHOPIFY_WEBHOOK_SECRET:
    raise RuntimeError("SHOPIFY_WEBHOOK_SECRET env var is required")

# --------------------
# Helpers
# --------------------
def _verify_shopify_hmac(raw_body: bytes, header_hmac: str) -> bool:
    try:
        digest = hmac.new(SHOPIFY_WEBHOOK_SECRET.encode("utf-8"), raw_body, digestmod="sha256").digest()
        expected = base64.b64encode(digest).decode("utf-8")
        return hmac.compare_digest(expected, header_hmac or "")
    except Exception:
        return False

def gql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"https://{ADMIN_HOST}/admin/api/{API_VERSION}/graphql.json"
    r = requests.post(url, headers=HEADERS, json={"query": query, "variables": variables or {}}, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(f"GraphQL errors: {json.dumps(data['errors'], indent=2)}")
    return data.get("data", {})

def rest_get_variants_by_inventory_item_ids(inv_ids: list) -> list:
    """
    Map inventory_item_ids -> variants (then -> product_id).
    Uses REST: /variants.json?inventory_item_ids=...
    """
    base = f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/variants.json"
    out = []
    CHUNK = 50
    for i in range(0, len(inv_ids), CHUNK):
        ids = ",".join(inv_ids[i:i+CHUNK])
        r = requests.get(base, headers=HEADERS, params={"inventory_item_ids": ids, "limit": 250}, timeout=30)
        if r.status_code != 200:
            print(f"[REST] variants http {r.status_code}: {r.text[:200]}")
            continue
        js = r.json()
        out.extend(js.get("variants", []))
        time.sleep(0.2)
    return out


# --------------------
# Availability logic
# --------------------
def compute_availability_from_variants(variants: List[Dict[str, Any]]) -> int:
    total = 0
    any_tracked = False
    for v in variants:
        tracked = bool((v.get("inventoryItem") or {}).get("tracked"))
        qty = int(v.get("inventoryQuantity") or 0)
        if tracked:
            any_tracked = True
            total += max(qty, 0)
    return total if any_tracked else 0

def desired_state_for_availability(total_avail: int) -> Tuple[str, str, str, str]:
    if total_avail > 0:
        return ("ACTIVE", BADGE_READY, DELIVERY_READY, "READY")
    else:
        return ("DRAFT", "", DELIVERY_MTO, "MTO")

# --------------------
# GraphQL pieces
# --------------------
QUERY_PRODUCT_FOR_AVAIL = """
query ProductAvailability($id: ID!) {
  product(id: $id) {
    id
    status
    variants(first: 100) {
      nodes {
        id
        title
        inventoryQuantity
        inventoryPolicy
        inventoryItem { id tracked }
      }
    }
    badges: metafield(namespace: "%(NS)s", key: "%(BK)s") { id value type }
    dtime:  metafield(namespace: "%(NS)s", key: "%(DK)s") { id value type }
  }
}
""".replace("%(NS)s", MF_NAMESPACE).replace("%(BK)s", MF_BADGES_KEY).replace("%(DK)s", MF_DELIVERY_KEY)

MUTATION_PRODUCT_UPDATE = """
mutation ProductUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id status }
    userErrors { field message }
  }
}
"""

MUTATION_METAFIELDS_SET = """
mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key value type }
    userErrors { field message }
  }
}
"""

MUTATION_METAFIELD_DELETE = """
mutation MetafieldDelete($input: MetafieldDeleteInput!) {
  metafieldDelete(input: $input) {
    deletedId
    userErrors { field message }
  }
}
"""

def resolve_mf_type(ns: str, key: str, node: Optional[Dict[str, Any]]) -> str:
    t = TYPE_OVERRIDE.get((ns, key))
    if t:
        return t
    if node and isinstance(node.get("type"), str) and node["type"]:
        return node["type"]
    return "single_line_text_field"

def format_value_for_type(desired_value: str, mf_type: str) -> str:
    if mf_type.startswith("list."):
        return "[]" if not desired_value else json.dumps([desired_value])
    return desired_value or ""

def update_product_status(product_gid: str, target_status: str) -> None:
    if DRY_RUN:
        print(f"[DRY] productUpdate {product_gid} → status={target_status}")
        return
    data = gql(MUTATION_PRODUCT_UPDATE, {"input": {"id": product_gid, "status": target_status}})
    ue = (data.get("productUpdate") or {}).get("userErrors") or []
    if ue:
        print(f"[WARN] productUpdate errors: {ue}")
    time.sleep(0.3)

def delete_metafield(metafield_id: Optional[str]) -> None:
    if not metafield_id:
        return
    if DRY_RUN:
        print(f"[DRY] metafieldDelete {metafield_id}")
        return
    data = gql(MUTATION_METAFIELD_DELETE, {"input": {"id": metafield_id}})
    ue = (data.get("metafieldDelete") or {}).get("userErrors") or []
    if ue:
        print(f"[WARN] metafieldDelete errors: {ue}")
    time.sleep(0.3)

def set_metafields(product_gid: str,
                   badges_value: str,
                   delivery_value: str,
                   badges_node: Optional[Dict[str, Any]],
                   dtime_node: Optional[Dict[str, Any]]) -> None:
    mf_inputs = []
    badges_type = resolve_mf_type(MF_NAMESPACE, MF_BADGES_KEY, badges_node)
    delivery_type = resolve_mf_type(MF_NAMESPACE, MF_DELIVERY_KEY, dtime_node)

    # badges
    if not badges_value:
        # delete to mirror "Clear all"
        delete_metafield((badges_node or {}).get("id"))
    else:
        mf_inputs.append({
            "ownerId": product_gid,
            "namespace": MF_NAMESPACE,
            "key": MF_BADGES_KEY,
            "type": badges_type,
            "value": format_value_for_type(badges_value, badges_type),
        })

    # delivery_time
    mf_inputs.append({
        "ownerId": product_gid,
        "namespace": MF_NAMESPACE,
        "key": MF_DELIVERY_KEY,
        "type": delivery_type,
        "value": format_value_for_type(delivery_value, delivery_type),
    })

    if mf_inputs:
        if DRY_RUN:
            print(f"[DRY] metafieldsSet {product_gid} -> {[(i['key'], i['type'], i['value']) for i in mf_inputs]}")
            return
        data = gql(MUTATION_METAFIELDS_SET, {"metafields": mf_inputs})
        ue = (data.get("metafieldsSet") or {}).get("userErrors") or []
        if ue:
            print(f"[WARN] metafieldsSet errors: {ue}")
        time.sleep(0.3)

# --------------------
# Worker: recompute & apply for a product
# --------------------
def recompute_and_apply(product_id_numeric: str) -> None:
    try:
        product_gid = f"gid://shopify/Product/{product_id_numeric}"
        data = gql(QUERY_PRODUCT_FOR_AVAIL, {"id": product_gid})
        p = (data.get("product") or {})
        if not p:
            print(f"[WARN] product not found: {product_id_numeric}")
            return

        variants = ((p.get("variants") or {}).get("nodes")) or []
        total = compute_availability_from_variants(variants)
        target_status, target_badge, target_delivery, target_mode = desired_state_for_availability(total)

        print(f"[AVAIL] product {product_id_numeric} total={total} → mode={target_mode}")

        if CHANGE_STATUS and (p.get("status") != target_status):
            update_product_status(product_gid, target_status)

        set_metafields(
            product_gid,
            badges_value=target_badge,
            delivery_value=target_delivery,
            badges_node=p.get("badges") or None,
            dtime_node=p.get("dtime") or None,
        )
    except Exception as e:
        print(f"[ERR] recompute_and_apply({product_id_numeric}): {e}")

# Simple per-product debounce
_last_run_by_product: Dict[str, float] = {}
_run_lock = threading.Lock()

def schedule_product_update(product_id_numeric: str):
    now = time.time()
    with _run_lock:
        last = _last_run_by_product.get(product_id_numeric, 0)
        if now - last < DEBOUNCE_SEC:
            return
        _last_run_by_product[product_id_numeric] = now

    threading.Thread(target=recompute_and_apply, args=(product_id_numeric,), daemon=True).start()

# --------------------
# Routes
# --------------------
@app.get("/")
def root():
    return "ok", 200

@app.get("/health")
def health():
    return "ok", 200

@app.get("/run")
def run_manual():
    """Manual test: ?product_id=1234567890"""
    pid = (request.args.get("product_id") or "").strip()
    if not pid:
        return "missing product_id", 400
    schedule_product_update(pid)
    return jsonify({"queued": pid}), 200

@app.route("/webhook/inventory", methods=["POST", "GET"])
@app.route("/webhook/inventory", methods=["POST", "GET"])
def webhook_inventory():
    # Quick liveness probe (Shopify will POST; GET is for you)
    if request.method == "GET":
        return "route-alive", 200

    raw = request.get_data()
    header_hmac = request.headers.get("X-Shopify-Hmac-SHA256", "")
    if not _verify_shopify_hmac(raw, header_hmac):
        print("[WEBHOOK] 401 bad HMAC")
        return "unauthorized", 401

    topic = (request.headers.get("X-Shopify-Topic", "") or "").strip()
    SHOP  = (request.headers.get("X-Shopify-Shop-Domain", "") or "").strip()
    print(f"[WEBHOOK] {topic} from {shop}")

    payload = request.get_json(silent=True) or {}
    inv_ids = set()

    def _add(val):
        s = str(val or "").strip()
        if s:
            # only keep digits (inventory item IDs are numeric)
            s = "".join(ch for ch in s if ch.isdigit())
            if s:
                inv_ids.add(s)

    # ----- Accept both shapes -----
    # A) inventory_levels/update (what fires when you change "Available" on a variant)
    if "levels" in topic:
        if isinstance(payload, dict):
            if "inventory_item_id" in payload:
                _add(payload.get("inventory_item_id"))
            gid = payload.get("admin_graphql_api_id", "")
            # gid://shopify/InventoryLevel/<level>?inventory_item_id=27187834659643
            if "InventoryLevel" in gid and "inventory_item_id=" in gid:
                _add(gid.split("inventory_item_id=")[-1].split("&")[0])

    # B) inventory_items/update (top-level id is the inventory item id)
    if "items" in topic:
        if isinstance(payload, dict):
            if "id" in payload:
                _add(payload.get("id"))
            gid = payload.get("admin_graphql_api_id", "")
            # gid://shopify/InventoryItem/27187834659643
            if "InventoryItem" in gid:
                _add(gid.split("/")[-1].split("?")[0])

    # C) Some send arrays
    if isinstance(payload, list):
        for it in payload:
            if not isinstance(it, dict):
                continue
            if "inventory_item_id" in it:
                _add(it.get("inventory_item_id"))
            if "id" in it and "items" in topic:
                _add(it.get("id"))
            gid = it.get("admin_graphql_api_id", "")
            if "InventoryLevel" in gid and "inventory_item_id=" in gid:
                _add(gid.split("inventory_item_id=")[-1].split("&")[0])
            if "InventoryItem" in gid:
                _add(gid.split("/")[-1].split("?")[0])

    if not inv_ids:
        print("[WEBHOOK] no inventory_item_id found in payload")
        return "ok", 200

    try:
        variants = rest_get_variants_by_inventory_item_ids(sorted(inv_ids))
        product_ids = {str(v.get("product_id")) for v in variants if v.get("product_id")}
        if not product_ids:
            print(f"[WEBHOOK] no variants for inventory_item_ids={sorted(inv_ids)}")
            return "ok", 200

        print(f"[WEBHOOK] will recompute {len(product_ids)} product(s)")
        for pid in product_ids:
            # If your code has a queue: schedule_product_update(pid)
            try:
                schedule_product_update(pid)  # preferred if you already have it
            except NameError:
                # direct fallback
                recompute_and_push_for_product(pid)
    except Exception as e:
        print(f"[ERR] mapping inventory_item -> product: {e}")

    return "ok", 200

    try:
        variants = rest_get_variants_by_inventory_item_ids(sorted(inv_ids))
        product_ids = {str(v.get("product_id")) for v in variants if v.get("product_id")}
        if not product_ids:
            print(f"[INFO] no variants found for inv_item_ids={sorted(inv_ids)}")
        for pid in product_ids:
            schedule_product_update(pid)
    except Exception as e:
        print(f"[ERR] mapping inventory_item -> product: {e}")

    return "ok", 200

# Print routes at boot (useful in Render logs)
def _print_routes():
    print("[ROUTES] URL map:")
    for rule in app.url_map.iter_rules():
        print(f"  {rule.rule}  ->  methods={','.join(sorted(rule.methods or []))}")
_print_routes()

# --------------------
# Gunicorn entrypoint
# --------------------
if __name__ == "__main__":
    # Local dev
    port = int(os.environ.get("PORT", "5050"))
    app.run(host="0.0.0.0", port=port, debug=False)
