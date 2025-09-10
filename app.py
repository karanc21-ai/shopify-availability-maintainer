#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shopify Availability Maintainer — Web App (Flask)

Endpoints:
- GET  /           : Status page (last run summary, recent logs)
- POST /run-now    : Trigger a run; requires ?token=YOUR_WEB_TRIGGER_TOKEN (or X-Run-Token header)
                     You can also use GET for convenience: /run-now?token=...

What it does per product in the target collection:
- Compute availability = sum of inventoryLevels.available across all variants tracked by Shopify (inventoryManagement == "SHOPIFY")
- If availability == 0:
    * If product.status == ACTIVE -> set status=DRAFT
    * metafield custom.badge        = "none"
    * metafield custom.delivery_time = "12-15 days across india"
- If availability > 0:
    * metafield custom.badge        = "Ready to Ship"
    * metafield custom.delivery_time = "2-5 days across india"
- It NEVER edits images/media, variants, titles, etc.
- After any write, it re-reads the product and verifies:
    * status and metafields match target values
    * media IDs and count are unchanged
  On mismatch: retries with backoff; aborts run on persistent mismatch.

Config (file-based, to match your preference):
- Put `access_token.txt` (Admin API token) next to this file (no quotes/spaces/newlines).
- Put `config.json` next to this file; example below.

Example config.json:
{
  "shop_domain": "your-shop.myshopify.com",
  "collection_handle": "gold-plated-rings",
  "collection_id": null,                    // or "gid://shopify/Collection/1234567890"
  "dry_run": true,
  "api_version": "2024-10",
  "rate_limit_sleep_sec": 0.6,
  "request_timeout_sec": 30,
  "canary_limit": 0,                        // 0 = no limit; e.g., 10 to test first 10 products
  "web_trigger_token": "CHANGE_ME_LONG_RANDOM",
  "log_dir": "./data/logs",                 // CSV audit logs here
  "run_state_dir": "./data/state",          // status json, last summary
  "lockfile_path": "/tmp/availability_maintainer.lock",
  "slack_webhook_url": null                 // or "https://hooks.slack.com/services/XXX/YYY/ZZZ"
}

Deploy notes:
- On Render: create a Web Service, add a Disk (e.g., 1 GB) mounted at /opt/render/project/src/data so logs persist.
- Add a "Secret File" for access_token.txt and another for config.json if you prefer; otherwise you can paste them in the repo.
"""

from __future__ import annotations
import os
import sys
import json
import csv
import time
import hmac
import hashlib
import signal
import typing as t
import pathlib
import threading
from datetime import datetime, timezone
from dataclasses import dataclass

import requests
from flask import Flask, request, jsonify, Response, abort

# ----------------------------
# Load config & setup paths
# ----------------------------
BASE_DIR = pathlib.Path(__file__).resolve().parent
CFG_PATH = BASE_DIR / "config.json"
TOKEN_PATH = BASE_DIR / "access_token.txt"

def load_config() -> dict:
    if not CFG_PATH.exists():
        raise SystemExit(f"[FATAL] Missing config.json at {CFG_PATH}")
    try:
        cfg = json.loads(CFG_PATH.read_text())
    except Exception as e:
        raise SystemExit(f"[FATAL] Failed to parse config.json: {e}")
    required = ["shop_domain", "dry_run", "api_version", "web_trigger_token"]
    for k in required:
        if k not in cfg:
            raise SystemExit(f"[FATAL] config.json missing required key: {k}")
    # defaults
    cfg.setdefault("collection_handle", None)
    cfg.setdefault("collection_id", None)
    cfg.setdefault("rate_limit_sleep_sec", 0.6)
    cfg.setdefault("request_timeout_sec", 30)
    cfg.setdefault("canary_limit", 0)
    cfg.setdefault("log_dir", str(BASE_DIR / "data" / "logs"))
    cfg.setdefault("run_state_dir", str(BASE_DIR / "data" / "state"))
    cfg.setdefault("lockfile_path", "/tmp/availability_maintainer.lock")
    cfg.setdefault("slack_webhook_url", None)
    return cfg

CONFIG = load_config()

def read_access_token() -> str:
    if not TOKEN_PATH.exists():
        raise SystemExit(f"[FATAL] Missing access_token.txt at {TOKEN_PATH}")
    tok = TOKEN_PATH.read_text().strip()
    if not tok:
        raise SystemExit("[FATAL] access_token.txt is empty")
    return tok

ACCESS_TOKEN = read_access_token()

SHOP_DOMAIN = CONFIG["shop_domain"]
API_VERSION = CONFIG["api_version"]
API_URL = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/graphql.json"
DRY_RUN = bool(CONFIG["dry_run"])
RATE_LIMIT_SLEEP = float(CONFIG["rate_limit_sleep_sec"])
REQUEST_TIMEOUT = int(CONFIG["request_timeout_sec"])
CANARY_LIMIT = int(CONFIG["canary_limit"])
LOCKFILE_PATH = CONFIG["lockfile_path"]
SLACK_WEBHOOK_URL = CONFIG["slack_webhook_url"]
WEB_TRIGGER_TOKEN = str(CONFIG["web_trigger_token"])

LOG_DIR = pathlib.Path(CONFIG["log_dir"])
STATE_DIR = pathlib.Path(CONFIG["run_state_dir"])
LOG_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

SESSION = requests.Session()
HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN,
}

# ----------------------------
# Flask app
# ----------------------------
app = Flask(__name__)

# ----------------------------
# Utilities
# ----------------------------
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def slack_alert(text: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=15)
    except Exception:
        pass

def safe_write_json(path: pathlib.Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(path)

def locked() -> bool:
    return pathlib.Path(LOCKFILE_PATH).exists()

def acquire_lock() -> None:
    if locked():
        raise RuntimeError("Another run is in progress (lockfile present).")
    pathlib.Path(LOCKFILE_PATH).write_text(
        json.dumps({"pid": os.getpid(), "start": now_utc_iso()})
    )

def release_lock() -> None:
    try:
        pathlib.Path(LOCKFILE_PATH).unlink(missing_ok=True)
    except Exception:
        pass

def gql(query: str, variables: dict | None = None, for_read: bool = True, max_attempts: int = 6) -> dict:
    """
    Robust GraphQL call with retries on 429, 5xx, and transient network errors.
    Exponential backoff: 0.8s, 1.6s, 3.2s, 6.4s, 12.8s, 25.6s (caps long before that in practice).
    """
    payload = {"query": query, "variables": variables or {}}
    delay = 0.8
    for attempt in range(1, max_attempts + 1):
        try:
            resp = SESSION.post(API_URL, headers=HEADERS, json=payload, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", "1"))
                time.sleep(retry_after)
                continue
            if resp.status_code >= 500:
                time.sleep(delay)
                delay *= 2
                continue
            if not resp.ok:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
            data = resp.json()
            if "errors" in data:
                # retry for reads; for writes retry only if clearly transient
                # here treat all as transient up to max_attempts
                time.sleep(delay)
                delay *= 2
                continue
            return data["data"]
        except requests.RequestException as e:
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f"GraphQL failed after {max_attempts} attempts: {query[:60]}...")

# ----------------------------
# GraphQL documents
# ----------------------------
Q_COLLECTION_BY_HANDLE = """
query($handle: String!, $cursor: String) {
  collectionByHandle(handle: $handle) {
    id
    products(first: 100, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        handle
        title
        status
        variants(first: 100) {
          nodes {
            id
            inventoryManagement
            inventoryItem {
              id
              inventoryLevels(first: 100) {
                nodes { available }
              }
            }
          }
        }
        media(first: 100) {
          nodes {
            id
            mediaContentType
          }
        }
        metafields(identifiers: [
          {namespace: "custom", key: "badge"},
          {namespace: "custom", key: "delivery_time"}
        ]) {
          key
          namespace
          value
        }
      }
    }
  }
}
"""

Q_COLLECTION_BY_ID = """
query($id: ID!, $cursor: String) {
  collection(id: $id) {
    id
    products(first: 100, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        handle
        title
        status
        variants(first: 100) {
          nodes {
            id
            inventoryManagement
            inventoryItem {
              id
              inventoryLevels(first: 100) {
                nodes { available }
              }
            }
          }
        }
        media(first: 100) {
          nodes {
            id
            mediaContentType
          }
        }
        metafields(identifiers: [
          {namespace: "custom", key: "badge"},
          {namespace: "custom", key: "delivery_time"}
        ]) {
          key
          namespace
          value
        }
      }
    }
  }
}
"""

M_PRODUCT_UPDATE_STATUS = """
mutation($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id status }
    userErrors { field message }
  }
}
"""

M_METAFIELDS_SET = """
mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields {
      id
      key
      namespace
      type
      value
      owner { __typename ... on Product { id } }
    }
    userErrors { field message code }
  }
}
"""

Q_PRODUCT_MINIMAL = """
query($id: ID!) {
  product(id: $id) {
    id
    status
    media(first: 100) {
      nodes { id mediaContentType }
    }
    metafields(identifiers: [
      {namespace: "custom", key: "badge"},
      {namespace: "custom", key: "delivery_time"}
    ]) {
      key
      namespace
      value
    }
  }
}
"""

# ----------------------------
# Core logic
# ----------------------------
BADGE_READY = "Ready to Ship"
BADGE_NONE  = "none"
DELIVERY_READY = "2-5 days across india"
DELIVERY_MTO   = "12-15 days across india"

@dataclass
class ProductState:
    id: str
    handle: str
    title: str
    status: str
    badge: str | None
    delivery: str | None
    media_ids: list[str]
    availability: int

def parse_metafields(mf_list: list[dict]) -> tuple[str|None, str|None]:
    badge = None
    delivery = None
    for mf in mf_list or []:
        if mf.get("namespace") == "custom" and mf.get("key") == "badge":
            badge = mf.get("value")
        elif mf.get("namespace") == "custom" and mf.get("key") == "delivery_time":
            delivery = mf.get("value")
    return badge, delivery

def compute_availability(prod_node: dict) -> int:
    total = 0
    tracked_found = False
    for v in (prod_node.get("variants", {}) or {}).get("nodes", []):
        if v.get("inventoryManagement") == "SHOPIFY":
            tracked_found = True
            levels = (v.get("inventoryItem", {}) or {}).get("inventoryLevels", {}) or {}
            for lvl in levels.get("nodes", []):
                av = lvl.get("available") or 0
                try:
                    total += int(av)
                except Exception:
                    pass
    if not tracked_found:
        return 0  # conservative: all-untracked treated as 0
    return total

def media_id_list(prod_node: dict) -> list[str]:
    return sorted([m.get("id") for m in (prod_node.get("media", {}) or {}).get("nodes", []) if m.get("id")])

def fetch_collection_products(collection_handle: str | None, collection_id: str | None) -> list[dict]:
    products = []
    cursor = None
    if collection_handle and not collection_id:
        while True:
            data = gql(Q_COLLECTION_BY_HANDLE, {"handle": collection_handle, "cursor": cursor})
            col = data.get("collectionByHandle")
            if not col:
                raise RuntimeError(f"Collection handle not found: {collection_handle}")
            page = col["products"]
            products.extend(page["nodes"])
            if not page["pageInfo"]["hasNextPage"]:
                break
            cursor = page["pageInfo"]["endCursor"]
            time.sleep(RATE_LIMIT_SLEEP)
    elif collection_id:
        while True:
            data = gql(Q_COLLECTION_BY_ID, {"id": collection_id, "cursor": cursor})
            col = data.get("collection")
            if not col:
                raise RuntimeError(f"Collection id not found: {collection_id}")
            page = col["products"]
            products.extend(page["nodes"])
            if not page["pageInfo"]["hasNextPage"]:
                break
            cursor = page["pageInfo"]["endCursor"]
            time.sleep(RATE_LIMIT_SLEEP)
    else:
        raise RuntimeError("Set either collection_handle or collection_id in config.json")
    return products

def build_state(prod_node: dict) -> ProductState:
    badge, delivery = parse_metafields(prod_node.get("metafields"))
    return ProductState(
        id=prod_node["id"],
        handle=prod_node.get("handle") or "",
        title=prod_node.get("title") or "",
        status=prod_node.get("status") or "",
        badge=badge,
        delivery=delivery,
        media_ids=media_id_list(prod_node),
        availability=compute_availability(prod_node),
    )

def desired_values(avail: int, current_status: str) -> tuple[str|None, str, str]:
    """
    Returns: (new_status_or_None_to_keep, target_badge, target_delivery)
    We ONLY change status when availability==0 and current status is ACTIVE -> DRAFT.
    We DO NOT auto-activate when availability>0.
    """
    if avail <= 0:
        new_status = "DRAFT" if current_status == "ACTIVE" else None
        return new_status, BADGE_NONE, DELIVERY_MTO
    else:
        return None, BADGE_READY, DELIVERY_READY

def product_update_status(product_id: str, new_status: str) -> list[dict]:
    if DRY_RUN:
        return []
    data = gql(M_PRODUCT_UPDATE_STATUS, {"input": {"id": product_id, "status": new_status}}, for_read=False)
    return data["productUpdate"]["userErrors"]

def metafields_set(product_id: str, badge_val: str, delivery_val: str) -> list[dict]:
    metas = [
        {
            "ownerId": product_id,
            "namespace": "custom",
            "key": "badge",
            "type": "single_line_text_field",
            "value": badge_val
        },
        {
            "ownerId": product_id,
            "namespace": "custom",
            "key": "delivery_time",
            "type": "single_line_text_field",
            "value": delivery_val
        }
    ]
    if DRY_RUN:
        return []
    data = gql(M_METAFIELDS_SET, {"metafields": metas}, for_read=False)
    return data["metafieldsSet"]["userErrors"]

def fetch_product_minimal(product_id: str) -> dict:
    data = gql(Q_PRODUCT_MINIMAL, {"id": product_id})
    return data["product"]

def verify_product(product_id: str, target_status: str | None, target_badge: str, target_delivery: str, before_media_ids: list[str]) -> bool:
    """
    Fresh re-read: check that fields match targets and media IDs unchanged.
    """
    prod = fetch_product_minimal(product_id)
    # Verify status (only if we intended to change it)
    if target_status is not None:
        if (prod.get("status") or "").upper() != (target_status or "").upper():
            return False
    # Verify metafields
    badge, delivery = parse_metafields(prod.get("metafields"))
    if (badge or "").strip() != target_badge or (delivery or "").strip() != target_delivery:
        return False
    # Verify media integrity
    after_media = sorted([m.get("id") for m in (prod.get("media", {}) or {}).get("nodes", []) if m.get("id")])
    return after_media == sorted(before_media_ids)

def ensure_dirs():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

def audit_log_path() -> pathlib.Path:
    ts = datetime.now().strftime("%Y%m%d")
    return LOG_DIR / f"audit_{ts}.csv"

def append_audit_row(row: dict) -> None:
    path = audit_log_path()
    new_file = not path.exists()
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "ts", "product_id", "handle", "title",
            "availability",
            "status_before", "status_after",
            "badge_before", "badge_after",
            "delivery_before", "delivery_after",
            "media_count_before", "media_count_after",
            "changed_status", "wrote_metafields",
            "verification_passed", "retries_used", "error"
        ])
        if new_file:
            writer.writeheader()
        writer.writerow(row)

def run_job() -> dict:
    """
    The main run. Returns a summary dict.
    """
    ensure_dirs()
    start = time.time()
    summary = {
        "start_utc": now_utc_iso(),
        "shop_domain": SHOP_DOMAIN,
        "dry_run": DRY_RUN,
        "canary_limit": CANARY_LIMIT,
        "products_seen": 0,
        "products_touched": 0,
        "status_changes": 0,
        "metafields_updates": 0,
        "verify_failures": 0,
        "errors": 0,
        "aborted": False,
        "message": "",
        "duration_sec": None,
    }

    try:
        acquire_lock()
    except Exception as e:
        summary["aborted"] = True
        summary["message"] = f"Lockfile present: {e}"
        return summary

    try:
        # Fetch scope
        products = fetch_collection_products(CONFIG.get("collection_handle"), CONFIG.get("collection_id"))
        total = len(products)
        summary["products_seen"] = total

        # Canary
        if CANARY_LIMIT > 0:
            products = products[:CANARY_LIMIT]

        for idx, p in enumerate(products, start=1):
            state = build_state(p)
            before_status = state.status
            before_badge = state.badge
            before_delivery = state.delivery
            before_media = list(state.media_ids)
            avail = state.availability

            new_status, want_badge, want_delivery = desired_values(avail, before_status)

            need_status = new_status is not None
            need_mf = ((before_badge or "") != want_badge) or ((before_delivery or "") != want_delivery)

            if not need_status and not need_mf:
                # no-op
                continue

            summary["products_touched"] += 1
            changed_status = False
            wrote_mf = False
            err_text = ""
            retries_used = 0

            # ---- Apply writes (surgical) ----
            try:
                if need_status:
                    ue = product_update_status(state.id, new_status)
                    if ue:
                        raise RuntimeError(f"productUpdate userErrors: {ue}")
                    changed_status = True
                    time.sleep(RATE_LIMIT_SLEEP)

                if need_mf:
                    ue = metafields_set(state.id, want_badge, want_delivery)
                    if ue:
                        raise RuntimeError(f"metafieldsSet userErrors: {ue}")
                    wrote_mf = True
                    time.sleep(RATE_LIMIT_SLEEP)

                # ---- Verify with bounded retries ----
                ok = False
                for attempt in range(1, 4):  # 3 attempts: 0s, 1s, 2s backoff
                    if attempt > 1:
                        time.sleep(attempt - 1)
                    if verify_product(state.id, new_status, want_badge, want_delivery, before_media):
                        ok = True
                        retries_used = attempt - 1
                        break

                if not ok:
                    summary["verify_failures"] += 1
                    err_text = "Post-write verification failed (status/metafields/media mismatch)."
                    # hard abort on first verification failure to minimize blast radius
                    summary["aborted"] = True
                    summary["message"] = f"Aborting due to verification failure on product {state.handle} ({state.id})."
                    slack_alert(f":warning: Shopify job aborted — verification failed for product {state.handle} ({state.id}).")
                    # log audit row then break
                else:
                    if changed_status:
                        summary["status_changes"] += 1
                    if wrote_mf:
                        summary["metafields_updates"] += 1

            except Exception as e:
                summary["errors"] += 1
                err_text = f"{e}"
                # Do not abort entire run for isolated API errors; continue to next product.

            # Audit row (even for no-ops we skip writing; for touches we record)
            try:
                # Re-read minimal (for after values & media count), but only if we didn't already abort on verify
                if not summary["aborted"]:
                    prod_min = fetch_product_minimal(state.id)
                    after_status = prod_min.get("status") or before_status
                    a_badge, a_delivery = parse_metafields(prod_min.get("metafields"))
                    after_media_count = len((prod_min.get("media") or {}).get("nodes", []))
                else:
                    after_status = before_status
                    a_badge, a_delivery = before_badge, before_delivery
                    after_media_count = len(before_media)

                append_audit_row({
                    "ts": datetime.now().isoformat(),
                    "product_id": state.id,
                    "handle": state.handle,
                    "title": state.title,
                    "availability": avail,
                    "status_before": before_status,
                    "status_after": after_status,
                    "badge_before": before_badge,
                    "badge_after": a_badge,
                    "delivery_before": before_delivery,
                    "delivery_after": a_delivery,
                    "media_count_before": len(before_media),
                    "media_count_after": after_media_count,
                    "changed_status": changed_status,
                    "wrote_metafields": wrote_mf,
                    "verification_passed": (err_text == ""),
                    "retries_used": retries_used,
                    "error": err_text,
                })
            except Exception:
                # ignore logging errors
                pass

            if summary["aborted"]:
                break

            # be gentle
            time.sleep(RATE_LIMIT_SLEEP)

    except Exception as e:
        summary["errors"] += 1
        summary["aborted"] = True
        summary["message"] = f"Run-level exception: {e}"
        slack_alert(f":x: Shopify job crashed: {e}")
    finally:
        release_lock()
        summary["duration_sec"] = round(time.time() - start, 2)
        # persist summary for status page
        safe_write_json(STATE_DIR / "last_run_summary.json", summary)

    return summary

# ----------------------------
# Web endpoints
# ----------------------------
def _authorized(req) -> bool:
    # token in query string or header
    qtok = req.args.get("token")
    htok = req.headers.get("X-Run-Token")
    token = qtok or htok or ""
    # simple constant-time compare
    return hmac.compare_digest(token, WEB_TRIGGER_TOKEN)

@app.route("/", methods=["GET"])
def status_page():
    summary_path = STATE_DIR / "last_run_summary.json"
    if summary_path.exists():
        last = json.loads(summary_path.read_text())
    else:
        last = {"message": "No runs yet."}

    # Show last 200 lines of latest audit log (if present)
    latest_csv = None
    try:
        csv_files = sorted(LOG_DIR.glob("audit_*.csv"))
        if csv_files:
            latest_csv = csv_files[-1]
    except Exception:
        pass

    recent_lines = []
    if latest_csv and latest_csv.exists():
        try:
            with latest_csv.open("r") as f:
                lines = f.readlines()
            recent_lines = lines[-200:]
        except Exception:
            pass

    html = f"""
    <html>
    <head>
      <meta charset="utf-8" />
      <title>Shopify Availability Maintainer</title>
      <style>
        body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; padding: 20px; color: #eee; background:#111; }}
        .card {{ background:#1b1b1b; border:1px solid #333; border-radius:12px; padding:16px; margin-bottom:20px; }}
        pre {{ white-space: pre-wrap; word-wrap: break-word; background:#0c0c0c; padding:12px; border-radius:8px; border:1px solid #222; }}
        .btn {{ display:inline-block; padding:10px 14px; background:#2d6cdf; color:#fff; border-radius:8px; text-decoration:none; }}
        code {{ color:#ddd; }}
      </style>
    </head>
    <body>
      <h1>Shopify Availability Maintainer</h1>

      <div class="card">
        <h2>Config</h2>
        <pre>{json.dumps({k: CONFIG[k] for k in CONFIG if k not in ["web_trigger_token", "slack_webhook_url"]}, indent=2)}</pre>
      </div>

      <div class="card">
        <h2>Last Run Summary</h2>
        <pre>{json.dumps(last, indent=2)}</pre>
      </div>

      <div class="card">
        <h2>Manual Trigger</h2>
        <p>Send a POST/GET to <code>/run-now?token=YOUR_WEB_TRIGGER_TOKEN</code></p>
        <a class="btn" href="/run-now?token={WEB_TRIGGER_TOKEN}">Run now</a>
      </div>

      <div class="card">
        <h2>Recent Audit Log</h2>
        <p>Latest file: <code>{latest_csv}</code></p>
        <pre>{''.join(recent_lines) if recent_lines else 'No audit log yet.'}</pre>
      </div>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")

@app.route("/run-now", methods=["GET", "POST"])
def run_now():
    if not _authorized(request):
        abort(401)
    if locked():
        return jsonify({"ok": False, "message": "Run already in progress."}), 409
    summary = run_job()
    return jsonify({"ok": True, "summary": summary})

# ----------------------------
# Entry
# ----------------------------
if __name__ == "__main__":
    # Local dev: python app.py
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
