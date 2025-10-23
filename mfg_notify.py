# mfg_notify.py — App A → App B notifier (idempotent + retries)

import os, json, time, hmac, base64, hashlib, random
from typing import Optional
import requests

# ---- config helpers ----
def env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and str(v).strip() != "" else default

B_ENDPOINT_URL        = env("B_ENDPOINT_URL", "")           # e.g. https://manufacturing-handler.onrender.com/manufacturing/start
A_TO_B_SHARED_SECRET  = env("A_TO_B_SHARED_SECRET", "")     # must match App B

# ---- internal helpers ----
def _idem_key(source_site: str, sku: str, product_id: str, variant_id: str, prev_avail: int, new_avail: int) -> str:
    """
    Must match App B's de-dupe logic (same concatenation & hashing).
    """
    raw = f"{source_site}|{sku}|{product_id}|{variant_id}|{prev_avail}->{new_avail}"
    return "idem-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

def _sign_b64(secret: str, body_bytes: bytes) -> str:
    dig = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).digest()
    return base64.b64encode(dig).decode("utf-8")

def _jitter_backoff(attempt: int, base: int = 2, cap: int = 8) -> float:
    # 1,2,4,8 (capped), + small jitter
    delay = min(base ** attempt, cap)
    return delay + random.random()

# ---- public API ----
def notify_mfg_sale(
    *,
    endpoint_url: Optional[str],
    shared_secret: Optional[str],
    source_site: str,         # e.g. "IN" / "US"
    sku: str,                 # e.g. "PS 056"
    product_id: str,          # Shopify product id
    variant_id: str,          # Shopify variant id (use "0" if not applicable)
    prev_avail: int,          # availability_before
    new_avail: int,           # availability_after
    title: str = "",          # optional product title
    image_url: Optional[str] = None,  # optional media URL for WhatsApp
    body: Optional[str] = None,       # optional custom text
    max_retries: int = 4,
    timeout_connect: int = 3,
    timeout_read: int = 5,
) -> bool:
    """
    Returns True if the event was accepted/queued/duplicate (no further action needed),
    False for non-retryable errors or after all retries fail.
    """
    endpoint_url = (endpoint_url or B_ENDPOINT_URL).strip()
    shared_secret = (shared_secret or A_TO_B_SHARED_SECRET).strip()

    if not endpoint_url or not shared_secret:
        print("[MFG] skipped: endpoint or secret not configured", flush=True)
        return False

    idem = _idem_key(source_site, sku, str(product_id), str(variant_id), prev_avail, new_avail)
    payload = {
        "idempotency_key": idem,
        "source_site": source_site,
        "sku": sku,
        "availability_before": int(prev_avail),
        "availability_after": int(new_avail),
        "delta": int(new_avail) - int(prev_avail),
        "product_id": str(product_id),
        "variant_id": str(variant_id),
        "title": title or "",
        "image_url": image_url or None,
        "body": body or "",
    }
    body_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "X-A-Signature": _sign_b64(shared_secret, body_bytes),
    }

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(
                endpoint_url,
                data=body_bytes,
                headers=headers,
                timeout=(timeout_connect, timeout_read),
            )
            # Success paths
            if resp.status_code in (200, 201, 202):
                print(f"[MFG] queued ok (attempt {attempt}) idem={idem}", flush=True)
                return True
            if resp.status_code == 409:
                print(f"[MFG] duplicate (already queued/sent) idem={idem}", flush=True)
                return True
            # Retryable: 5xx/429
            if resp.status_code >= 500 or resp.status_code == 429:
                last_err = f"{resp.status_code} {resp.text[:200]}"
            else:
                # Non-retryable client error
                print(f"[MFG] non-retryable {resp.status_code}: {resp.text[:200]}", flush=True)
                return False
        except requests.RequestException as e:
            last_err = str(e)

        # Backoff with jitter
        time.sleep(_jitter_backoff(attempt))

    print(f"[MFG] error (final): {last_err}", flush=True)
    return False
