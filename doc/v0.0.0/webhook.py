"""Webhook delivery (contract): POST the payload to the registered callback URL with the contract
headers, retrying on the 30s / 2m / 10m schedule. A delivery fails if the callback does not return
HTTP 2xx within the configured timeout (10s). Runs on the job thread after processing completes."""
import json
import time
import urllib.request
import uuid

from config import Config
from logging_setup import get_logger

log = get_logger("webhook")

EVENT_SCHEMA_GENERATED = "schema.generated"
EVENT_MAPPING_COMPLETED = "mapping.completed"


def _post(url, body_bytes, headers, timeout):
    req = urllib.request.Request(url, data=body_bytes, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return getattr(resp, "status", resp.getcode())


def deliver(event, profile_id, payload, callback_url=None):
    """Deliver one webhook with the contract retry policy. Returns True on a 2xx, else False."""
    url = callback_url or Config.WEBHOOK_CALLBACK_URL
    delivery_id = str(uuid.uuid4())
    if not url:
        log.warning("no webhook callback configured; skipping delivery",
                    extra={"profile_id": profile_id, "event": event, "delivery_id": delivery_id})
        return False

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Event": event,
        "X-Webhook-Profile-Id": str(profile_id),
        "X-Delivery-Id": delivery_id,
    }
    delays = [0] + list(Config.WEBHOOK_RETRY_DELAYS_S)   # immediate attempt, then the retry schedule
    for attempt, delay in enumerate(delays):
        if delay:
            time.sleep(delay)
        t = time.time()
        try:
            status = _post(url, body, headers, Config.WEBHOOK_TIMEOUT_S)
            dur = int((time.time() - t) * 1000)
            if 200 <= int(status) < 300:
                log.info("webhook delivered", extra={"profile_id": profile_id, "event": event,
                         "delivery_id": delivery_id, "attempt": attempt, "status_code": int(status), "duration_ms": dur})
                return True
            log.warning("webhook non-2xx response", extra={"profile_id": profile_id, "event": event,
                        "delivery_id": delivery_id, "attempt": attempt, "status_code": int(status), "duration_ms": dur})
        except Exception as e:
            dur = int((time.time() - t) * 1000)
            log.warning(f"webhook delivery error: {e}", extra={"profile_id": profile_id, "event": event,
                        "delivery_id": delivery_id, "attempt": attempt, "duration_ms": dur})
    log.error("webhook delivery failed after all retries",
              extra={"profile_id": profile_id, "event": event, "delivery_id": delivery_id})
    return False
