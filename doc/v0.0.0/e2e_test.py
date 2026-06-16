#!/usr/bin/env python3
"""End-to-end test through the real service against the live vLLM model.

Spins up an in-process webhook sink, POSTs a /api/v1/schema-generation request (via FastAPI TestClient)
with a few SAMPLE pdfs, waits for the schema.generated webhook callback, and validates the payload
against the contract. Usage:  python e2e_test.py [sample1.pdf sample2.pdf ...]
"""
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

# webhook deliveries land here (in-process sink, started in main())
CAPTURED = []


class Sink(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(n)
        try:
            payload = json.loads(body)
        except Exception:
            payload = {"_raw": body.decode("utf-8", "replace")}
        CAPTURED.append({"event": self.headers.get("X-Webhook-Event"),
                         "profile_id": self.headers.get("X-Webhook-Profile-Id"),
                         "delivery_id": self.headers.get("X-Delivery-Id"),
                         "payload": payload})
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, *a):
        pass


def check_node(node, leaf_types={"STRING", "INTEGER", "BOOLEAN", "ARRAY"}):
    assert node["node_type"] in ("OBJECT", "FIELD"), node["field_name"]
    if node["node_type"] == "FIELD":
        assert node["data_type"] in leaf_types, node
        assert node["children"] == []
        assert str(node.get("fieldProfileRef", "")).startswith("FIELD#")
    else:
        assert node["data_type"] in ("OBJECT", "ARRAY")
    for ch in node["children"]:
        check_node(ch)


def main():
    samples = sys.argv[1:] or [
        "data/fs_small/WILLMANN INC.-FS2025.pdf",
        "data/fs_small/WEST BAY LEARNING CENTER INC.-FS2025.pdf",
        "data/fs_small/KAIROS CHRISTIAN FELLOWSHIP INC.-FS2024.pdf",
        "data/fs_small/KINGDOM THEOLOGICAL SCHOOL & COLLEGES INC.-FS2024.pdf",
    ]
    samples = [os.path.abspath(os.path.join("/home/sid/work/data_ingestion", s)) if not os.path.isabs(s) else s
               for s in samples]
    for s in samples:
        assert os.path.exists(s), f"missing sample: {s}"

    srv = HTTPServer(("127.0.0.1", 0), Sink)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    os.environ["WEBHOOK_CALLBACK_URL"] = f"http://127.0.0.1:{port}/webhook"
    os.environ.setdefault("SI_MAX_PAGES", "50")
    os.environ.setdefault("SI_PAGE_PARALLEL", "16")

    from fastapi.testclient import TestClient
    import main

    client = TestClient(main.app)
    req = {"metadata": {"profile_id": 1111}, "sampleFiles": samples, "guideDoc": []}
    print(f"POST /api/v1/schema-generation  ({len(samples)} sample files)")
    t0 = time.time()
    r = client.post("/api/v1/schema-generation", json=req)
    print("  -> HTTP", r.status_code, r.json())
    assert r.status_code == 202, r.text

    print("waiting for schema.generated webhook ...")
    while not CAPTURED and time.time() - t0 < 900:
        time.sleep(2)
    assert CAPTURED, "no webhook received within timeout"

    cb = CAPTURED[0]
    dt = time.time() - t0
    print(f"\n=== WEBHOOK RECEIVED in {dt:.0f}s ===")
    print("event:", cb["event"], "| profile:", cb["profile_id"], "| delivery:", cb["delivery_id"])
    body = cb["payload"]
    print("envelope:", {k: body.get(k) for k in ("code", "message", "status")})
    assert cb["event"] == "schema.generated"
    assert body.get("status") == "success", body
    profile = body["profile"]
    schema_info = profile["schemaInfo"]
    field_profiles = profile["fieldProfiles"]
    n_fields = sum(1 for n in schema_info if n["node_type"] == "FIELD")
    n_tables = sum(1 for n in schema_info if n["node_type"] == "OBJECT")
    for n in schema_info:
        check_node(n)
    print(f"schemaInfo: {len(schema_info)} top nodes ({n_fields} FIELD, {n_tables} OBJECT/table) | "
          f"fieldProfiles: {len(field_profiles)}")
    print("contract invariants: OK")
    print("\n--- first 2 top nodes ---")
    print(json.dumps(schema_info[:2], indent=2, ensure_ascii=False)[:1500])
    print("\nE2E PASSED")


if __name__ == "__main__":
    main()
