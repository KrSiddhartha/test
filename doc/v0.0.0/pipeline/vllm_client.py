#!/usr/bin/env python3
"""
vllm_client.py - thin OpenAI-compatible client for the fixed production model
(gemma-4-26B served by vLLM on :8000). Stdlib only.

The sampling defaults and the json_object/enable_thinking handling mirror what is
known to work against this exact model; do not "improve" them blindly.
"""
import base64, json, mimetypes, os, re, time, urllib.request, urllib.error

DEFAULTS = {
    "base_url": os.environ.get("VLLM_BASE_URL", "http://localhost:8000/v1"),
    "model":    os.environ.get("VLLM_MODEL", "RedHatAI/gemma-4-26B-A4B-it-FP8-Dynamic"),
    "api_key":  os.environ.get("VLLM_API_KEY", "none"),
}


def data_uri(path):
    with open(path, "rb") as fh:
        raw = fh.read()
    mime, _ = mimetypes.guess_type(path)
    if not mime or not mime.startswith("image/"):
        mime = "image/png"
    return f"data:{mime};base64,{base64.b64encode(raw).decode('ascii')}"


def parse_json(text):
    """Best-effort JSON out of a model reply (handles ```fences``` and trailing prose)."""
    if not text:
        return None
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE).strip()
    try:
        return json.loads(t)
    except Exception:
        pass
    s, e = t.find("{"), t.rfind("}")
    if s != -1 and e > s:
        try:
            return json.loads(t[s:e + 1])
        except Exception:
            return None
    return None


class VLLM:
    def __init__(self, base_url=None, model=None, api_key=None, timeout=int(os.environ.get("VLLM_TIMEOUT", "1800")),
                 temperature=float(os.environ.get("VLLM_TEMPERATURE", "1.0")),
                 top_p=float(os.environ.get("VLLM_TOP_P", "0.95")),
                 top_k=int(os.environ.get("VLLM_TOP_K", "64")), json_mode=True):
        self.base_url = (base_url or DEFAULTS["base_url"]).rstrip("/")
        self.model = model or DEFAULTS["model"]
        self.api_key = api_key or DEFAULTS["api_key"]
        self.timeout = timeout
        self.temperature, self.top_p, self.top_k = temperature, top_p, top_k
        self.json_mode = json_mode   # use response_format=json_object; auto-disabled on 4xx

    def _post(self, payload):
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            self.base_url + "/chat/completions", data=body, method="POST",
            headers={"Authorization": "Bearer " + self.api_key, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            msg = ((data.get("choices") or [{}])[0]).get("message", {}) or {}
            return True, (msg.get("content") or msg.get("reasoning_content") or ""), data.get("usage")
        except urllib.error.HTTPError as e:
            return False, f"HTTP {e.code}: {e.read().decode('utf-8', 'replace')[:300]}", None
        except Exception as e:
            return False, f"{type(e).__name__}: {e}", None

    def chat(self, content, max_tokens, retries=2, system=None, json=None):
        """content: str or OpenAI multimodal list. Returns (ok, text, usage).
        json=False -> RAW text output (no response_format), e.g. Markdown OCR; json=None -> client default."""
        use_json = self.json_mode if json is None else bool(json)
        messages = ([{"role": "system", "content": system}] if system else []) + [{"role": "user", "content": content}]
        payload = {"model": self.model, "messages": messages,
                   "max_tokens": max_tokens, "temperature": self.temperature,
                   "top_p": self.top_p, "top_k": self.top_k,
                   "chat_template_kwargs": {"enable_thinking": False}}
        if use_json:
            payload["response_format"] = {"type": "json_object"}
        ok = False; out = ""; usage = None
        for attempt in range(retries):
            ok, out, usage = self._post(payload)
            if ok:
                return ok, out, usage
            if use_json and "response_format" in payload and "HTTP 4" in out:
                payload.pop("response_format", None)   # this build rejects json_object: drop + retry raw
                continue
            time.sleep(1.5 * (attempt + 1))
        return ok, out, usage

    def chat_json(self, content, max_tokens, retries=2, system=None):
        """chat() + robust JSON parse. Returns (obj_or_None, raw_text, usage)."""
        ok, out, usage = self.chat(content, max_tokens, retries=retries, system=system)
        if not ok:
            return None, out, usage
        return parse_json(out), out, usage
