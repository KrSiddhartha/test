"""Environment-driven configuration for the Schema Intelligence service (v0.0.0)."""
import os


class Config:
    # model / vLLM (OpenAI-compatible)
    VLLM_BASE_URL = os.environ.get("VLLM_BASE_URL", "http://localhost:8000/v1")
    VLLM_MODEL = os.environ.get("VLLM_MODEL", "RedHatAI/gemma-4-26B-A4B-it-FP8-Dynamic")
    VLLM_API_KEY = os.environ.get("VLLM_API_KEY", "none")
    VLLM_TIMEOUT = int(os.environ.get("VLLM_TIMEOUT", "1800"))

    # webhook delivery (contract: registered callback URL; retry 30s / 2m / 10m; 10s timeout)
    WEBHOOK_CALLBACK_URL = os.environ.get("WEBHOOK_CALLBACK_URL", "")
    WEBHOOK_TIMEOUT_S = int(os.environ.get("WEBHOOK_TIMEOUT_S", "10"))
    WEBHOOK_RETRY_DELAYS_S = [30, 120, 600]

    # pipeline
    WORK_DIR = os.environ.get("SI_WORK_DIR", "/tmp/schema_intelligence")
    DPI = int(os.environ.get("SI_DPI", "150"))
    MAX_PAGES = int(os.environ.get("SI_MAX_PAGES", "200"))
    PAGE_PARALLEL = int(os.environ.get("SI_PAGE_PARALLEL", "64"))
    MAX_TOKENS = int(os.environ.get("SI_MAX_TOKENS", "30000"))

    # service
    HOST = os.environ.get("SI_HOST", "0.0.0.0")
    PORT = int(os.environ.get("SI_PORT", "8080"))
    LOG_LEVEL = os.environ.get("SI_LOG_LEVEL", "INFO")
    JOB_WORKERS = int(os.environ.get("SI_JOB_WORKERS", "4"))   # concurrent background jobs
