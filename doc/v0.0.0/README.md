# Schema Intelligence API — v0.0.0

Self-contained service implementing the contract in `/home/sid/work/data_ingestion/API_DOCUMENTATION.md`.
Does **not** import or modify the experimental code in `schema_infer/` — the pipeline is ported here.

## Endpoints (async + webhook)
- `POST /api/v1/schema-generation` — **real**: sample PDFs → induced schema tree (`schemaInfo`) + `fieldProfiles`.
- `POST /api/v1/mapper-agent` — **stub** (contract-shaped mock for now; real mapping later).

Both return `202 Accepted` immediately, process in the background, then `POST` the full payload to the
registered webhook callback (`X-Webhook-Event: schema.generated | mapping.completed`) with retry
(30s / 2m / 10m, 10s timeout).

## Layout
```
v0.0.0/
  main.py             FastAPI app + the two endpoints (async dispatch)
  models.py           Pydantic contract models (SchemaNode, FieldProfile, requests/responses)
  config.py           env-driven settings (vLLM, webhook URL, work dir, parallelism, log level)
  logging_setup.py    structured logging (per-request profile_id / delivery_id / stage context)
  webhook.py          webhook delivery + retry policy + contract headers
  schema_generation.py  orchestrates the real schema-generation pipeline -> Profile
  mapper_agent.py     stub mapper (echoes schemaInfo + mock fieldProfiles)
  pipeline/           ported schema-creation pipeline (render, extract, induce, concepts) + schema_build
```

## Run
```
pip install fastapi uvicorn pydantic           # + poppler (pdftoppm) for rendering
export VLLM_BASE_URL=http://localhost:8000/v1
export VLLM_MODEL=RedHatAI/gemma-4-26B-A4B-it-FP8-Dynamic
export WEBHOOK_CALLBACK_URL=http://caller/webhook     # registered callback
uvicorn main:app --host 0.0.0.0 --port 8080
```

## Status
v0.0.0 scaffolding: contract + async/webhook + logging in place; schema-generation pipeline being
ported from the validated `schema_infer/` design. mapper-agent is a stub per the contract.
