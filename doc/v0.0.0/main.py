"""Schema Intelligence API (v0.0.0) — FastAPI app implementing API_DOCUMENTATION.md.

Both endpoints validate the request, return 202 Accepted immediately, and process on a background
thread pool. On completion the full response payload is delivered to the registered webhook callback
(X-Webhook-Event: schema.generated | mapping.completed)."""
import os
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from config import Config
from logging_setup import configure_logging, get_logger
from models import SchemaGenerationRequest, MapperAgentRequest
import webhook
import schema_generation
import mapper_agent

configure_logging()
log = get_logger("api")

app = FastAPI(title="Schema Intelligence API", version="v0.0.0")
EXECUTOR = ThreadPoolExecutor(max_workers=Config.JOB_WORKERS, thread_name_prefix="job")


def _accepted(profile_id, msg):
    return JSONResponse(status_code=202, content={
        "code": 202, "message": msg, "status": "accepted",
        "profile": {"metadata": {"profile_id": profile_id}}})


def _error(code, message):
    return JSONResponse(status_code=code, content={"code": code, "message": message, "status": "error"})


# ---------------- /api/v1/schema-generation ----------------
def _run_schema_generation(req: SchemaGenerationRequest):
    pid = req.metadata.profile_id
    try:
        profile = schema_generation.generate(req)
        body = {"code": 200, "message": "Profile created successfully", "status": "success", "profile": profile}
    except Exception as e:
        log.exception("schema-generation job failed", extra={"profile_id": pid})
        body = {"code": 500, "message": f"Internal server error: {e}", "status": "error"}
    webhook.deliver(webhook.EVENT_SCHEMA_GENERATED, pid, body)


@app.post("/api/v1/schema-generation")
async def schema_generation_endpoint(req: SchemaGenerationRequest):
    pid = req.metadata.profile_id
    log.info("schema-generation request accepted",
             extra={"profile_id": pid, "stage": "accept", "count": len(req.sampleFiles)})
    if not req.sampleFiles:
        return _error(400, "Invalid request: sampleFiles must not be empty")
    missing = [f for f in req.sampleFiles if not os.path.exists(f)]
    if missing:
        return _error(422, f"Unprocessable entity: file path invalid or unreadable: {missing[0]}")
    EXECUTOR.submit(_run_schema_generation, req)
    return _accepted(pid, "Accepted; schema generation in progress")


# ---------------- /api/v1/mapper-agent (stub) ----------------
def _run_mapper(pid, schema_info):
    try:
        profile = mapper_agent.run(pid, schema_info)
        body = {"code": 200, "message": "Profile created successfully", "status": "success", "profile": profile}
    except Exception as e:
        log.exception("mapper-agent job failed", extra={"profile_id": pid})
        body = {"code": 500, "message": f"Internal server error: {e}", "status": "error"}
    webhook.deliver(webhook.EVENT_MAPPING_COMPLETED, pid, body)


@app.post("/api/v1/mapper-agent")
async def mapper_agent_endpoint(req: MapperAgentRequest):
    pid = req.metadata.profile_id
    log.info("mapper-agent request accepted", extra={"profile_id": pid, "stage": "accept"})
    if not req.schemaInfo:
        return _error(400, "Invalid request: schemaInfo must not be empty")
    schema_info = [n.model_dump() if hasattr(n, "model_dump") else n.dict() for n in req.schemaInfo]
    EXECUTOR.submit(_run_mapper, pid, schema_info)
    return _accepted(pid, "Accepted; mapping in progress")


@app.get("/health")
async def health():
    return {"status": "ok", "version": "v0.0.0"}
