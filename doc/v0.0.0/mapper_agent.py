"""Mapper-agent STUB (per the agreed scope for v0.0.0).

The contract's /mapper-agent maps source fields to a canonical target schema and scores confidence.
Here it is mocked: it echoes the incoming schemaInfo and emits one self-mapping per FIELD leaf with a
placeholder `confidence`. The response shape matches API_DOCUMENTATION.md exactly so callers can
integrate now; the real mapping logic replaces `run()` later (same approach as the runtime).
"""
from logging_setup import get_logger

log = get_logger("mapper")

STUB_CONFIDENCE = 0.5


def _leaf_paths(nodes, parent=""):
    out = []
    for n in nodes:
        path = f"{parent}.{n['field_name']}" if parent else n["field_name"]
        if n.get("node_type") == "FIELD":
            out.append(path)
        out.extend(_leaf_paths(n.get("children", []), path))
    return out


def run(profile_id, schema_info):
    """Return a contract-shaped Profile dict: echoed schemaInfo + stub fieldProfiles (confidence)."""
    paths = _leaf_paths(schema_info)
    profiles = [{"sourceMapping": {"sourcePath": p, "targetPath": p, "confidence": STUB_CONFIDENCE}} for p in paths]
    log.info("mapper stub produced field profiles", extra={"profile_id": profile_id, "count": len(profiles)})
    return {"metadata": {"profile_id": profile_id}, "schemaInfo": schema_info, "fieldProfiles": profiles}
