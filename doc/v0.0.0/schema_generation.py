"""Schema-generation orchestration.

  sample PDFs ── render ──▶ page images
              ── extract ─▶ generic nodes per page (vision; fixed node vocabulary, zero coined keys)
              ── induce ──▶ deterministic structural schema
              ── concepts ▶ PURE concept dictionary (FIELD scalars + TABLE arrays with column children)
              ── build ───▶ contract Profile: schemaInfo (SchemaNode tree) + fieldProfiles

The heavy pipeline lives in pipeline/ and is imported lazily so the API process boots even before the
pipeline modules are present. guideDoc / suggestedSchema are read best-effort as hints (never as data).
"""
import json
import os
import time

from config import Config
from logging_setup import get_logger

log = get_logger("schema_gen")


def _stage(pid, name, t0, **extra):
    log.info(f"stage complete: {name}",
             extra={"profile_id": pid, "stage": name, "duration_ms": int((time.time() - t0) * 1000), **extra})


def generate(req):
    """req: SchemaGenerationRequest (or any object exposing the same fields). Returns a Profile dict."""
    pid = req.metadata.profile_id
    sample_files = list(req.sampleFiles)
    work_dir = os.path.join(Config.WORK_DIR, f"profile_{pid}")
    os.makedirs(work_dir, exist_ok=True)
    log.info("schema generation started",
             extra={"profile_id": pid, "stage": "start", "count": len(sample_files)})

    # lazy imports: keep the API bootable before the pipeline is ported
    from pipeline.vllm_client import VLLM
    from pipeline import render as render_mod
    from pipeline import extract as extract_mod
    from pipeline import induce as induce_mod
    from pipeline import concepts as concepts_mod
    from pipeline import schema_build
    from pipeline import guides

    client = VLLM()

    # 1) RENDER every sample file -> page images (grouped per file)
    t = time.time()
    docs_pages = []
    total_pages = 0
    for f in sample_files:
        out = os.path.join(work_dir, "_pages", os.path.splitext(os.path.basename(f))[0])
        pages = render_mod.render(f, out, Config.DPI, Config.MAX_PAGES)
        docs_pages.append((os.path.basename(f), pages))
        total_pages += len(pages)
        log.info("rendered file", extra={"profile_id": pid, "stage": "render",
                 "file": os.path.basename(f), "count": len(pages)})
    _stage(pid, "render", t, count=total_pages)

    # 2) EXTRACT generic nodes for every page of every file in ONE parallel pool (max parallelism)
    t = time.time()
    docs = extract_mod.extract_documents(client, docs_pages,
                                         parallel=Config.PAGE_PARALLEL, max_tokens=Config.MAX_TOKENS)
    _stage(pid, "extract", t, count=sum(len(d["pages"]) for d in docs))

    # 3) INDUCE deterministic structural schema
    t = time.time()
    structural = induce_mod.induce(docs)
    _stage(pid, "induce", t)

    # 4) guide docs / suggested schema -> best-effort hints (concept naming/definition only)
    hints = guides.load_hints(getattr(req, "guideDoc", None) or [], getattr(req, "suggestedSchema", None))

    # 5) CONCEPTS: pure concept dictionary (field scalars + table arrays w/ children)
    t = time.time()
    concept_dict = concepts_mod.induce_concepts(client, docs, structural, hints=hints)
    _stage(pid, "concepts", t,
           count=len(concept_dict.get("fields", [])) + len(concept_dict.get("tables", [])))

    # 6) BUILD the contract Profile
    schema_info = schema_build.build_schema_info(concept_dict)
    field_profiles = schema_build.build_field_profiles(schema_info, suggested=hints.get("suggested_schema"))

    # persist artifacts for inspection
    json.dump(concept_dict, open(os.path.join(work_dir, "concepts.json"), "w"), indent=2, ensure_ascii=False)
    json.dump(schema_info, open(os.path.join(work_dir, "schemaInfo.json"), "w"), indent=2, ensure_ascii=False)

    log.info("schema generation complete",
             extra={"profile_id": pid, "stage": "done", "count": len(schema_info)})
    return {"metadata": {"profile_id": pid}, "schemaInfo": schema_info, "fieldProfiles": field_profiles}
