#!/usr/bin/env python3
"""
concepts.py - induce a UNIFIED, HIERARCHICAL concept dictionary from in-memory extractions.

The OUTPUT (the dictionary) is PURE: every concept is only name + description + node_type +
node_structure (+ column children for tables). It contains NO sample values, NO sample names, NO
examples - so the dictionary and the runtime prompts that consume it stay domain-pure. (Induction
must read the sample labels/headers to cluster them - transient input, never stored or shipped.)

  * FIELD concepts - scalar labelled values (node_structure "scalar").
  * TABLE concepts - a ROOT per distinct kind of table/schedule (node_structure "array") with COLUMN
    CHILDREN (scalar leaves). Rows are DATA, never concepts; repeated dimension columns collapse into
    a {dimension, value} pair.

`induce_concepts(client, docs, ...)` is the service entry point - one global LLM pass per family,
over the extraction docs already in memory. Optional `hints` (from guideDoc/suggestedSchema) are
appended as authoritative *reference* naming guidance - never as sample data.
"""
import re
from collections import Counter, defaultdict

FIELD_PROMPT = """You are given a CATALOG of data labels observed across many documents of one kind,
each with its value type. Different documents name the same thing differently - abbreviations,
extra/missing words, leading numbers, pluralization, transcription noise. Collapse the catalog into
CONCEPTS (one distinct kind of scalar datum each).

Group every label denoting the SAME datum into one concept, however differently worded. Keep
genuinely different data separate - never merge two different dates, two identifiers, or a part and
its whole. Ignore entries that are clearly a specific value mis-captured as a label.

Names and descriptions must be GENERIC: never quote a specific value, name, date, amount, or any
example from the data. Describe the concept itself.

For each concept return: "name" (snake_case), "description" (defines what it is AND what sets it apart
from neighbours, standing on its own), "value_type" (string|integer|float|date|boolean).

Return only JSON: {{"concepts": [{{"name", "description", "value_type"}}]}}

CATALOG:
{catalog}"""

TABLE_PROMPT = """You are given the distinct TABLE structures found across many documents of one kind
(each: a caption if any, and its columns with role+type). Identify each DISTINCT KIND of table/
schedule and define it as a concept. Group structurally/semantically equivalent tables into ONE
concept, however differently they are worded across documents.

Names and descriptions must be GENERIC: never quote a specific value, name, date, amount, or example
from the data. Describe the kind of table and its columns.

For each distinct table concept return:
  - "name": a short, generic snake_case identifier for this KIND of table,
  - "description": what this table holds and what distinguishes it from other tables, standing on its own,
  - "children": its columns, each {{"name": snake_case, "description": "...", "node_type": "string|integer|float|date|boolean"}}.
    Columns that are a repeated DIMENSION (periods, categories - their headers are themselves data)
    collapse into exactly TWO children: one for the dimension label, one for the value. Never emit a
    child per period. The row-identifying column is a child too.

Return only JSON: {{"tables": [{{"name", "description", "children":[{{"name","description","node_type"}}]}}]}}

TABLE STRUCTURES:
{catalog}"""

_GUIDE_SUFFIX = """

REFERENCE FIELD DEFINITIONS (authoritative; from guide/reference documents, NOT the samples). Prefer
these names and meanings when one matches a concept you are forming. Do not invent concepts that have
no support in the catalog above, and never copy sample data:
{guide}"""


def _norm(s):
    return re.sub(r"[\s:.\-]+$", "", re.sub(r"\s+", " ", str(s).strip().lower()))


def field_catalog(docs):
    info = defaultdict(lambda: {"types": Counter(), "count": 0})
    for d in docs:
        for pg in d.get("pages", []):
            for n in pg.get("nodes", []):
                if n.get("type") == "field":
                    lab = _norm(n.get("label", ""))
                    if lab:
                        info[lab]["types"][n.get("value_type", "string")] += 1
                        info[lab]["count"] += 1
    lines = []
    for lab, e in sorted(info.items(), key=lambda kv: -kv[1]["count"]):
        vt = e["types"].most_common(1)[0][0] if e["types"] else "string"
        lines.append(f"- {lab}  [{vt}]")
    return "\n".join(lines), len(info)


def table_catalog(docs, max_groups=60):
    """One representative per distinct (column role/type signature + caption). Columns + caption only -
    NO sample row values (those are data)."""
    groups = {}
    for d in docs:
        for pg in d.get("pages", []):
            for n in pg.get("nodes", []):
                if n.get("type") == "table" and n.get("columns"):
                    sig = (frozenset((c["role"], c["type"]) for c in n["columns"]), _norm(n.get("caption", ""))[:30])
                    if sig not in groups:
                        groups[sig] = n
    blocks = []
    for i, n in enumerate(list(groups.values())[:max_groups]):
        cols = [f'{c["header"][:24]}({c["role"][:3]},{c["type"][:3]})' for c in n["columns"]]
        blocks.append(f"TABLE {i}: caption={n.get('caption','')[:40]!r}  columns: {cols}")
    return "\n".join(blocks), len(groups)


def _induce(client, prompt, key, max_tokens):
    obj, _r, _u = client.chat_json(prompt, max_tokens)
    if not (isinstance(obj, dict) and isinstance(obj.get(key), list)):
        obj, _r, _u = client.chat_json(prompt, min(int(max_tokens * 1.5), 32000))
    return (obj.get(key) if isinstance(obj, dict) else None) or []


def induce_concepts(client, docs, structural=None, hints=None, max_tokens=16000):
    """In-memory induction. docs: [{file, pages:[{page, markdown, nodes}]}].
    Returns {field_count, table_count, fields:[...], tables:[...]} - the pure concept dictionary.
    `structural` (from induce()) is accepted for future use; `hints` adds reference guidance only."""
    hints = hints or {}
    guide = (hints.get("guide_block") or "").strip()
    suffix = _GUIDE_SUFFIX.format(guide=guide[:4000]) if guide else ""

    fcat, _nf = field_catalog(docs)
    fields = []
    for i, c in enumerate(_induce(client, FIELD_PROMPT.format(catalog=fcat) + suffix, "concepts", max_tokens)):
        if isinstance(c, dict) and c.get("name") and c.get("description"):
            fields.append({"id": f"f{i+1:03d}", "kind": "field", "name": str(c["name"]),
                           "description": str(c["description"]), "node_type": str(c.get("value_type", "string")),
                           "node_structure": "scalar"})

    tcat, _ng = table_catalog(docs)
    tables = []
    for i, c in enumerate(_induce(client, TABLE_PROMPT.format(catalog=tcat) + suffix, "tables", max_tokens)):
        if isinstance(c, dict) and c.get("name") and isinstance(c.get("children"), list) and c["children"]:
            children = [{"name": str(ch.get("name", f"col_{k+1}")), "description": str(ch.get("description", "")),
                         "node_type": str(ch.get("node_type", "string")), "node_structure": "scalar"}
                        for k, ch in enumerate(c["children"]) if isinstance(ch, dict)]
            tables.append({"id": f"t{i+1:03d}", "kind": "table", "name": str(c["name"]),
                           "description": str(c.get("description", "")), "node_type": "object",
                           "node_structure": "array", "children": children})

    return {"field_count": len(fields), "table_count": len(tables), "fields": fields, "tables": tables}
