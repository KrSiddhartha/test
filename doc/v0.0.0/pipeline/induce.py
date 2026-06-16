#!/usr/bin/env python3
"""
induce.py - Stage 2: many generic trees -> ONE schema. Deterministic. No LLM, no network.

The schema has two clearly separated parts, and this separation is the whole point:

  * STRUCTURE (bounded, generic): the fixed node grammar (5 types) plus the set of distinct
    *table shapes* (column role/type signatures, with series columns collapsed). This is the
    actual schema - it saturates: once the shapes have been seen, new documents add nothing.

  * DATA VOCABULARY (not schema keys): the observed field labels and series headers, normalized
    and typed. These are VALUES of the generic slots (field.label, the series dimension), recorded
    so you can see what data occurred - never promoted to first-class schema keys. This is exactly
    what the old pipeline got wrong: it turned every observed label into its own schema key,
    so the schema grew without bound.

`series` columns (a column whose header is itself a value) collapse into a synthesized
{series_label, value} pair, generically - the domain-neutral generalization of the old tool's
hardcoded special-casing of repeated columns.
"""
import re
from collections import Counter

NODE_GRAMMAR = {
    "heading":   {"level": "integer", "text": "string"},
    "paragraph": {"text": "string"},
    "field":     {"label": "string (data)", "value": "<value_type>", "value_type": "string|integer|float|date|boolean"},
    "list":      {"ordered": "boolean", "items": "string[]"},
    "table":     {"caption": "string", "columns": "[{header(data), role, type}]", "rows": "[[cell]]"},
}
TYPES = ("string", "integer", "float", "date", "boolean")


def norm_label(s):
    s = str(s).strip().lower()
    s = re.sub(r"[\s:.\-]+$", "", s)      # trailing colon / dot / dash / space
    s = re.sub(r"\s+", " ", s)
    return s


def agg_type(types):
    s = {t for t in types if t in TYPES}
    if not s:
        return "string"
    if len(s) == 1:
        return next(iter(s))
    if s <= {"integer", "float"}:
        return "float"
    return "string"


def table_signature(tbl):
    """A generalized shape: the SET of (role,type) among the non-series columns - column COUNT is
    data (how many series/period columns, how many repeated descriptors), not structure - plus the
    type of the series measure. Using a set (not a multiset) collapses count-variants of the same
    shape. Series headers are data (values, not keys)."""
    roles = frozenset((c["role"], c["type"]) for c in tbl["columns"] if c["role"] != "series")
    series_types = [c["type"] for c in tbl["columns"] if c["role"] == "series"]
    return roles, (agg_type(series_types) if series_types else None)


def induce(docs):
    """docs: [{file, pages:[{page, markdown, nodes}]}]. Returns the schema dict."""
    field_labels = {}     # norm label -> {value_types: Counter, count, examples:[]}
    table_shapes = {}     # signature -> {count, captions:set, series_headers:set, columns}
    node_counts = Counter()
    saturation = []
    seen_labels, seen_shapes = set(), set()

    for doc in docs:
        new_labels = new_shapes = 0
        for pg in doc["pages"]:
            for n in pg["nodes"]:
                node_counts[n["type"]] += 1
                if n["type"] == "field":
                    lab = norm_label(n["label"]) or "(unlabelled)"
                    e = field_labels.setdefault(lab, {"value_types": Counter(), "count": 0, "examples": []})
                    e["value_types"][n["value_type"]] += 1
                    e["count"] += 1
                    if len(e["examples"]) < 3 and str(n["value"]).strip():
                        e["examples"].append(str(n["value"])[:60])
                    if lab not in seen_labels:
                        seen_labels.add(lab); new_labels += 1
                elif n["type"] == "table":
                    sig, series_type = table_signature(n)
                    key = (sig, series_type)
                    s = table_shapes.setdefault(key, {"count": 0, "captions": set(), "series_headers": set(),
                                                       "columns": [(c["role"], c["type"]) for c in n["columns"]]})
                    s["count"] += 1
                    if n.get("caption"):
                        s["captions"].add(n["caption"][:48])
                    for c in n["columns"]:
                        if c["role"] == "series" and c["header"]:
                            s["series_headers"].add(c["header"][:24])
                    if key not in seen_shapes:
                        seen_shapes.add(key); new_shapes += 1
        saturation.append({"file": doc["file"], "new_field_labels": new_labels,
                           "new_table_shapes": new_shapes,
                           "cum_field_labels": len(seen_labels), "cum_table_shapes": len(seen_shapes)})

    # render table shapes readably
    shapes_out = []
    for i, ((sig, series_type), s) in enumerate(sorted(table_shapes.items(), key=lambda kv: -kv[1]["count"])):
        cols = [{"role": r, "type": t} for (r, t) in s["columns"]]
        shape = {"id": f"table_shape_{i+1}", "occurrences": s["count"], "columns": cols}
        if series_type:
            shape["series"] = {"collapses_to": {"series_label": "string (data)", "value": series_type},
                               "series_header_examples": sorted(s["series_headers"])[:8]}
        if s["captions"]:
            shape["caption_examples"] = sorted(s["captions"])[:5]
        shapes_out.append(shape)

    labels_out = {lab: {"value_type": agg_type(e["value_types"]), "count": e["count"], "examples": e["examples"]}
                  for lab, e in sorted(field_labels.items())}

    return {
        "structure": {                       # THE SCHEMA - bounded & generic
            "node_types": NODE_GRAMMAR,
            "table_shapes": shapes_out,
        },
        "data_vocabulary": {                 # observed DATA, not schema keys
            "field_labels": labels_out,
        },
        "stats": {
            "documents": len(docs),
            "node_type_count": len(NODE_GRAMMAR),          # FIXED = 5
            "distinct_table_shapes": len(shapes_out),       # bounded structural growth
            "distinct_field_labels": len(labels_out),       # data vocabulary size
            "node_counts": dict(node_counts),
        },
        "saturation": saturation,
    }
