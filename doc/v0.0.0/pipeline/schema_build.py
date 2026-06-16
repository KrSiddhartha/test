#!/usr/bin/env python3
"""
schema_build.py - transform the pure concept dictionary into the API contract artifacts:

  * schemaInfo  : a SchemaNode tree (API_DOCUMENTATION.md).
        FIELD concept  -> FIELD leaf  (data_type STRING/INTEGER/BOOLEAN; carries fieldProfileRef).
        TABLE concept  -> OBJECT node with data_type ARRAY whose children are the row's FIELD leaves
                          (the agreed representation of a repeating group).
  * fieldProfiles: one sourceMapping per FIELD leaf. For v0.0.0 the target defaults to the field's own
        generated path (self-target); if a suggestedSchema is supplied we attach a naive name match.
        Real source->target mapping + scoring is the mapper-agent's job (stubbed for now).

The contract's leaf data_type enum is {STRING, INTEGER, BOOLEAN, ARRAY}; it has no FLOAT/DATE, so those
map to STRING - which also preserves the value verbatim (no rounding), matching the "copy exactly" rule.
"""
import re

# concept node_type -> contract leaf data_type
_TYPE_MAP = {
    "string": "STRING", "str": "STRING", "text": "STRING",
    "integer": "INTEGER", "int": "INTEGER",
    "boolean": "BOOLEAN", "bool": "BOOLEAN",
    "float": "STRING", "double": "STRING", "decimal": "STRING", "number": "STRING",
    "date": "STRING", "datetime": "STRING", "time": "STRING",
}


def _data_type(node_type):
    return _TYPE_MAP.get(str(node_type).strip().lower(), "STRING")


def _label(name):
    s = re.sub(r"[_\-]+", " ", str(name)).strip()
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)        # split camelCase
    s = re.sub(r"\s+", " ", s).strip()
    return (s[:1].upper() + s[1:]) if s else str(name)


def _field_node(concept, parent_path):
    name = str(concept.get("name", "")).strip() or "field"
    path = f"{parent_path}.{name}" if parent_path else name
    return {
        "field_name": name,
        "fieldProfileRef": f"FIELD#{path}",
        "label": _label(name),
        "description": str(concept.get("description", "")),
        "node_type": "FIELD",
        "data_type": _data_type(concept.get("node_type", "string")),
        "children": [],
    }


def _table_node(concept, parent_path):
    name = str(concept.get("name", "")).strip() or "table"
    path = f"{parent_path}.{name}" if parent_path else name
    children = [_field_node(ch, path) for ch in concept.get("children", []) if isinstance(ch, dict)]
    return {
        "field_name": name,
        "label": _label(name),
        "description": str(concept.get("description", "")),
        "node_type": "OBJECT",
        "data_type": "ARRAY",                # repeating group: array of row objects (per agreed convention)
        "children": children,
    }


def build_schema_info(concept_dict):
    """concept_dict: {fields:[...], tables:[...]} -> list[SchemaNode] (dicts)."""
    nodes = []
    for c in concept_dict.get("fields", []):
        nodes.append(_field_node(c, ""))
    for c in concept_dict.get("tables", []):
        nodes.append(_table_node(c, ""))
    return nodes


# ---- field profiles (source -> target mappings) ----
def _norm(s):
    return re.sub(r"[^a-z0-9]+", "", str(s).lower())


def _leaf_paths(nodes, parent=""):
    out = []
    for n in nodes:
        path = f"{parent}.{n['field_name']}" if parent else n["field_name"]
        if n.get("node_type") == "FIELD":
            out.append(path)
        out.extend(_leaf_paths(n.get("children", []), path))
    return out


def build_field_profiles(schema_info, suggested=None):
    """One sourceMapping per FIELD leaf. self-target by default; if `suggested` (a parsed suggested
    schema) is given, attach a naive normalized-name match to one of its leaf paths."""
    suggested_paths = _suggested_leaf_paths(suggested) if suggested else []
    sugg_index = {}
    for sp in suggested_paths:
        sugg_index.setdefault(_norm(sp.split(".")[-1]), sp)

    profiles = []
    for path in _leaf_paths(schema_info):
        leaf = path.split(".")[-1]
        match = sugg_index.get(_norm(leaf))
        if match:
            profiles.append({"sourceMapping": {"sourcePath": path, "targetPath": match, "confidenceScore": 0.7}})
        else:
            profiles.append({"sourceMapping": {"sourcePath": path, "targetPath": path, "confidenceScore": 1.0}})
    return profiles


def _suggested_leaf_paths(obj, parent=""):
    """Leaf dot-paths of an arbitrary JSON suggested-schema object (best effort)."""
    out = []
    if isinstance(obj, dict):
        # contract-style SchemaNode list?  {schemaInfo:[...]} or a node with children
        if "children" in obj and isinstance(obj.get("children"), list):
            name = obj.get("field_name") or obj.get("name") or ""
            path = f"{parent}.{name}" if parent and name else (name or parent)
            if not obj["children"]:
                return [path] if path else []
            for ch in obj["children"]:
                out.extend(_suggested_leaf_paths(ch, path))
            return out
        for k, v in obj.items():
            path = f"{parent}.{k}" if parent else k
            if isinstance(v, (dict, list)):
                out.extend(_suggested_leaf_paths(v, path))
            else:
                out.append(path)
    elif isinstance(obj, list):
        for item in obj:
            out.extend(_suggested_leaf_paths(item, parent))
    return out
