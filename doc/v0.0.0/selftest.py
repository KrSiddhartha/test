#!/usr/bin/env python3
"""Offline self-test (no network, no vLLM): import wiring + schema_build transform + contract
invariants + optional pydantic validation. Run from v0.0.0/:  python selftest.py"""
import json


def main():
    # 1) pipeline modules import (stdlib only)
    from pipeline import render, vllm_client, extract, induce, concepts, schema_build, guides  # noqa: F401
    print("OK   pipeline modules import")

    # 2) schema_build transform on a synthetic PURE concept dict (no sample data anywhere)
    concept_dict = {
        "fields": [
            {"id": "f001", "kind": "field", "name": "document_identifier",
             "description": "unique identifier of the document", "node_type": "string", "node_structure": "scalar"},
            {"id": "f002", "kind": "field", "name": "effective_date",
             "description": "date the document takes effect", "node_type": "date", "node_structure": "scalar"},
            {"id": "f003", "kind": "field", "name": "total_count",
             "description": "count of items", "node_type": "integer", "node_structure": "scalar"},
        ],
        "tables": [
            {"id": "t001", "kind": "table", "name": "line_items",
             "description": "repeating itemized entries", "node_type": "object", "node_structure": "array",
             "children": [
                 {"name": "item_identifier", "description": "identifier of the row", "node_type": "string", "node_structure": "scalar"},
                 {"name": "period_label", "description": "dimension label for a repeated period", "node_type": "string", "node_structure": "scalar"},
                 {"name": "period_value", "description": "measure value under the period", "node_type": "float", "node_structure": "scalar"},
             ]},
        ],
    }
    schema_info = schema_build.build_schema_info(concept_dict)
    profiles = schema_build.build_field_profiles(schema_info)
    print(f"OK   schema_build -> {len(schema_info)} top nodes, {len(profiles)} field profiles")

    # 3) contract invariants
    LEAF_TYPES = {"STRING", "INTEGER", "BOOLEAN", "ARRAY"}

    def check(node):
        assert node["node_type"] in ("OBJECT", "FIELD"), node
        if node["node_type"] == "FIELD":
            assert node["data_type"] in LEAF_TYPES, node
            assert node["children"] == [], node
            assert str(node.get("fieldProfileRef", "")).startswith("FIELD#"), node
        else:
            assert node["data_type"] in ("OBJECT", "ARRAY"), node
            assert not node.get("fieldProfileRef"), node
        for ch in node["children"]:
            check(ch)

    for n in schema_info:
        check(n)
    tbl = next(n for n in schema_info if n["field_name"] == "line_items")
    assert tbl["node_type"] == "OBJECT" and tbl["data_type"] == "ARRAY" and len(tbl["children"]) == 3
    assert len(profiles) == 6  # 3 top fields + 3 table-row leaves
    print("OK   contract invariants (OBJECT/FIELD, leaf data_type enum, table=OBJECT+ARRAY+children)")

    # float/date -> STRING (no precision loss; contract has no FLOAT/DATE leaf type)
    pv = next(c for c in tbl["children"] if c["field_name"] == "period_value")
    assert pv["data_type"] == "STRING", pv
    print("OK   float/date map to STRING (verbatim, lossless)")

    # 4) optional pydantic validation against the contract models
    try:
        from models import Profile
        prof = Profile(**{"metadata": {"profile_id": 1111}, "schemaInfo": schema_info, "fieldProfiles": profiles})
        n = len(prof.schemaInfo) if hasattr(prof, "schemaInfo") else "?"
        print(f"OK   pydantic Profile validates ({n} nodes)")
    except ImportError:
        print("SKIP pydantic not installed (models/main need it to run the service)")

    print("\n--- sample schemaInfo (line_items table node) ---")
    print(json.dumps(tbl, indent=2))
    print("\n--- sample fieldProfiles[0] ---")
    print(json.dumps(profiles[0], indent=2))
    print("\nALL OFFLINE CHECKS PASSED")


if __name__ == "__main__":
    main()
