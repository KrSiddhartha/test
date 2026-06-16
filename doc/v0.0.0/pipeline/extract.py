#!/usr/bin/env python3
"""
extract.py - Stage 1: one page image -> a FAITHFUL, GENERIC structure.

The single most important rule of this whole tool lives here: the model NEVER
invents a concept name. It only sorts content into a *fixed, tiny* set of node
types and copies the text exactly. Headings, labels, captions and column headers
are all DATA (string values in fixed slots), never schema keys. That is what keeps
the induced schema generic and bounded - the old pipeline exploded precisely because
it asked the model to coin a snake_case key per concept per page.

Output per page: {"markdown": <verbatim archive>, "nodes": [<typed node>, ...]}.
"markdown" is the lossless record; "nodes" is that same content sorted into types.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed

from .vllm_client import data_uri

NODE_TYPES = ("heading", "paragraph", "field", "list", "table")
VALUE_TYPES = ("string", "integer", "float", "date", "boolean")
COLUMN_ROLES = ("identifier", "series", "attribute")

EXTRACT_PROMPT = """You convert ONE page of a document into a faithful, generic structure.
You do not interpret what the document is about and you NEVER invent a name for what something
"means" - you only sort content into a fixed set of node types and copy the text exactly.

Return a single JSON object:
{
  "markdown": "<the WHOLE page transcribed verbatim as GitHub-flavored Markdown - headings, lists, and a pipe table for every block of column-aligned values (columns may not be ruled - align by position). Copy every character exactly - words, digits, punctuation, signs and symbols. This is the lossless record - omit nothing visible and invent nothing.>",
  "nodes": [ <node>, ... ]
}

"nodes" is the SAME content as "markdown", in reading order, sorted into nodes. Each node is
EXACTLY ONE of these fixed types. Use no other "type", and never add a key that names the subject:

  {"type":"heading","level":<1-6>,"text":"<heading exactly as printed>"}
  {"type":"paragraph","text":"<a run of prose exactly as printed>"}
  {"type":"field","label":"<the label exactly as printed>","value":<the value exactly as printed>,"value_type":"string|integer|float|date|boolean"}
  {"type":"list","ordered":<true|false>,"items":["<item>", ...]}
  {"type":"table","caption":"<text labelling the table, or \\"\\">",
     "columns":[{"header":"<header exactly>","role":"identifier|series|attribute","type":"string|integer|float|date|boolean"}],
     "rows":[ ["<cell>","<cell>", ...] ]   // one array per row, cells in the SAME order as columns, copied exactly
  }

How to assign:
- "field": one label paired with one value. The label is data, never a key.
- "table": any block whose values line up in columns, even with no ruled lines. Keep every header and cell exactly.
- column "role" is purely structural - decide it by position and shape, never by what a header means:
    identifier = a column whose cells name or identify each row;
    series     = a column whose HEADER is itself one value of a repeated dimension, so that several such columns are slices of one measure; the cells under it are that measure's values;
    attribute  = any other column.
- Copy every value verbatim - never compute, round, reformat, translate, or drop leading or trailing characters.
- Put the same information in BOTH "markdown" and "nodes"; lose nothing in either.

Return only the JSON object."""


def _vtype(v):
    v = str(v).strip().lower()
    return v if v in VALUE_TYPES else "string"


def _clean_nodes(raw):
    """Keep only well-formed nodes of the fixed vocabulary; coerce defensively."""
    out = []
    for n in (raw or []):
        if not isinstance(n, dict):
            continue
        t = n.get("type")
        if t == "heading" and isinstance(n.get("text"), str):
            lvl = n.get("level")
            out.append({"type": "heading", "level": int(lvl) if isinstance(lvl, (int, float)) else 1,
                        "text": n["text"]})
        elif t == "paragraph" and isinstance(n.get("text"), str):
            out.append({"type": "paragraph", "text": n["text"]})
        elif t == "field" and ("value" in n):
            out.append({"type": "field", "label": str(n.get("label", "")),
                        "value": n.get("value"), "value_type": _vtype(n.get("value_type", "string"))})
        elif t == "list" and isinstance(n.get("items"), list):
            out.append({"type": "list", "ordered": bool(n.get("ordered", False)),
                        "items": [str(x) for x in n["items"]]})
        elif t == "table":
            cols = []
            for c in (n.get("columns") or []):
                if isinstance(c, dict):
                    role = c.get("role"); role = role if role in COLUMN_ROLES else "attribute"
                    cols.append({"header": str(c.get("header", "")), "role": role,
                                 "type": _vtype(c.get("type", "string"))})
                else:
                    cols.append({"header": str(c), "role": "attribute", "type": "string"})
            rows = _coerce_rows(n.get("rows"), cols)
            out.append({"type": "table", "caption": str(n.get("caption", "")),
                        "columns": cols, "rows": rows})
    return out


def _coerce_rows(rows, cols):
    """Accept positional arrays (preferred) OR header-keyed objects; emit positional arrays."""
    headers = [c["header"] for c in cols]
    out = []
    for r in (rows or []):
        if isinstance(r, list):
            out.append([("" if x is None else str(x)) for x in r])
        elif isinstance(r, dict):
            cells = r.get("cells") if isinstance(r.get("cells"), dict) else r
            out.append([("" if cells.get(h) is None else str(cells.get(h, ""))) for h in headers])
    return out


def extract_page(client, image_path, max_tokens=30000):
    content = [{"type": "text", "text": EXTRACT_PROMPT},
               {"type": "image_url", "image_url": {"url": data_uri(image_path)}}]
    err, usage, mt = None, None, max_tokens
    for _attempt in range(3):
        obj, raw, usage = client.chat_json(content, mt)
        if isinstance(obj, dict):
            markdown = obj.get("markdown") if isinstance(obj.get("markdown"), str) else ""
            nodes = _clean_nodes(obj.get("nodes"))
            if markdown.strip() or nodes:
                return {"markdown": markdown, "nodes": nodes, "error": None, "usage": usage}
            err = "empty markdown and nodes"
        else:
            err = "no/invalid JSON: " + str(raw)[:160]
            mt = min(int(mt * 1.6), 32000)   # a parse failure is usually truncation -> grow the budget and retry
    return {"markdown": "", "nodes": [], "error": err, "usage": usage}


def extract_jobs(client, jobs, max_tokens=30000, parallel=8, on_done=None):
    """Global extraction over (key, image_path) jobs drawn from ANY documents, in ONE pool.
    Each page is independent (no cross-page or cross-doc state), so this maximizes parallelism:
    pages from every document are in flight together up to `parallel`. Returns {key: result}."""
    out = {}
    with ThreadPoolExecutor(max_workers=max(1, parallel)) as pool:
        futs = {pool.submit(extract_page, client, img, max_tokens): key for key, img in jobs}
        for f in as_completed(futs):
            key = futs[f]
            out[key] = f.result()
            if on_done:
                on_done(key, out[key])
    return out


def extract_documents(client, docs_pages, max_tokens=30000, parallel=64):
    """docs_pages: [(file_label, [image_path, ...]), ...].
    Runs EVERY page of EVERY file in ONE parallel pool (maximum parallelism), then regroups results
    into the document structure induce()/concepts() consume:
        [{file, pages:[{page, markdown, nodes}]}], file & page order preserved.
    """
    jobs = []
    for di, (_label, paths) in enumerate(docs_pages):
        for pi, p in enumerate(paths):
            jobs.append(((di, pi + 1), p))
    results = extract_jobs(client, jobs, max_tokens=max_tokens, parallel=parallel)
    docs = []
    for di, (label, paths) in enumerate(docs_pages):
        pages = []
        for pi in range(len(paths)):
            r = results.get((di, pi + 1)) or {"markdown": "", "nodes": []}
            pages.append({"page": pi + 1, "markdown": r.get("markdown", ""), "nodes": r.get("nodes", [])})
        docs.append({"file": label, "pages": pages})
    return docs
