"""Native-format parsers, driven entirely by binding configuration.

There is no service name anywhere in this package. Each parser reads its
layout, delimiters, header shape and null tokens from the binding. Adding a
fifth service in an existing format needs no code at all; adding a genuinely
new format adds one parser and one `source.format` value.
"""
from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Iterator

PARSERS = {}


def parser(fmt: str):
    def deco(fn):
        PARSERS[fmt] = fn
        return fn
    return deco


def parse(binding: dict, path: Path) -> Iterator[dict]:
    fmt = binding["source"]["format"]
    if fmt not in PARSERS:
        raise ValueError(f"no parser for declared format {fmt!r}")
    yield from PARSERS[fmt](binding, path)


# --------------------------------------------------------------- fixed width

@parser("fixed_width")
def _fixed_width(binding, path):
    src = binding["source"]
    layout = binding["layout"]
    reclen = src["record_length"]
    hdr = src.get("header_records", 0)
    trl = src.get("trailer_records", 0)

    lines = path.read_text(encoding="utf-8").splitlines()
    body = lines[hdr:len(lines) - trl] if trl else lines[hdr:]
    for i, line in enumerate(body):
        if not line.strip():
            continue
        if len(line) != reclen:
            raise ValueError(f"record {i} is {len(line)} bytes, declared length is {reclen} "
                             f"(quality rule record_length_exact)")
        rec = {}
        for name, spec in layout.items():
            s = spec["start"] - 1
            rec[name] = line[s:s + spec["len"]]
        yield rec


# ----------------------------------------------------------------------- xml

@parser("xml")
def _xml(binding, path):
    """Nested, attribute-heavy XML. Reporting blocks are the record grain, but
    identity and agreement data live on ancestors, so each yielded record
    carries its inherited context."""
    from xml.etree import ElementTree as ET

    src = binding["source"]
    ns = src.get("namespaces", {})
    ns.setdefault("xsi", "http://www.w3.org/2001/XMLSchema-instance")
    layout = binding["layout"]
    tree = ET.parse(path)
    root = tree.getroot()

    def get(node, xpath, repeating=False):
        """Evaluate the declared xpath subset: attributes, text, repeating
        groups, and attribute predicates."""
        if xpath.startswith("@"):
            return node.get(_qname(xpath[1:], ns))
        p = xpath.removesuffix("/text()")
        # An attribute selector only when the path ENDS in /@name -- a
        # predicate such as [@basis='txn30d'] is not an attribute selector.
        if re.search(r"/@[\w:]+$", p):
            head, _, attr = p.rpartition("/@")
            found = node.findall(head, ns) if head else [node]
            vals = [f.get(_qname(attr, ns)) for f in found if f is not None]
            return vals[0] if vals else None
        found = node.findall(p, ns)
        if not found:
            return [] if repeating else None
        vals = []
        for f in found:
            if f.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true":
                vals.append(None)
            else:
                vals.append(f.text)
        return vals if repeating else vals[0]

    entity_path = src["entity_record_path"]
    fact_path = src["fact_record_path"]
    child = src.get("child_paths", {})
    agr_path = child.get("agreement")
    agr_key = child.get("agreement_key_attr", "ref")
    fact_parent = child.get("fact_parent_attr", "agreement")

    for m in root.findall(entity_path, ns):
        for rep in m.findall(fact_path, ns):
            rec = {}
            for name, spec in layout.items():
                xp = spec["xpath"]
                rep_flag = bool(spec.get("repeating"))
                if xp.startswith(fact_path + "/"):
                    rec[name] = get(rep, xp.removeprefix(fact_path + "/"), rep_flag)
                else:
                    rec[name] = get(m, xp, rep_flag)
            rec["fiscal_period"] = rep.get("period")
            parent_ref = rep.get(fact_parent)
            if agr_path:
                for a in m.findall(agr_path, ns):
                    if a.get(agr_key) == parent_ref:
                        rec["contract_ref"] = a.get(agr_key)
                        rec["contract_tier"] = a.get("tier")
                        rec["currency_code"] = a.get("ccy")
                        term = a.find("mp:Term", ns)
                        if term is not None:
                            rec["contract_from"] = term.get("from")
                            rec["contract_to"] = term.get("to")
                        break
            yield rec


def _qname(name: str, ns: dict) -> str:
    if ":" in name:
        prefix, _, local = name.partition(":")
        if prefix in ns:
            return f"{{{ns[prefix]}}}{local}"
    return name


# ------------------------------------------------------- csv, multi-row header

@parser("csv_multiheader")
def _csv_multiheader(binding, path):
    """Analyst-authored CSV: a title row, a metadata row, a units row, then
    column names, then data, then trailing commentary. Every one of those is a
    human convention a parser must be told about explicitly."""
    src = binding["source"]
    hb = src["header_block"]
    names_row = next(int(k) for k, v in hb["row_roles"].items() if v == "column_names")
    start = src["data_starts_row"]
    detector = src.get("trailing_notes_detector", {}).get("first_cell_matches")
    note_re = re.compile(detector) if detector else None

    text = path.read_text(encoding=src.get("encoding", "utf-8"))
    rows = list(csv.reader(io.StringIO(text)))
    header = rows[names_row - 1]
    layout = binding["layout"]
    col_index = {}
    for name, spec in layout.items():
        col = spec["column"]
        if col not in header:
            raise ValueError(f"declared column {col!r} not present in the header row "
                             f"(quality rule header_block_shape)")
        col_index[name] = header.index(col)

    for row in rows[start - 1:]:
        if not row or not any(c.strip() for c in row):
            continue
        if note_re and note_re.match(row[0].strip()):
            continue                       # trailing commentary, not a record
        yield {name: (row[i] if i < len(row) else None) for name, i in col_index.items()}


# ------------------------------------------------- delimited, hierarchical

@parser("delimited_hierarchical")
def _delimited_hierarchical(binding, path):
    """Pipe-delimited records with caret subfields and positional parentage.
    Metric records are the grain; merchant and agreement context is inherited
    from the most recent preceding record of that type."""
    src = binding["source"]
    fd = src["field_delimiter"]
    sd = src["subfield_delimiter"]
    types = src["record_types"]
    layout = binding["layout"]

    current: dict[str, dict] = {}
    for raw in path.read_text(encoding=src.get("encoding", "ascii")).splitlines():
        if not raw.strip():
            continue
        parts = raw.split(fd)
        rtype = parts[0]
        if rtype not in types:
            continue
        role = types[rtype]
        if role in ("file_header", "file_trailer"):
            continue

        spec = layout.get(rtype, {})
        rec = {}
        for pos, fs in spec.items():
            idx = int(pos)
            val = parts[idx] if idx < len(parts) else None
            if "subfields" in fs and val is not None:
                bits = val.split(sd)
                for j, sub in enumerate(fs["subfields"]):
                    rec[f"{rtype}.{fs['name']}^{sub}"] = bits[j] if j < len(bits) else None
            rec[f"{rtype}.{fs['name']}"] = val
        current[rtype] = rec

        if role == "metric_record":
            merged = {}
            for t in ("MER", "AGR"):
                merged.update(current.get(t, {}))
            merged.update(rec)
            yield merged
