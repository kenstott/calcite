# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Catalog conformance: what the server serves against what a real PostgreSQL serves.

The reference (reference/pg16-catalog.json, written by generate_reference.py from a stock
PostgreSQL) lists every pg_catalog and information_schema relation with its columns, in order,
with each column's type. For each one this test asks the server's catalog the same question a
client does -- ``SELECT * FROM <relation> LIMIT 0`` -- and compares:

* coverage    -- is the relation served at all;
* shape       -- the same columns, in the same order;
* type family -- an integer column is an integer, text is text (a client that joins a text
                 column to an oid fails on the cast; ``pg_description.classoid`` did exactly that).

Every difference must be listed in accepted_deviations.json with a reason and a status:
``accepted`` is a difference that follows from what this server is (a Calcite-backed, read-mostly
SQL front end with no storage engine, replication or statistics collector); ``gap`` is a
difference that is a defect to fix. The test fails on a difference that is not listed and on a
listed difference that no longer exists, so the list can neither hide a regression nor go stale.
"""

from __future__ import annotations

import fnmatch
import json
import logging
from pathlib import Path

import pytest

from pgwire_calcite import catalog, launcher
from pgwire_calcite.backend import StubBackend
from pgwire_calcite.catalog_populate import install_catalog
from pgwire_calcite.compiler.sql_gen import ColumnMeta, CompilationContext, TableMeta

HERE = Path(__file__).parent
REFERENCE = json.loads((HERE / "reference" / "pg16-catalog.json").read_text())
DEVIATIONS = json.loads((HERE / "accepted_deviations.json").read_text())

_PG_FAMILY = {
    "bool": "bool",
    "int2": "int", "int4": "int", "int8": "int", "oid": "int", "xid": "int", "cid": "int",
    "xid8": "int", "regproc": "int", "regprocedure": "int", "regoper": "int",
    "regoperator": "int", "regclass": "int", "regtype": "int", "regconfig": "int",
    "regdictionary": "int", "regnamespace": "int", "regrole": "int", "regcollation": "int",
    "pg_lsn": "int",
    "float4": "float", "float8": "float", "numeric": "float",
    "text": "text", "varchar": "text", "name": "text", "bpchar": "text", "char": "text",
    "citext": "text", "pg_node_tree": "text", "pg_ndistinct": "text",
    "pg_dependencies": "text", "pg_mcv_list": "text", "pg_ddl_command": "text",
    "timestamp": "temporal", "timestamptz": "temporal", "date": "temporal",
    "time": "temporal", "timetz": "temporal", "interval": "temporal",
    "bytea": "bytea", "json": "json", "jsonb": "json",
    "inet": "text", "cidr": "text", "macaddr": "text", "uuid": "text",
    "int2vector": "array", "oidvector": "array", "aclitem": "text",
    # information_schema's own domains: each is its base type on the wire, a distinct OID only.
    "sql_identifier": "text", "character_data": "text", "yes_or_no": "text",
    "cardinal_number": "int", "time_stamp": "temporal",
}


def _pg_family(typname: str) -> str:
    if typname.startswith("_"):
        return "array"
    return _PG_FAMILY.get(typname, "other:" + typname)


def _served_family(duckdb_type) -> str | None:
    """The family of a column type the server declares; None when it leaves the type to be
    inferred from the values (the wire layer then types it from the first row)."""
    if not duckdb_type:
        return None
    t = str(duckdb_type).upper()
    if t.endswith("[]") or t.startswith("LIST"):
        return "array"
    if t in ("BOOLEAN", "BOOL"):
        return "bool"
    if t in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT",
             "UINTEGER", "UBIGINT", "INT", "INT2", "INT4", "INT8"):
        return "int"
    if t in ("FLOAT", "REAL", "DOUBLE") or t.startswith("DECIMAL") or t.startswith("NUMERIC"):
        return "float"
    if t in ("VARCHAR", "TEXT", "STRING", "CHAR", "BPCHAR", "UUID") or t.startswith("VARCHAR"):
        return "text"
    if t.startswith("TIMESTAMP") or t in ("DATE", "TIME", "INTERVAL") or t.startswith("TIME "):
        return "temporal"
    if t in ("BLOB", "BYTEA"):
        return "bytea"
    if t == "JSON":
        return "json"
    return "other:" + t.lower()


def build_served() -> dict[str, tuple[list[str], list[str | None]]]:
    """relation -> (column names, column type families) as the catalog answers a client."""
    depts = TableMeta(1, "postgres", "SALES", "depts", "SALES.depts")
    ctx = CompilationContext(tables={depts.type_name: depts}, pk_columns={1: ["deptno"]}, joins={})
    state = launcher.build_state(backend=StubBackend())
    install_catalog(
        state, ctx, {1: [ColumnMeta("deptno", "INTEGER", False), ColumnMeta("dname", "VARCHAR")]}
    )
    out: dict[str, tuple[list[str], list[str | None]]] = {}
    # A catalog that cannot be built answers nothing; that is a failure of the build, not 209
    # unserved relations, so build it outside the loop that treats an error as a finding.
    assert catalog.answer("SELECT * FROM pg_catalog.pg_class LIMIT 0", "conformance", state)
    # An unserved relation is a finding, and the catalog logs each one it cannot answer.
    logging.disable(logging.ERROR)
    for rel in REFERENCE["relations"]:
        try:
            r = catalog.answer(f"SELECT * FROM {rel} LIMIT 0", "conformance", state)
        except Exception:  # noqa: BLE001 - an unserved relation is the finding, not an error
            continue
        if r is None:
            continue
        types = list(r.column_types or [])
        types += [None] * (len(r.column_names) - len(types))
        out[rel] = (list(r.column_names), [_served_family(t) for t in types])
    logging.disable(logging.NOTSET)
    catalog.invalidate_catalog_cache()
    return out


@pytest.fixture(scope="module")
def served():
    return build_served()


def _matches(patterns: list[dict], name: str) -> dict | None:
    for entry in patterns:
        if fnmatch.fnmatchcase(name, entry["match"]):
            return entry
    return None


def _findings(served) -> list[tuple[str, str, str]]:
    """(kind, key, detail) for every difference between the reference and the server."""
    found: list[tuple[str, str, str]] = []
    for rel, ref_cols in REFERENCE["relations"].items():
        if rel not in served:
            found.append(("unserved", rel, "relation is not served"))
            continue
        names, families = served[rel]
        ref_names = [c["name"] for c in ref_cols]
        for c in ref_cols:
            if c["name"] not in names:
                found.append(("missing", f"{rel}.{c['name']}", "column is not served"))
        for n in names:
            if n not in ref_names:
                found.append(("extra", f"{rel}.{n}", "column is not in PostgreSQL"))
        common_ref = [n for n in ref_names if n in names]
        common_got = [n for n in names if n in ref_names]
        if common_ref != common_got:
            found.append(("order", rel, "shared columns are in a different order"))
        for c in ref_cols:
            if c["name"] in names:
                fam = families[names.index(c["name"])]
                want = _pg_family(c["type"])
                if fam is not None and fam != want:
                    found.append(
                        ("type", f"{rel}.{c['name']}", f"PostgreSQL {c['type']} ({want}), served {fam}")
                    )
    for rel in served:
        if rel not in REFERENCE["relations"]:
            found.append(("extra_relation", rel, "relation is not in PostgreSQL"))
    return found


def test_every_difference_from_postgres_is_listed(served):
    unlisted = []
    for kind, key, detail in _findings(served):
        if _matches(DEVIATIONS.get(kind, []), key) is None:
            unlisted.append(f"{kind:14} {key:62} {detail}")
    assert not unlisted, (
        "differences from PostgreSQL that accepted_deviations.json does not list "
        "(fix the server, or add the entry with its reason):\n" + "\n".join(unlisted)
    )


def test_no_listed_difference_is_stale(served):
    seen: dict[str, set[str]] = {}
    for kind, key, _detail in _findings(served):
        entry = _matches(DEVIATIONS.get(kind, []), key)
        if entry is not None:
            seen.setdefault(kind, set()).add(entry["match"])
    stale = [
        f"{kind}: {e['match']}"
        for kind, entries in DEVIATIONS.items()
        if kind != "about"
        for e in entries
        if e["match"] not in seen.get(kind, set())
    ]
    assert not stale, "listed differences that no longer exist (remove them):\n" + "\n".join(stale)


def test_every_listed_difference_has_a_reason_and_a_status():
    bad = [
        f"{kind}: {e.get('match')}"
        for kind, entries in DEVIATIONS.items()
        if kind != "about"
        for e in entries
        if e.get("status") not in ("accepted", "gap") or len(str(e.get("reason", "")).strip()) < 15
    ]
    assert not bad, "entries need status accepted|gap and a reason:\n" + "\n".join(bad)


def test_report_open_gaps(served, capsys):
    """Not a failure: prints how many differences are accepted and how many are defects."""
    counts = {"accepted": 0, "gap": 0}
    for kind, key, _detail in _findings(served):
        entry = _matches(DEVIATIONS.get(kind, []), key)
        if entry is not None:
            counts[entry["status"]] += 1
    with capsys.disabled():
        print(f"\ncatalog conformance vs PostgreSQL {REFERENCE['postgres_version']}: "
              f"{len(REFERENCE['relations'])} relations, "
              f"{counts['accepted']} accepted differences, {counts['gap']} open gaps")
