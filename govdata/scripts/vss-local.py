#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
vss-local.py — LOCAL, CPU, per-year incremental VSS builder.

Replaces the remote-Vultr flow (vss-gpu-runner.sh + vss-bulk-gpu.py). Everything
runs on the ETL box: reads chunks from the vectorized_chunks Iceberg table on the
local MinIO endpoint, embeds NEW chunks on CPU (snowflake-arctic-embed-xs, tiny),
and INSERTs them into a persisted DuckDB HNSW index. The index is shard-keyed by
`yr` (year) — the pluggable shard dimension — and grows incrementally; a periodic
full rebuild keeps the HNSW healthy. The finished .duckdb is published atomically
to MinIO for clients to cache on first run.

Design notes:
  * No Vultr, no ship-up/ship-down, MinIO never exposed off-box.
  * No write-back to the Iceberg embedding column — embeddings live only in the
    published cache, so ETL materialization stays append-clean.
  * Embedder is an interface (CPU default); a GPU backend can drop in later.

Commands:
  year  --year N        Embed + insert the delta (Iceberg year N chunks not yet in
                        the cache). Incremental: safe to re-run; only new chunk_ids
                        are embedded. Creates the cache + index on first use.
  rebuild               Drop and rebuild the HNSW index over all cached rows (hygiene).
  publish               Atomically upload the cache .duckdb + metadata.json to MinIO.
  stats                 Print per-year row/accession counts in the cache.
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time

import duckdb

MODEL = os.environ.get("VSS_EMBED_MODEL", "Snowflake/snowflake-arctic-embed-xs")
DIM = int(os.environ.get("VSS_EMBED_DIM", "384"))
BATCH = int(os.environ.get("VSS_EMBED_BATCH", "10000"))         # rows per read+embed+insert cycle
ENCODE_BATCH = int(os.environ.get("VSS_ENCODE_BATCH", "64"))    # sentence-transformers micro-batch

GOVDATA_HOME = os.environ.get("GOVDATA_HOME") or os.path.abspath(
    os.path.join(os.path.dirname(__file__), ".."))
VSS_DB = os.environ.get("VSS_DB", os.path.join(GOVDATA_HOME, "build", ".aperio", "vss", "chunks_vss.duckdb"))
PARQUET_BUCKET = os.environ.get("GOVDATA_PARQUET_DIR", "s3://govdata-parquet-v1")
ICEBERG_CHUNKS = f"{PARQUET_BUCKET}/sec/vectorized_chunks"
CACHE_PREFIX = "cache/vss"  # under the parquet bucket
RCLONE_REMOTE = os.environ.get("GOVDATA_RCLONE_REMOTE", "minio")
MEM_LIMIT = os.environ.get("VSS_MEM_LIMIT", "4GB")
TEMP_DIR = os.environ.get("VSS_TEMP_DIR", "/home/adminwsl/tmp_duck")


# ── Embedder interface ────────────────────────────────────────────────────────
class Embedder:
    def embed(self, texts):
        raise NotImplementedError


class CpuEmbedder(Embedder):
    """sentence-transformers on CPU. The model is tiny; this is the default backend."""

    def __init__(self, model_name=MODEL):
        from sentence_transformers import SentenceTransformer
        t = time.time()
        self.model = SentenceTransformer(model_name, device="cpu")
        print(f"  [embedder] loaded {model_name} on CPU in {time.time()-t:.1f}s", flush=True)

    def embed(self, texts):
        return self.model.encode(
            texts, batch_size=ENCODE_BATCH, normalize_embeddings=True,
            convert_to_numpy=True, show_progress_bar=False)


def make_embedder():
    backend = os.environ.get("VSS_EMBED_BACKEND", "cpu")
    if backend == "cpu":
        return CpuEmbedder()
    raise SystemExit(f"Unknown VSS_EMBED_BACKEND={backend!r} (only 'cpu' is implemented)")


# ── DuckDB helpers ────────────────────────────────────────────────────────────
def _endpoint_parts():
    ep = os.environ["AWS_ENDPOINT_OVERRIDE"]
    if ep.startswith("http://"):
        return ep[len("http://"):], "false"
    if ep.startswith("https://"):
        return ep[len("https://"):], "true"
    return ep, "false"


def connect_cache():
    """Open the persisted cache DB with vss loaded and S3/iceberg configured."""
    os.makedirs(os.path.dirname(VSS_DB), exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)
    con = duckdb.connect(VSS_DB)
    con.execute(f"SET memory_limit='{MEM_LIMIT}'")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    con.execute("INSTALL vss; LOAD vss")
    con.execute("SET hnsw_enable_experimental_persistence=true")
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute("INSTALL iceberg; LOAD iceberg")
    host, ssl = _endpoint_parts()
    for k, v in (("AWS_ACCESS_KEY_ID", None), ("AWS_SECRET_ACCESS_KEY", None)):
        if not os.environ.get(k):
            raise SystemExit(f"{k} not set — source .env.prod first")
    con.execute("SET s3_region='us-east-1'")
    con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}'")
    con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}'")
    con.execute(f"SET s3_endpoint='{host}'")
    con.execute(f"SET s3_use_ssl={ssl}")
    con.execute("SET s3_url_style='path'")
    con.execute("SET unsafe_enable_version_guessing=true")
    return con


def ensure_schema(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS chunks (
            chunk_id VARCHAR,
            cik VARCHAR,
            accession_number VARCHAR,
            yr INTEGER,
            section VARCHAR,
            source_type VARCHAR,
            content_type VARCHAR,
            chunk_text VARCHAR,
            embedding FLOAT[{DIM}]
        )
    """)


def has_index(con):
    r = con.execute(
        "SELECT count(*) FROM duckdb_indexes() WHERE index_name='chunks_hnsw'").fetchone()
    return r[0] > 0


def ensure_index(con):
    if not has_index(con):
        print("  [index] creating HNSW index (cosine)...", flush=True)
        t = time.time()
        con.execute("CREATE INDEX chunks_hnsw ON chunks USING HNSW (embedding) WITH (metric='cosine')")
        print(f"  [index] built in {time.time()-t:.1f}s", flush=True)


# ── Commands ──────────────────────────────────────────────────────────────────
def cmd_year(year):
    con = connect_cache()
    ensure_schema(con)

    # Delta = Iceberg year-N chunks whose chunk_id is not already cached. Materialize
    # the Iceberg side to a temp table first (avoids DuckDB's iceberg_scan list-column
    # filter-pushdown quirk and gives a stable count), then anti-join the cache.
    print(f"[year {year}] reading Iceberg delta from {ICEBERG_CHUNKS} ...", flush=True)
    t = time.time()
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _src AS
        SELECT chunk_id, cik, accession_number, year AS yr, section,
               source_type, content_type, chunk_text
        FROM iceberg_scan('{ICEBERG_CHUNKS}')
        WHERE year = {int(year)}
          AND chunk_text IS NOT NULL AND length(chunk_text) > 10
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _todo AS
        SELECT s.* FROM _src s
        LEFT JOIN chunks c ON c.chunk_id = s.chunk_id
        WHERE c.chunk_id IS NULL
    """)
    total = con.execute("SELECT count(*) FROM _todo").fetchone()[0]
    src_total = con.execute("SELECT count(*) FROM _src").fetchone()[0]
    print(f"[year {year}] source={src_total} chunks, {total} new to embed "
          f"({src_total-total} already cached) — scan {time.time()-t:.1f}s", flush=True)
    if total == 0:
        print(f"[year {year}] nothing to do", flush=True)
        con.close()
        return

    emb = make_embedder()
    done = 0
    t0 = time.time()
    while done < total:
        rows = con.execute(
            f"SELECT chunk_id, cik, accession_number, yr, section, source_type, "
            f"content_type, chunk_text FROM _todo LIMIT {BATCH} OFFSET {done}").fetchall()
        if not rows:
            break
        vecs = emb.embed([r[7] for r in rows])
        con.executemany(
            "INSERT INTO chunks (chunk_id, cik, accession_number, yr, section, "
            "source_type, content_type, chunk_text, embedding) VALUES (?,?,?,?,?,?,?,?,?)",
            [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], vecs[k].tolist())
             for k, r in enumerate(rows)])
        done += len(rows)
        rate = done / max(time.time() - t0, 1e-6)
        print(f"[year {year}] embedded {done}/{total} ({rate:.0f}/sec)", flush=True)

    # Index insert: the HNSW index updates on INSERT once it exists. Create it after the
    # first batch load so the very first year still gets an index; later years insert
    # into the live index incrementally.
    ensure_index(con)
    print(f"[year {year}] complete: +{total} chunks in {time.time()-t0:.0f}s", flush=True)
    con.close()


def cmd_rebuild():
    con = connect_cache()
    ensure_schema(con)
    print("[rebuild] dropping and rebuilding HNSW index...", flush=True)
    con.execute("DROP INDEX IF EXISTS chunks_hnsw")
    t = time.time()
    con.execute("CREATE INDEX chunks_hnsw ON chunks USING HNSW (embedding) WITH (metric='cosine')")
    n = con.execute("SELECT count(*) FROM chunks").fetchone()[0]
    print(f"[rebuild] index rebuilt over {n} chunks in {time.time()-t:.0f}s", flush=True)
    con.close()


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for blk in iter(lambda: f.read(1 << 20), b""):
            h.update(blk)
    return h.hexdigest()


def cmd_publish():
    if not os.path.exists(VSS_DB):
        raise SystemExit(f"No cache DB at {VSS_DB} — build a year first")
    con = duckdb.connect(VSS_DB, read_only=True)
    con.execute("LOAD vss")
    rows = con.execute("SELECT count(*) FROM chunks").fetchone()[0]
    con.close()

    sha = _sha256(VSS_DB)
    size = os.path.getsize(VSS_DB)
    base = f"{RCLONE_REMOTE}:{PARQUET_BUCKET[len('s3://'):]}/{CACHE_PREFIX}"
    tmp_key = f"{base}/.chunks_vss.duckdb.tmp"
    final_key = f"{base}/chunks_vss.duckdb"
    meta_key = f"{base}/metadata.json"

    print(f"[publish] {rows} rows, {size/1e6:.1f} MB, sha256={sha[:12]}… -> {final_key}", flush=True)
    # Atomic-ish: upload to a temp key, then server-side move into place so clients
    # never observe a half-written cache.
    subprocess.run(["rclone", "copyto", VSS_DB, tmp_key], check=True)
    subprocess.run(["rclone", "moveto", tmp_key, final_key], check=True)

    meta = {
        "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "rows": rows, "embed_dim": DIM, "model": MODEL,
        "sha256": sha, "bytes": size,
    }
    p = subprocess.Popen(["rclone", "rcat", meta_key], stdin=subprocess.PIPE)
    p.communicate(json.dumps(meta).encode())
    if p.returncode != 0:
        raise SystemExit("metadata.json upload failed")
    print(f"[publish] done -> {final_key} (+ metadata.json)", flush=True)


def cmd_stats():
    if not os.path.exists(VSS_DB):
        print("(no cache DB yet)")
        return
    con = duckdb.connect(VSS_DB, read_only=True)
    con.execute("LOAD vss")
    for row in con.execute(
        "SELECT yr, count(DISTINCT accession_number) AS accessions, count(*) AS chunks "
        "FROM chunks GROUP BY yr ORDER BY yr").fetchall():
        print(f"  year={row[0]}  accessions={row[1]}  chunks={row[2]}")
    con.close()


def main():
    ap = argparse.ArgumentParser(description="Local CPU per-year incremental VSS builder")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_year = sub.add_parser("year"); p_year.add_argument("--year", type=int, required=True)
    sub.add_parser("rebuild")
    sub.add_parser("publish")
    sub.add_parser("stats")
    args = ap.parse_args()

    if args.cmd == "year":
        cmd_year(args.year)
    elif args.cmd == "rebuild":
        cmd_rebuild()
    elif args.cmd == "publish":
        cmd_publish()
    elif args.cmd == "stats":
        cmd_stats()
    else:
        ap.error("unknown command")


if __name__ == "__main__":
    main()
