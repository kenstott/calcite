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
vss-local.py — LOCAL, CPU, delta-driven quantized-code producer for semantic search.

Embeds un-coded chunks from the vectorized_chunks Iceberg table on CPU
(snowflake-arctic-embed-xs), quantizes each 384-d unit vector to:
  * a 48-byte BINARY code  (sign bits, packed into 6x uint64)  — Hamming prefilter
  * a 384-byte INT8 vector (scalar-quantized)                  — rerank
and appends them as a parquet file to the `vectorized_chunk_codes` dataset in the
lake. That dataset is the delivered semantic-search artifact (Path B): a Java
intercept materializes it into a persistent local DuckDB and runs a two-stage
`bit_count` Hamming prefilter + int8 rerank (fast on repeat via DuckDB persistence).

No HNSW, no DuckDB HNSW cache, no Postgres — vectors live only as quantized codes in
the lake, and the "what's un-coded" delta is a self-describing Iceberg-vs-codes
anti-join (chunk_ids in vectorized_chunks not yet in vectorized_chunk_codes).

This script only ever embeds already-organized chunks -- it never builds chunk_text
itself. Organizing text into chunk rows (row-concat mode's naive per-row chunker,
document-blob mode's SemanticTextChunker) is always Java's job, writing into a
vectorized_chunks-shaped Iceberg table (see
org.apache.calcite.adapter.govdata.ref.ChunkOrganizer for the row-concat producer).

Commands:
  backlog [--max-rows N --max-seconds S]
                        PRIMARY job. ref.vectorized_chunks is ONE table holding chunks from
                        every source (sec, ref, fedregister, cyber_threat, ...) -- this is
                        ONE queue over the whole table's un-coded delta, not a per-schema
                        job. Capped at --max-rows and time-boxed to --max-seconds (~2h); the
                        backlog drains over successive runs, resuming via the one watermark
                        (see WATERMARK_DIR below). CPU only — no GPU.
  year --year N         Code a single SEC year's delta (manual/targeted).
  stats                 Per-(source_schema, year) counts across every codes dataset.
"""

import argparse
import os
import subprocess
import time

import duckdb

MODEL = os.environ.get("VSS_EMBED_MODEL", "Snowflake/snowflake-arctic-embed-xs")
DIM = int(os.environ.get("VSS_EMBED_DIM", "384"))
BATCH = int(os.environ.get("VSS_EMBED_BATCH", "10000"))         # rows per embed+pack cycle
ENCODE_BATCH = int(os.environ.get("VSS_ENCODE_BATCH", "128"))   # sentence-transformers micro-batch
TORCH_THREADS = int(os.environ.get("VSS_TORCH_THREADS", str(os.cpu_count() or 8)))
FLUSH_ROWS = int(os.environ.get("VSS_FLUSH_ROWS", "100000"))    # write a codes parquet every N rows

GOVDATA_HOME = os.environ.get("GOVDATA_HOME") or os.path.abspath(
    os.path.join(os.path.dirname(__file__), ".."))
PARQUET_BUCKET = os.environ.get("GOVDATA_PARQUET_DIR", "s3://govdata-parquet-v1")
# SEC's chunks are promoted into the shared ref.vectorized_chunks table (source_schema='sec'
# partition) -- this schema no longer materializes its own vectorized_chunks Iceberg table.
ICEBERG_CHUNKS = f"{PARQUET_BUCKET}/ref/vectorized_chunks"
RCLONE_REMOTE = os.environ.get("GOVDATA_RCLONE_REMOTE", "minio")
# Consolidate the codes dataset once it exceeds this many files (incremental flush + daily runs
# accumulate small files; too many slows the query glob + the delta anti-join).
COMPACT_MIN_FILES = int(os.environ.get("VSS_COMPACT_MIN_FILES", "16"))
# int8 = round(clip(x, -I8_SCALE, I8_SCALE) / I8_SCALE * 127). arctic vectors are unit
# norm so components sit well inside +/-0.35; a fixed global scale keeps int8 dot
# products comparable across rows for rerank.
I8_SCALE = float(os.environ.get("VSS_I8_SCALE", "0.35"))
# The unified backlog query joins+sorts across every source_schema partition of
# ref.vectorized_chunks at once (no more per-schema partition pruning to shrink the working set
# first), so it needs more headroom than the old per-schema scans did -- 4GB OOM'd live on just
# an ORDER BY chunk_id LIMIT 50 test. No safe fixed default exists here: this runs alongside
# whatever else the pool is doing on the same box at the time, so the caller must set
# VSS_MEM_LIMIT explicitly based on actual free memory when launching a real run, not have the
# script assume a number.
MEM_LIMIT = os.environ.get("VSS_MEM_LIMIT")
if not MEM_LIMIT:
    raise SystemExit(
        "VSS_MEM_LIMIT not set -- check free memory on this box and set it explicitly "
        "(the unified backlog query needs more than the old default; there is no safe "
        "one-size-fits-all default with a shared pool running other jobs concurrently)")
TEMP_DIR = os.environ.get("VSS_TEMP_DIR", "/var/tmp/govdata/vss_tmp_duck")

# ONE watermark for the ONE queue: ref.vectorized_chunks is a single table holding chunks from
# every source, so there is exactly one backlog and exactly one resume position, not one per
# source_schema. It is just the chunk_id of the last chunk this process actually finished
# embedding -- the chunks scan and the codes write both walk chunk_id in the same ascending
# order, so "we got through here" is always a safe place to resume, whether a run drained
# everything or stopped at --max-seconds. It advances after every run, never gated on the whole
# backlog being exhaustively drained first.
#
# The `_done` anti-join stays as a safety net on top of this: a crash between writing a codes
# file and saving the watermark just gets re-scanned and found already-coded next run, never
# double-embedded. No watermark file (first run) means start of the queue.
WATERMARK_DIR = os.environ.get("VSS_WATERMARK_DIR", os.path.join(GOVDATA_HOME, ".vss-watermarks"))
WATERMARK_PATH = os.path.join(WATERMARK_DIR, "chunk_id_watermark")


def _load_watermark():
    try:
        with open(WATERMARK_PATH) as f:
            return f.read().strip() or None
    except FileNotFoundError:
        return None


def _save_watermark(chunk_id):
    os.makedirs(WATERMARK_DIR, exist_ok=True)
    tmp = WATERMARK_PATH + ".tmp"
    with open(tmp, "w") as f:
        f.write(chunk_id)
    os.replace(tmp, WATERMARK_PATH)


def codes_dataset_for(source_schema):
    """Hive-partitioned codes output path for a given source_schema -- physical storage stays
    partitioned this way (same idea as ref.vectorized_chunks itself: one table, one queue, but
    partitioned on disk) even though the backlog queue that fills it is unified across all of
    them. VSS_CODES_DATASET overrides to a single scratch path for every schema (tests only)."""
    override = os.environ.get("VSS_CODES_DATASET")
    if override:
        return override
    return f"{PARQUET_BUCKET}/{source_schema}/vectorized_chunk_codes/source_schema={source_schema}"


# Every source_schema value ChunkOrganizer (govdata/.../ref/ChunkOrganizer.java) ever writes into
# the shared ref.vectorized_chunks table -- keep in sync with SemanticSearch.java's own
# DEFAULT_SOURCE_SCHEMAS, which reads the same codes for search. A source_schema present there
# but missing here just means its un-coded chunks keep getting found (harmless, re-embedded into
# its own new partition the moment this list catches up); NOT keeping this in sync with
# ChunkOrganizer's own registries would mean a real embedding never gets picked up as "done" (its
# codes exist but this list can't see them), which is the failure mode worth avoiding.
CODES_SOURCE_SCHEMAS = ("sec", "ref", "fedregister", "cyber_threat")


def _codes_glob_arg():
    """Bracketed list of every known codes dataset's glob, for one read_parquet() call spanning
    all of them -- NOT a bare '*' at the bucket root, which would make DuckDB list every prefix
    in the entire multi-schema bucket (confirmed live: a multi-minute paginated LIST across
    unrelated schemas' data, not just the ~4 relevant ones)."""
    return "[" + ", ".join(
        f"'{codes_dataset_for(s)}/*.parquet'" for s in CODES_SOURCE_SCHEMAS) + "]"


# ── Embedder interface ────────────────────────────────────────────────────────
class Embedder:
    def embed(self, texts):
        raise NotImplementedError


class CpuEmbedder(Embedder):
    """sentence-transformers on CPU. The model is tiny; this is the default backend."""

    def __init__(self, model_name=MODEL):
        import torch
        # Saturate the box: torch defaults to a subset of cores (~8) and in practice
        # only used ~3.6. Pin to all cores.
        torch.set_num_threads(TORCH_THREADS)
        from sentence_transformers import SentenceTransformer
        t = time.time()
        self.model = SentenceTransformer(model_name, device="cpu")
        print(f"  [embedder] loaded {model_name} on CPU in {time.time()-t:.1f}s "
              f"(threads={TORCH_THREADS}, encode_batch={ENCODE_BATCH})", flush=True)

    def embed(self, texts):
        return self.model.encode(
            texts, batch_size=ENCODE_BATCH, normalize_embeddings=True,
            convert_to_numpy=True, show_progress_bar=False)


def make_embedder():
    backend = os.environ.get("VSS_EMBED_BACKEND", "cpu")
    if backend == "cpu":
        return CpuEmbedder()
    raise SystemExit(f"Unknown VSS_EMBED_BACKEND={backend!r} (only 'cpu' is implemented)")


# ── DuckDB / lake ─────────────────────────────────────────────────────────────
def _endpoint_parts():
    ep = os.environ["AWS_ENDPOINT_OVERRIDE"]
    if ep.startswith("http://"):
        return ep[len("http://"):], "false"
    if ep.startswith("https://"):
        return ep[len("https://"):], "true"
    return ep, "false"


def connect_lake():
    """In-memory DuckDB configured to read Iceberg and read/write parquet on MinIO."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{MEM_LIMIT}'")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    # The backlog query has its own explicit ORDER BY chunk_id, so DuckDB doesn't also need to
    # preserve arbitrary intermediate-operator ordering just to keep output order stable --
    # turning that off frees the buffering it costs.
    con.execute("SET preserve_insertion_order=false")
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute("INSTALL iceberg; LOAD iceberg")
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        if not os.environ.get(k):
            raise SystemExit(f"{k} not set — source .env.prod first")
    host, ssl = _endpoint_parts()
    con.execute("SET s3_region='us-east-1'")
    con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}'")
    con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}'")
    con.execute(f"SET s3_endpoint='{host}'")
    con.execute(f"SET s3_use_ssl={ssl}")
    con.execute("SET s3_url_style='path'")
    con.execute("SET unsafe_enable_version_guessing=true")
    return con


def _load_done(con, dataset=None):
    """Temp table `_done` of already-coded chunk_ids across EVERY source's codes dataset (a
    bracketed list of each known dataset's own glob -- one queue needs one combined view of
    what's already coded, not a per-schema one). `dataset`, when passed, narrows to a single
    dataset's own glob (used by compact/dedup, which do operate one physical dataset at a time)."""
    codes_arg = f"'{dataset}/*.parquet'" if dataset else _codes_glob_arg()
    try:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _done AS "
            f"SELECT DISTINCT chunk_id FROM read_parquet({codes_arg})")
        return con.execute("SELECT count(*) FROM _done").fetchone()[0]
    except Exception as e:
        # First run: no codes dataset exists yet anywhere. Only swallow "no files"; re-raise
        # anything else (bad creds, endpoint, etc.) so config errors aren't masked.
        if "No files found" in str(e) or "does not exist" in str(e):
            con.execute("CREATE OR REPLACE TEMP TABLE _done(chunk_id VARCHAR)")
            return 0
        raise


# ── Quantization + write ──────────────────────────────────────────────────────
def _pack_codes(X):
    """(n,384) unit float32 -> (W:(n,6) uint64 sign-bit code, I8:(n,384) int8 rerank)."""
    import numpy as np
    packed = np.packbits(X > 0, axis=1)                  # (n,48) uint8, big-endian bit order
    W = packed.view(np.uint64).reshape(len(X), 6)        # (n,6) uint64
    I8 = np.clip(np.round(X / I8_SCALE * 127.0), -127, 127).astype(np.int8)
    return W, I8


def _write_codes(con, ids, schemas, yrs, W, I8, label):
    """Append one parquet file of codes per source_schema present in this batch -- the queue is
    one unified thing, but physical storage stays Hive-partitioned by source_schema (same layout
    as ref.vectorized_chunks itself: one table, partitioned, not one table per source). Returns
    the set of source_schema values actually written this call, for compaction bookkeeping."""
    import numpy as np
    import pyarrow as pa
    schemas_arr = np.asarray(schemas)
    touched = set()
    for schema in sorted(set(schemas)):
        idx = np.where(schemas_arr == schema)[0]
        rerank = pa.FixedSizeListArray.from_arrays(
            pa.array(I8[idx].reshape(-1), type=pa.int8()), 384)
        tbl = pa.table({
            "chunk_id": pa.array([ids[i] for i in idx], pa.string()),
            "year": pa.array(np.asarray([yrs[i] for i in idx], dtype=np.int32)),
            "w0": pa.array(np.ascontiguousarray(W[idx, 0])),
            "w1": pa.array(np.ascontiguousarray(W[idx, 1])),
            "w2": pa.array(np.ascontiguousarray(W[idx, 2])),
            "w3": pa.array(np.ascontiguousarray(W[idx, 3])),
            "w4": pa.array(np.ascontiguousarray(W[idx, 4])),
            "w5": pa.array(np.ascontiguousarray(W[idx, 5])),
            "rerank_i8": rerank,
        })
        con.register("_codes_out", tbl)
        out = f"{codes_dataset_for(schema)}/codes-{label}-{schema}.parquet"
        con.execute(f"COPY _codes_out TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
        con.unregister("_codes_out")
        print(f"[{label}] flushed {len(idx)} {schema} codes -> {out}", flush=True)
        touched.add(schema)
    return touched


def _embed_and_write(con, todo, label, max_seconds=None):
    """Embed every (chunk_id, source_schema, yr, chunk_text) in `todo` -- ALREADY in ascending
    chunk_id order, the one queue's scan order -- quantize, and write codes, flushing every
    FLUSH_ROWS so memory stays flat and each flush is durable. Stops early if max_seconds
    elapses; the remainder is picked up next run from wherever this one actually got to.
    Returns (rows actually embedded, chunk_id of the last one, set of schemas touched)."""
    import numpy as np
    total = len(todo)
    if total == 0:
        return 0, None, set()
    emb = make_embedder()
    ids, schemas, yrs, Ws, I8s = [], [], [], [], []
    buffered = 0
    flush_idx = 0
    done = 0
    t0 = time.time()
    touched = set()

    def _flush():
        nonlocal ids, schemas, yrs, Ws, I8s, buffered, flush_idx
        if not ids:
            return
        touched.update(_write_codes(con, ids, schemas, yrs, np.concatenate(Ws),
                                     np.concatenate(I8s), f"{label}-{flush_idx:04d}"))
        ids, schemas, yrs, Ws, I8s = [], [], [], [], []
        buffered = 0
        flush_idx += 1

    while done < total:
        batch = todo[done:done + BATCH]
        X = np.asarray(emb.embed([r[3] for r in batch]), dtype=np.float32)
        W, I8 = _pack_codes(X)
        ids.extend(r[0] for r in batch)
        schemas.extend(r[1] for r in batch)
        yrs.extend(int(r[2]) for r in batch)
        Ws.append(W)
        I8s.append(I8)
        buffered += len(batch)
        done += len(batch)
        el = time.time() - t0
        print(f"[{label}] coded {done}/{total} ({done/max(el,1e-6):.0f}/sec, {el:.0f}s)",
              flush=True)
        if buffered >= FLUSH_ROWS:
            _flush()
        if max_seconds is not None and el >= max_seconds:
            print(f"[{label}] time budget {max_seconds}s reached — stopping at "
                  f"{done}/{total}; remainder next run", flush=True)
            break
    _flush()
    print(f"[{label}] complete: coded {done} chunks this run", flush=True)
    last_chunk_id = todo[done - 1][0] if done > 0 else None
    return done, last_chunk_id, touched


def _run_label(prefix):
    return f"{prefix}-{time.strftime('%Y%m%d-%H%M%S')}-{os.getpid()}"


# ── Compaction ────────────────────────────────────────────────────────────────
def _rclone_path(s3path):
    """s3://bucket/key -> <remote>:bucket/key for rclone."""
    return f"{RCLONE_REMOTE}:{s3path[len('s3://'):]}"


def _list_code_files(dataset):
    """Basenames of the *.parquet files currently in the codes dataset (via rclone)."""
    r = subprocess.run(["rclone", "lsf", _rclone_path(dataset) + "/"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        return []
    return [f.strip() for f in r.stdout.splitlines() if f.strip().endswith(".parquet")]


def cmd_compact(con=None, dataset=None, force=False):
    """Merge the codes dataset's many small files into one, once it exceeds COMPACT_MIN_FILES
    (or unconditionally when force=True). The consolidated file is written to a .compact/
    staging key (outside the flat *.parquet glob), moved into place, then the sources are
    deleted — so the query glob never sees a half-written file. The merge keeps one row per
    chunk_id (arbitrary pick among duplicates -- they're re-encodings of the same chunk_text,
    so any is as good as any other): overlapping backlog runs can each code the same
    not-yet-`_done`-visible chunk before either commits, and without this the duplicates would
    only ever be merged together, never removed, growing every compaction."""
    if dataset is None:
        raise ValueError("cmd_compact requires an explicit dataset (which source_schema's codes)")
    own = con is None
    con = con or connect_lake()
    try:
        files = _list_code_files(dataset)
        if not force and len(files) <= COMPACT_MIN_FILES:
            print(f"[compact] {len(files)} files <= threshold {COMPACT_MIN_FILES} — nothing to do",
                  flush=True)
            return
        if not files:
            print("[compact] no files to compact", flush=True)
            return
        tag = time.strftime("%Y%m%d-%H%M%S") + f"-{os.getpid()}"
        staged = f"{dataset}/.compact/codes-compact-{tag}.parquet"
        final = f"{dataset}/codes-compact-{tag}.parquet"
        print(f"[compact] merging {len(files)} files (deduped by chunk_id) ...", flush=True)
        t = time.time()
        con.execute(
            f"COPY (SELECT * EXCLUDE (_rn) FROM "
            f"(SELECT *, row_number() OVER (PARTITION BY chunk_id) AS _rn "
            f"FROM read_parquet('{dataset}/*.parquet')) WHERE _rn = 1) "
            f"TO '{staged}' (FORMAT parquet, COMPRESSION zstd)")
        subprocess.run(["rclone", "moveto", _rclone_path(staged), _rclone_path(final)],
                       capture_output=True, text=True, check=True)
        base = _rclone_path(dataset)
        for f in files:
            subprocess.run(["rclone", "deletefile", f"{base}/{f}"], capture_output=True, text=True)
        subprocess.run(["rclone", "purge", _rclone_path(f"{dataset}/.compact")],
                       capture_output=True, text=True)
        print(f"[compact] done in {time.time()-t:.0f}s — {len(files)} files -> 1", flush=True)
    finally:
        if own:
            con.close()


# ── Commands ──────────────────────────────────────────────────────────────────
def cmd_backlog(max_rows, max_seconds):
    """PRIMARY job: one queue over the WHOLE ref.vectorized_chunks table's un-coded delta
    (every source_schema together, ascending chunk_id order), time-boxed and resumable via the
    one chunk_id watermark -- if time runs out, the rest picks up next run from wherever this
    one actually got to."""
    con = connect_lake()
    coded = _load_done(con)

    watermark = _load_watermark()
    watermark_clause = ""
    if watermark:
        watermark_clause = f"AND s.chunk_id > '{watermark.replace(chr(39), chr(39)*2)}'\n          "
        print(f"[backlog] {coded} chunks already coded; resuming after {watermark} ...",
              flush=True)
    else:
        print(f"[backlog] {coded} chunks already coded; "
              f"no watermark yet — starting from the beginning of the queue ...", flush=True)
    t = time.time()
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _todo AS
        SELECT s.chunk_id, s.source_schema,
               CASE WHEN s.source_schema = 'sec' THEN EXTRACT(year FROM s.filing_date)
                    ELSE 0 END AS yr,
               s.chunk_text
        FROM iceberg_scan('{ICEBERG_CHUNKS}', allow_moved_paths=true) s
        LEFT JOIN _done d ON d.chunk_id = s.chunk_id
        WHERE d.chunk_id IS NULL
          {watermark_clause}AND s.chunk_text IS NOT NULL AND length(s.chunk_text) > 10
        ORDER BY s.chunk_id
        LIMIT {int(max_rows)}
    """)
    todo = con.execute("SELECT chunk_id, source_schema, yr, chunk_text FROM _todo").fetchall()
    total = len(todo)
    tail = " (cap hit — more remain for next run)" if total >= max_rows else " (drains the backlog)"
    print(f"[backlog] taking {total} un-coded chunks{tail} — scan {time.time()-t:.1f}s",
          flush=True)
    if total == 0:
        print("[backlog] nothing to do — fully caught up", flush=True)
        con.close()
        return
    done, last_chunk_id, touched = _embed_and_write(con, todo, _run_label("backlog"),
                                                     max_seconds=max_seconds)
    if last_chunk_id is not None:
        _save_watermark(last_chunk_id)
    for schema in touched:
        cmd_compact(con, dataset=codes_dataset_for(schema))  # no-op below the threshold
    con.close()


def cmd_dedup(source_schema):
    """One-time/on-demand forced compaction+dedup of a source's codes dataset, regardless of
    current file count -- for when duplicates already ended up merged inside <= COMPACT_MIN_FILES
    files by a prior un-deduped compact, so the normal threshold-gated path would report
    'nothing to do' despite duplicates still being present."""
    dataset = codes_dataset_for(source_schema)
    cmd_compact(dataset=dataset, force=True)


def cmd_year(year):
    """Manual/targeted escape hatch: code a single SEC year's delta directly, bypassing the
    backlog queue and its watermark entirely."""
    con = connect_lake()
    _load_done(con)
    print(f"[year {year}] scanning Iceberg for the un-coded delta ...", flush=True)
    t = time.time()
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _todo AS
        SELECT s.chunk_id, 'sec' AS source_schema,
               EXTRACT(year FROM s.filing_date) AS yr, s.chunk_text
        FROM iceberg_scan('{ICEBERG_CHUNKS}') s
        LEFT JOIN _done d ON d.chunk_id = s.chunk_id
        WHERE d.chunk_id IS NULL AND s.source_schema = 'sec'
          AND EXTRACT(year FROM s.filing_date) = {int(year)}
          AND s.chunk_text IS NOT NULL AND length(s.chunk_text) > 10
        ORDER BY s.accession_number DESC
    """)
    todo = con.execute("SELECT chunk_id, source_schema, yr, chunk_text FROM _todo").fetchall()
    print(f"[year {year}] {len(todo)} un-coded chunks — scan {time.time()-t:.1f}s", flush=True)
    if not todo:
        print(f"[year {year}] nothing to do", flush=True)
        con.close()
        return
    _embed_and_write(con, todo, _run_label(f"year{year}"))
    con.close()


def cmd_stats():
    """Per-(source_schema, year) counts across every source's codes dataset."""
    con = connect_lake()
    try:
        rows = con.execute(
            f"SELECT source_schema, year, count(*) AS codes FROM "
            f"read_parquet({_codes_glob_arg()}, hive_partitioning=1) "
            f"GROUP BY source_schema, year ORDER BY source_schema, year"
        ).fetchall()
    except duckdb.Exception:
        print("(no codes dataset yet)")
        con.close()
        return
    total = 0
    for schema, y, c in rows:
        print(f"  source_schema={schema}  year={y}  codes={c}")
        total += c
    print(f"  TOTAL codes={total}")
    con.close()


def _default_max_seconds():
    """Time budget for a backlog run: 20h on weekends (drain faster when there's slack),
    2h on weekdays. VSS_MAX_SECONDS overrides both."""
    env = os.environ.get("VSS_MAX_SECONDS")
    if env:
        return int(env)
    import datetime
    return 72000 if datetime.datetime.now().weekday() >= 5 else 7200


def main():
    ap = argparse.ArgumentParser(description="Local CPU delta-driven quantized-code producer")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_bk = sub.add_parser("backlog")
    p_bk.add_argument("--max-rows", type=int,
                      default=int(os.environ.get("VSS_MAX_ROWS", "1000000")))
    p_bk.add_argument("--max-seconds", type=int, default=_default_max_seconds())
    p_year = sub.add_parser("year")
    p_year.add_argument("--year", type=int, required=True)
    sub.add_parser("stats")
    p_compact = sub.add_parser("compact")
    p_compact.add_argument("--source-schema", default="sec",
                           help="which codes dataset to compact (default: sec)")
    p_dedup = sub.add_parser("dedup")
    p_dedup.add_argument("--source-schema", default="sec",
                         help="which codes dataset to force-dedup (default: sec)")
    args = ap.parse_args()

    if args.cmd == "backlog":
        cmd_backlog(args.max_rows, args.max_seconds)
    elif args.cmd == "year":
        cmd_year(args.year)
    elif args.cmd == "stats":
        cmd_stats()
    elif args.cmd == "compact":
        cmd_compact(dataset=codes_dataset_for(args.source_schema))
    elif args.cmd == "dedup":
        cmd_dedup(args.source_schema)
    else:
        ap.error("unknown command")


if __name__ == "__main__":
    main()
