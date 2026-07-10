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

Commands:
  backlog [--max-rows N --max-seconds S]
                        PRIMARY daily job. Code the un-coded delta across ALL years
                        (newest filings first), capped at --max-rows and time-boxed
                        to --max-seconds (~2h). Run daily; the backlog drains over
                        successive runs. CPU only — no GPU.
  year --year N         Code a single year's delta (manual/targeted).
  stats                 Per-year counts in the codes dataset.
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
ICEBERG_CHUNKS = f"{PARQUET_BUCKET}/sec/vectorized_chunks"
# The delivered semantic-search artifact: an append-only parquet dataset of quantized
# codes (one file per producer run). Overridable so tests can target a scratch prefix.
CODES_DATASET = os.environ.get("VSS_CODES_DATASET", f"{PARQUET_BUCKET}/sec/vectorized_chunk_codes")
RCLONE_REMOTE = os.environ.get("GOVDATA_RCLONE_REMOTE", "minio")
# Consolidate the codes dataset once it exceeds this many files (incremental flush + daily runs
# accumulate small files; too many slows the query glob + the delta anti-join).
COMPACT_MIN_FILES = int(os.environ.get("VSS_COMPACT_MIN_FILES", "16"))
# int8 = round(clip(x, -I8_SCALE, I8_SCALE) / I8_SCALE * 127). arctic vectors are unit
# norm so components sit well inside +/-0.35; a fixed global scale keeps int8 dot
# products comparable across rows for rerank.
I8_SCALE = float(os.environ.get("VSS_I8_SCALE", "0.35"))
MEM_LIMIT = os.environ.get("VSS_MEM_LIMIT", "4GB")
TEMP_DIR = os.environ.get("VSS_TEMP_DIR", "/home/adminwsl/tmp_duck")


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


def _load_done(con):
    """Temp table `_done` of already-coded chunk_ids; empty if the dataset is new."""
    try:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _done AS "
            f"SELECT DISTINCT chunk_id FROM read_parquet('{CODES_DATASET}/*.parquet')")
        return con.execute("SELECT count(*) FROM _done").fetchone()[0]
    except Exception as e:
        # First run: the dataset doesn't exist yet. Only swallow "no files"; re-raise
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


def _write_codes(con, ids, yrs, W, I8, label):
    """Append one parquet file of codes for this run to the lake dataset."""
    import numpy as np
    import pyarrow as pa
    rerank = pa.FixedSizeListArray.from_arrays(pa.array(I8.reshape(-1), type=pa.int8()), 384)
    tbl = pa.table({
        "chunk_id": pa.array(ids, pa.string()),
        "year": pa.array(np.asarray(yrs, dtype=np.int32)),
        "w0": pa.array(np.ascontiguousarray(W[:, 0])),
        "w1": pa.array(np.ascontiguousarray(W[:, 1])),
        "w2": pa.array(np.ascontiguousarray(W[:, 2])),
        "w3": pa.array(np.ascontiguousarray(W[:, 3])),
        "w4": pa.array(np.ascontiguousarray(W[:, 4])),
        "w5": pa.array(np.ascontiguousarray(W[:, 5])),
        "rerank_i8": rerank,
    })
    con.register("_codes_out", tbl)
    out = f"{CODES_DATASET}/codes-{label}.parquet"
    con.execute(f"COPY _codes_out TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
    con.unregister("_codes_out")
    return out


def _embed_and_write(con, todo, label, max_seconds=None):
    """Embed every (chunk_id, yr, chunk_text) in `todo`, quantize, and APPEND codes to the lake
    dataset — flushing a parquet file every FLUSH_ROWS so memory stays flat and each flush is
    durable (a crash/time-box costs only the un-flushed tail, not the whole run). Stops early if
    max_seconds elapses; the remainder is picked up next run."""
    import numpy as np
    total = len(todo)
    if total == 0:
        return 0
    emb = make_embedder()
    ids, yrs, Ws, I8s = [], [], [], []
    buffered = 0
    flush_idx = 0
    done = 0
    t0 = time.time()

    def _flush():
        nonlocal ids, yrs, Ws, I8s, buffered, flush_idx
        if not ids:
            return
        out = _write_codes(con, ids, yrs, np.concatenate(Ws), np.concatenate(I8s),
                           f"{label}-{flush_idx:04d}")
        print(f"[{label}] flushed {len(ids)} codes -> {out}", flush=True)
        ids, yrs, Ws, I8s = [], [], [], []
        buffered = 0
        flush_idx += 1

    while done < total:
        batch = todo[done:done + BATCH]
        X = np.asarray(emb.embed([r[2] for r in batch]), dtype=np.float32)
        W, I8 = _pack_codes(X)
        ids.extend(r[0] for r in batch)
        yrs.extend(int(r[1]) for r in batch)
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
    return done


def _run_label(prefix):
    return f"{prefix}-{time.strftime('%Y%m%d-%H%M%S')}-{os.getpid()}"


# ── Compaction ────────────────────────────────────────────────────────────────
def _rclone_path(s3path):
    """s3://bucket/key -> <remote>:bucket/key for rclone."""
    return f"{RCLONE_REMOTE}:{s3path[len('s3://'):]}"


def _list_code_files():
    """Basenames of the *.parquet files currently in the codes dataset (via rclone)."""
    r = subprocess.run(["rclone", "lsf", _rclone_path(CODES_DATASET) + "/"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        return []
    return [f.strip() for f in r.stdout.splitlines() if f.strip().endswith(".parquet")]


def cmd_compact(con=None):
    """Merge the codes dataset's many small files into one, once it exceeds COMPACT_MIN_FILES.
    The consolidated file is written to a .compact/ staging key (outside the flat *.parquet glob),
    moved into place, then the sources are deleted — so the query glob never sees a half-written
    file. The brief overlap duplicates some chunk_ids, which is benign for search (redundant, not
    missing) and for the delta anti-join (still 'coded')."""
    own = con is None
    con = con or connect_lake()
    try:
        files = _list_code_files()
        if len(files) <= COMPACT_MIN_FILES:
            print(f"[compact] {len(files)} files <= threshold {COMPACT_MIN_FILES} — nothing to do",
                  flush=True)
            return
        tag = time.strftime("%Y%m%d-%H%M%S") + f"-{os.getpid()}"
        staged = f"{CODES_DATASET}/.compact/codes-compact-{tag}.parquet"
        final = f"{CODES_DATASET}/codes-compact-{tag}.parquet"
        print(f"[compact] merging {len(files)} files ...", flush=True)
        t = time.time()
        con.execute(f"COPY (SELECT * FROM read_parquet('{CODES_DATASET}/*.parquet')) "
                    f"TO '{staged}' (FORMAT parquet, COMPRESSION zstd)")
        subprocess.run(["rclone", "moveto", _rclone_path(staged), _rclone_path(final)],
                       capture_output=True, text=True, check=True)
        base = _rclone_path(CODES_DATASET)
        for f in files:
            subprocess.run(["rclone", "deletefile", f"{base}/{f}"], capture_output=True, text=True)
        subprocess.run(["rclone", "purge", _rclone_path(f"{CODES_DATASET}/.compact")],
                       capture_output=True, text=True)
        print(f"[compact] done in {time.time()-t:.0f}s — {len(files)} files -> 1", flush=True)
    finally:
        if own:
            con.close()


# ── Commands ──────────────────────────────────────────────────────────────────
def cmd_backlog(max_rows, max_seconds):
    con = connect_lake()
    coded = _load_done(con)
    print(f"[backlog] {coded} chunks already coded; scanning Iceberg for the un-coded delta ...",
          flush=True)
    t = time.time()
    # newest-first via (year, accession_number); accession encodes YY-seq, so recently
    # filed / most-queried chunks lead and the historical backlog drains behind them.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _todo AS
        SELECT s.chunk_id, s.year AS yr, s.chunk_text
        FROM iceberg_scan('{ICEBERG_CHUNKS}') s
        LEFT JOIN _done d ON d.chunk_id = s.chunk_id
        WHERE d.chunk_id IS NULL
          AND s.chunk_text IS NOT NULL AND length(s.chunk_text) > 10
        ORDER BY s.year DESC, s.accession_number DESC
        LIMIT {int(max_rows)}
    """)
    todo = con.execute("SELECT chunk_id, yr, chunk_text FROM _todo").fetchall()
    total = len(todo)
    capped = total >= max_rows
    tail = " (cap hit — more remain for next run)" if capped else " (drains the backlog)"
    print(f"[backlog] taking {total} newest un-coded chunks{tail} — scan {time.time()-t:.1f}s",
          flush=True)
    if total == 0:
        print("[backlog] nothing to do — fully caught up", flush=True)
        con.close()
        return
    _embed_and_write(con, todo, _run_label("backlog"), max_seconds=max_seconds)
    cmd_compact(con)   # consolidate small files at end of run (no-op below the threshold)
    con.close()


def cmd_year(year):
    con = connect_lake()
    _load_done(con)
    print(f"[year {year}] scanning Iceberg for the un-coded delta ...", flush=True)
    t = time.time()
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _todo AS
        SELECT s.chunk_id, s.year AS yr, s.chunk_text
        FROM iceberg_scan('{ICEBERG_CHUNKS}') s
        LEFT JOIN _done d ON d.chunk_id = s.chunk_id
        WHERE d.chunk_id IS NULL AND s.year = {int(year)}
          AND s.chunk_text IS NOT NULL AND length(s.chunk_text) > 10
        ORDER BY s.accession_number DESC
    """)
    todo = con.execute("SELECT chunk_id, yr, chunk_text FROM _todo").fetchall()
    print(f"[year {year}] {len(todo)} un-coded chunks — scan {time.time()-t:.1f}s", flush=True)
    if not todo:
        print(f"[year {year}] nothing to do", flush=True)
        con.close()
        return
    _embed_and_write(con, todo, _run_label(f"year{year}"))
    con.close()


def cmd_stats():
    con = connect_lake()
    try:
        rows = con.execute(
            f"SELECT year, count(*) AS codes FROM read_parquet('{CODES_DATASET}/*.parquet') "
            "GROUP BY year ORDER BY year").fetchall()
    except duckdb.Exception:
        print("(no codes dataset yet)")
        con.close()
        return
    total = 0
    for y, c in rows:
        print(f"  year={y}  codes={c}")
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
    sub.add_parser("compact")
    args = ap.parse_args()

    if args.cmd == "backlog":
        cmd_backlog(args.max_rows, args.max_seconds)
    elif args.cmd == "year":
        cmd_year(args.year)
    elif args.cmd == "stats":
        cmd_stats()
    elif args.cmd == "compact":
        cmd_compact()
    else:
        ap.error("unknown command")


if __name__ == "__main__":
    main()
