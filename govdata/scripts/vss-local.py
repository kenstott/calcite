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

Embeds un-coded chunks from Postgres's vc_staging table on CPU
(snowflake-arctic-embed-xs), quantizes each 384-d unit vector to:
  * a 48-byte BINARY code  (sign bits, packed into 6x uint64)  — Hamming prefilter
  * a 384-byte INT8 vector (scalar-quantized)                  — rerank
and appends them as a parquet file to the `vectorized_chunk_codes` dataset in the
lake. That dataset is the delivered semantic-search artifact (Path B): a Java
intercept materializes it into a persistent local DuckDB and runs a two-stage
`bit_count` Hamming prefilter + int8 rerank (fast on repeat via DuckDB persistence).

No HNSW, no DuckDB HNSW cache -- vectors live only as quantized codes in the lake.
The chunks being embedded live in Postgres, not Iceberg: vc_staging is the sole
durable copy of organized-but-not-yet-embedded chunk text (see
org.apache.calcite.adapter.govdata.ref.ChunkOrganizer's class javadoc for why there
is deliberately no Iceberg copy of it), and the "what's un-coded" delta is a
DuckDB-side anti-join between a Postgres-attached vc_staging and the lake's own
codes parquet files.

This script only ever embeds already-organized chunks -- it never builds chunk_text
itself. Organizing text into chunk rows (row-concat mode's naive per-row chunker,
document-blob mode's SemanticTextChunker) is always ChunkOrganizer's job (run via
x-schema.sh), writing into vc_staging -- every registered source, SEC's own
mda_sections/earnings_transcripts text included, goes through that one sweep.

Commands:
  backlog [--max-rows N --max-seconds S]
                        PRIMARY job. vc_staging holds chunks from every registered source
                        (sec, ref, fedregister, cyber_threat, ...) together -- this is ONE
                        queue over the whole table's un-coded delta, not a per-schema job.
                        Capped at --max-rows and time-boxed to --max-seconds (~2h); the
                        backlog drains over successive runs, resuming via the one watermark
                        (see WATERMARK_DIR below). CPU only — no GPU.
  stats                 Per-(source_schema, year) counts across every codes dataset.
"""

import argparse
import os
import re
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
RCLONE_REMOTE = os.environ.get("GOVDATA_RCLONE_REMOTE", "minio")

# vc_staging lives in a Postgres schema derived from the bucket name -- same derivation as
# PGPipelineTracker.sanitizeNamespace (Java) and ChunkOrganizer's own main(): strip the
# scheme, lowercase, collapse non-alphanumerics to underscores, trim leading/trailing ones.
# Kept in sync by hand (no shared config between the JVM and this script's own process).
def _pg_namespace(bucket_uri):
    s = re.sub(r"^[a-zA-Z0-9+.-]+://", "", bucket_uri)
    s = re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    return s or None


PG_NAMESPACE = _pg_namespace(PARQUET_BUCKET)
PG_URL = os.environ.get("CALCITE_TRACKER_PG_URL") or os.environ.get("GOVDATA_TRACKER_PG_URL")
PG_USER = os.environ.get("CALCITE_TRACKER_PG_USER") or os.environ.get("GOVDATA_TRACKER_PG_USER")
PG_PASSWORD = (os.environ.get("CALCITE_TRACKER_PG_PASSWORD")
               or os.environ.get("GOVDATA_TRACKER_PG_PASSWORD"))
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

# ONE watermark for the ONE queue: vc_staging is a single table holding chunks from every
# source, so there is exactly one backlog and exactly one resume position, not one per
# source_schema. The position is the (updated_at, chunk_id) of the last chunk this process got
# through, and the queue walks vc_staging in that same order.
#
# updated_at leads, NOT chunk_id, and that ordering is the whole point. chunk_id begins with the
# source_schema name, so ordering by it makes the queue walk the schemas alphabetically and stop
# at the last one; a chunk staged afterwards for any earlier-sorting schema sorts BELOW the
# watermark and can never be selected again, because the watermark filter is applied before the
# `_done` anti-join can vouch for it. updated_at is monotonic in staging-write order, so newly
# staged and re-chunked rows always land above the watermark no matter which source they belong
# to. chunk_id breaks ties, since one insert batch stamps many rows with the same updated_at and
# a bare `updated_at >` would skip the remainder of whichever timestamp a run stopped inside.
#
# The `_done` anti-join stays as a safety net on top of this: a crash between writing a codes
# file and saving the watermark just gets re-scanned and found already-coded next run, never
# double-embedded. No watermark file (first run) means start of the queue.
WATERMARK_DIR = os.environ.get("VSS_WATERMARK_DIR", os.path.join(GOVDATA_HOME, ".vss-watermarks"))
WATERMARK_PATH = os.path.join(WATERMARK_DIR, "staged_watermark")


def _load_watermark():
    """(updated_at, chunk_id) to resume after, or None to start from the beginning of the queue.

    Deliberately a different filename from the chunk_id-only watermark this replaces: a stale
    file from that scheme carries no updated_at and must not be silently reinterpreted as one.
    Its absence just means "start from the beginning", which the `_done` anti-join makes cheap.
    """
    try:
        with open(WATERMARK_PATH) as f:
            raw = f.read().strip()
    except FileNotFoundError:
        return None
    if not raw:
        return None
    updated_at, _, chunk_id = raw.partition("\t")
    if not chunk_id:
        return None
    return int(updated_at), chunk_id


def _save_watermark(updated_at, chunk_id):
    os.makedirs(WATERMARK_DIR, exist_ok=True)
    tmp = WATERMARK_PATH + ".tmp"
    with open(tmp, "w") as f:
        f.write(f"{int(updated_at)}\t{chunk_id}")
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
# vc_staging -- keep in sync with SemanticSearch.java's own DEFAULT_SOURCE_SCHEMAS, which reads
# the same codes for search. A source_schema present there but missing here just means its
# un-coded chunks keep getting found (harmless, re-embedded into its own new partition the moment
# this list catches up); NOT keeping this in sync with ChunkOrganizer's own registries would mean
# a real embedding never gets picked up as "done" (its codes exist but this list can't see them),
# which is the failure mode worth avoiding.
CODES_SOURCE_SCHEMAS = ("sec", "ref", "fedregister", "cyber_threat", "transport", "patents",
                         "disasters", "geo", "officials", "health")


# IVF centroids: the coarse quantizer search probes before it looks at any codes. Kept as ONE
# artifact for the whole corpus, not one per source_schema -- a query is answered from the whole
# queue, so partitioning the centroid space by source would make every search probe every source.
CENTROIDS_PATH = os.environ.get(
    "VSS_CENTROIDS_PATH", f"{PARQUET_BUCKET}/ref/vss_centroids/centroids.parquet")
IVF_K = int(os.environ.get("VSS_IVF_K", "4096"))
IVF_TRAIN_SAMPLE = int(os.environ.get("VSS_IVF_TRAIN_SAMPLE", "1000000"))
IVF_TRAIN_ITERS = int(os.environ.get("VSS_IVF_TRAIN_ITERS", "20"))


def _existing_codes_globs(con):
    """The codes globs that actually have files behind them.

    read_parquet() fails the ENTIRE call if any single glob in its bracketed list matches nothing,
    so one schema that has never been embedded -- or whose codes were purged for a rebuild -- takes
    every other schema's codes down with it. glob() returns no rows rather than raising, so the
    candidates are probed first and only populated prefixes are read. Deduplicated because
    VSS_CODES_DATASET (tests) maps every schema onto one path, which would otherwise read it once
    per schema and count every row ten times over."""
    globs = []
    for schema in CODES_SOURCE_SCHEMAS:
        pattern = f"{codes_dataset_for(schema)}/*.parquet"
        if pattern in globs:
            continue
        if con.execute("SELECT count(*) FROM glob(?)", [pattern]).fetchone()[0]:
            globs.append(pattern)
    return globs


def _codes_glob_arg(con):
    """Bracketed list of every populated codes dataset, for one read_parquet() call spanning all
    of them -- NOT a bare '*' at the bucket root, which would make DuckDB list every prefix in the
    entire multi-schema bucket (confirmed live: a multi-minute paginated LIST across unrelated
    schemas' data, not just the ~4 relevant ones). None when nothing is coded anywhere yet."""
    globs = _existing_codes_globs(con)
    if not globs:
        return None
    return "[" + ", ".join(f"'{g}'" for g in globs) + "]"


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


def attach_pg(con):
    """Attaches vc_staging's Postgres database onto an existing DuckDB connection (DuckDB's
    postgres scanner, not a separate Python driver -- keeps this script's dependencies exactly
    what vss-embed-setup.sh already installs). Queried as pg.<namespace>.vc_staging afterward."""
    if not PG_URL:
        raise SystemExit(
            "CALCITE_TRACKER_PG_URL not set -- vc_staging's connection info is required for "
            "the backlog command")
    if not PG_NAMESPACE:
        raise SystemExit(f"cannot derive a PG namespace from GOVDATA_PARQUET_DIR={PARQUET_BUCKET!r}")
    # jdbc:postgresql://host:port/db -> host, port, db
    hostport, _, db = PG_URL[len("jdbc:postgresql://"):].partition("/")
    host, _, port = hostport.partition(":")
    port = port or "5432"
    con.execute("INSTALL postgres; LOAD postgres")
    dsn = f"host={host} port={port} dbname={db}"
    if PG_USER:
        dsn += f" user={PG_USER}"
    if PG_PASSWORD:
        dsn += f" password={PG_PASSWORD}"
    con.execute(f"ATTACH '{dsn}' AS pg (TYPE postgres, READ_ONLY)")


def _load_done_for_batch(con):
    """Temp table `_done`: which of `_batch`'s chunk_ids already have codes, across EVERY
    source's codes dataset (one queue needs one combined view of what's already coded, not a
    per-schema one).

    Scoped to the batch on purpose. A DISTINCT over every coded chunk_id in the lake answers the
    same question but costs a full pass over the whole codes dataset, and materialises every id
    in it, on every run -- work proportional to the corpus for a batch-sized decision. Restricting
    to the ids actually in hand keeps only the chunk_id column in play (a few percent of the
    dataset's bytes) and lets the semi-join discard the rest as it streams.
    """
    codes_arg = _codes_glob_arg(con)
    if codes_arg is None:
        # Genuinely nothing coded anywhere yet (first run, or every dataset purged for a rebuild).
        # Distinct from a read failure, which must NOT land here: silently treating a broken read
        # as "nothing is coded" re-embeds the entire corpus.
        con.execute("CREATE OR REPLACE TEMP TABLE _done(chunk_id VARCHAR)")
        return 0
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _done AS "
        f"SELECT DISTINCT c.chunk_id FROM read_parquet({codes_arg}) c "
        f"WHERE c.chunk_id IN (SELECT chunk_id FROM _batch)")
    return con.execute("SELECT count(*) FROM _done").fetchone()[0]


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
    """Embed every (chunk_id, source_schema, yr, chunk_text, updated_at) in `todo` -- ALREADY in
    ascending (updated_at, chunk_id) order, the one queue's scan order -- quantize, and write
    codes, flushing every FLUSH_ROWS so memory stays flat and each flush is durable. Stops early
    if max_seconds elapses; the remainder is picked up next run from wherever this one actually
    got to. Returns (rows actually embedded, (updated_at, chunk_id) of the last one, set of
    schemas touched)."""
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
    last_pos = (todo[done - 1][4], todo[done - 1][0]) if done > 0 else None
    return done, last_pos, touched


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
    """Merge the codes dataset's accumulated small files, once there are more than
    COMPACT_MIN_FILES of them (or unconditionally when force=True). The consolidated file is
    written to a .compact/ staging key (outside the flat *.parquet glob), moved into place, then
    the merged sources are deleted — so the query glob never sees a half-written file.

    Only the files a backlog run produced are read and rewritten; anything already carrying the
    codes-compact- prefix is left in place and merely consulted. Compaction therefore costs what
    has arrived since the last one, not a rewrite of the entire dataset each time the file count
    trips the threshold -- which is the same work whether one new file or fifteen triggered it,
    and grows with the corpus rather than with the change.

    The merge keeps one row per chunk_id (arbitrary pick among duplicates -- they're re-encodings
    of the same chunk_text, so any is as good as any other): overlapping backlog runs can each
    code the same not-yet-`_done`-visible chunk before either commits. Rows already present in a
    file being left in place are dropped here for the same reason, since that file is not itself
    being rewritten to remove them.

    force=True merges everything, compacted files included -- what cmd_dedup needs for duplicates
    that already ended up inside one.
    """
    if dataset is None:
        raise ValueError("cmd_compact requires an explicit dataset (which source_schema's codes)")
    own = con is None
    con = con or connect_lake()
    try:
        files = _list_code_files(dataset)
        compacted = [f for f in files if f.startswith("codes-compact-")]
        fresh = [f for f in files if not f.startswith("codes-compact-")]
        merging = files if force else fresh
        if not force and len(fresh) <= COMPACT_MIN_FILES:
            print(f"[compact] {len(fresh)} new files <= threshold {COMPACT_MIN_FILES} — "
                  f"nothing to do", flush=True)
            return
        if not merging:
            print("[compact] no files to compact", flush=True)
            return
        tag = time.strftime("%Y%m%d-%H%M%S") + f"-{os.getpid()}"
        staged = f"{dataset}/.compact/codes-compact-{tag}.parquet"
        final = f"{dataset}/codes-compact-{tag}.parquet"
        src = "[" + ", ".join(f"'{dataset}/{f}'" for f in merging) + "]"
        keep = ""
        if not force and compacted:
            prior = "[" + ", ".join(f"'{dataset}/{f}'" for f in compacted) + "]"
            keep = f" AND chunk_id NOT IN (SELECT chunk_id FROM read_parquet({prior}))"
        print(f"[compact] merging {len(merging)} files (deduped by chunk_id), "
              f"{len(files) - len(merging)} left in place ...", flush=True)
        t = time.time()
        con.execute(
            f"COPY (SELECT * EXCLUDE (_rn) FROM "
            f"(SELECT *, row_number() OVER (PARTITION BY chunk_id) AS _rn "
            f"FROM read_parquet({src})) WHERE _rn = 1{keep}) "
            f"TO '{staged}' (FORMAT parquet, COMPRESSION zstd)")
        subprocess.run(["rclone", "moveto", _rclone_path(staged), _rclone_path(final)],
                       capture_output=True, text=True, check=True)
        base = _rclone_path(dataset)
        for f in merging:
            subprocess.run(["rclone", "deletefile", f"{base}/{f}"], capture_output=True, text=True)
        subprocess.run(["rclone", "purge", _rclone_path(f"{dataset}/.compact")],
                       capture_output=True, text=True)
        print(f"[compact] done in {time.time()-t:.0f}s — {len(merging)} files -> 1", flush=True)
    finally:
        if own:
            con.close()


# ── Commands ──────────────────────────────────────────────────────────────────
def cmd_backlog(max_rows, max_seconds):
    """PRIMARY job: one queue over the WHOLE vc_staging table's un-coded delta (every
    source_schema together, in (updated_at, chunk_id) order), time-boxed and resumable via the
    one watermark -- if time runs out, the rest picks up next run from wherever this one
    actually got to."""
    con = connect_lake()
    attach_pg(con)

    watermark = _load_watermark()
    if watermark:
        print(f"[backlog] resuming after updated_at={watermark[0]} chunk_id={watermark[1]} ...",
              flush=True)
    else:
        print("[backlog] no watermark yet — starting from the beginning of the queue ...",
              flush=True)
    t = time.time()

    # The ORDER BY and LIMIT execute INSIDE Postgres -- postgres_query passes the statement
    # through verbatim -- so idx_vc_staging_resume (scripts/sql/vc_schema.sql) serves them as an
    # index range scan that stops as soon as max_rows rows are found, making the fetch cost
    # proportional to the batch rather than to the table. Selecting through a DuckDB-side join
    # to _done instead leaves the sort and limit ABOVE that join, where all Postgres is asked
    # for is an unselective range filter -- which it answers with a full sequential scan of the
    # whole table however many indexes exist, because most of the table qualifies whenever
    # embedding is running behind chunking.
    pg_where = "chunk_text IS NOT NULL AND length(chunk_text) > 10"
    if watermark:
        pg_where += (" AND (updated_at, chunk_id) > (" + str(int(watermark[0])) + ", '"
                     + watermark[1].replace("'", "''") + "')")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _batch AS
        SELECT * FROM postgres_query('pg', $$
            SELECT chunk_id, source_schema, COALESCE(year, 0) AS yr, chunk_text, updated_at
            FROM "{PG_NAMESPACE}".vc_staging
            WHERE {pg_where}
            ORDER BY updated_at, chunk_id
            LIMIT {int(max_rows)}
        $$)
    """)
    fetched = con.execute("SELECT count(*) FROM _batch").fetchone()[0]
    batch_end = con.execute(
        "SELECT updated_at, chunk_id FROM _batch "
        "ORDER BY updated_at DESC, chunk_id DESC LIMIT 1").fetchone()
    already = _load_done_for_batch(con)

    # _done remains the safety net it has always been -- a crash between writing a codes file
    # and saving the watermark leaves chunks coded but not yet skipped -- so it filters the
    # batch AFTER the limit rather than deciding what the batch is.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _todo AS
        SELECT b.chunk_id, b.source_schema, b.yr, b.chunk_text, b.updated_at
        FROM _batch b LEFT JOIN _done d ON d.chunk_id = b.chunk_id
        WHERE d.chunk_id IS NULL
        ORDER BY b.updated_at, b.chunk_id
    """)
    todo = con.execute(
        "SELECT chunk_id, source_schema, yr, chunk_text, updated_at FROM _todo").fetchall()
    total = len(todo)
    tail = (" (cap hit — more remain for next run)" if fetched >= max_rows
            else " (drains the backlog)")
    print(f"[backlog] fetched {fetched}, {already} already coded, {total} to embed{tail} — "
          f"scan {time.time()-t:.1f}s", flush=True)
    if fetched == 0:
        print("[backlog] nothing to do — fully caught up", flush=True)
        con.close()
        return
    if total == 0:
        # Every row this batch returned was already coded. The watermark still has to clear the
        # batch, or the next run fetches and discards exactly these rows again and never moves.
        _save_watermark(batch_end[0], batch_end[1])
        print(f"[backlog] whole batch already coded — watermark advanced past {batch_end[1]}",
              flush=True)
        con.close()
        return
    done, last_pos, touched = _embed_and_write(con, todo, _run_label("backlog"),
                                               max_seconds=max_seconds)
    # A batch carried to completion clears to its own end, not to the last row embedded: rows the
    # anti-join dropped can sort above that one, and resuming below them would re-fetch the same
    # already-coded rows every run. A run cut short by --max-seconds resumes exactly where it
    # stopped instead.
    if done == total:
        _save_watermark(batch_end[0], batch_end[1])
    elif last_pos is not None:
        _save_watermark(last_pos[0], last_pos[1])
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


def cmd_ivf_train(k=IVF_K, sample_rows=IVF_TRAIN_SAMPLE, iters=IVF_TRAIN_ITERS):
    """Train the IVF coarse quantizer -- k-means over a sample of the coded corpus -- and write
    the centroids to CENTROIDS_PATH.

    Trained on a SAMPLE rather than the whole corpus, because one Lloyd iteration is
    O(rows x k x dim): a full-corpus iteration costs about what a full assignment pass costs, and
    there are `iters` of them. Centroids converge on a sample of a fraction of the size, and the
    corpus is assigned once against the converged result.

    Seeds from random rows rather than k-means++: at this sample size k-means++'s serial seeding
    pass costs more than the extra Lloyd iterations plain seeding needs to catch up.

    Vectors are L2-normalised, so a dot product IS cosine similarity and assignment is one matmul.
    That matters because assignment is the inner loop here and again for every embedded chunk.
    """
    import numpy as np
    import pyarrow as pa
    con = connect_lake()
    try:
        t = time.time()
        codes_arg = _codes_glob_arg(con)
        if codes_arg is None:
            raise SystemExit("no codes exist yet -- run the backlog before training centroids")
        arrow = con.execute(
            f"SELECT rerank_i8 FROM read_parquet({codes_arg}) "
            f"USING SAMPLE {int(sample_rows)} ROWS (reservoir, 20260908)").arrow()
        X = np.stack(arrow.column("rerank_i8").to_numpy(zero_copy_only=False)).astype(np.float32)
        norms = np.linalg.norm(X, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        X /= norms
        print(f"[ivf-train] sampled {len(X)} vectors in {time.time()-t:.1f}s; "
              f"k={k}, iters={iters}", flush=True)
        if len(X) < k:
            raise SystemExit(f"cannot train {k} centroids from {len(X)} sampled vectors")

        rng = np.random.default_rng(20260908)
        C = X[rng.choice(len(X), size=k, replace=False)].copy()
        labels = np.empty(len(X), dtype=np.int32)
        for it in range(iters):
            t0 = time.time()
            # Chunked so the (rows x k) similarity matrix stays bounded rather than scaling with
            # the sample size.
            for lo in range(0, len(X), 50000):
                hi = min(lo + 50000, len(X))
                labels[lo:hi] = np.argmax(X[lo:hi] @ C.T, axis=1)
            counts = np.bincount(labels, minlength=k)
            newC = np.empty_like(C)
            for d in range(C.shape[1]):
                newC[:, d] = np.bincount(labels, weights=X[:, d], minlength=k)
            empty = counts == 0
            newC[~empty] /= counts[~empty, None]
            # A centroid that captured nothing keeps its previous position: zeroing it would make
            # it the nearest neighbour of everything at once on the following pass.
            newC[empty] = C[empty]
            n = np.linalg.norm(newC, axis=1, keepdims=True)
            n[n == 0] = 1.0
            C = newC / n
            shift = float(counts.max()) / max(1, int(counts[counts > 0].min()))
            print(f"[ivf-train] iter {it+1}/{iters} {time.time()-t0:.1f}s — "
                  f"{int(empty.sum())} empty, largest/smallest partition ratio {shift:.0f}x",
                  flush=True)

        tbl = pa.table({
            "cid": pa.array(np.arange(k, dtype=np.int32)),
            "v": pa.FixedSizeListArray.from_arrays(
                pa.array(C.reshape(-1), type=pa.float32()), DIM),
        })
        con.register("_cent", tbl)
        con.execute(f"COPY _cent TO '{CENTROIDS_PATH}' (FORMAT parquet, COMPRESSION zstd)")
        con.unregister("_cent")
        print(f"[ivf-train] wrote {k} centroids -> {CENTROIDS_PATH} "
              f"(total {time.time()-t:.0f}s)", flush=True)
    finally:
        con.close()


def cmd_stats():
    """Per-(source_schema, year) counts across every source's codes dataset."""
    con = connect_lake()
    try:
        codes_arg = _codes_glob_arg(con)
        if codes_arg is None:
            print("(no codes dataset yet)")
            return
        rows = con.execute(
            f"SELECT source_schema, year, count(*) AS codes FROM "
            f"read_parquet({codes_arg}, hive_partitioning=1) "
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
    sub.add_parser("stats")
    p_compact = sub.add_parser("compact")
    p_compact.add_argument("--source-schema", default="sec",
                           help="which codes dataset to compact (default: sec)")
    p_dedup = sub.add_parser("dedup")
    p_dedup.add_argument("--source-schema", default="sec",
                         help="which codes dataset to force-dedup (default: sec)")
    p_ivf = sub.add_parser("ivf-train",
                           help="train the IVF coarse quantizer over a sample of the codes")
    p_ivf.add_argument("--k", type=int, default=IVF_K)
    p_ivf.add_argument("--sample-rows", type=int, default=IVF_TRAIN_SAMPLE)
    p_ivf.add_argument("--iters", type=int, default=IVF_TRAIN_ITERS)
    args = ap.parse_args()

    if args.cmd == "backlog":
        cmd_backlog(args.max_rows, args.max_seconds)
    elif args.cmd == "stats":
        cmd_stats()
    elif args.cmd == "compact":
        cmd_compact(dataset=codes_dataset_for(args.source_schema))
    elif args.cmd == "dedup":
        cmd_dedup(args.source_schema)
    elif args.cmd == "ivf-train":
        cmd_ivf_train(k=args.k, sample_rows=args.sample_rows, iters=args.iters)
    else:
        ap.error("unknown command")


if __name__ == "__main__":
    main()
