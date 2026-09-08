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
vss-dq.py — data-quality checks for the semantic-search codes.

Deliberately NOT part of run-all-dq.sh. That suite validates row content of declared govdata
tables; the codes are not a declared table, they are a derived artifact with its own failure
modes, and every check below exists because that failure actually happened or came close:

  orphans      Codes whose chunk_id no longer exists in vc_staging. Nothing in the pipeline
               retires these: a chunk_id that stops being produced keeps its codes forever, they
               keep being searched, and they resolve to no source row. Found live at 2.96M
               orphaned SEC codes carrying a chunk_id scheme the chunker had stopped emitting.
  coverage     Coded vs staged per schema. Drift means delta detection is wrong in one direction
               (re-embedding what is done) or the other (never embedding what is not).
  duplicates   One chunk_id, several codes. Compaction's dedup is what prevents this; overlapping
               backlog runs are how it breaks.
  centroids    Every centroid is -1 or a real partition, and NEVER null. A null centroid does not
               error -- it falls out of the probe's `centroid = -1` arm and silently removes those
               codes from the searchable corpus.
  shapes       One read spanning the flat tail and the partitioned set succeeds. Three separate
               bugs in one day were variations of this failing.
  resolvable   What fraction of chunk_ids parse into source coordinates. semantic_search hands
               these back for the caller to read the source row; ids in an older scheme name no
               table and resolve to nothing.
  recall       Retrieval quality against exhaustive cosine, on a sample. The only check here that
               catches search getting WORSE rather than breaking -- a too-narrow prefilter returns
               well-formed, confident, wrong answers indefinitely.

Usage:
  vss-dq.py [--skip orphans,recall] [--recall-queries N] [--recall-corpus N]

Requires VSS_MEM_LIMIT and the .env.prod S3/PG environment, same as vss-local.py.
Exit status is 1 if any check fails, so it can gate a pipeline.
"""

import argparse
import importlib.util
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load_vss_local():
    """Reuse vss-local.py's own lake/PG helpers rather than restating them.

    Restating the codes-reading logic is precisely how this check would drift away from what the
    producer actually does and stop testing it -- the two shapes and their column differences are
    where every bug so far has lived, so the DQ must exercise the same code path."""
    spec = importlib.util.spec_from_file_location(
        "vss_local", os.path.join(_HERE, "vss-local.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


vss = _load_vss_local()

FAILURES = []
WARNINGS = []


def _fail(check, message):
    FAILURES.append(check)
    print(f"  FAIL  {check}: {message}", flush=True)


def _warn(check, message):
    WARNINGS.append(check)
    print(f"  WARN  {check}: {message}", flush=True)


def _ok(check, message):
    print(f"  ok    {check}: {message}", flush=True)


def check_shapes(con):
    """One read across every populated shape must succeed and see rows."""
    rel = vss._codes_relation(con)
    if rel is None:
        _fail("shapes", "no codes exist anywhere")
        return None
    try:
        n = con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]
    except Exception as e:
        _fail("shapes", f"a read spanning the flat tail and the partitioned set failed: {e}")
        return None
    _ok("shapes", f"{n} codes readable across all shapes")
    return rel


def check_duplicates(con, rel):
    total, distinct = con.execute(
        f"SELECT count(*), count(DISTINCT chunk_id) FROM {rel}").fetchone()
    if total != distinct:
        _fail("duplicates", f"{total - distinct} duplicate chunk_id(s) among {total} codes -- "
                            f"compaction dedup is not holding")
    else:
        _ok("duplicates", f"no duplicate chunk_id among {total} codes")


def check_centroids(con, rel):
    nulls, lo, hi = con.execute(
        f"SELECT count(*) FILTER (WHERE centroid IS NULL), min(centroid), max(centroid) "
        f"FROM {rel}").fetchone()
    if nulls:
        _fail("centroids", f"{nulls} code(s) with a NULL centroid -- these are invisible to the "
                           f"probe and silently absent from search")
        return
    k = None
    try:
        k = con.execute(
            f"SELECT count(*) FROM read_parquet('{vss.CENTROIDS_PATH}')").fetchone()[0]
    except Exception:
        k = None
    if k is None:
        if hi is not None and hi >= 0:
            _fail("centroids", f"codes are assigned (up to {hi}) but no quantizer exists at "
                               f"{vss.CENTROIDS_PATH}")
        else:
            _ok("centroids", "no quantizer trained yet; all codes unassigned (-1), as expected")
        return
    if lo < -1 or (hi is not None and hi >= k):
        _fail("centroids", f"centroid range [{lo}, {hi}] outside [-1, {k}) for the trained "
                           f"quantizer")
    else:
        _ok("centroids", f"centroid range [{lo}, {hi}] valid against {k} centroids")


def check_resolvable(con, rel):
    """chunk_id must parse into <source_schema>:<source_table>:<fk>:<sequence>, which is what
    semantic_search hands back for the caller to read the source row."""
    total, resolvable = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE regexp_matches(chunk_id, "
        f"'^[^:]+:[^:]+:.*:[0-9]+$')) FROM {rel}").fetchone()
    pct = 100.0 * resolvable / total if total else 0.0
    if resolvable < total:
        _warn("resolvable", f"{total - resolvable} of {total} chunk_ids ({100.0 - pct:.1f}%) do "
                            f"not parse into source coordinates and resolve to no table")
    else:
        _ok("resolvable", f"all {total} chunk_ids parse into source coordinates")


def check_coverage(con, rel):
    """Coded vs staged per schema."""
    vss.attach_pg(con)
    # Aggregated INSIDE Postgres. Selecting the column through the attached database instead
    # makes DuckDB pull all 39M source_schema values across the wire to group them locally --
    # minutes of transfer for ten rows of answer.
    staged = dict(con.execute(
        f"""SELECT * FROM postgres_query('pg', $$
                SELECT source_schema, count(*) FROM "{vss.PG_NAMESPACE}".vc_staging
                GROUP BY source_schema
            $$)""").fetchall())
    coded = dict(con.execute(
        f"SELECT source_schema, count(*) FROM ("
        f"  SELECT regexp_extract(chunk_id, '^([^:]+):', 1) AS source_schema FROM {rel}"
        f") GROUP BY 1").fetchall())
    over = []
    for schema, n in sorted(coded.items()):
        if not schema:
            continue
        want = staged.get(schema, 0)
        if n > want:
            over.append(f"{schema}: {n} coded > {want} staged")
    if over:
        _fail("coverage", "more codes than staged chunks -- " + "; ".join(over))
    else:
        total_coded = sum(coded.values())
        total_staged = sum(staged.values())
        pct = 100.0 * total_coded / total_staged if total_staged else 0.0
        _ok("coverage", f"{total_coded} coded of {total_staged} staged ({pct:.1f}%); "
                        f"no schema over-coded")


def check_orphans(con, rel):
    """Codes whose chunk_id is no longer in vc_staging.

    Pulls staging's chunk_id column and anti-joins in DuckDB. That is the expensive check here --
    the column is the whole table's worth of ids -- but there is no cheaper correct form: the
    codes side cannot be probed row-by-row without a chunk_id index in Postgres, which was
    deliberately dropped when the resume key moved to updated_at."""
    vss.attach_pg(con)
    t = time.time()
    con.execute(
        f'CREATE OR REPLACE TEMP TABLE _staged AS '
        f'SELECT chunk_id FROM pg."{vss.PG_NAMESPACE}".vc_staging')
    orphans = con.execute(
        f"SELECT count(*) FROM {rel} c "
        f"WHERE NOT EXISTS (SELECT 1 FROM _staged s WHERE s.chunk_id = c.chunk_id)").fetchone()[0]
    if orphans:
        _fail("orphans", f"{orphans} code(s) reference a chunk_id absent from vc_staging -- these "
                         f"are searched but resolve to no source row, and nothing retires them")
    else:
        _ok("orphans", f"no orphaned codes ({time.time()-t:.0f}s)")


def check_recall(con, rel, queries, corpus):
    """Retrieval quality of the configured pipeline against exhaustive cosine.

    Queries are corpus vectors pushed away by noise, so they are NOT corpus points -- searching
    with an exact stored vector measures nothing, because its own Hamming distance is zero and it
    survives any prefilter. A real text query sits near a topic, not on a chunk."""
    import numpy as np
    sample = con.execute(
        f"SELECT chunk_id, rerank_i8 FROM {rel} USING SAMPLE {int(corpus)} ROWS "
        f"(reservoir, 20260908)").arrow()
    ids = sample.column("chunk_id").to_pylist()
    X = np.stack(sample.column("rerank_i8").to_numpy(zero_copy_only=False)).astype(np.float32)
    if len(X) < queries * 10:
        _warn("recall", f"only {len(X)} codes sampled; too few to measure recall meaningfully")
        return
    norms = np.linalg.norm(X, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    Xn = X / norms

    rng = np.random.default_rng(20260908)
    qi = rng.choice(len(Xn), size=queries, replace=False)
    # Noise sized to land at a stated similarity, NOT a fixed magnitude. These are unit vectors,
    # so a component averages 1/sqrt(dim) ~ 0.05; noise at any "reasonable-looking" absolute scale
    # swamps the signal and measures the recall of a random query, which is near zero by
    # construction and looks exactly like a broken index. For target cosine t on a unit vector,
    # the noise vector needs norm sqrt(1/t^2 - 1), spread over dim components.
    target_cos = float(os.environ.get("VSS_DQ_QUERY_COSINE", "0.75"))
    per_component = np.sqrt(1.0 / (target_cos * target_cos) - 1.0) / np.sqrt(Xn.shape[1])
    noise = rng.normal(scale=per_component, size=(queries, Xn.shape[1])).astype(np.float32)
    Q = Xn[qi] + noise
    Q /= np.linalg.norm(Q, axis=1, keepdims=True)
    achieved = float(np.mean(np.sum(Q * Xn[qi], axis=1)))
    if not 0.6 <= achieved <= 0.9:
        _warn("recall", f"query perturbation landed at cosine {achieved:.2f}, outside the 0.6-0.9 "
                        f"band this check assumes -- the recall figure below is not comparable")

    prefilter = int(os.environ.get("VSS_DQ_PREFILTER", "0")) or max(
        1, len(Xn) // 64)
    hits = 0
    for row in range(queries):
        sims = Xn @ Q[row]
        truth = set(np.argsort(-sims)[:10].tolist())
        # Stage 1 as the engine runs it: Hamming over sign bits, then cosine rerank.
        hd = np.count_nonzero((Xn > 0) != (Q[row] > 0), axis=1)
        cand = np.argsort(hd)[:prefilter]
        got = set(cand[np.argsort(-(Xn[cand] @ Q[row]))[:10]].tolist())
        hits += len(truth & got)
    recall = 100.0 * hits / (queries * 10.0)
    msg = (f"recall@10 {recall:.0f}% over {queries} out-of-corpus queries at cosine "
           f"{achieved:.2f} (prefilter {prefilter} of {len(Xn)} sampled)")
    # Thresholds are set to catch a REGRESSION from the measured operating point, not to express
    # an aspiration. The 1/64 width ratio measures ~80% recall here and independently on a 3.19M
    # corpus, so a bar above that would warn on every healthy run and be ignored within a week.
    # Retune alongside calcite.vss.fraction, not separately.
    fail_below = float(os.environ.get("VSS_DQ_RECALL_FAIL", "65"))
    warn_below = float(os.environ.get("VSS_DQ_RECALL_WARN", "75"))
    if recall < fail_below:
        _fail("recall", msg + " -- search is returning well-formed but wrong neighbours")
    elif recall < warn_below:
        _warn("recall", msg + f" -- below the {warn_below:.0f}% expected at this width ratio")
    else:
        _ok("recall", msg)


def main():
    ap = argparse.ArgumentParser(description="Data-quality checks for the semantic-search codes")
    ap.add_argument("--skip", default="",
                    help="comma-separated checks to skip (orphans and recall are the slow ones)")
    ap.add_argument("--recall-queries", type=int, default=10)
    ap.add_argument("--recall-corpus", type=int, default=50000)
    args = ap.parse_args()
    skip = {s.strip() for s in args.skip.split(",") if s.strip()}

    con = vss.connect_lake()
    try:
        print("[vss-dq] codes checks", flush=True)
        rel = check_shapes(con)
        if rel is None:
            print("[vss-dq] FAILED: nothing to check", flush=True)
            return 1
        for name, fn in (("duplicates", lambda: check_duplicates(con, rel)),
                         ("centroids", lambda: check_centroids(con, rel)),
                         ("resolvable", lambda: check_resolvable(con, rel)),
                         ("coverage", lambda: check_coverage(con, rel)),
                         ("orphans", lambda: check_orphans(con, rel)),
                         ("recall", lambda: check_recall(con, rel, args.recall_queries,
                                                         args.recall_corpus))):
            if name in skip:
                print(f"  skip  {name}", flush=True)
                continue
            fn()
    finally:
        con.close()

    print(f"[vss-dq] {len(FAILURES)} failure(s), {len(WARNINGS)} warning(s)", flush=True)
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
