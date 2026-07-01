# Design Note: COUNT(DISTINCT) Exactness, Approximation, and Backstops

**Status:** proposed (not implemented) · **Scope:** file-adapter cardinality estimation
**Touches:** `statistics/HyperLogLogSketch.java`, `statistics/HLLSketchCache.java`,
`rules/SimpleHLLCountDistinctRule.java`, `rules/CountStarStatisticsRule.java` · **Related:** C-18,
C-02 (`docs/testing/contradictions.md`), FILE-039/124/152.

> The cap is a **local safety primitive** of this engine (with a heap-safe default + a hard ceiling),
> independent of any overlay. Federated resource governance is a Trino-platform concern, not this leaf.

## Motivation

Two complaints meet here:

1. **`APPROX_COUNT_DISTINCT` is needlessly inaccurate for small cardinalities.** The sketch is a
   plain HyperLogLog (`HyperLogLogSketch`, precision 14). It already does the standard small-range
   correction — LinearCounting below `rawEstimate ≤ 2.5·numBuckets` (~40k at p=14) — but
   LinearCounting is still ~1–2% off and needs an empty bucket. The columns where users most want an
   exact distinct count (keys, enums, low-card categoricals) are exactly the ones HLL handles worst.

2. **Exact `COUNT(DISTINCT)` is a footgun at scale.** Per C-18, an exact `COUNT(DISTINCT)` must return
   an exact value (the HLL rewrite is only legal for `APPROX_COUNT_DISTINCT`). But an exact distinct
   on a high-cardinality column of a trillion-row table will OOM the box. There is no guardrail today.

`COUNT(*)` is out of scope: it is already served exactly and cheaply from a stored `rowCount`
(`CountStarStatisticsRule`).

## Part A — Exact-up-to-K hybrid sketch (the `APPROX` path)

Give `HyperLogLogSketch` a **sparse/exact mode** that holds an exact set of value hashes up to a cap
`K`, and **promotes to dense HLL on overflow** — single pass, no second scan.

- New state: `HashSet<Long> exact` (the 64-bit value hashes; `null` once dense) + `int exactCap (K)`.
- `add(value)`: if sparse, add the hash; when `exact.size() > K`, replay the buffered hashes through
  the existing bucket-update and set `exact = null` (dense). If dense, do the current bucket update.
  (Factor the bucket update out of today's `add()` body so both paths and the promotion loop share it.)
- `getEstimate()`: `precomputedEstimate` if present; else `exact.size()` when still sparse (**EXACT**);
  else the existing LinearCounting/HLL estimate.
- `merge()`: sparse+sparse = set union (promote if over `K`); anything-dense = dense.

**No `HLLSketchCache` API change** — the hybrid lives inside the sketch, which is `Serializable`.
Low-card columns persist a tiny exact set (`8·K` bytes worst case, usually far less); high-card
columns persist the 16 KB dense buckets. `SimpleHLLCountDistinctRule` needs no change — it already
reads `getEstimate()`, which is now exact for any column that never crossed `K`.

This is the same idea as HLL++ sparse mode / DataSketches / Redis HLL "promote on overflow." Pure win
on the approx path: exact when small, approximate when large, bounded memory throughout.

**Default `K`:** ~10k distinct (≈160 KB/column worst case with a primitive set; near zero for the
common low-card column). Configurable via the inference/statistics config.

## Part B — Exact `COUNT(DISTINCT)` backstop (two complementary guards)

The same promote-on-overflow machinery, but here we are protecting the **heap**, and the result must
not silently become approximate (rule #6: no silent fallback). Two independent guards:

### B1. Heap-budget cap (memory-sized, fan-out aware)

The exact distinct aggregation maintains real distinct sets. Cap them by **total operator memory**,
not a round count, because the per-element cost is what threatens the heap and because `GROUP BY`
multiplies the number of sets.

- **Express the cap as a memory budget** (default **256 MB**), and derive the element ceiling from the
  set's per-element cost. Per-element cost of a distinct set of 64-bit hashes:

  | Structure | Bytes/elem | 1 M | 10 M | 100 M | 1 B |
  |---|---|---|---|---|---|
  | `HashSet<Long>` (boxed) | ~74 B | ~74 MB | ~740 MB | ~7.4 GB | **~70 GB** |
  | primitive open-addr `long` set | ~16 B | ~16 MB | ~160 MB | ~1.6 GB | **~16 GB** |

  So **1 B distinct is not a viable in-memory default** — it would try to allocate 16–70 GB before
  the backstop ever fires, i.e. it *causes* the OOM it is meant to prevent. A 256 MB budget ≈ 16 M
  distinct with a primitive set; raise it explicitly if you have the heap.
- **Fan-out:** the budget is for the **whole operator** (all per-group distinct sets combined), not
  per set. When the aggregate budget is hit, the operator stops growing exact state.
- **Use a primitive `long` open-addressing set**, not `HashSet<Long>` — ~5× denser.

### B2. Row-count pre-flight (cheap, catches the trillion-row footgun directly)

Before building any exact state, check the estimated input rows from statistics (the same `rowCount`
`CountStarStatisticsRule` uses). If estimated rows exceed a guardrail (default **1 B rows**), refuse
exact distinct up front — no memory spent, the "dumb thing on a trillion-row table" is caught before
it starts. This is a *row-count* threshold (a number like 1 B is fine here precisely because it costs
nothing — it reads a stat, it does not allocate).

### Behavior at a guard (default vs opt-in)

- **Default — fail fast:** throw a clear error, e.g.
  `exact COUNT(DISTINCT) exceeded <N> distinct values / <input> rows; use APPROX_COUNT_DISTINCT`.
  Protects the box AND points the user at the right tool, without ever returning a wrong number.
- **Opt-in degrade** (`approxFallback=true`): promote the exact sets to HLL and continue, with a loud
  WARN. Now it is a documented choice, not a silent lie. (Ideally surface that the result is
  approximate; at minimum log it.)
- **Spill-to-disk** (future): stay exact, bounded RAM, slower — what DuckDB/Spark do. Then the cap
  becomes a disk/time budget rather than a memory one.

## Config knobs (proposed)

| Knob | Default | Meaning |
|---|---|---|
| `approxDistinct.exactCapMemory` | 256 MB | Part A `K` (approx path) and Part B1 budget, in bytes. |
| `exactDistinct.maxRows` | 1 B rows | Part B2 pre-flight guardrail on estimated input rows. |
| `exactDistinct.onOverflow` | `fail` | `fail` \| `approx` \| `spill` (spill = future). |

All belong in the schema/operand config (per CLAUDE.md GovData rule #7, declared in the model), not
read from `System.getenv` in code.

## Orthogonal perf note

`HyperLogLogSketch.add()` hashes each value with **MD5** (`MessageDigest`). At the scale this note
targets, the hash dominates runtime — MD5 is ~an order of magnitude slower than a fast 64-bit hash.
Switch to xxHash64/Murmur3 independently of the above; it is the single biggest lever on the
backstop's actual cost.

## Summary

- **Approx path:** exact-up-to-`K` then HLL, inside the sketch, no cache/rule changes — exact for the
  common low-card column at near-zero cost.
- **Exact path:** a **memory budget** cap (fan-out aware, primitive set; default 256 MB ≈ 16 M
  distinct — NOT 1 B, which is 16–70 GB) **plus** a cheap **row-count pre-flight** (default 1 B rows).
- At either guard, **fail by default** (no silent approximation); degrade only on explicit opt-in.
- Independently, replace MD5 with a fast hash.
