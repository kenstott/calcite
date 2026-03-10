# Raw Cache Bundle Design

## Problem

ETL fetches millions of small responses (API JSON, document files) and caches them locally for incremental re-runs. Persisting these to S3 for audit/recreation is desirable but per-file PUTs are prohibitively expensive (~$14/run for 2.88M files). We need a bundling strategy that amortizes PUT cost while preserving random-access reads.

## Architecture

Three-tier cache hierarchy:

```
Tier 1: Local filesystem  (<operatingDir>/cache/raw/...)     — hot, used during ETL
Tier 2: S3 bundle          (s3://bucket/.raw/bundles/...)     — cold archive, byte-range accessible
Tier 3: Origin API/URL      (census.gov, SEC EDGAR, etc.)     — re-fetch if tiers 1+2 miss
```

## Cache Key Abstraction

Both ETL modes produce cache entries identified by a **source key** — the dimensions that identify the origin request. The bundle system operates on `(sourceKey, bytes)` tuples — agnostic to the ETL mode or target table.

### Table-based ETL (HttpSource)

Source key = sorted dimension variables as hive-style path:
```
year=2023/state=06/response.json
type=regional_income/year=2020/tablename=CAGDP2/response.json
```

### Document-based ETL (DocumentSource)

Source key = entity-oriented path:
```
0000070502/000007050224000001/form4.xml
0000070502/000007050224000002/R1.htm
```

The bundle scope is the **ETL session** (one bundle per session). The source key is always derived from what was fetched, never from what target it feeds.

## Bundle Format

A bundle is a flat binary concatenation of cache entries with a sidecar index.

### Data file: `{bundleId}.bin`

Raw concatenation of cache entry bytes. No framing, no headers, no compression (responses are already small). Entries are appended in write order.

```
[entry-0 bytes][entry-1 bytes][entry-2 bytes]...
```

### Index file: `{bundleId}.idx.jsonl`

One JSON line per entry, written in append order:

```jsonl
{"key":"year=2023/state=06/response.json","offset":0,"length":4821,"ts":1710000000}
{"key":"year=2023/state=08/response.json","offset":4821,"length":3102,"ts":1710000001}
{"key":"year=2016/state=47/tl_2016_47_tract.zip","storage":"object","length":98123456,"ts":1710000005}
```

Fields:
- `key` — source key (relative path identifying the origin request)
- `offset` — byte offset in the `.bin` file (bundled entries only)
- `length` — byte length of entry
- `ts` — epoch seconds when entry was cached
- `storage` — `"object"` for individually stored large files (omitted for bundled entries)

### S3 layout

```
s3://bucket/.raw/bundles/
  census/                          # schema name
    run-20260310T1423.bin          # bundle data (entire ETL session)
    run-20260310T1423.idx.jsonl    # bundle index
  sec/                             # schema name
    run-20260310T1600.bin
    run-20260310T1600.idx.jsonl
```

Bundle ID = `run-{ISO timestamp of ETL start}`. One bundle per ETL session. The source key inside the index identifies the origin request (e.g., `year=2023/state=06/response.json` for API calls, `0000070502/000007050224000001/form4.xml` for document downloads).

## Write Path

Writing happens in two phases: **accumulate locally** during ETL, then **archive to S3** at ETL completion.

### Phase 1: Local accumulation (during ETL)

No change to current behavior. HttpSource and DocumentSource write cache entries to local filesystem under `<operatingDir>/cache/raw/<sourceKey>`. This is the tier-1 cache.

### Phase 2: Archive to S3 (post-ETL, per schema)

After the schema's ETL completes (success or partial success):

```
BundleArchiver.archive(localCacheDir, storageProvider, bundlePath, sizeThreshold)
```

1. Scan `<operatingDir>/cache/raw/` recursively for all files
2. Classify each file by size against a threshold (default 1MB):
   - **Small files** (API responses, small documents) → bundled
   - **Large files** (shapefiles, bulk downloads) → individual S3 PUTs
3. For small files:
   - Append bytes to data buffer, record `(key, offset, length, timestamp)` in index
   - Write `{bundleId}.bin` to S3 via single PUT
   - Write `{bundleId}.idx.jsonl` to S3 via single PUT
4. For large files:
   - PUT individually to `s3://bucket/.raw/objects/<schema>/<sourceKey>`
   - Record in the same index with `"storage":"object"` (no offset/length)
5. Log: `Archived {n} bundled entries ({size}MB) + {m} individual objects to {bundlePath}`

The size threshold is a pragmatic split: millions of 1KB API responses benefit from bundling; hundreds of 100MB+ shapefiles don't — they're already few enough that per-file PUTs are cheap and individual storage makes them directly accessible.

**Streaming for large bundles:** If the bundled portion exceeds ~256MB, use S3 multipart upload. The index is still built in memory since it's small.

**Error handling:** If archive fails, it's not fatal. Local cache remains. Next run will re-archive. No state to clean up.

### Integration point

In `SchemaLifecycleProcessor`, after all tables have been processed:

```java
if (rawCacheEnabled) {
  BundleArchiver.archive(
    localCacheDir,                 // <operatingDir>/cache/raw
    sourceStorageProvider,         // S3 storage provider
    schemaName,                    // for S3 path prefix
    runTimestamp                   // for bundle ID
  );
}
```

This is the same for both table-based and document-based ETL — the archiver simply bundles whatever files are in the local cache directory, regardless of how they got there.

## Read Path

When a cache entry is needed (HttpSource.fetch or DocumentSource.downloadDocument):

```
CacheResolver.resolve(sourceKey, localCacheDir, storageProvider, bundlePath)
  → String localPath  (or null if not found anywhere)
```

### Lookup order

```
1. Check local: <localCacheDir>/<sourceKey>
   → exists? return path

2. Check index (cached in memory after first load):
   a. Lookup sourceKey → BundleEntry
   b. If bundled (has offset): byte-range GET from .bin file, write to local path
   c. If individual object (storage=object): GET from s3://bucket/.raw/objects/<schema>/<sourceKey>, write to local path
   → return local path (promoted to tier 1)

3. Return null (caller falls through to origin fetch)
```

### Index caching

Bundle indexes are small (a few MB for millions of entries). On first access:

1. List `s3://bucket/.raw/bundles/<schema>/` for all `.idx.jsonl` files
2. Parse and merge into a single in-memory `Map<String, BundleEntry>` (key → bundle file + offset + length)
3. Cache this map for the duration of the ETL run (or schema lifecycle)

If multiple bundles exist for a schema (from multiple ETL runs), later bundles take precedence (newer data). The merged index naturally handles this since we process bundles in timestamp order.

### BundleEntry

```java
class BundleEntry {
  final String bundleFile;   // e.g., "run-20260310T1423.bin" (null for individual objects)
  final long offset;         // byte offset in bundle (-1 for individual objects)
  final long length;
  final long timestamp;
  final boolean individualObject;  // true = stored as separate S3 object

  boolean isBundled() { return !individualObject; }
}
```

### Byte-range extraction

```java
// S3 native byte-range GET
String bundlePath = "bundles/" + schemaName + "/" + entry.bundleFile;
byte[] content = storageProvider.readRange(bundlePath, entry.offset, entry.length);

// Write to local cache (tier 1 promotion)
String localPath = localCacheDir + "/" + sourceKey;
Files.createDirectories(Paths.get(localPath).getParent());
Files.write(Paths.get(localPath), content);
```

**Note:** `StorageProvider.readRange(path, offset, length)` is a new method needed on the storage provider interface. For S3, this maps to `GetObjectRequest.withRange()`. For local filesystem, this maps to `RandomAccessFile.seek() + read()`.

## Cost Analysis

For a census schema run with 2.88M API responses at ~1KB average:

| Approach | S3 PUTs | PUT Cost | S3 GETs (full re-read) | GET Cost |
|----------|---------|----------|----------------------|----------|
| Per-file | 2,880,000 | $14.40 | 2,880,000 | $1.15 |
| Bundled (one per schema) | 2 (1 data + 1 index) | $0.00001 | 1 index + N range GETs | ~$0.001 + per-entry |

Storage cost is identical (~2.8GB at $0.067/month).

## LRU Eviction Interaction

The existing local LRU eviction (`evictLocalCacheIfNeeded`) continues to operate on the local tier-1 cache. Evicted local files can be recovered from S3 bundles on next access. This makes eviction safe — it's a cache eviction, not data loss.

## Classes

```
BundleArchiver          — Write path: scans local cache, produces bundle + index, uploads to S3
BundleIndex             — Parsed in-memory index: Map<sourceKey, BundleEntry>
BundleIndexLoader       — Loads and merges .idx.jsonl files from S3 into BundleIndex
CacheResolver           — Read path: tier-1 local → tier-2 bundle → null (caller goes to tier-3)
BundleEntry             — Value object: bundleFile, offset, length, timestamp
```

## Non-goals

- **Compression**: Not worth the complexity for ~1KB JSON responses. Revisit if document-based ETL produces large cached files.
- **Bundle compaction**: Multiple bundles accumulate over runs. No compaction — the index merge handles it. If storage bloat becomes an issue, a separate cleanup job can remove old bundles.
- **Crash recovery**: If ETL crashes mid-run, no bundle is written. Local cache may be partial. Next run re-fetches as needed. No WAL, no partial bundle cleanup.
- **Encryption**: Relies on S3 server-side encryption (SSE-S3 or SSE-KMS) configured at the bucket level.
