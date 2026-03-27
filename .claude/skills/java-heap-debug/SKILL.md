---
description: JVM tuning for this project, heap dumps, common memory patterns in Calcite metadata caches
model: sonnet
effort: medium
---

# JVM Heap Debugging Guide

Reference when diagnosing memory issues for $ARGUMENTS.

## JVM Settings in This Project

### Gradle daemon
`gradle.properties`: `-XX:+UseG1GC -Xmx2g -XX:MaxMetaspaceSize=512m`

### Test JVM
Build sets `-Xmx1536m` for standard tests, `-Xmx6g` for large suites.

### ETL Workers (parallel runners)
Configured per-worker in `govdata/scripts/parallel/common.sh` (`get_heap_config()`):

| Worker(s) | Heap Min/Max | Reason |
|-----------|-------------|--------|
| 20 (GEO) | 4g / 6g | TIGER shapefiles, geometry parsing |
| 41 (REF) | 3g / 4g | GLEIF golden copy ~450MB |
| 60 (FEC) | 4g / 5g | 3M+ rows/year bulk downloads |
| 21 (Crime) | 3g / 4g | Large dimension expansion |
| Others | 2g / 3g | Standard heap |

Override with env vars: `ETL_HEAP_MIN`, `ETL_HEAP_MAX`

## Taking a Heap Dump

```bash
# On OOM (add to JVM args)
-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/calcite-heap.hprof

# Manual dump of running process
jmap -dump:format=b,file=/tmp/calcite-heap.hprof <PID>

# Analyze with jhat (quick) or Eclipse MAT (thorough)
jhat /tmp/calcite-heap.hprof  # Opens web UI on port 7000
```

## Common Memory Patterns in Calcite

### 1. Metadata Handler Cache
`JaninoRelMetadataProvider` compiles and caches metadata handlers.

```
Symptom: Metaspace OOM or slow first-query performance
Control: -Dcalcite.metadata.handler.cache.maximum.size=1000
Fix: Reduce cache size or increase MaxMetaspaceSize
```

### 2. VolcanoPlanner Search Space
Large queries create many RelNode alternatives.

```
Symptom: Heap OOM during optimization with large join counts
Indicators: mapDigestToRel grows unbounded
Fix: Limit join reordering rules, use HepPlanner for preprocessing
```

### 3. Parquet Metadata Caching
`ParquetTranslatableTable` caches statistics and HLL sketches.

```
Symptom: Heap grows with number of Parquet files scanned
Location: file/src/.../table/ParquetTranslatableTable.java
Fix: Reduce cache scope, use WeakReferences
```

### 4. S3/Tracker State
ETL workers preload full tracker state into memory (~2.5GB for SEC).

```
Symptom: OOM during worker startup "Preloaded tracker state"
Fix: Increase -Xmx (set via get_heap_config in common.sh)
```

## Useful JVM Diagnostic Flags

```bash
# GC logging (diagnose long pauses)
-Xlog:gc*:file=/tmp/gc.log:time,uptime:filecount=5,filesize=10m

# Java 8 compatible GC logging
-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:/tmp/gc.log

# Native memory tracking (for off-heap issues)
-XX:NativeMemoryTracking=summary
# Then: jcmd <PID> VM.native_memory summary

# Print class histogram (find what's using heap)
jmap -histo <PID> | head -30
```

## Quick Diagnosis Checklist

1. [ ] Check `-Xmx` setting for the affected process
2. [ ] Enable `-XX:+HeapDumpOnOutOfMemoryError` to capture next OOM
3. [ ] Run `jmap -histo <PID>` to find largest object types
4. [ ] Check `calcite.metadata.handler.cache.maximum.size`
5. [ ] For ETL workers: verify heap config in `get_heap_config()` matches workload
6. [ ] For planners: check if query has too many joins (exponential search space)
