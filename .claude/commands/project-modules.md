---
description: What each module owns, inter-module dependency rules, and where new functionality belongs
---

# Project Module Guide

Reference when deciding where code belongs for $ARGUMENTS.

## Module Ownership

| Module | Owns | Depends On |
|--------|------|-----------|
| `core` | SQL parser, planner, RelNode, RexNode, adapters framework, JDBC adapter, Enumerable | `linq4j` |
| `linq4j` | LINQ implementation, Enumerator/Enumerable, Expression trees | (none) |
| `testkit` | Test fixtures (SqlParserFixture, RelOptFixture, etc.), DiffRepository | `core` |
| `babel` | Extended SQL parser (cross-dialect compatibility) | `core` |
| `server` | JDBC server, Avatica integration | `core` |
| `file` | File adapter: CSV, Parquet, JSON, DuckDB engine, Iceberg, materialization, statistics | `core` |
| `splunk` | Splunk adapter: REST API, SPL query pushdown | `core` |
| `sharepoint-list` | SharePoint adapter: list CRUD, DDL executor | `core` |
| `govdata` | Government data: SEC, Census, Crime, Econ, Geo, Weather, FEC, ETL pipelines | `core`, `file` |
| `arrow` | Apache Arrow integration, columnar data exchange | `core` |

## Where Does New Code Go?

```
New functionality?
├─ New SQL operator/function?
│  └─ core/src/.../sql/fun/  (SqlStdOperatorTable)
├─ New planner rule (general)?
│  └─ core/src/.../rel/rules/  (add constant to CoreRules.java)
├─ New planner rule (adapter-specific)?
│  └─ <adapter>/src/.../rules/
├─ New data source adapter?
│  └─ New top-level module + SchemaFactory
├─ New file format (CSV, JSON, Parquet variant)?
│  └─ file/src/.../format/
├─ New table type?
│  └─ file/src/.../table/
├─ New government data source?
│  └─ govdata/src/.../adapter/govdata/<source>/
├─ New test fixture?
│  └─ testkit/src/main/.../test/
├─ DuckDB-specific optimization?
│  └─ file/src/.../duckdb/
├─ Iceberg-specific code?
│  └─ file/src/.../iceberg/
└─ Build/CI/deployment scripts?
    └─ Root or govdata/scripts/
```

## Inter-Module Dependency Rules

1. **No circular dependencies**: `core` never depends on adapter modules
2. **Adapters depend on `core`**: Always `implementation(project(":core"))`
3. **`file` is special**: `govdata` depends on `file` for Parquet/Iceberg infrastructure
4. **`testkit` is test-only**: Other modules use `testImplementation(project(":testkit"))`
5. **`linq4j` is foundational**: `core` depends on `linq4j`, nothing else should directly

## Key Source Directories per Module

### core
```
src/main/java/org/apache/calcite/
├── plan/           # RelOptPlanner, RelOptRule, VolcanoPlanner, HepPlanner
├── rel/            # RelNode hierarchy, rules/
├── rex/            # RexNode, RexBuilder, RexSimplify
├── sql/            # SqlNode, SqlParser, SqlValidator, operators
├── adapter/        # Built-in adapters (enumerable/, jdbc/)
├── runtime/        # Runtime functions, CalciteException
└── util/           # Utilities, CalciteTrace
```

### file
```
src/main/java/org/apache/calcite/adapter/file/
├── table/          # Table implementations (Parquet, CSV, JSON, Glob)
├── rules/          # Pushdown rules (filter, join, HLL, count)
├── duckdb/         # DuckDB convention, rules, schema
├── iceberg/        # Iceberg tables, metadata, time travel
├── format/         # File format readers (csv/, json/, parquet/)
├── etl/            # ETL pipeline, materialization writer
├── statistics/     # Table statistics, HLL sketches
└── metadata/       # pg_catalog, information_schema
```

### govdata
```
src/main/java/org/apache/calcite/adapter/govdata/
├── sec/            # SEC EDGAR (filings, CIK, XBRL)
├── census/         # Census/ACS data
├── crime/          # FBI/BJS crime statistics
├── econ/           # BLS, FRED, BEA, Treasury
├── geo/            # TIGER, HUD geographic data
├── weather/        # NWS, NOAA, EPA
├── fec/            # FEC campaign finance
├── ref/            # GLEIF, OpenFIGI reference data
└── etl/            # EtlRunner, EtlPipeline
```
