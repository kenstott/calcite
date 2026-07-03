# pgwire-calcite Server — Requirements & Design Decisions

**Status:** Proposed (planning complete; no implementation yet)
**Origin:** kenstott/calcite issue #251
**Supersedes:** the "new `pgwire-server` Calcite module" framing in #251 — the design **copies the existing provisa pgwire into a new, independent `pgwire-calcite` version** and swaps its backend, rather than building a Calcite-native codec. A shared/extracted library was considered and set aside (see §2, D1).

---

## 1. Objective

Expose Calcite's planner and adapters (file, govdata, salesforce, splunk, graphql, openapi, sharepoint-list, …) over the **PostgreSQL wire protocol**, so that:

- **DuckDB** can `ATTACH … (TYPE postgres)` and query Calcite-backed tables.
- **DBeaver / DataGrip** can connect, browse schemas, and **diagram** (ER relationships) against Calcite data.
- **psql / any libpq or JDBC client** can connect to the same endpoint.

**Why pgwire.** DuckDB has no JDBC/ODBC attach path; Calcite exposes only JDBC (`jdbc:calcite:`) and Avatica, and Avatica ships no ODBC driver. The PostgreSQL wire protocol is the one open surface where a DuckDB-class client and a Calcite backend can meet. (PGW references issue #251.)

---

## 2. Foundational Decisions (with rationale)

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | **Copy provisa's pgwire (`provisa/pgwire`, on vendored buenavista) into a standalone `pgwire-calcite` and create a Calcite-specific version.** Do **not** build a Calcite-native codec, fork camille, or greenfield. A shared/extracted library both projects consume was considered and **set aside** in favor of an independent copy. | provisa's pgwire is production-shaped and client-proven (DBeaver/DataGrip/DuckDB, incl. diagramming) — it implements the expensive 80% (PG-14 handshake, extended protocol, sophisticated `pg_catalog` intercept, COPY, DDL, session commands). A copy keeps the two backends fully decoupled, needs no plug-point abstraction or provisa refactor, and lets each version evolve independently. **Accepted cost:** the hard, client-tested catalog rewriter and protocol quirk-fixes now live in two places — a new DuckDB probe or protocol edge fixed in one must be hand-ported to the other, or they drift. Mitigate by sharing the probe/regression corpus (D-test), not the code. |
| D2 | **Do not route through Trino / the trino-calcite connector.** Execute directly against Calcite. | The connector composition path was explicitly rejected. Direct execution removes an engine hop and lets sqlglot own dialect consistency. |
| D3 | **Execution seam = sqlglot transpile PG→Calcite**, then run against an embedded Calcite JDBC connection. | Replaces provisa's `_pipeline`→Trino route. |
| D4 | **sqlglot owns a single canonical identifier + type normalization that drives BOTH catalog/discovery population AND query transpile.** Implement a bidirectional sqlglot **Calcite dialect**, not a one-shot `write="oracle"`. | Calcite's own conformance/dialect handling is leaky and cannot be trusted to be self-consistent. If discovery is normalized one way and queries another, they drift → clients that discover-then-query (DuckDB/DataGrip) break. Consistency must be guaranteed by construction, in one place we control. This co-location is only possible because discovery and transpile both live in Python. |
| D5 | **Serialization Calcite→Python is Arrow**; the client wire stays pg protocol. | Calcite is row-based, so a transpose happens somewhere; do it once in the JVM (`arrow-jdbc` `JdbcToArrow`, already on the build), stream Arrow IPC to Python (pyarrow, zero-copy), then vectorized-encode to pg. Arrow ends at the pg-encode boundary — you cannot send Arrow to DuckDB. |
| D6 | **Delivery is an OS-agnostic desktop Java app, not Docker.** | Desktop distribution requirement; Docker dropped. |
| D7 | **The Java app supervises a CPython pgwire child + a recyclable Calcite JVM child**, bridged over a local Arrow socket. | CPython is required by two C-extension deps (`duckdb`, `pyarrow`), not by the pgwire logic. Calcite must be a *child*, not in the supervisor's heap, so it can be recycled for memory. |
| D8 | **Airgapped delivery:** predownload standalone CPython + platform wheelhouses for the 3–4 OS/arch variants into the artifact; provision on first run; no network. | Matches existing provisa airgap posture. Accepts ~1 GB artifact. |
| D9 | **Treat Calcite as an unreliable-but-valuable dependency and *contain* it**, rather than trying to fix it. | Calcite's planner memory, metadata caches, and dialect conformance are community-paced and not ours to solve. The value we depend on the community for is the **adapter ecosystem**; the internals are the risk we fence off. |

---

## 3. Architecture

```
DuckDB / DBeaver / DataGrip / psql
        │  PostgreSQL wire protocol (libpq / JDBC), localhost:<port>
        ▼
┌───────────────────────────────────────────────────────────────┐
│  OS-agnostic Java launcher (thin SUPERVISOR — must stay up)    │
│                                                                 │
│   ├─ CPython pgwire child  (recyclable)                         │
│   │    • buenavista codec, extended protocol, COPY, DDL         │
│   │    • pg_catalog/information_schema intercept (in-mem DuckDB)│
│   │    • sqlglot Calcite dialect (discovery + transpile)        │
│   │    • pyarrow result path                                    │
│   │            ▲  Arrow IPC over local socket                   │
│   │            │                                                │
│   └─ Calcite JVM child   (recyclable, own heap)                │
│        • jdbc:calcite: embedded engine (model-configured)      │
│        • arrow-jdbc JdbcToArrow (row→Arrow transpose)           │
└───────────────────────────────────────────────────────────────┘
```

- **One opaque deployable, one local pg port.** Clients see a Postgres endpoint and cannot see the JVM/Python/Arrow inside.
- DuckDB **does not start** the server — it connects to the already-listening local socket. The server is up because the always-on desktop app put it there.

---

## 4. Requirements

### 4.1 Protocol & client compatibility
- **PGW-001** The server MUST speak the PostgreSQL v3 wire protocol and report server version ≥ 14 (clears DuckDB's ≥12 gate).
- **PGW-002** The server MUST support the **extended query protocol** (Parse/Bind/Describe/Execute), including portal suspension for client-driven paging.
- **PGW-003** The server MUST support the **simple query protocol** and multi-statement simple queries.
- **PGW-004** `SET`/`BEGIN`/`COMMIT`/`ROLLBACK`/`DISCARD`/`RESET`/`DEALLOCATE`/`SAVEPOINT`/`RELEASE` MUST be accepted (intercepted, no real transaction isolation).
- **PGW-005** DuckDB MUST be able to `ATTACH 'host=… port=…' (TYPE postgres)` and read tables. Until binary COPY exists (PGW-021), DuckDB attaches with `pg_use_text_protocol`.
- **PGW-006** DBeaver and DataGrip MUST complete their full connect-time introspection and render **ER diagrams** (relationship lines) for adapters that expose keys.
- **PGW-007** Optional TLS (cert/key) and cleartext-password auth (trust / simple provider) MUST be supported, carried over from provisa.

### 4.2 Catalog / discovery (reuse provisa's sophisticated intercept)
- **PGW-010** `information_schema` and `pg_catalog` queries MUST be answered locally by the intercept: materialize a per-request in-memory DuckDB catalog, AST-rewrite Postgres-isms, and execute the client's *actual* SQL (arbitrary JOINs/WHEREs) against it. No naive query-string matching.
- **PGW-011** Catalog OIDs MUST be stable and self-consistent across `pg_class`/`pg_attribute`/`pg_type`/`pg_constraint` (single-source-of-truth index, as in provisa's `CatalogIndex`).
- **PGW-012** The catalog MUST be populated from **Calcite metadata**: schemas/tables/columns, plus keys and referential constraints from `Statistic.getKeys()`/`getReferentialConstraints()`, govdata YAML-declared keys/joins, and the file adapter's `TableConstraints`.
- **PGW-013** The pgwire MUST **invent no constraints**. It faithfully reports what Calcite exposes: real rows where keys exist, well-formed **empty** results where they don't. Constraint-less sources (most live adapters, warehouse-style) are reported as such; clients (DataGrip) add virtual FKs on their side. There is **no** pgwire-side constraint overlay and nothing to merge.
- **PGW-014** Functions irrelevant to the target clients MAY remain stubbed to constants (`pg_get_constraintdef`/`viewdef`/`indexdef`, relation sizes, `*_is_visible`), as in provisa. DDL reverse-engineering is a non-goal (§6). If a specific target client's probe hits a stub, extend that single rewriter branch.

### 4.3 Execution & dialect consistency
- **PGW-015** Non-catalog queries MUST be transpiled PG→Calcite via sqlglot and executed against the embedded Calcite JDBC connection, returning `(rows/columns/types)` for the existing result adapter.
- **PGW-016** A **single canonical normalization** (identifier case, quoting, reserved words; type↔OID mapping) MUST be implemented once in sqlglot and applied to BOTH catalog population (PGW-012) and query transpile (PGW-015), so discovery and execution are consistent **by construction**.
- **PGW-017** The normalization MUST be a **bidirectional sqlglot Calcite dialect**, not ad-hoc regex fixups and not a bare `write="oracle"`. It replaces provisa's `endpoint_dev.py` regex adaptations (LIMIT→FETCH FIRST, subquery-alias unwrap, quoting).
- **PGW-018** PG-only constructs with no clean Calcite mapping (`DISTINCT ON`, `~`/`~*`, array/JSON ops, `generate_series`, `string_agg`→`LISTAGG`) MUST be handled in a targeted transform layer or rejected explicitly — never silently mistranslated.

### 4.4 Serialization, streaming, paging
- **PGW-019** Results MUST cross Calcite→Python as **Arrow** (`JdbcToArrow` → Arrow IPC stream → pyarrow). Arrow is the internal representation; the client wire remains pg protocol.
- **PGW-020** The result path MUST be **Arrow-batch streaming** (batch iterator, backpressure via the socket), NOT a materialized list. Never buffer a whole result at any hop.
- **PGW-021** Server-side `COPY … TO STDOUT` MUST support **binary** format (Arrow batch → pg binary `CopyData`) for DuckDB's default read path. (Text/csv exists today; binary is net-new.)
- **PGW-022** Long-running scans MUST propagate **cancel/close**: a client disconnect or `LIMIT`-few must cancel the Calcite query and release buffers — no leaked running queries.
- **PGW-023** Client-driven paging MUST be supported via portal suspension (`Execute` row-limit) and (Phase 2) DuckDB's `ctid` ranges. Server-internal keyset paging is a fallback only for known-materializing engines with a usable key. Blocking operators (sort/aggregate/join) that materialize in Calcite's Enumerable engine SHOULD be pushed down to a spilling engine (e.g. the file adapter's DuckDB execution engine) rather than paged.
- **PGW-024** The server MUST faithfully expose each adapter's capabilities and MUST NOT paper over bad adapter design. Where an adapter materializes and offers no pushdown/key, the memory cost is surfaced, not hidden — it becomes the end user's problem.

### 4.5 Packaging & delivery
- **PGW-025** Delivery MUST be an **OS-agnostic Java application** (JRE), not a Docker image.
- **PGW-026** The Java app MUST provision a **standalone CPython + locked wheelhouse** on first run (python-build-standalone + uv), reused thereafter. CPython is required by `duckdb` and `pyarrow` (C-extensions); the pgwire protocol code itself is pure Python.
- **PGW-027** Delivery MUST be **fully airgapped**: the CPython runtimes and platform wheelhouses for the 3–4 target OS/arch variants MUST be predownloaded into the artifact; first-run install MUST work with no network.
- **PGW-028** Cross-platform wheel fetching MUST be done at build time (per-target `--platform`/uv), pinned to one CPython version/ABI, with a lockfile for reproducibility.
- **PGW-029** All autoload/autofetch MUST be disabled and fail loud: DuckDB `autoinstall_known_extensions`/`autoload_known_extensions` off with any needed extensions bundled; no pip/JVM network fallback. The offline install MUST be verified in CI **including rarely-used code paths**.
- **PGW-030** Preferred form is **per-OS installers** (~150 MB each, own variant only) over a universal ~600 MB–1 GB artifact. Total ~1 GB is accepted; it is the airgap tax (the rarely-used tail must be resident because it cannot be fetched later).
- **PGW-031** macOS bundled native libs MUST be codesigned/notarized; Windows installers signed (Gatekeeper/SmartScreen). Linux wheels MUST match the target glibc baseline (manylinux tag; musllinux if Alpine is targeted).

### 4.6 Process model, lifecycle & reliability
- **PGW-032** The deployable is **long-running / always-on**; cold start is a one-time launch cost and is not a design concern. Warm reuse (kept-warm Calcite connection, compiled-plan cache, connection pooling, JIT) SHOULD be exploited.
- **PGW-033** Topology MUST be a **thin supervisor + recyclable Calcite JVM child + recyclable CPython pgwire child**. Calcite MUST NOT run in the supervisor's process (so it can be recycled for memory). The supervisor MUST carry minimal failure-prone code.
- **PGW-034** Children MUST **auto-restart** on: process exit/crash; **liveness** failure (active health probe over the bridge — not just process-alive); and **RSS threshold** (proactive memory recycle). Optional scheduled recycle for slow leaks.
- **PGW-035** Restart MUST use exponential backoff with a **crash-loop circuit breaker that fails loud** — after N rapid failures, stop and surface the failure rather than spin silently.
- **PGW-036** Intentional recycles MUST **drain gracefully** (stop new connections, let in-flight finish or time out); crashes restart abruptly.
- **PGW-037** The **pgwire lifecycle MUST be decoupled from the Calcite lifecycle**: recycling Calcite MUST fail only in-flight queries, not drop idle client sessions — the pgwire reconnects to the new Calcite over the bridge. On restart, the pgwire MUST readiness-gate on the Calcite child's socket before routing queries.
- **PGW-038** Recyclable children MUST hold **nothing un-rebuildable**; the Calcite model and schema registry are the durable source of truth, re-provided by the supervisor on each restart.
- **PGW-039** Native memory MUST be released per request: close the per-request in-memory DuckDB catalog, release Arrow off-heap buffers, and hold no leaked source connections/fds. Calcite planner/metadata caches MUST be bounded.

### 4.7 Calcite containment
- **PGW-040** Calcite failures MUST be contained, not solved: process isolation + supervised restart (PGW-033/34), the Arrow bridge bounding blast radius, and the sqlglot dialect firewall (PGW-016/17) keeping client discovery insulated from Calcite's dialect internals.
- **PGW-041** A **pinned Calcite version** MUST be maintained together with a **regression corpus of the actual query/introspection patterns** the target clients (DuckDB/DBeaver/DataGrip) emit. Calcite upgrades are gated on that corpus — community fixes are adopted on our schedule, verified against our paths, not adopted blindly.

---

## 5. Reuse vs. net-new

**Reused from provisa/buenavista (verbatim or near):** wire codec, startup/SSL/auth, extended query protocol (incl. asyncpg OID quirks), the `pg_catalog`/`information_schema` intercept engine (materialized DuckDB + AST rewriter + `CatalogIndex` OID consistency), DDL dispatch, session-command intercept, text/csv COPY.

**Net-new for the Calcite fork:**
1. **sqlglot Calcite dialect** + the shared canonical identifier/type normalization driving both discovery and transpile (PGW-016/17).
2. **Calcite-metadata catalog population** replacing provisa's CompilationContext source (PGW-012).
3. **Arrow result path**: JVM-side `JdbcToArrow` emit, Arrow IPC bridge, Arrow-backed streaming `QueryResult`, cancel propagation (PGW-019/20/22).
4. **Binary COPY** (PGW-021).
5. **Desktop packaging**: OS-agnostic Java launcher, first-run CPython provisioning, airgapped per-OS wheelhouses (PGW-025–031).
6. **Supervisor + recyclable children + auto-restart** with pgwire/Calcite lifecycle decoupling (PGW-032–039).

---

## 6. Non-goals

- Full PostgreSQL system-catalog fidelity beyond what the target clients probe.
- DDL reverse-engineering (`pg_get_constraintdef`/`viewdef`/`indexdef` stay stubbed).
- Real transaction isolation / rollback.
- Avatica or ODBC integration.
- Inventing/overlaying constraints the underlying adapter does not declare.
- Modifications to calcite-core.

---

## 7. Open / deferred decisions

- **Bridge topology detail:** jpype in-process vs. Calcite JVM sidecar for the Arrow hand-off — resolved in favor of a **separate recyclable Calcite JVM child** (PGW-033) for memory-recycle and concurrency, but the exact Arrow transport (raw Arrow IPC socket vs. Arrow Flight) is unfixed.
- **~~Shared library vs. copy~~ (resolved, D1):** copy provisa's pgwire into a standalone, independent `pgwire-calcite` and diverge; provisa is untouched. A shared backend-agnostic library remains a future option if the duplicated catalog/protocol maintenance becomes painful. The `pgwire-calcite` copy still needs a minimal launcher (boots the Calcite backend + `serve_forever` + a minimal schema registry) rather than dragging in provisa's FastAPI `state`.
- **`ctid` parallel scans** (multiple concurrent COPY streams per attach) — Phase 2.
- **Which sqlglot base dialect** the Calcite dialect derives from (Oracle-conformance is the current evidence) and the exact Calcite connection conformance/`lex`/`fun` settings it must agree with.
