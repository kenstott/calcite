# pgwire-calcite Server — Phased Implementation Plan

**Companion to:** [pgwire-calcite-server-requirements.md](pgwire-calcite-server-requirements.md)
**Status:** Planning. Requirements frozen; this document sequences them into deliverable phases.

## How to read this plan

Each phase is an independently demonstrable increment with an explicit **exit gate**. The
ordering front-loads an end-to-end query path (so we prove D2/D3/D4 — direct-to-Calcite via a
sqlglot dialect — before investing in Arrow, packaging, or supervision), then hardens streaming,
then productionizes delivery. Development phases (0–5) run against a **simple dev harness** (single
box, both children launched by a shell script); the supervisor and airgapped packaging that make it
a shippable desktop app come last (6–7), once there is something worth supervising and shipping.

Each phase lists the requirement IDs (PGW-nnn) it closes. Every PGW requirement is assigned to
exactly one phase; the coverage matrix at the end is the audit.

Reused-from-provisa work (§5 of requirements) is *carried in*, not reimplemented — the effort in
those areas is porting + re-testing against our regression corpus, not greenfield.

---

## Phase 0 — Fork, harness, and corpus baseline

**Goal:** A standalone `pgwire-calcite` that boots the copied provisa pgwire against a **stub
backend** and answers a `psql` handshake, with the regression harness in place from day one.

**Work**
- Copy `provisa/pgwire` (on vendored buenavista) into a new, independent `pgwire-calcite`
  tree (D1). No shared library; provisa is untouched.
- Strip provisa's FastAPI `state`; write the **minimal launcher** (open 251 §7): boot backend +
  `serve_forever` + a minimal schema registry. No `_pipeline`/Trino route.
- Pin the Calcite version and stand up the **regression corpus harness** (PGW-041): capture the
  actual query/introspection wire traffic DuckDB, DBeaver, and DataGrip emit against a reference
  Postgres, stored as a replayable corpus. This corpus is the shared asset that mitigates D1's
  two-copies drift risk and gates every later phase.
- Wire a throwaway stub backend (returns fixed rows) so the handshake, SSL, and auth paths from
  provisa can be smoke-tested in isolation.

**Exit gate:** `psql` connects, authenticates (trust + cleartext), runs `SELECT 1` against the
stub, and disconnects cleanly. Corpus harness records and replays at least one full DBeaver
connect-time introspection sequence (captured, not yet answered correctly).

**Closes:** PGW-001, PGW-007 (auth/TLS carried over), PGW-041 (corpus + pin established).

---

## Phase 1 — Execution seam: sqlglot Calcite dialect + JDBC (rows)

**Goal:** Non-catalog `SELECT` queries transpiled PG→Calcite and executed against an **embedded
`jdbc:calcite:` connection**, returning rows over the wire. Proves the core thesis (D2/D3) with the
*simplest* result path — plain JDBC rows, **no Arrow yet**.

**Work**
- Build the **bidirectional sqlglot Calcite dialect** (D4, PGW-017), deriving from the best-fit base
  (Oracle-conformance is the current evidence — resolve open decision §7). Establish the exact
  Calcite connection `lex`/`fun`/conformance settings the dialect must agree with.
- Implement the **single canonical identifier + type↔OID normalization** in sqlglot (PGW-016) as a
  standalone module. In this phase it drives transpile only; Phase 2 reuses the *same* module for
  discovery, which is what guarantees discover-then-query consistency by construction.
- Route non-catalog queries: parse → transpile → execute on embedded Calcite JDBC → adapt
  `(rows/columns/types)` into the existing result adapter (PGW-015).
- Implement the **targeted transform layer** for PG-only constructs (`DISTINCT ON`, `~`/`~*`, array/
  JSON ops, `generate_series`, `string_agg`→`LISTAGG`) — translate where clean, **reject explicitly**
  otherwise, never silently mistranslate (PGW-018).
- Support simple + extended query protocols and session-command intercept end-to-end against the
  live Calcite backend (PGW-002, PGW-003, PGW-004 — ported from provisa, re-tested here).

**Exit gate:** `psql` runs real analytic SQL (joins, filters, aggregates, `LIMIT`) against a
file-adapter model and gets correct results. A curated set of PG-only constructs either transpiles
correctly or returns a clear rejection error. Extended-protocol Parse/Bind/Describe/Execute verified.

**Closes:** PGW-002, PGW-003, PGW-004, PGW-015, PGW-016 (transpile side), PGW-017, PGW-018.

---

## Phase 2 — Catalog / discovery from Calcite metadata

**Goal:** DBeaver and DataGrip complete full connect-time introspection and render **ER diagrams**.
Catalog is populated from **Calcite metadata** and normalized by the *same* Phase-1 module, so
discovery and execution agree by construction.

**Work**
- Populate the intercept's catalog from Calcite metadata (PGW-012): schemas/tables/columns, plus
  keys and referential constraints from `Statistic.getKeys()`/`getReferentialConstraints()`, govdata
  YAML-declared keys/joins, and the file adapter's `TableConstraints`.
- Apply the shared canonical normalization (PGW-016) to catalog population — closes the discovery
  side of the consistency guarantee opened in Phase 1.
- Retain provisa's intercept engine (PGW-010): per-request in-memory DuckDB catalog + AST rewrite of
  Postgres-isms + execution of the client's *actual* SQL. No naive query-string matching.
- Preserve `CatalogIndex` single-source-of-truth OID consistency across `pg_class`/`pg_attribute`/
  `pg_type`/`pg_constraint` (PGW-011).
- **Invent no constraints** (PGW-013): real rows where keys exist, well-formed empty results where
  they don't. No pgwire-side overlay.
- Keep client-irrelevant functions stubbed (PGW-014); extend a single rewriter branch only if a
  target client's probe hits a stub. DDL reverse-engineering stays a non-goal.

**Exit gate:** DBeaver **and** DataGrip complete introspection with no errors and draw ER
relationship lines for a key-bearing adapter (file/govdata). A constraint-less adapter introspects
cleanly with empty constraint results (client adds virtual FKs on its side). Full replay of the
Phase-0 corpus passes.

**Closes:** PGW-006, PGW-010, PGW-011, PGW-012, PGW-013, PGW-014.

---

## Phase 3 — Arrow result path & streaming

**Goal:** Replace Phase-1's JDBC-row path with the real **Arrow** seam: JVM-side transpose, Arrow
IPC bridge to Python, batch-streaming result, and cancel propagation. Never materialize a full
result at any hop.

**Work**
- JVM side: emit results via `arrow-jdbc` `JdbcToArrow` (row→Arrow transpose done once in the JVM,
  PGW-019).
- Bridge: stream Arrow IPC over the local socket to pyarrow (zero-copy). (Raw Arrow IPC vs. Arrow
  Flight is an open §7 decision — pick and record here.) Arrow ends at the pg-encode boundary.
- Python side: **Arrow-batch streaming** `QueryResult` (batch iterator; backpressure via the pg
  socket), vectorized-encoded to pg protocol. No materialized list (PGW-020).
- **Cancel/close propagation** (PGW-022): client disconnect or satisfied `LIMIT` cancels the Calcite
  query and releases buffers — no leaked running queries.
- Client-driven paging via **portal suspension** (`Execute` row-limit) (PGW-023, Phase-1 slice;
  `ctid` ranges deferred to Phase 7). Push blocking operators (sort/aggregate/join) down to a
  spilling engine (file adapter's DuckDB engine) rather than paging them.
- **Faithful capability exposure** (PGW-024): where an adapter materializes with no pushdown/key,
  surface the memory cost rather than paper over it.
- Per-request native-memory release (Arrow off-heap buffers; PGW-039 partial — Arrow scope).

**Exit gate:** A large scan streams in bounded memory (RSS flat across a multi-GB result); `LIMIT 5`
on a long scan cancels the underlying Calcite query promptly; client disconnect mid-scan leaves no
running query or leaked buffer. Portal suspension paging verified from a JDBC client.

**Closes:** PGW-019, PGW-020, PGW-022, PGW-023 (portal-suspension slice), PGW-024.

---

## Phase 4 — Binary COPY & DuckDB ATTACH

**Goal:** DuckDB `ATTACH … (TYPE postgres)` reads Calcite tables via its default binary path.

**Work**
- Server-side `COPY … TO STDOUT` **binary** format: Arrow batch → pg binary `CopyData` (PGW-021).
  Text/csv COPY is carried over from provisa; binary is the net-new piece and reuses Phase-3's Arrow
  batch stream directly.
- Validate DuckDB `ATTACH 'host=… port=…' (TYPE postgres)` end-to-end; until/unless binary COPY is
  complete, DuckDB attaches with `pg_use_text_protocol` as the documented fallback (PGW-005).
- Confirm report of server version ≥14 clears DuckDB's gate in the attach path (PGW-001 re-verified
  in the DuckDB context).

**Exit gate:** DuckDB attaches over binary COPY and correctly reads a multi-column, multi-type
Calcite table (numeric, text, temporal, null-bearing). Text-protocol fallback also verified.

**Closes:** PGW-005, PGW-021.

> **Findings from the Phase-3 spike.** A first DuckDB `ATTACH` attempt surfaced that DuckDB's attach
> handshake issues `pg_catalog` probes the intercept doesn't yet answer (e.g.
> `TO_REGCLASS('duckdb_secrets')` → must return NULL). Each needs one catalog rewriter branch
> (PGW-014 pattern), discovered by replaying DuckDB's attach traffic into the corpus. A GIL/thread
> fault also appeared — but *only on the error path* when that probe fell through to Calcite and raised
> mid-teardown; all other JVM calls on handler threads are stable across the suite. So supporting the
> probe (and tearing down cleanly) resolves the storm; Phase 4 is **not** blocked on the Phase-5
> sidecar (the sidecar remains the eventual isolation/robustness story, not a prerequisite here).
>
> **Status: DONE (verified end-to-end with real DuckDB).** `to_regclass` intercepted; DuckDB `ATTACH`
> succeeds and shuts down clean. Binary COPY (`binary_copy.py`) runs over both simple and extended
> protocols (DuckDB reads via extended), reusing the Phase-3 Arrow stream; emitted PGCOPY stream is
> spec-exact byte-for-byte; the int4 width/OID consistency bug (PGW-016: `INTEGER`→4-byte
> `BVType.INTEGER`, was 8-byte) is fixed. **Root cause of the earlier "invalid header":** DuckDB's
> binary-COPY reader rejects the PGCOPY header when it arrives alone in its own tiny `CopyData`
> message — the fix is to coalesce output into ~64 KiB `CopyData` flushes (header ships with the first
> rows), which keeps memory bounded (PGW-020). An integration test now `ATTACH`es DuckDB and reads a
> small table plus a 10k-row result across multiple flushes. **Closes PGW-005, PGW-021.**

---

## Phase 5 — Supervisor, recyclable children & lifecycle decoupling

**Goal:** Promote the dev harness to the real **thin-supervisor + two recyclable children**
topology, with auto-restart, memory recycling, and pgwire/Calcite lifecycle decoupling. This is
where Calcite becomes *contained* rather than trusted (D9).

**Work**
- Thin supervisor process that owns neither Calcite nor pgwire logic; carries minimal failure-prone
  code (PGW-033). Calcite runs **only** in its own recyclable JVM child (own heap); CPython pgwire
  in its own recyclable child (PGW-007/031 topology, PGW-031-independent).
- **Auto-restart** on process exit/crash, **active liveness-probe** failure over the bridge (not just
  process-alive), and **RSS threshold** proactive recycle; optional scheduled recycle for slow leaks
  (PGW-034).
- **Exponential backoff + crash-loop circuit breaker that fails loud** — stop and surface after N
  rapid failures (PGW-035).
- **Graceful drain** on intentional recycle (stop new connections, let in-flight finish or time
  out); abrupt restart on crash (PGW-036).
- **Lifecycle decoupling** (PGW-037): recycling Calcite fails only in-flight queries, not idle client
  sessions; pgwire reconnects to the new Calcite over the bridge and **readiness-gates** on the
  Calcite child's socket before routing.
- Recyclable children hold **nothing un-rebuildable**; supervisor re-provides the Calcite model +
  schema registry on every restart (PGW-038).
- Complete per-request native-memory discipline (PGW-039): close per-request in-memory DuckDB
  catalog, release Arrow buffers, no leaked source connections/fds; bound Calcite planner/metadata
  caches.
- Exploit warm reuse where free: kept-warm Calcite connection, compiled-plan cache, pooling, JIT
  (PGW-032). Cold start is explicitly a non-concern.

**Exit gate:** Kill -9 either child under load → auto-restart with idle sessions preserved and only
in-flight queries failing; RSS-threshold recycle of Calcite fires and drains gracefully; induced
crash-loop trips the breaker and surfaces loudly instead of spinning; soak test shows flat RSS and
zero leaked fds/queries over N hours.

**Closes:** PGW-032, PGW-033, PGW-034, PGW-035, PGW-036, PGW-037, PGW-038, PGW-039.

> **Status.** The **supervisor reliability core is done and tested** (`supervisor.py`): auto-restart
> with exponential backoff, a crash-loop circuit breaker that fails loud, proactive RSS-threshold
> recycle (via `/proc`, no new deps), liveness-probe restart, readiness gating, and graceful drain vs.
> abrupt restart — driven by a `tick()` loop with an injectable clock (8 tests incl. a real
> memory-hog subprocess recycle). It closes PGW-034/035/036/038 and the supervisor half of PGW-033.
> **Remaining:** the two-child *topology* — running Calcite as a **separate recyclable JVM child** with
> an Arrow bridge (vs. today's in-process JPype) and the pgwire↔Calcite **lifecycle decoupling**
> (PGW-037: recycle Calcite without dropping idle sessions; readiness-gated reconnect). That is a
> distinct, larger rearchitecture of the (working, verified) in-process execution path; the supervisor
> is built to drive it. PGW-039 (per-request native-memory release) is satisfied on the Arrow path
> (Phase 3) and the catalog's per-request DuckDB close (Phase 2).
>
> **Topology: DONE.** `sidecar.py` + `calcite_child.py` implement the two-process split: Calcite runs
> in a **separate recyclable child** (`python -m pgwire_calcite.calcite_child`, its own JVM/heap),
> serving an Arrow-IPC socket bridge; the pgwire process uses `BridgeBackend` (transpiles PG→Calcite
> on the Python side per D4, so PG-only rejects happen before the child; streams Arrow IPC back).
> `BridgeBackend` dials per query, so recycling Calcite fails only in-flight — idle sessions survive
> and the next query reconnects to the fresh child (**PGW-037**); `ready()` readiness-gates on the
> socket. Verified by 9 tests incl. a large streamed result over the socket, reconnect-after-recycle,
> and a **real two-process** spawn (child in its own JVM). Closes the topology half of PGW-033 + PGW-037.
> **Follow-on:** catalog population over the bridge (the child would proxy JDBC metadata; the
> in-process backend already does PGW-012) — a protocol extension, not a blocker for query execution.

---

## Phase 5b — Authentication & authorization providers

**Goal:** Replace the carried-over trust/cleartext auth with a pluggable provider interface:
persisted local accounts (SCRAM-SHA-256), external OIDC/Firebase token auth, and per-role
schema/table visibility. Off the DuckDB/query critical path, so it follows the supervisor phase.

**Work**
- Pluggable **auth provider interface** on the server state, selected by `--auth {trust,local,oidc}`;
  `trust` stays the localhost default (PGW-042). Refactors Phase-0's `state`-carried trust/cleartext
  into the formal interface.
- **Local accounts** provider: persisted store with **SCRAM-SHA-256** verifiers at rest + wire-level
  SCRAM-SHA-256 mechanism (secure without TLS) + `pgwire-calcite users add/rm/list` CLI (PGW-043).
- **External-token** provider: OIDC ID token (JWT) as password, verified against issuer JWKS
  (signature/`exp`/`aud`/`iss`); issuer-generic — Firebase/Auth0/Google/Entra (PGW-044).
- **Per-role authorization** (PGW-045): map account → allowed Calcite schemas/tables; the Phase-2
  catalog intercept filters discovery to the role's objects, and execution rejects out-of-grant
  access — enforced in both discovery and execution (hooks back into Phase 2).

**Exit gate:** local account authenticates via SCRAM (verified with psql/DBeaver, no TLS); an OIDC
token authenticates and an expired/wrong-audience token is rejected; a restricted role sees only its
granted tables in discovery **and** is denied on direct access to a non-granted table.

**Closes:** PGW-042, PGW-043, PGW-044, PGW-045.

> **Status.** Provider interface + `trust` + **local accounts** are done and tested (`auth.py`): a
> pluggable `AuthProvider`, SCRAM-SHA-256 verifiers at rest (PBKDF2, stdlib — no plaintext, no new
> deps), a JSON `AccountStore`, a `pgwire-calcite-users add/rm/list` CLI, and wire integration
> (trust connects with no challenge; local verifies the cleartext password against the stored
> verifier). 7 tests + end-to-end accept/reject over the wire.
>
> **Phase 5b: DONE (all four, PGW-042/043/044/045).** Added since:
> - **SASL SCRAM-SHA-256 wire exchange** (`scram.py`, RFC 5802/7677): the server drives the SASL
>   messages from the at-rest verifier — no password on the wire. Proven with a self-contained SCRAM
>   client AND a **real psql** SCRAM handshake (accept/reject). Completes PGW-043.
> - **OIDC/Firebase token-as-password** (`auth.OidcProvider`, optional `[oidc]` extra = PyJWT+crypto):
>   verifies an RS256 ID token's signature + iss/aud/exp against a PEM/JWKS/jwks_url; issuer-generic.
>   6 tests (valid + expired/wrong-aud/wrong-iss/bad-sig + over-the-wire). Closes PGW-044.
> - **Per-role authorization** (`authz.py`): a per-role *filtered* CompilationContext feeds the catalog
>   (discovery shows only granted objects) and non-catalog queries are enforced on the same grants
>   (out-of-grant → 42501) — both paths by construction. 5 tests. Closes PGW-045.

---

## Phase 6 — Airgapped desktop packaging & delivery

**Goal:** Ship as an **OS-agnostic Java desktop app** with fully airgapped first-run provisioning —
no Docker, no network.

**Work**
- OS-agnostic Java launcher/JRE delivery, not a Docker image (PGW-025, PGW-006-independent).
- First-run provisioning of **standalone CPython + locked wheelhouse** (python-build-standalone +
  uv), reused thereafter (PGW-026).
- **Airgapped**: predownload CPython runtimes + platform wheelhouses for the 3–4 target OS/arch
  variants into the artifact; first-run install works with **no network** (PGW-027).
- Cross-platform wheel fetching at **build time** (per-target `--platform`/uv), pinned to one CPython
  version/ABI, with a lockfile for reproducibility (PGW-028).
- **Fail loud, no autofetch** (PGW-029): DuckDB `autoinstall_known_extensions`/
  `autoload_known_extensions` off, needed extensions bundled; no pip/JVM network fallback. CI
  verifies the offline install **including rarely-used code paths**.
- **Per-OS installers** (~150 MB each, own variant) preferred over a universal ~600 MB–1 GB artifact;
  ~1 GB total accepted as the airgap tax (PGW-030).
- **Signing**: macOS bundled native libs codesigned/notarized; Windows installers signed
  (Gatekeeper/SmartScreen); Linux wheels match target glibc baseline (manylinux; musllinux if Alpine)
  (PGW-031).

**Exit gate:** On a **network-disconnected** clean machine of each target OS/arch, the installer
provisions and the server serves the Phase-1–4 client flows. CI offline-install job passes,
exercising rarely-used paths. macOS notarization + Windows signing verified on real hardware/VMs.

**Closes:** PGW-025, PGW-026, PGW-027, PGW-028, PGW-029, PGW-030, PGW-031.

> **Status (delivery simplified — signing blocker removed).** Since this is a
> driver/CLI-server for technical users, delivery is **package managers + tarballs**
> (Homebrew tap, Scoop bucket, `curl | sh`), which are NOT Gatekeeper/SmartScreen
> quarantined and need **no OS code-signing** (PGW-030/031 revised). Committed +
> buildable here with no credentials: `airgap.py` (fail-loud no-autofetch, PGW-029,
> tested), `build-wheelhouse.sh` + `requirements.lock` (PGW-027/028), `pack.sh`
> (tarball), and the Homebrew formula + Scoop manifest (PGW-030). **Remaining (your
> hardware, not credentials):** final per-OS assembly bundling that OS's JRE +
> standalone CPython (build must run on each target OS), the offline-install CI
> job on a disconnected runner, and a thin Java launcher shim if PGW-025's exact
> "Java top-level" is desired (the supervision logic already exists in Python).
> Signed DMG/MSI is now an explicit **optional** add-on only.

## Phase 7 — Containment hardening, corpus gating & deferred Phase-2 items

**Goal:** Lock in the "contain Calcite, don't fix it" posture as an ongoing discipline, and land the
deferred paging/scan work.

**Work**
- Formalize **containment** (PGW-040): verify process isolation + supervised restart (from Phase 5),
  the Arrow bridge blast-radius bound, and the sqlglot dialect firewall (from Phases 1–2) together
  insulate client discovery from Calcite's dialect internals — as an explicit, tested property, not
  an emergent one.
- **Calcite-upgrade gate** (PGW-041, ongoing): upgrades adopted on our schedule, gated on the
  regression corpus passing against our paths — never adopted blindly.
- **`ctid` range paging** and multiple concurrent COPY streams per attach (parallel scans) — the
  explicitly deferred Phase-2 items (PGW-023 `ctid` slice; requirements §7).
- Resolve any remaining §7 open decisions not closed in earlier phases and record them.

**Exit gate:** A Calcite version bump is dry-run through the corpus gate (pass/fail demonstrated);
containment properties have explicit passing tests; `ctid` parallel scans verified from DuckDB.

**Closes:** PGW-040, PGW-023 (`ctid` slice completes the requirement).

> **Status.** **Containment (PGW-040) + corpus gate (PGW-041): DONE + tested.**
> `corpus_gate.py` replays the client corpus against a running server and fails
> loud on any regression — and it earned its keep immediately, catching a real
> PG-compat gap (Calcite's `EXPR$0` vs PostgreSQL's `?column?` for unnamed
> expressions), now fixed via `normalize.pg_column_label` on both result paths.
> Containment is asserted as an explicit property: a dialect-firewall reject and a
> Calcite execution error each surface cleanly and the session keeps working
> (blast radius bounded). **`ctid` parallel scans (PGW-023 slice): deferred as a
> performance optimization, not correctness.** DuckDB reads correctly via a single
> binary-COPY stream (verified Phase 4); it only parallelizes when the server
> advertises `ctid`-range scans, which we don't — so reads stay correct. Emulating
> Postgres `ctid` (which Calcite has no equivalent for) for *parallel* streams
> requires deterministic row-range partitioning; done naively it returns duplicate
> rows, so it is intentionally left as a future optimization rather than shipped
> unsafe.

---

## Phase 8 — Extension surfaces (optional, post-core)

**Goal:** Present popular PostgreSQL **extension surfaces** so pgwire-calcite looks like a
Postgres-with-superpowers to existing tooling — vector/semantic search, spatial, JSON — backed by
Calcite functions/adapters or the file adapter's DuckDB engine. Reuses the Phase-1/2 machinery
(transform layer + catalog + normalization); adds no new core.

**Work**
- **Pluggable surface mechanism** (PGW-046): each surface advertises in `pg_extension`, declares its
  types/OIDs in normalize.py, and maps operators/functions in the transform layer to a Calcite
  function, adapter, or the DuckDB engine — convert-or-reject-loudly, no faked capability.
- **pgvector surface** (PGW-047): `vector` type + `<->`/`<=>`/`<#>` and `ORDER BY emb <-> $1`,
  lowered to DuckDB array-distance; no index-acceleration claims.
- **PostGIS subset** (PGW-048): `geometry`/`geography` + core `ST_*` mapped to Calcite spatial
  (`fun=spatial`).
- **JSON operators** (PGW-049): `->`/`->>`/`#>` → Calcite `JSON_VALUE`/`JSON_QUERY` (converts
  Phase-1 rejects to support).
- **Compatibility functions** (PGW-050): opt-in `pg_trgm`/`gen_random_uuid`/`pgcrypto` modules.

**Exit gate:** with a surface enabled, its `pg_extension` row is discoverable; a representative query
(`ORDER BY emb <-> $1` for pgvector; `ST_DWithin(...)` for PostGIS; `data->>'k'` for JSON) executes
with correct results through the wire; a not-implemented operator in an enabled surface rejects
loudly rather than mistranslating.

**Closes:** PGW-046, PGW-047, PGW-048, PGW-049, PGW-050.

> **Status.** **Mechanism (PGW-046) + JSON surface (PGW-049): DONE + tested.**
> `extensions.py` is the opt-in registry (`--extension json`); it advertises
> enabled surfaces for a `pg_extension` probe. The **JSON surface** converts
> `->`/`->>` to Calcite `JSON_QUERY`/`JSON_VALUE` in the transform layer when
> enabled, and rejects them loudly when not (convert-or-reject, PGW-018) — verified
> transpiling AND executing on real Calcite over the wire (`'{"a":42}'->>'a'` →
> `42`). **Follow-ons (advertised, not yet lowering operators):** `postgis`
> (ST_* → Calcite spatial via `fun=spatial`, PGW-048) and `vector`/pgvector
> (distance ops → the file adapter's DuckDB engine, PGW-047), plus `pg_trgm`/
> compat fns (PGW-050). They are registered so the surface/OID plumbing is in
> place; enabling their operators is incremental work on the same mechanism, and
> until then those operators stay loud rejects (no faked capability).

---

## Requirement → phase coverage matrix

| Phase | Requirements closed |
|-------|--------------------|
| 0 | PGW-001, 007, 041 (established) |
| 1 | PGW-002, 003, 004, 015, 016 (transpile), 017, 018 |
| 2 | PGW-006, 010, 011, 012, 013, 014, 016 (discovery) |
| 3 | PGW-019, 020, 022, 023 (portal), 024 |
| 4 | PGW-005, 021 |
| 5 | PGW-032, 033, 034, 035, 036, 037, 038, 039 |
| 5b | PGW-042, 043, 044, 045 (auth/authz; added post-planning) |
| 6 | PGW-025, 026, 027, 028, 029, 030, 031 |
| 7 | PGW-040, 041 (ongoing gate), 023 (`ctid`) |
| 8 | PGW-046, 047, 048, 049, 050 (extension surfaces; optional, added post-planning) |

All 50 requirements (PGW-001…050) are assigned. PGW-016 spans Phases 1–2 by design (one module,
two consumers); PGW-023 spans Phases 3 and 7 (portal-suspension now, `ctid` parallel scans deferred);
PGW-041 is established in Phase 0 and enforced as an ongoing gate in Phase 7. Phases 5b (auth) and 8
(extension surfaces) were added post-planning; Phase 8 is optional/opt-in.

## Critical-path & sequencing notes

- **1 → 2 is the consistency spine.** The normalization module (PGW-016) is built in Phase 1 for
  transpile and reused verbatim in Phase 2 for discovery. Building discovery on a *different*
  normalization would reintroduce exactly the discover-then-query drift D4 exists to prevent — so
  Phase 2 must not fork the module.
- **3 before 4.** Binary COPY (Phase 4) is a thin re-encoding of Phase 3's Arrow batch stream;
  building it before the streaming result path exists would mean throwaway work.
- **Dev harness carries Phases 1–4; Phase 5 productionizes it.** Functionality is proven on a simple
  two-process shell harness first; the supervisor/recycle machinery is added once there is a working
  server to supervise. This keeps early phases from paying the lifecycle-complexity tax before the
  query path is proven.
- **Corpus (Phase 0) gates every later phase.** It is both the acceptance oracle for Phases 2–4 and
  the D1 drift-mitigation asset (shared probe/regression corpus, not shared code).
