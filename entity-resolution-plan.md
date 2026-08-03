# Cross-Schema Entity-Resolution Bridge (Orgs + Individuals)

## Context

AskAmerica's schemas carry free-text entity names — both organizations and individuals —
with no cross-reference to each other or to a canonical registry. There IS a working GLEIF
bridge (`ref.gleif_cik_mapping`), but it's narrow and exact-key only: it links LEI↔CIK for
SEC-registered filers via a field GLEIF itself populates, with no name-similarity logic
anywhere. It doesn't help a patent assignee, a PAC's connected org, a mine operator, a
nonprofit, or any individual resolve to anything.

This surfaced from a design conversation about improving AskAmerica's own data quality via
"high quality corp and individual entity resolution... vs. GLEIF (and some maybe potential
other source of high quality names — perhaps just picked known names — like political
candidates and maybe corp officers and boards)." It directly unblocks the comparative-eval
question `campaign-contributions-industry-roi` (currently blocked partly on industry↔PAC
linkage quality) and a new innovation question being drafted (SEC R&D spend vs. patent
output), which needs patent assignees joined to SEC filers to be possible at all.

**Both entity types are in scope together**, not sequenced — per direct correction: "this
need to include individuals and corporations BOTH as entities." The scope then grew twice
more through direct correction: first "aren't there some env, cdc related entities?", then
"aren't there other potential entity references in env, lands, energy, cdc, etc." — a live
catalog sweep across every AskAmerica schema turned up far more candidate sources than the
original 3-schema (fec/patents/sec) design, which is why the design below is now a
**generic, table-driven source registry** rather than hardcoded per-schema logic — the
user's explicit choice over keeping it hardcoded, given how many real sources exist and how
likely a 9th will turn up later.

Two format-level findings that shape the whole design, both confirmed live rather than
assumed:

- **`sec.insider_transactions.reporting_person_name` is not individuals-only.** Sampled live
  (`SELECT DISTINCT reporting_person_name FROM sec.insider_transactions FETCH FIRST 15 ROWS
  ONLY`), it mixes people ("Eydelman Mikhail", "Dunbar David A.") with institutional
  10%-owners ("VISTA EQUITY PARTNERS MANAGEMENT, LLC", "ADVENT INTERNATIONAL, L.P.") — Form
  3/4/5 reporting owners can be entities, not just people. Several other "mixed" sources
  below have the same shape.
- **Name formats don't agree across sources.** `fec.candidates.candidate_name` is
  comma-delimited "LAST, FIRST MIDDLE [SUFFIX]" (e.g. `"BROWN, THOMAS WILLIAM JR."`); SEC's
  individual-type `reporting_person_name` rows have no comma — space-separated, last name
  first (e.g. `"Watson Clyde David"`). Person-track matching must parse into
  `(last, first, middle)` parts on both sides, never fuzzy-match raw strings.

This plan is design only. Implementation is a distinct next step requiring explicit
go-ahead — it touches production ETL scaffolding (`ref-schema.yaml`, a new Java hook class)
that other schemas' pool runs depend on.

## Source registry

The matching **pipeline** is generic (add a registry row, not new Java match logic, to add a
9th source). The **wide canonical tables' columns are still a fixed, versioned schema** —
SQL/Iceberg requires declared columns, so a genuinely open-ended nullable-FK table isn't
achievable without going to an EAV shape, which would defeat the "nullable FKs" the user
explicitly asked for. Adding a source later means: one registry entry (cheap) + one column
pair on the relevant wide table (a small, explicit migration). Below is the full v1 list —
"everything found today," confirmed live via `describe_table`/`list_tables`, not the
original 3-schema cut.

### Org-type sources

| schema.table.name_column | key_column | key type | notes |
|---|---|---|---|
| `fec.committees.connected_org_name` | `committee_id` | structured | raw PK is `[type, year, committee_id]`, dedup to one row per committee |
| `patents.patent_assignees.assignee_organization` | `assignee_id` | structured | filter `assignee_type IN ('US company','foreign company')`; PatentsView's own disambiguated ID |
| `sec.insider_transactions.reporting_person_name` (entity-type rows) | `reporting_person_cik` | structured | SEC's own stable per-filer CIK; see classification step |
| `health.fda_drug_approvals.sponsor_name` | *(name-only)* | **unstructured** | schema comment already says "text FK to sec and ref.gleif_entities" — intended, never built |
| `environment.ghg_facilities.parent_company` | *(name-only)* | **unstructured** | `facility_id` is per-facility, not per-parent-company |
| `health.cms_open_payments.paying_entity_name` | *(name-only)* | **unstructured** | pharma/device manufacturer paying physicians |
| `energy.eia_utility_annual.utility_name` | `utility_id` | structured | EIA's own utility identifier |
| `energy.eia_coal_mines.controller_name` | *(name-only)* | **unstructured** | mine *controller* — logically distinct from operator below, same table |
| `energy.eia_coal_mines.operator_name` | *(name-only)* | **unstructured** | mine *operator* |
| `transport.fmcsa_carriers.carrier_name` | `dot_number` | structured | schema comment already says "fuzzy-join to sec.filing_metadata"; **~4.47M rows — see scale caveat** |
| `transport.faa_aircraft_registry.registrant_name` (entity-type, via `registrant_type`) | *(name-only)* | **unstructured** | no per-registrant ID; `n_number`/`unique_id` are per-aircraft |
| `fiscal.exempt_org_master.org_name` | `ein` | structured, **EIN** | see EIN hub-path finding below |
| `fiscal.sba_loan_approvals.borrower_name` | *(name-only)* | **unstructured** | schema comment claims "join by name/EIN" but **no EIN column actually exists** on this table — comment is aspirational, verified false via `describe_table` |
| `fiscal.sba_loan_approvals.lender_name` | *(name-only)* | **unstructured** | banks/lenders — logically distinct from borrower, same table |

...all matched against `ref.current_gleif_entities` (`ref-schema.yaml:1902`) — **not** raw
`ref.gleif_entities`, which is an append-only changelog (`overwritePartitions: false`) that
would multi-match any LEI whose name changed across snapshots.

**EIN hub-path finding**: `sec.filing_metadata.irs_number` is SEC's own EIN column
(confirmed live via `describe_table`). This means EIN is a **second exact-match hub key**
alongside GLEIF's LEI — `fiscal.exempt_org_master.ein` can exact-match
`sec.filing_metadata.irs_number` directly, no fuzzy step needed, for the (likely small) set
of orgs appearing in both. Every EIN-bearing source should attempt this exact match *first*,
falling back to the normal GLEIF name-fuzzy pipeline when no EIN match exists — EIN-matched
rows get `match_method = 'exact_ein'`, `match_confidence = 'high'`.

**Unstructured-key caveat, called out rather than silently treated as equivalent to the
structured sources**: sources with no per-entity ID (marked "unstructured" above) can only
be deduped/keyed by their *normalized name itself*. This is inherently weaker than an
ID-keyed source — a company that files under two slightly different name spellings across
years (e.g. a mine operator renamed mid-decade) will fragment into two distinct bridge
entries instead of one, something `assignee_id`/`committee_id`/`dot_number`/`utility_id`
don't suffer from. Not a blocker, but downstream consumers of these specific sources'
bridge rows should expect somewhat noisier coverage than the ID-keyed sources.

**Scale caveat**: `fmcsa_carriers` is ~4.47M rows — an order of magnitude larger than any
other org source here. The first-normalized-token blocking strategy (below) still applies,
but this source specifically should be spot-checked for build-time performance during
implementation before assuming the same approach scales cleanly; a tighter block key (e.g.
first-token + state) may be needed just for this one source.

### Person-type sources

| schema.table.name_column(s) | key_column | key type | notes |
|---|---|---|---|
| `fec.candidates.candidate_name` | `candidate_id` | structured | comma-delimited "LAST, FIRST MIDDLE" |
| `sec.insider_transactions.reporting_person_name` (individual-type rows) | `reporting_person_cik` | structured | space-separated "LAST FIRST [MIDDLE]", no comma |
| `health.cms_open_payments.physician_first_name` + `physician_last_name` | `physician_profile_id` | structured | **already split into first/last — no parsing needed**, CMS's own stable ID |
| `transport.faa_aircraft_registry.registrant_name` (individual-type, via `registrant_type`) | *(name-only)* | **unstructured** | |

...matched **against each other**, N-way pairwise across all person-type sources within a
block — there is no third-party canonical individual registry equivalent to GLEIF, so this
track is direct pairwise linking between known-name pools, not a resolve-to-external-hub
design like the org track. Physician-industry-payment data joined to corporate insider or
political-candidate identity is a real, sensitive use case (conflict-of-interest research)
— the conservative matching posture below applies with full force here, arguably more so.

### Mixed sources needing row-level classification

Two sources require classifying each row as org-type or person-type *before* either track
runs:

- **`sec.insider_transactions.reporting_person_name`** — no structured type flag exists.
  Heuristic: a name is **entity-type** if it contains any legal-suffix/entity-marker token
  (LLC, LP, LLLP, INC, CORP, HOLDINGS, PARTNERS, FUND, TRUST, N A, CO, LTD, PARTNERSHIP,
  MANAGEMENT, GROUP) or is ALL-CAPS with no space-separated 2-4-token person-name shape;
  otherwise **individual-type**. Spot-check against a larger live sample during
  implementation — the 15-row sample supports it but isn't proof for every filer.
- **`transport.faa_aircraft_registry.registrant_name`** — **has a structured
  `registrant_type` field already** (decoded from the FAA registry's registrant-type codes)
  — use it directly instead of a name-shape heuristic; confirm its exact value set (likely
  Individual / Partnership / Corporation / Co-Owned / Government / LLC / Non-Citizen
  variants) during implementation and map each to org-type or person-type explicitly. This
  is strictly more reliable than the `insider_transactions` heuristic and should be
  preferred wherever a source has any structured type signal at all.

Not in scope: `fec.committee_summaries.connected_org_name` (a second, likely same-lineage
occurrence of the org-name field on a different table) — note if it ever disagrees with
`committees.connected_org_name` for the same `committee_id`, don't chase it now.

## Matching algorithm

### Org track

**Normalization** (one shared function, applied identically on both sides of every
comparison — never let the exact and fuzzy passes normalize differently): lowercase → strip
punctuation (`.`/`,` dropped, `&`→`and`) → strip a fixed legal-suffix token list (INC, LLC,
CORP, CO, LTD, LP, LLP, PLC, PC, PA, N A, …) from the **end** of the string only → collapse
whitespace. No existing legal-suffix-aware normalizer exists in the repo — this is new,
small, single-purpose code, applied identically across every org-type source in the
registry.

**Pipeline, run once per org-type source in the registry:**
1. **EIN exact match** (EIN-bearing sources only, currently just `exempt_org_master`) →
   `match_method = 'exact_ein'`, `match_confidence = 'high'`, `match_score = 1.0`, against
   `sec.filing_metadata.irs_number`.
2. **Exact match on normalized name** (against `current_gleif_entities.legal_name`) →
   `match_method = 'exact_normalized'`, `match_confidence = 'high'`, `match_score = 1.0`.
   Resolves the bulk of well-known companies at zero fuzzy cost.
3. **Blocking** for the fuzzy remainder, by **first normalized token** (e.g. "general
   electric" / "general dynamics" both block under "general") — not SOUNDEX, which collapses
   too aggressively on shared corporate-name word roots. Precompute
   `current_gleif_entities` block keys **once**, reused across every org-type source's
   fuzzy pass, not recomputed per source.
4. **Fuzzy scoring within a block**, using **DuckDB's built-in `jaro_winkler_similarity()`**
   via the JDBC connection the listener already opens for `iceberg_scan()` reads (see
   below) — not a hand-rolled Java pass over `commons-text`'s `JaroWinklerSimilarity`. Both
   are available (DuckDB JDBC 1.4.4.0 is already a `govdata` dependency;
   `commons-text-1.14.0` resolves on the compile classpath per the `XbrlToParquetConverter`
   precedent), but doing the join+score in one DuckDB SQL statement avoids pulling GLEIF's
   ~3.2M rows into JVM heap row-by-row.
5. **Confidence tiers, both written into the same table** (no separate review table):
   `match_confidence = 'high'` for `jaro_winkler_similarity >= 0.92`; `'low'` for
   `0.85–0.92`, `match_score` always populated so any consumer query can filter/weight by
   it. Below 0.85: no row written — absence is a clean "no confident match" signal. Track
   the unresolved count per source in the listener's log output.
6. Before calling 0.92/0.85 final, spot-check against ~50 known name-variant pairs
   (abbreviation/suffix differences across real Fortune-500-adjacent orgs) during
   implementation and eyeball precision/recall — starting thresholds from general
   entity-resolution practice, not values validated against this specific name distribution.

### Person track

**Name parsing, not raw fuzzy string matching** — each source is parsed into
`(last_name, first_name, middle_name_or_initial, suffix)` before any comparison:
- FEC: split on the comma (`"BROWN, THOMAS WILLIAM JR."` → last=`BROWN`, first=`THOMAS`,
  middle=`WILLIAM`, suffix=`JR`).
- SEC individual-type rows: no comma — first space-separated token is last name, remainder
  is first/middle (`"Watson Clyde David"` → last=`WATSON`, first=`CLYDE`,
  middle=`DAVID`). **Verify this assumption against a larger live sample (and, if
  feasible, the raw Form 3/4/5 XML `rptOwnerName` structure, which may carry the parts
  separately already) before committing to it in code.**
- CMS Open Payments: already split into `physician_first_name`/`physician_last_name` — no
  parsing needed, use directly.
- FAA individual-type registrants: verify the raw name format during implementation (not
  yet sampled) before assuming a parsing rule.

**Pipeline, deliberately more conservative than the org track**, run pairwise across every
combination of person-type sources within a block, given the reputational stakes of a wrong
political-candidate/corporate-officer/physician identity claim, and the absence of any
secondary corroborating field reliably present across all sources (no DOB/SSN anywhere; FEC
carries an address, the others don't):
1. **Exact match on normalized (last, first) pair** → `match_confidence = 'high'`,
   `match_score = 1.0`.
2. **Fuzzy fallback requires an EXACT last-name match** — never fuzzy-match the last name
   itself; common surnames (Smith, Johnson, Brown) make cross-pool last-name fuzzy matching
   the single biggest false-positive risk in this whole feature, and there's no other field
   to arbitrate a collision. Within an exact-last-name block, fuzzy-score the first name
   with `jaro_winkler_similarity()` to catch nickname/spelling variants (Bill/William,
   Bob/Robert) → `match_confidence = 'low'` for `first_name` score `>= 0.85`, no row
   otherwise.
3. Middle name/initial, when present on both sides, is a tie-breaker signal only (e.g.
   promote a `low`-confidence first-name-fuzzy match toward `high` if the middle initial
   also agrees) — not a hard filter, since it's often just missing on one side.
4. **No `match_confidence = 'high'` is ever assigned from a less-than-exact name match
   alone** — fuzzy-first-name matches stay `low` regardless of score, always visibly
   flagged, never silently trusted as equivalent to an exact match downstream.

## Storage shape

Two shapes, serving different purposes — per direct correction, the wide canonical-entity
hub is a required addition, not optional: "there must be a wide table with nullable FKs
that connect a canonical entity to a dataset PK."

- **Tall bridge tables** (`entity_org_bridge`, `entity_person_bridge`) — the audit/match
  log. One row per source mention, carrying `match_method`/`match_confidence`/`match_score`
  per link. Schema-agnostic by design (`source_schema`/`source_table`/`source_key`), so
  adding a registry source needs no tall-table migration — only the wide table's fixed
  columns need one.
- **Wide canonical-entity tables** (`canonical_org_entity`, `canonical_person_entity`) —
  the hub. One row per real-world entity, nullable FK columns pointing at each dataset's
  natural key. Primary query interface most consumers should join against. Built as a
  pivot of the tall tables, not hand-maintained separately.

### Tall: `ref.entity_org_bridge`

One row per **(source entity, best-match LEI)** — grain matches the deduped/keyed source
entities from the registry above, not raw fact rows:

| column | type | notes |
|---|---|---|
| `source_schema` | string | e.g. `'fec'`, `'patents'`, `'sec'`, `'health'`, `'environment'`, `'energy'`, `'transport'`, `'fiscal'` |
| `source_table` | string | e.g. `'committees'`, `'patent_assignees'`, `'insider_transactions'`, `'fda_drug_approvals'`, `'ghg_facilities'`, `'cms_open_payments'`, `'eia_utility_annual'`, `'eia_coal_mines'`, `'fmcsa_carriers'`, `'faa_aircraft_registry'`, `'exempt_org_master'`, `'sba_loan_approvals'` |
| `source_column` | string | the name column matched, e.g. `'controller_name'` vs `'operator_name'` on the same `eia_coal_mines` row, or `'borrower_name'` vs `'lender_name'` on `sba_loan_approvals` |
| `source_key` | string | the registry's key_column value; for unstructured sources, the normalized name itself |
| `source_name_raw` | string | original name, for audit |
| `source_name_normalized` | string | for debugging match misses |
| `lei` | string, nullable | populated for GLEIF-path matches |
| `sec_cik` | string, nullable | populated for EIN-path matches (via `filing_metadata.irs_number`) |
| `gleif_legal_name` | string, nullable | denormalized from `current_gleif_entities`, avoids a join for the common case |
| `match_method` | string | `exact_ein` \| `exact_normalized` \| `fuzzy` |
| `match_confidence` | string | `high` \| `low` (fuzzy only; exact is always implicitly high) |
| `match_score` | double | `1.0` for exact; raw `jaro_winkler_similarity` for fuzzy |
| `match_run_id` | string | ISO timestamp of the build run, so bridge quality is diffable across reruns |

Primary key: `[source_schema, source_table, source_column, source_key]` — single best match
per source entity, so downstream joins stay unambiguous.

### Tall: `ref.entity_person_bridge`

One row per **matched pair of person-type source entities** — this table only holds rows
where a match was found; it is not a full census of any source (most people known to one
source are never in another, so a "no match" majority is expected and not written):

| column | type | notes |
|---|---|---|
| `source_a_schema` / `source_a_table` / `source_a_key` / `source_a_name_raw` | string | first side of the match |
| `source_b_schema` / `source_b_table` / `source_b_key` / `source_b_name_raw` | string | second side of the match |
| `match_method` | string | `exact_normalized` \| `fuzzy_first_name` |
| `match_confidence` | string | `high` \| `low` — see person-track pipeline; `low` is never silently promoted |
| `match_score` | double | first-name `jaro_winkler_similarity` for fuzzy rows; `1.0` for exact |
| `match_run_id` | string | ISO timestamp, diffable across reruns |

Primary key: `[source_a_schema, source_a_table, source_a_key, source_b_schema,
source_b_table, source_b_key]` — generalizes the original 2-source (FEC↔SEC) design to
N-way pairwise matching across all person-type registry sources; a person appearing in 3+
sources yields multiple pairwise rows rather than one N-ary row, keeping the schema simple
and each row independently auditable.

Both tall tables' `materialize:` blocks copy `gleif_cik_mapping`'s pattern verbatim
(`format: iceberg`, `catalogType: hadoop`, `overwritePartitions: true`, `runMaintenance:
true`) — full-replace semantics are correct since both are derived, fully recomputable
tables.

### Wide: `ref.canonical_org_entity`

One row per real-world org **known to any registry source**, not just ones with a GLEIF or
EIN match — an org identified only in `patents.patent_assignees` (never resolved) still
gets a row here, with `lei` left null, so callers can query this table uniformly regardless
of match status:

| column | type | notes |
|---|---|---|
| `canonical_entity_id` | string | surrogate PK — the LEI when present; the SEC CIK when EIN-matched but no LEI; else a deterministic hash of the first-seen `(source_schema, source_table, source_column, source_key)` |
| `canonical_name` | string | GLEIF `legal_name` when `lei` is populated, else the longest/most-complete raw source name seen |
| `lei` | string, nullable | |
| `sec_cik` | string, nullable | via EIN exact match |
| `fec_committee_id` | string, nullable | scalar FK — safe 1:1, a committee represents one org |
| `fec_committee_id_confidence` | string, nullable | |
| `sec_reporting_person_cik` | string, nullable | entity-type SEC insiders — SEC's own stable per-filer ID, safe 1:1 |
| `sec_reporting_person_cik_confidence` | string, nullable | |
| `patents_assignee_id` | string, nullable | **lossy, many-to-one — see caveat below** |
| `patents_assignee_id_confidence` | string, nullable | |
| `fda_sponsor_name` | string, nullable | unstructured source — see caveat |
| `fda_sponsor_name_confidence` | string, nullable | |
| `ghg_parent_company_name` | string, nullable | unstructured source |
| `ghg_parent_company_name_confidence` | string, nullable | |
| `cms_paying_entity_name` | string, nullable | unstructured source |
| `cms_paying_entity_name_confidence` | string, nullable | |
| `eia_utility_id` | string, nullable | structured |
| `eia_utility_id_confidence` | string, nullable | |
| `eia_coal_controller_name` | string, nullable | unstructured; `eia_coal_operator_name` is a **separate** column — a mine's controller and operator are logically distinct orgs, don't collapse them |
| `eia_coal_controller_name_confidence` / `eia_coal_operator_name` / `eia_coal_operator_name_confidence` | | |
| `fmcsa_dot_number` | string, nullable | structured; **lossy potential given ~4.47M-row scale — see caveat** |
| `fmcsa_dot_number_confidence` | string, nullable | |
| `faa_registrant_name` | string, nullable | unstructured, entity-type registrants only |
| `faa_registrant_name_confidence` | string, nullable | |
| `exempt_org_ein` | string, nullable | structured, **EIN** |
| `exempt_org_ein_confidence` | string, nullable | |
| `sba_borrower_name` / `sba_lender_name` | string, nullable | unstructured, two distinct roles from the same table |
| `sba_borrower_name_confidence` / `sba_lender_name_confidence` | string, nullable | |

**Multiplicity caveat, called out rather than silently modeled around**: several sources
(`patents_assignee_id` most prominently, but also `fmcsa_dot_number` and any other
ID-keyed-but-many-per-org source) are genuinely many-to-one against a canonical org in
practice — a single scalar column can only carry one representative value per canonical
org, not the full set. Acceptable lossy convenience for the common "give me one row" case;
any query needing the **complete** set for a canonical org must go back to the tall
`entity_org_bridge` table (`WHERE lei = X AND source_schema = 'patents'`, etc.) — the wide
table is a denormalized convenience view, not a substitute, whenever multiplicity matters.

### Wide: `ref.canonical_person_entity`

Same shape, for individuals — one row per real-world person known to any person-type
registry source, whether or not they cross-matched:

| column | type | notes |
|---|---|---|
| `canonical_entity_id` | string | surrogate PK — deterministic hash of the first-seen source key; no external hub key exists for individuals |
| `canonical_name` | string | best-available parsed `(first, last)` name |
| `fec_candidate_id` / `fec_candidate_id_confidence` | string, nullable | |
| `sec_reporting_person_cik` / `sec_reporting_person_cik_confidence` | string, nullable | already 1:1 by SEC's own construction |
| `cms_physician_profile_id` / `cms_physician_profile_id_confidence` | string, nullable | already 1:1 by CMS's own construction |
| `faa_registrant_name` / `faa_registrant_name_confidence` | string, nullable | unstructured, individual-type registrants only |

A row with 2+ FKs populated is a person-track match (e.g. a political candidate who is also
a corporate insider, or a physician who is also a patent inventor's namesake — the latter
not yet in scope since patent inventors weren't added to the person registry this round, see
Explicitly Deferred). A row with only one FK populated is simply "known to that one source,
never cross-matched" — both are legitimate, expected outcomes, not a partial-failure state.

Both wide tables are built as a **pivot over the tall tables plus each source's
otherwise-unmatched rows** (a `FULL OUTER JOIN`-shaped union in the same DuckDB SQL pass),
not hand-maintained independently — same `materialize:` pattern (Iceberg,
`overwritePartitions: true`) as the tall tables, since they're equally derived and
recomputable.

## Extension mechanism (Java-native)

**One listener class, driven by the source registry, builds all four tables in a single
pass** — every org-type and person-type/mixed source shares the same classification,
normalization, and matching code paths; computing them per-source ad hoc would risk drift
between sources. Implement `org.apache.calcite.adapter.govdata.ref.EntityBridgeListener
implements TableLifecycleListener` (interface at
`file/src/main/java/org/apache/calcite/adapter/file/etl/TableLifecycleListener.java:41`),
overriding `afterMaterialize(TableContext context, MaterializeResult result)` (line 158).
The registry itself should be a small, explicit Java data structure (a `List<SourceConfig>`
constant, not external config file, for v1 — no requirement yet for it to be user-editable
outside a code change) enumerating every row in the two tables above.

Add four new tables to `ref-schema.yaml` — `entity_org_bridge`, `entity_person_bridge`,
`canonical_org_entity`, `canonical_person_entity` — each materialized from an empty
placeholder source (`source: type: constants, data: []` — the same pattern already used
elsewhere in this file, e.g. lines 1037/1110/1157/1211, for "table exists with the right
schema, content supplied externally"). Wire the listener on **one** of the four
(`entity_org_bridge`, arbitrarily — the listener writes all four regardless of which one
triggered it):

```yaml
hooks:
  enabled: true
  tableLifecycleListenerClass: "org.apache.calcite.adapter.govdata.ref.EntityBridgeListener"
```

`afterMaterialize` fires once the placeholder(s) commit, and then:
1. Opens its own `jdbc:duckdb:` connection — the same pattern already used by
   `DuckDBExecutionEngine`/`IcebergMaterializer` (`file/src/main/java/.../duckdb/` and
   `.../iceberg/IcebergMaterializer.java`, both open raw DuckDB JDBC connections and run
   `iceberg_scan(...)` today).
2. For each registry entry, reads its source table via
   `iceberg_scan('<warehouse-path>/<schema>/<table>')` against whatever is currently
   materialized, plus `ref.current_gleif_entities` and `sec.filing_metadata` (for the EIN
   hub path) once each, shared across all sources.
3. Runs the classification step on the two mixed sources (`insider_transactions` via the
   token heuristic; `faa_aircraft_registry` via its structured `registrant_type` field),
   then the org-track pipeline (iterating every org-type registry entry) and the
   person-track pipeline (N-way pairwise across every person-type registry entry) per the
   algorithm above — all in DuckDB SQL, reusing the same normalization logic throughout —
   producing the two tall bridge result sets.
4. Pivots each tall result set (plus each source's otherwise-unmatched rows, via a `FULL
   OUTER JOIN`-shaped union across every registry source of that entity type) into the two
   wide canonical result sets.
5. Collects each of the four result sets as its own `List<Map<String,Object>>` and commits
   into its respective Iceberg table via `IcebergTableWriter.writeRecords(...)`
   (`file/src/main/java/org/apache/calcite/adapter/file/iceberg/IcebergTableWriter.java:530`),
   using its `replacePartitionsDataFiles` path to match `overwritePartitions: true`
   semantics — the closest existing "hand it a list of rows" write API; no `List<Row>`/Arrow
   batch API exists in this codebase.

**Ordering caveat, inherent to the architecture, not specific to this design choice**:
there is no cross-schema ordering primitive anywhere in the ETL pool (`run-pool.sh` runs
each schema's worker independently) — nothing guarantees any given source schema finished a
fresh ingest before this listener fires on `ref`. The listener always reads "whatever is
currently materialized" for each registry source, same as any analyst query would. If this
needs tighter freshness guarantees later, that's a pool-scheduling change, out of scope
here.

## Verification plan

1. **`verify-tables` skill** (`govdata/scripts/model-verify.sh --source ref`) after a jar
   rebuild — confirms all four new tables are reachable and readable through the real
   Calcite→DuckDB path, and specifically that each declared primary key has **zero
   duplicates** — the cheapest automated check that "one best match per entity" actually
   holds, and that the wide-table pivot didn't fan out a canonical entity into more than one
   row.
2. **`data-fix` skill** (`--schema ref --table entity_org_bridge`, run once — the listener
   rebuilds all four together) for the dev loop — drops the tables + tracker so the listener
   can be re-triggered repeatedly while tuning thresholds, without a full `ref` re-ingest.
3. **Smoke-test SQL**, once live:
   ```sql
   -- Known companies should resolve exact/high-confidence in the wide table directly
   SELECT canonical_name, lei, sec_cik, fec_committee_id, patents_assignee_id, exempt_org_ein
   FROM ref.canonical_org_entity
   WHERE canonical_name ILIKE '%boeing%' OR canonical_name ILIKE '%apple%'
      OR canonical_name ILIKE '%general electric%' OR canonical_name ILIKE '%lockheed%';

   -- Coverage by source and confidence tier, across every registry entry
   SELECT source_schema, source_table, source_column, match_method, match_confidence,
          COUNT(*) AS n, AVG(match_score) AS avg_score
   FROM ref.entity_org_bridge
   GROUP BY source_schema, source_table, source_column, match_method, match_confidence;

   -- The payoff query this feature exists for: patent output joined to GLEIF-linked orgs
   SELECT c.canonical_name, COUNT(DISTINCT pa.patent_id) AS patents
   FROM patents.patent_assignees pa
   JOIN ref.canonical_org_entity c ON c.patents_assignee_id = pa.assignee_id
   GROUP BY c.canonical_name ORDER BY patents DESC LIMIT 20;

   -- A new cross-schema question this design enables: FDA sponsor R&D vs. patent output
   SELECT c.canonical_name, c.fda_sponsor_name, COUNT(DISTINCT pa.patent_id) AS patents
   FROM ref.canonical_org_entity c
   JOIN patents.patent_assignees pa ON pa.assignee_id = c.patents_assignee_id
   WHERE c.fda_sponsor_name IS NOT NULL
   GROUP BY c.canonical_name, c.fda_sponsor_name ORDER BY patents DESC LIMIT 20;

   -- Person track: any candidate who is also a corporate insider or a paid physician?
   SELECT canonical_name, fec_candidate_id, sec_reporting_person_cik, cms_physician_profile_id
   FROM ref.canonical_person_entity
   WHERE (fec_candidate_id IS NOT NULL AND sec_reporting_person_cik IS NOT NULL)
      OR (fec_candidate_id IS NOT NULL AND cms_physician_profile_id IS NOT NULL);
   ```
   Eyeball that the top-20 patent holders by matched-patent-count look like real large
   assignees (not an artifact of a bad blocking key merging distinct companies), and that
   any person-track matches returned are plausible, not a common-name false positive — both
   need a human glance, not just an automated check.
4. **`fmcsa_carriers` build-time spot check**: given its ~4.47M-row scale (an order of
   magnitude above every other source), time the org-track pass for this source
   specifically during implementation before assuming it's cheap; tighten blocking
   (first-token + state) if it isn't.
5. Log the unresolved-entity count per registry source in the listener's output each run
   (total distinct source entities minus bridged count) — a trend line for match-rate drift
   per source, without a dedicated dashboard.

## Critical files

- `govdata/src/main/resources/ref/ref-schema.yaml` — add all four new tables +
  `tableLifecycleListenerClass` hook; source of the `current_gleif_entities` dedup view
  (line 1902) and the `gleif_cik_mapping` materialize block to mirror (lines 260–345).
- `file/src/main/java/org/apache/calcite/adapter/file/etl/TableLifecycleListener.java` —
  the interface to implement (`afterMaterialize`, line 158).
- `file/src/main/java/org/apache/calcite/adapter/file/etl/SchemaLifecycleProcessor.java` —
  where the listener class is loaded via reflection (`loadTableListener`, ~lines 893–920)
  and invoked (`processTable`, ~line 335).
- `file/src/main/java/org/apache/calcite/adapter/file/iceberg/IcebergTableWriter.java` —
  `writeRecords(...)` (line 530) is the write path.
- `file/src/main/java/org/apache/calcite/adapter/file/execution/duckdb/DuckDBExecutionEngine.java`
  and `file/src/main/java/org/apache/calcite/adapter/file/iceberg/IcebergMaterializer.java` —
  existing examples of opening a raw DuckDB JDBC connection and running
  `iceberg_scan(...)`, the pattern the new listener's read side follows.
- Source-side schema YAMLs for every registry entry's column/PK definitions:
  `govdata/src/main/resources/fec/fec-schema.yaml`,
  `govdata/src/main/resources/patents/patents-schema.yaml`,
  `govdata/src/main/resources/sec/sec-schema.yaml` (also `filing_metadata.irs_number` for
  the EIN hub path), `govdata/src/main/resources/health/health-schema.yaml`
  (`fda_drug_approvals`, `cms_open_payments`), `govdata/src/main/resources/environment/environment-schema.yaml`
  (`ghg_facilities`), `govdata/src/main/resources/energy/energy-schema.yaml`
  (`eia_utility_annual`, `eia_coal_mines`), `govdata/src/main/resources/transport/transport-schema.yaml`
  (`fmcsa_carriers`, `faa_aircraft_registry`), `govdata/src/main/resources/fiscal/fiscal-schema.yaml`
  (`exempt_org_master`, `sba_loan_approvals`).

## Explicitly deferred

- A live, ad-hoc `JARO_WINKLER()` Calcite SQL function (the LEVENSHTEIN/SOUNDEX 4-site
  registration pattern) — not needed for this batch job, which never goes through Calcite's
  SQL layer. Worth revisiting only if someone wants to run fuzzy name queries ad hoc through
  AskAmerica itself.
- A `_candidates` sibling table for sub-0.85 (org) near-misses — currently just dropped +
  logged; revisit if a manual-review workflow becomes a real need.
- Cross-matching org-type sources directly against each other (bypassing GLEIF/EIN as the
  hub) — out of scope; org matching here is hub-and-spoke only, so two orgs that both fail
  to match GLEIF/EIN but would plausibly match each other are not linked. Revisit only if
  that gap turns out to matter in practice.
- Further sources not included in this sweep: patent **inventors** (`patents.patent_inventors`,
  a fourth potential person-type source — individuals, not orgs, deliberately not added
  this round since it wasn't part of the live catalog sweep that produced this registry);
  `health.clinical_trials.lead_sponsor` and `health.fda_drug_recalls.recalling_firm`/
  `labeler_name` (found during the sweep, both real org-name candidates, left out of the v1
  registry table above only because they weren't independently `describe_table`-confirmed
  for exact column names the way every table in the registry was — add them the same way as
  any other registry entry once confirmed, not a structurally different case); anything in
  `lands`, `ag`, `cyber_vuln`, `fedregister`, `disasters`, `housing`, `edu` — swept and found
  to have no real entity-name candidates worth bridging, not merely unexamined.
