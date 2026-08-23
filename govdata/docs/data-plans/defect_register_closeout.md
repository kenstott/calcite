# Govdata Defect Register — Close-Out Plan

**Status:** plan only, nothing in this document has been implemented.
**Date:** 2026-08-21.
**Goal:** land a real fix for every remaining open item in the Govdata Defect Register (Batch 2's
5 defects + 3 of its 4 "Recommendation" entries — `ref.entity_overrides` is out of scope here, see
`dq_cleansing.md`) and add each one to the Remediation Ledger as it lands, in the same
cross-referenced row format already used for Batch 1.

Batch 1 (9 defects) is fully closed out and in the ledger already — not covered here.

---

## 1. Inventory, current state

| Item | What it is | Size/risk | Status as of this plan |
|---|---|---|---|
| B2-5 | `sec.filing_metadata` table comment overstates what the table carries (item codes) | trivial — comment only | not started |
| B2-3 | `census.acs1_income` declares 2009–2025, real data is 2005–2024 | trivial — one yearRange decl | not started |
| Rec. 1 | `ref.org_entity_sec` view (canonical_org_entity ⋈ gleif_cik_mapping) | small — new view | not started |
| Rec. 2 | `ref.org_entity_ultimate_parent` view (canonical_org_entity ⋈ current_gleif_parents) | small — new view | not started |
| Rec. 3 | `ref.patent_assignee_firm` view (corrected scope: one predicate + ultimate-parent hop, not a 4-hop chain) | small — new view | not started |
| B2-2 | `entity_org_bridge.sec_cik` / `canonical_org_entity.sec_cik` ~0% populated | medium — new match path + full-table rebuild | not started; **root cause now confirmed in code**, see §3 |
| B2-4 | `canonical_org_entity.patents_assignee_id` picks an arbitrary id among duplicates (Schlumberger: 11 vs 10,227) | medium — match logic change + rebuild | not started |
| B2-1 | `entity_org_bridge` cross-product: 6 LEIs share `source_name_normalized='apple'`, each asserted with identical evidence | medium — match logic change + rebuild | not started |
| Rec. 6 | "Discovery ordering" — tool/description cross-references so callers find `ref` before guessing | mixed — 2 of 3 sub-items already done per the register | **sequence after B2-2** |
| Rec. 5 | "Backfill flag" — temporary `backfill: in_progress` schema field, surfaced by `describe_table` | small–medium — depends where `describe_table` lives | needs scoping (§3) |

---

## 2. Sequencing tiers

**Tier 0 — trivial, zero data-shape risk, no verification beyond a read-back.** Do these first;
there's no reason to sequence them behind anything else.
- B2-5 (comment fix)
- B2-3 (yearRange declaration fix)

**Tier 1 — new SQL views over existing `ref` tables.** No base-table schema change, so none of
these carry the drop+recreate risk that hit `eia_electricity_generation`/`usaspending_by_state`
this session (see the Remediation Ledger's incident note) — a view is query-time only.
- `ref.org_entity_sec`
- `ref.org_entity_ultimate_parent`
- `ref.patent_assignee_firm` (build to the corrected, narrower description — it pre-resolves the
  ultimate parent; it is not removing a 4-hop chain, `canonical_org_entity` already carries
  `sec_cik` and `patents_assignee_id` directly)

**Tier 2 — `EntityBridgeListener.java` match-logic changes, each requiring a real rebuild of
`entity_org_bridge`/`canonical_org_entity` (a ~773K-row and ~9M-row table respectively).** This is
exactly the class of change that triggered this session's `eia_electricity_generation`/
`usaspending_by_state` incident when tested directly against production — **verify every Tier 2
item against `govdata-parquet-v1-dq` first** (`worker-dq-run.sh --rebuild`, with
`govdata/.env.dq` sourced so `GOVDATA_DQ_BUCKET` actually resolves to the isolated bucket — see
memory `test-schema-changes-via-dq-bucket`). Only promote to a production rebuild once the DQ-
bucket run looks right.
- B2-2 (add the LEI→CIK match path — see §3, this is new logic, not a broken existing path)
- B2-4 (select `patents_assignee_id` by volume among duplicates — interim fix; the register's own
  "fix the shape first" tier, changing the column to an array, is a bigger redesign and is
  explicitly deferred here, not folded in)
- B2-1 (reserve `match_confidence='high'` for genuine 1:1 matches, demote fan-out to `ambiguous`
  — the register's own cheapest tier; the fuller bridge restructuring with an `is_primary` flag is
  deferred, and may end up folded into the `dq_cleansing.md` `entity_overrides` mechanism rather
  than built twice)

**Tier 3 — needs scoping before it can be sequenced at all.**
- Rec. 6's residual cross-reference work (`entity_org_bridge.sec_cik` → `gleif_cik_mapping`)
  cannot land before B2-2 populates `sec_cik` — pointing a caller at an empty column is worse than
  not pointing them anywhere. **Depends on Tier 2 / B2-2.** Also needs locating: the register
  describes this as tool descriptions and server instructions, which likely live in
  `askamerica-engine`, not `govdata` schema YAML — confirm the actual location before scoping
  further; not investigated for this plan.
- Rec. 5 ("backfill flag") needs one question answered before it can be sized: does
  `describe_table` live in `govdata`, or in the shared `file/` module? If the latter, this needs a
  presented plan and approval before implementation, per `file/CLAUDE.md`'s governance rule (the
  same rule that deferred `eia_electricity_generation`'s month-duplication fix earlier this
  session). Not investigated for this plan — first task when this tier is picked up.

---

## 3. What's actually broken, per Tier 2 item (grounded in the real code, not just the register's own read)

**B2-2 — confirmed root cause, more precise than the register's framing.**
`EntityBridgeListener.java:374-379` documents it directly: `sec_cik` is *only ever* populated when
`match_method='exact_ein'`, and the only source that can produce an EIN match is
`fiscal.exempt_org_master`. In the live dataset, `exact_ein` produced **zero** matches across the
entire ~773K-row result set (confirmed there via a full `GROUP BY match_method` scan) — there is
no non-null `sec_cik` value anywhere in the current pipeline for it to have missed. This is not a
bug in an existing path; it's a **missing path**. The register's suggested fix — join
`ref.gleif_cik_mapping` on the `lei` both tables already carry — is a second, independent way to
populate `sec_cik` that has never been wired into the match cascade at all. Concretely: add a
match tier alongside the existing `exact_ein`/`exact_normalized`/fuzzy tiers (near
`EntityBridgeListener.java:556-560`, where `match_method`/`match_confidence` are currently
assigned) that resolves `sec_cik` via `gleif_cik_mapping` when an LEI match already exists but
`exact_ein` didn't fire.

**B2-4 — confirmed location, not yet root-caused to a specific line.** `patents_assignee_id` is
one of the columns in `ORG_SOURCES` (`EntityBridgeListener.java:167`), pivoted per-entity in
`pivotOrg()` (`EntityBridgeListener.java:596`). The arbitrary-selection behavior is somewhere in
that pivot's aggregation when a name maps to more than one `assignee_id` — not yet traced to the
exact aggregate function (likely a bare `FIRST`/`MIN`/`ANY_VALUE` over the candidate ids). First
task when this item is picked up: confirm the exact aggregate, then change it to prefer the id
with the higher associated patent count.

**B2-1 — confirmed location.** `EntityBridgeListener.java:511` and `:545` are where
`match_confidence` gets set to `'high'` for `exact_normalized` and above-threshold fuzzy matches,
with no check for how many distinct LEIs a given `source_name_normalized` resolves to. The fix is
a post-match step (or a window-function check within the same query) that counts distinct LEIs per
normalized name and downgrades `match_confidence` to `'ambiguous'` wherever that count is `> 1`,
before the final write.

---

## 4. Verification approach (per tier)

- **Tier 0:** read the file back, confirm the string changed. No ETL run needed for B2-5 (pure
  comment). B2-3 needs one force-reprocess against `acs1_income` to confirm the corrected
  `yearRange` actually reflects 2005–2024 with 2020 still absent — cheap, single-table, no column
  change, no incident risk.
- **Tier 1:** each view is a `CREATE VIEW ... AS SELECT` over existing tables — verify by running
  the view's query directly against real data and spot-checking row counts against the numbers
  already measured in the register (`org_entity_sec` ≈ 20,231 rows today, `patent_assignee_firm` ≈
  515,241 entities carrying a patent id). No ETL rebuild needed at all.
- **Tier 2:** DQ-bucket rebuild first (§2), then spot-check the specific cases the register already
  measured — Schlumberger should link to the 10,227-patent id after B2-4, `source_name_normalized
  ='apple'` should show `ambiguous` (not 6× `high`) after B2-1, and a real sample of `sec_cik`
  values should resolve after B2-2 (cross-check a few against the register's `gleif_cik_mapping`
  pair count). Only promote to production once these hold in the DQ bucket.

---

## 5. Ledger integration

Each item gets a row (or small group, for Tier 1's three views) in the Remediation Ledger once
implemented and verified, following the existing "Defect Register batch 2" row format: slot
(`ref` for everything except B2-3, which is `census`), tables touched, issue text cross-referencing
the defect id, run/succeeded chips, and evidence quoting the real numbers from verification (§4) —
not `as_of` movement alone, consistent with every other row in the ledger.

Tier 3 items, once scoped, get their own row(s) too — or, if Rec. 5/6 turn out to require a
`file/`-module governance plan (per §2), a pointer row noting that status, same treatment as
`eia_electricity_generation`'s deferred month-duplication axis already gets today.

---

## 6. Related

- Govdata Defect Register, Batch 2 (B2-1 through B2-5) and "Recommendation · not a defect"
  (all four entries — `entity_overrides` handled separately).
- `dq_cleansing.md` — the `entity_overrides`/`db_cleansing` plan; B2-1's fuller restructuring may
  end up unified with that mechanism rather than built as a one-off (§2, Tier 2).
- Remediation Ledger — target for every completed item here; already carries the "Defect Register
  batch 2 · new findings, reviewed but not yet started" group this plan closes out.
- `govdata/src/main/java/org/apache/calcite/adapter/govdata/ref/EntityBridgeListener.java` — the
  single file that owns all of B2-1/B2-2/B2-4.
- Memory `test-schema-changes-via-dq-bucket` — the verification discipline every Tier 2 item must
  follow, written up after this session's own incident on two other tables.
