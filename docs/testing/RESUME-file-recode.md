# RESUME — file-adapter RECODE backlog (session checkpoint 2026-06-29)

Pick up here after the machine restart. WSL gradle was dropping connections
(`Wsl/Service/0x8007274c`) at checkpoint time; the reboot should clear it.

## Where we are

Working the **RECODE backlog** = the 23 requirements demoted from the 31 "verified-strong LINK"
pass (a golden-bar review found 23 of 54 grader-"STRONG" tests too weak). Each RECODE = write a
golden into a themed `*RequirementsTest`, run it green, link the ledger, flip coverage-map, archive
the fully-superseded weak test.

Earlier in the session (already committed history): applied 31 LINKs + marked 23 RECODE in
`docs/testing/coverage-map.yaml` and `docs/requirements/file.yaml`.

## VERIFIED GREEN (ran BUILD SUCCESSFUL before WSL died)

- **FILE-134, FILE-132** — `RefreshLogicRequirementsTest` (15 tests, 2 skipped, 0 fail). Refresh
  interval parse + lazy-trigger gate. `RefreshIntervalTest` `git mv`'d to
  `file/test-archive/org/apache/calcite/adapter/file/refresh/`.
- **FILE-130** — `ConverterUtilRequirementsTest` (22 tests, 0 fail). needsConversion all 5 branches.
- **FILE-055** — `CsvInferenceRequirementsTest` (15 tests, 3 skipped, 0 fail) — token set + binary
  nullable rule; `nullableThreshold` is dead code → **C-32**.

Ledger flips DONE for these: 134/132/130 → `complete`; 055 → `in-progress`. coverage-map: 134/132/130
→ DONE, 055 → STAGED. C-32 appended to contradictions.md. complete count was **131**, MUST **93/156**.

## UNVERIFIED — written after the last green run, NOT yet compiled/run

- **FILE-052** in `CsvInferenceRequirementsTest`: 3 active methods added
  (`defaultConfigObjectIsEnabledWithExpectedDefaults`, `schemaDefaultIsDisabledSoColumnsStayVarchar`,
  `enabledFromMapAppliesPerFieldDefaults`). Imports `HashMap`/`Map` added. The `@Disabled` alias
  target was **removed** (see decision below). **MUST re-run the class before claiming green.**

## Decisions from the user (apply these)

1. Doc-vs-code default when a recode finds an unimplemented clause: **keep as code-bug, leave req
   in-progress** with `@Disabled` target + contradiction. EXCEPT the two below.
2. **C-32 (FILE-055 nullableThreshold)**: nullability is *advisory* (parquet/duckdb never enforce
   NOT NULL; the flag is for humans + optimizer). So **downgrade C-32** from blocking bug to a
   low-priority "remove the dead `nullableThreshold` field" cleanup, **reword FILE-055**'s threshold
   clause to the advisory binary rule, and mark **FILE-055 complete**. (My recommendation — user
   said "thoughts?"; confirm if desired, but their intent is clear.)
3. **FILE-052 flat JDBC-URL aliases**: downgrade to a **proposed feature** (not a bug). FILE-052
   itself (config defaults) → **complete**; add a new `proposed` req **FILE-174** for the aliases.

## NEXT STEPS (in order)

1. Confirm WSL: `wsl bash -lc "cd /mnt/c/Users/Admin/calcite && ./gradlew :file:test --tests 'org.apache.calcite.adapter.file.CsvInferenceRequirementsTest' --console=plain"`.
   Expect ~18 tests, 3 skipped (C-08, C-09, C-32), 0 fail.
   IMPORTANT: gradle's exit code is masked when piping to `tail`; grep the log for
   `BUILD SUCCESSFUL/FAILED` instead, and read the junit XML for counts.
2. If green: bookkeep **FILE-052** → `complete` + the 3 `tests:` + `supersedes: [ format/csv/CsvTypeInferrerTest#testDefaultConfigReturnsExpectedDefaults ]` + notes; coverage-map FILE-052 → DONE.
3. Add **FILE-174** (proposed, group config, "flat JDBC-URL aliases csvInferTypes/... map to the
   csvTypeInference operand" — feature, no test yet).
4. Apply decision #2: reword FILE-055 description (advisory binary rule), set `complete`, reframe
   C-32 status to advisory/low-pri, coverage-map FILE-055 → DONE. (Keep the `@Disabled` C-32 method
   as documentation, or delete it — user's call.)
5. Validate: `python3 docs/testing/validate_requirements.py --coverage`.
6. Continue backlog. **FILE-060 already investigated** (this session): embedded multi-table `__`
   names emitted by converters as `<base>__<table>.json` (XmlToJsonConverter.java:152 etc.); subdir
   crawl joins path with `__` (FileSchema.java:5018 `replace("/","__")`). Target
   `TableExtractionRequirementsTest` (has `tableJsonNames`/`set` helpers). XML-embedded + subdir-join
   are clean; the literal `slides__slide__table` PPTX triple is the only risky bit (verify
   `PptxTableScanner` entry point / POI XSLF fixture hermeticity first).

## Remaining RECODE backlog after FILE-052

032, 049, 060, 066, 085, 087, 094, 114, 117, 118, 119, 146, 163, 164, 166, 169, 173.
Sub-groups: integration/not-hermetic (030 done? no—030 still pending: 030, 066, 087, 163, 169 need
hermetic rewrites or @Tag move); contradiction-blocked (117/C-07 throw, 119/C-02 duckdb-if-available)
need the code fix first; the rest are partials. (030 is in the demote set too — not yet done.)
</content>
