# Judge: Office of the Governor, "California is booming" (q12), 3-persona 2026-11-10

Rank: [askamerica, expert, everyman]

Source: https://www.gov.ca.gov/2026/11/10/california-is-booming/ (Nov. 10, 2026; governor's press release, not news).

**Engine build:** the private dev jar for 0.128.0 (`~/.askamerica/engine-dev`). **Data server:** `~/.askamerica/pgwire-dev` on port 45533.

Grades: askamerica 1 Pinocchio, expert 2 (with 3 for the zero-deficit line), everyman 2.

## Judge's own verification

- **Unemployment:** EDD confirms 5.1% for September 2026. BLS puts DC highest at 5.6% and the US at 4.2%. Newsweek has California tied with Connecticut just behind DC. Among the 50 states, that makes California tied for highest. Two arms describe this correctly. Askamerica's "tied second-highest among states, behind DC" counts DC as a state, which is a small slip in wording.
- **Deficit:** I did not confirm expert's "LAO (11/5/2026) $18.5B structural deficit". The LAO's Nov. 5, 2026 report is a retrospective, "From Pandemic to AI Boom", and its contents did not come up in my search. The $18B for 2026-27 is the November 2025 outlook, and the 2026 outlook is still pending. Expert's direction, that independent analysts project structural deficits, is right. The citation may be dated a year off.

## Why

**`askamerica` is first.** It is the only arm that checked the hard economic figures against primary data:
- **GDP:** BEA nominal GDP rose from $2.90T (2018) to $4.30T (2025), +48%. The "$4.5T, more than 50%" claim relies on 2026 figures BEA has not published, and the figure is nominal; real growth is about 20%. This is the most substantive finding about the release's lead claim.
- **Manufacturing:** $390.5B vs Texas $332.1B (+17.6%), confirming the "18% larger" claim. Expert graded this true from the governor's own number, and everyman could not check it.
- **University R&D:** $14.37B, highest of any state (NSF).
- **Wages:** 24% above the US average by QCEW, so the release's "20%" understates. It disclosed the suppressed-row and DC handling.
- **Exports:** USTR confirms the record. It also noted the second-place ranking behind Texas, which the release leaves out.
- **Small-business program:** it caught that the CalOSBA page contradicts the 17,600-business figure.

Two weaknesses:
- It left venture capital, payroll jobs, minimum wage and the $7.7B revenue claim ungraded, though everyman confirmed each from the web in minutes.
- It never addressed the "zero projected deficit" claim, the release's most contestable fiscal statement.

The 1 Pinocchio grade is defensible on what it checked. Its coverage is narrower than its depth.

**`expert` is second.** It graded the most claims:
- It is the only arm that confronted the zero-deficit line, with a separate 3-Pinocchio sub-grade.
- It caught that the national 496,000-jobs figure is not in the BLS state release.
- It gave Controller numbers for the revenue claim.

Weaknesses:
- It called $391B manufacturing "True" on the governor's own figure. That check is circular.
- It left GDP "plausibly true, unverified" and used trade-press numbers.
- Its LAO citation may be misdated (see above).
- It answered in a templated format that rates every claim "medium" confidence regardless of evidence.

**`everyman` is third.** It produced clean primary checks on jobs, minimum wage, exports, farm sales and VC. It was the only arm to note September's 9,300-job monthly loss behind the favourable 12-month window, and that PitchBook gives about 70% against Crunchbase's 75%. But manufacturing, patents and the $7.7B were left "not checkable", and the deficit claim is only "deserves scrutiny".

## Severity invariant

`askamerica` ranked above `everyman`. No flag.

## Defects

- **govdata-ops #1160:** `econ.trade_by_state` is unreadable on R2. The current snapshot references a missing `year=2025/month=11/compacted_08192c33.parquet`, so every query returns a 404, including year=2024. Verified live. Askamerica fell back to USTR for exports.
- **Engine SQL, not filed:** `GROUP BY 1` against a quoted column (`"year"`) fails with "Expression 'year' is not being grouped".

## Recipe check

Askamerica's depth-over-breadth pattern: it ran 11 warehouse queries but left web-checkable central claims ungraded, including the release's contestable fiscal claim. The same pattern appeared on the q-series 2026-11-09 runs, as bundling and skipping. A recipe nudge belongs here: for a government press release, enumerate every numeric claim and either grade it or say why not, and treat budget and deficit claims as central. Not changed in this run, because one more instance is needed under the second-instance discipline.
