# Judge: Sen. Rosen EAD/work-permit claim fact-check (q3), 3-persona 2026-09-29

Rank: [askamerica, expert, everyman]

## Why

Strong triple convergence: all three verified Rosen's quote verbatim against her own floor
remarks/press release, confirmed her figures (3.5M/600K/500K+) are a conservative rounding-down of
FWD.us's own published analysis (3.8M/613K/514K) — she understated, not exaggerated — and confirmed
the underlying DHS/USCIS rule remains in effect nationwide with no court-ordered block as of
2026-09-29.

**`askamerica`** used its own warehouse table (`econ.national_wages`, BLS QCEW) to independently
sanity-check the industry sub-figures (construction ~8.44M total → 7.1% share; leisure/hospitality
~17.25M total → 3.0% share), correctly disclosed the genuine gap (no EAD-holder-by-industry table
exists in this corpus) rather than guessing, and verified Rosen's quote directly against her own
site. Most direct provenance of the three.

**`expert`** independently reached the same figures via BLS's own August 2026 release (construction
8.36M, leisure/hospitality ~16.9-17.0M) — consistent with askamerica's QCEW-based numbers — and
added a genuinely useful nuance: FWD.us's own "1 in X" shorthand framing uses a somewhat different
denominator than the raw BLS comparison, a detail neither other persona flagged.

**`everyman`** reached the same substantive conclusion with comparable source quality (FWD.us
directly, BLS totals) but slightly less precise sourcing than the other two.

## Corpus gap — filed

No table covers EAD/work-permit holder counts by industry or immigration category. Checked
`gh issue list` first — no match. Filed as
[kenstott/govdata-ops#773](https://github.com/kenstott/govdata-ops/issues/773), `type:sourcing`,
`kind:gap`, `schema:census`, `status:open` — noted honestly as a harder-to-close gap (FWD.us's own
methodology combines ACS microdata with USCIS administrative data, not a simple missing ingest).

## Severity check

`askamerica` ranked #1 — no severity flag.

## Recipe check

Nothing filed — all three personas performed well; the gap is data availability, not guidance.
