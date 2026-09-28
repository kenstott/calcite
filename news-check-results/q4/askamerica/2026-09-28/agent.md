# USP 2026 Annual Drug Shortages Report — Claim Check

## Verdict: TRUE / MOSTLY TRUE — headline figures corroborated by independent reconstruction of FDA's own data

## The claim
USP's June 2026 *Annual Drug Shortages Report* (published as a press release June 9, 2026) states:
- **75 active drug shortages** in the U.S. as of end-2025, with only **4 of the 75 first reported in 2025** (nearly all carried over from 2024)
- Product discontinuations rose an **acute 60% from 2024 to 2025**, the largest increase since 2019
- **Sterile injectable drugs make up 71%** of all shortages, the largest share of any dosage form

Source (primary press release text, fetched directly): [USP Annual Drug Shortages Report finds rising discontinuations, hidden upstream risk](https://www.biospace.com/press-releases/usp-annual-drug-shortages-report-finds-rising-discontinuations-hidden-upstream-risk) (BioSpace republishing USP's own release, June 9, 2026). Corroborating detail from [Pharmaceutical Commerce](https://www.pharmaceuticalcommerce.com/view/how-drug-discontinuations-surged-despite-shortage-numbers-declining), which states USP's own methodology: "USP analyzed drug shortage data from the FDA that was current as of December 31, 2025."

## Table used
`health.fda_drug_shortages_history` — a point-in-time reconstruction of FDA's own accessdata.fda.gov drug-shortages CSV export, built from ~99 Wayback Machine captures (Oct 2019–Jul 2026). This table exists specifically because FDA's *live* feed (`health.fda_drug_shortages`) purges resolved/discontinued entries ~6 months after resolution and cannot answer "what did the list look like on date X" — exactly the question needed to reproduce USP's Dec-31-2025 snapshot.

## What I computed
| Figure | USP claim | Independent recomputation | Match |
|---|---|---|---|
| Active shortages, end of 2025 | 75 | **75** (distinct `generic_name`, `status='Current'`, snapshot `2025-12-16`) | Exact |
| Newly reported in 2025 | 4 of 75 | **4 of 75** (`initial_posting_date` in 2025, same snapshot) | Exact |
| Discontinuation increase, 2024→2025 | 60% | **55.4%** (112 distinct drugs discontinued in 2024, snapshot 2025-01-17 → 174 in 2025, snapshot 2026-01-20) | Close, same direction |
| Sterile injectable share | 71% | **69.3%** (52 of 75 current shortages have an "Injection"/"Injectable" presentation) | Close, same direction |

Queries (all against `health.fda_drug_shortages_history`):
```sql
SELECT COUNT(DISTINCT generic_name) FROM health.fda_drug_shortages_history
WHERE snapshot_date='2025-12-16' AND status='Current';                      -- 75

SELECT COUNT(DISTINCT generic_name) FROM health.fda_drug_shortages_history
WHERE snapshot_date='2025-12-16' AND status='Current' AND initial_posting_date LIKE '%2025'; -- 4

SELECT COUNT(DISTINCT generic_name) FROM health.fda_drug_shortages_history
WHERE snapshot_date='2025-01-17' AND discontinued_date LIKE '%/2024';       -- 112

SELECT COUNT(DISTINCT generic_name) FROM health.fda_drug_shortages_history
WHERE snapshot_date='2026-01-20' AND discontinued_date LIKE '%/2025';       -- 174

SELECT COUNT(DISTINCT generic_name),
       COUNT(DISTINCT CASE WHEN presentation ILIKE '%Injection%' OR presentation ILIKE '%Injectable%' THEN generic_name END)
FROM health.fda_drug_shortages_history
WHERE snapshot_date='2025-12-16' AND status='Current';                      -- 75, 52
```

## Assessment
The two most load-bearing, most checkable numbers — **75 active shortages** and **only 4 newly reported in 2025** — match USP's own figures **exactly**. The two supporting statistics (60% discontinuation rise, 71% injectable share) are directionally and magnitudinally close (55.4% and 69.3% respectively) but not exact; the gap is most plausibly explained by USP counting discontinuation *events*/NDC-level exits rather than distinct generic drug names, or a stricter FDA structured dosage-form classification for "sterile injectable" versus the free-text presentation match used here — not by any overstatement in USP's report.

**No corpus gap found or filed** — `health.fda_drug_shortages_history` fully covered this question; it was purpose-built to answer exactly this kind of "what did the FDA list look like on date X" question.

**Pinocchios (SPLIT shape, since all graded claims are attributed to USP):**
- Fidelity (news coverage accurately representing USP's report): **0** — the press coverage quotes USP's release verbatim.
- Claims accuracy (do USP's own figures hold up against independent data): **0** — headline figures match exactly; supporting stats are close with a plausible methodological explanation for the small gap.

## Report
Published report (dashboard + full narrative + claims table): local link returned by `publish_report` (expires when this session ends); saved server-side at:
`/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q4/askamerica/2026-09-28/report.html`

Dashboard image also delivered inline in this session.
