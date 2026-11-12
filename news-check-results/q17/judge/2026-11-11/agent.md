# q17 judge: AJMC "Uninsured Rate Rises to 11.2% as Coverage Erodes Before Medicaid Cuts"

Rank: [expert, askamerica, everyman]

The story ran Oct 22, 2026, the same day the Commonwealth Fund released its 2026 Biennial Health Insurance Survey ("The State of Health Insurance Coverage in 2026", doi 10.26099/e0eb-0g78). Judged 2026-11-11. askamerica ran on the production engine. The Commonwealth Fund and Fierce pages return HTTP 403, so I verified against AJMC's own text, actuary.info (which reproduces the report's text and exhibits), KFF (read directly), and search coverage (Becker's, Insurance Business, Healthcare Dive, AHA).

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| expert | 1 | KFF quick insight; KFF/CCF Georgetown CBO summaries; actuary.info report reproduction; CF report blocked |
| everyman | 1 | KFF quick insight; CF report blocked; survey figures via secondary coverage |
| askamerica | 0 | KFF quick insight; HHS ASPE brief; warehouse health.aca_effectuated_enrollment_monthly; CF report blocked |

## Assessment
- **Correct verdict: 0 Pinocchios.** Every figure that can be traced matches the source report exactly. The one doubtful item, the CBO 7.5M, is the report's own framing, which AJMC passed on. Nothing in the piece is shaded or false.
  - **Uninsured 8.9% (2024) to 11.2% (2026), ages 19-64: true.** It matches the report's text, AHA and Healthcare Dive. The headline omits "working-age", but the body states it.
  - **46% = 11% + 12% + 23%: true.**
  - **Marketplace/individual 8.5% (2024) to 6.4% (2026): true.** It matches the report's text. The exhibits round to 8% and 6%, which explains why coverage that used the exhibits showed "about 8% to 6%".
  - **Medicaid 15.5% (2022) to 13% (2026): true.** It matches the report's text; the exhibits show 16%/15%/13%. The baseline year differs from the headline's 2024, but the piece states 2022.
  - **$750 to $4,035 premium rise by income: true to the report.** Insurance Business and other coverage of the survey carry the same range.
  - **73% federal responsibility, with 93% Democrats / 76% Independents / 45% Republicans: consistent with the report.** No source independent of AJMC printed the party splits, but no source contradicts them either. Search summaries attribute the 73% wording to the report.
  - **SSRS, 9,360 adults 19+, May 7 to Jul 20, with 7,362 aged 19-64: true.**
  - **KFF 22.1M to 19.2M, -13%, with the administration citing fraud: true.** KFF's wording is "from a high of 22.1 million people in 2025", so 22.1M is the 2025 peak, not February 2025 (21.8M). The like-for-like February drop is 12%. This is a minor imprecision copied from KFF.
  - **CBO 11.5M net Medicaid losses and 7.5M marketplace reduction: the report's framing, not settled.** Becker's renders the same pair as "11.5 million in Medicaid and 7.5 million in marketplace plans", so AJMC is faithful to its source. KFF's CBO summary gives "more than 11 million" fewer Medicaid enrollees and 7.5M more *uninsured* in 2034 (from Medicaid changes). CBO's best-known 7.5M is therefore a Medicaid uninsured figure, not a marketplace enrollment figure. No CBO document tying 7.5M to marketplace enrollment was found, so the attribution is doubtful, and the error, if any, is upstream of AJMC. This is not enough for a Pinocchio on a summary article.
  - The work requirement of 80 hours a month starting in 2027 is true.
  - Unverifiable is not the same as shaded. A Pinocchio for "could not confirm" or "advocacy-adjacent survey" is a methodology error, not a finding against the piece.
- **expert (1st):** the most complete and accurate findings.
  - It confirmed marketplace 8.5 to 6.4, Medicaid 15.5 to 13 (noting the 2022 baseline), and the 93/76/45 splits.
  - It found the 7.5M CBO attribution problem by tracing CBO's actual 7.5M (uninsured from Medicaid changes, 2034). This is the one substantive issue in the piece, and only this arm identified it.
  - Weaknesses: it left $750-$4,035 unchecked when coverage carrying that range was findable. Its rating of 1 is one step off: it charged AJMC for a framing that comes from the source report.
- **askamerica (2nd):** correct rating, with good handling of what it could not verify.
  - It explicitly separated "unsupported" from "false".
  - It ran a warehouse like-for-like check (Feb 2025 21.78M to Feb 2026 19.17M, -12%) and correctly identified 22.1M as KFF's 2025 peak.
  - It confirmed the sample details and the subgroup figures.
  - Weaknesses: it never extracted or checked the marketplace 8.5 to 6.4 and Medicaid 15.5 to 13 claims, which carry the article's coverage-erosion theme. It did not find the $750-$4,035 range or the party splits. It did not notice the CBO 7.5M mislabel, citing an unrelated Urban Institute 10M instead.
- **everyman (3rd):** wrong rating, and the fewest claims verified.
  - It verified only "about 8% to 6%" for the marketplace claim, and it left Medicaid, premiums, CBO and the field dates unchecked.
  - Its 1 Pinocchio rests mainly on what it could not confirm and on a nuance in the 73% wording. Neither is shading by AJMC.
  - Positives: it read KFF directly, raised the federal-survey comparison (NHIS 8.3% all ages), and noted that 9,360 vs 7,362 is the full sample vs the analysis base.

## Recipe check
Partly. A reusable procedure for checking survey reports would have closed askamerica's gaps:
- **Claim extraction must cover every headline-theme number.** The marketplace and Medicaid share shifts were never put into claims.json. A recipe step that maps each number in the lede and subhead to a claim would have caught this.
- **When the primary report is blocked:** try report-mirroring analyses (actuary.info here reproduces the text and exhibits) and trade coverage before marking a claim unsupported. The $750-$4,035 range was in Insurance Business coverage.
- **"CBO projects X" claims:** fetch the CBO table or a KFF/CCF summary and match both the metric (enrollment vs uninsured) and the program. A metric mismatch is a finding even when the number matches.
- The log shows `find_recipe` ran at seq 13, after the first `preview_report` (seq 12) and after every verification fetch. The `recipe_not_consulted:high` diagnostic fired. Again, the procedure was consulted only after grading.
- Warehouse coverage: no table carries 2026 survey coverage shares (SAHIE ends 2023), so the web was the right route for those. The ACA effectuated enrollment table was used appropriately.

## Severity invariant
**Not triggered:** askamerica ranks above everyman. askamerica's rating (0) matches the correct verdict; everyman's (1) does not.

## Defects
- askamerica: claim extraction dropped two central coverage-share claims (marketplace 8.5 to 6.4, Medicaid 15.5 to 13). `find_recipe` was called only after the first report preview.
- askamerica: CBO-figure checks do not test metric/program attribution (enrollment vs uninsured), so the 7.5M mislabel went unnoticed.
