# q5 judge: CCF Georgetown "Millions of low-income children are losing both Medicaid/CHIP coverage and SNAP benefits"

Rank: [askamerica, expert, everyman]

The story ran Nov 9, 2026 (Yafimenka and Park). Judged 2026-11-12. The CCF page and the CBPP tracker blocked direct reads for some arms; I read the article through WebFetch (summary form) and verified against CBPP's published blog and tracker summaries and news coverage of them. askamerica's answer carries a harness note: its report could not be saved because no article copy loaded.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| askamerica | 1 | Warehouse health.medicaid_chip_enrollment_monthly and fiscal.snap_benefits_by_geography; article via WebFetch summary; report not saved |
| expert | 1 | KFF tracker, PBS/CBPP, Newsweek/CBPP, CMS SHO 23-004; CBPP and CCF tracker not read |
| everyman | 1 | FRAC and No Kid Hungry H.R. 1 summaries, CBPP search summaries; no tracker or data files opened |

## Assessment
- **Correct verdict: 1 Pinocchio.** The figures trace to CCF and CBPP data and the article states its own caveats (state totals, not matched children; 2.56M all-state through Aug 2026 versus 26-state through May 2026; 42% only for CBPP-tracked states; causes offered as likely contributors). The headline "losing both" implies the same children lose both programs, which state totals cannot show. That is mild headline overreach, not false facts.
  - **2.56M child Medicaid/CHIP decline, Jan 2025 to Aug 2026: supported in scale.** The article reports it. The warehouse brackets it (about -2.25M by May 2026, -2.77M preliminary by Jul 2026). KFF shows child enrollment down in 49 states plus DC.
  - **15.7% vs 6.3%, 2.5x: arithmetic holds** (15.7/6.3 = 2.49). The article states the two windows and state sets differ for the 2.56M figure; the 2.5x ratio is within the same 26 states. No independent child-level check exists; all-ages SNAP persons fall about 14% over a similar span.
  - **42% children share: supported.** CBPP's tracker total row shows 43% (about 1.2M of 2.76M, 25 states, data as of Aug 2026). The article's 42% and 1.5M reflect an earlier 28-state CBPP update (1.5M children).
  - **Arizona about half of children, four states 19-26%: consistent.** CBPP shows Arizona total SNAP participation down 45-51% depending on the date; all-ages warehouse data gives AZ -51%, FL -25%, LA -24%, TX -21%, MA -18%.
  - **H.R. 1 provisions: true.** State admin share 50% to 75% in Oct 2026; benefit cost share 5/10/15% by error rate from FY2028 (Oct 2027). 12-month continuous eligibility for children since Jan 2024 is true (CAA 2023).
  - **Shading:** causal link to H.R. 1 is the authors' inference (the window starts Jan 2025, before the law); advocacy source. These justify 1, not 2.
- **askamerica (1st):** the only arm that tested the central 2.56M claim against data.
  - It bracketed 2.56M with warehouse enrollment (37.67M Jan 2025, 35.41M May 2026, 34.89M Jul 2026) and correctly called it an estimate because August is not loaded.
  - It ran an all-ages SNAP proxy, flagged that proxy as such, and noted MA falls just below the article's 19% floor.
  - It flagged the "losing both" aggregate problem and the causal inference. Rating is correct.
  - Weaknesses: H.R. 1 provisions and the 42% share left unverified when CBPP's tracker summaries were findable; the continuous-eligibility claim rested on background knowledge. The report was not saved, a delivery failure, though the answer text is complete and honest about it.
- **expert (2nd):** correct rating, good source triangulation, but the central 2.56M claim was marked not checkable.
  - Verified the 42% (43% per PBS/CBPP), Arizona (49%), and gave sensible window and basis flags (the 2.56M vs 6.3% bases differ; window predates H.R. 1).
  - Weaknesses: no data-level test of any headline number; H.R. 1 dates were "consistent with law" but not re-fetched; it marked the CBPP 25-state figure as supporting the article's 26-state figure loosely.
- **everyman (3rd):** correct rating but least verified.
  - Verified H.R. 1 provisions through FRAC and No Kid Hungry, and the arithmetic. Candid about limits.
  - Weaknesses: the headline numbers were all "not checked"; the 42% was left as unconfirmed when it was findable; Arizona was compared on all participants, not children; the continuous-eligibility claim was labeled True while admitting it was unsearched. It also called its rating "provisional."

## Recipe check
Partly. A recipe for tracker-derived advocacy analyses would have helped:
- When the article's source tracker blocks reads, check the warehouse for the underlying series (Medicaid/CHIP enrollment is there) and mirror coverage of CBPP tables before marking a claim unverified.
- Separate the figures' bases (all-state vs 26-state, differing windows) as the first misleading-ness check.
- The article load failure (403) blocked report delivery. A fallback of reading the article through a syndicated or archived copy would have avoided a lost report.
- Warehouse coverage: child-specific SNAP participation is not in the warehouse; the all-ages proxy is the best available and must be labeled as such, which askamerica did.

## Severity invariant
**Not triggered:** askamerica ranks above everyman. All three arms gave 1 Pinocchio, matching the correct verdict.

## Defects
- askamerica: report not delivered because preview_report requires a loaded article copy and the CCF page returned 403.
- askamerica: the 42% CBPP share and H.R. 1 provisions were left unverified despite available public sources.
