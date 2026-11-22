# Judge: news q8, Water Online / EWG "Toxic 'Forever Chemicals' Found In Tap Water Of 197M Americans" (2026-11-19) (2026-11-22)

Rank: [askamerica, expert, everyman]

Story claims (an EWG press release republished by Water Online): about 197 million Americans drink PFAS-contaminated water per EPA UCMR 5 data, 23 million more than earlier tests; 3,595 newly detected sites, 13,459 in total; EPA is "in the final stages" of rolling back limits on four PFAS while keeping PFOA/PFOS at 4 ppt; 83% of waterways have PFAS (Environment America); CDC finds PFAS in 99% of Americans; about 30,000 industrial dischargers; quotes from David Andrews and Scott Faber.

## What each answer found

- **askamerica (1 Pinocchio).** It confirmed from EPA pages: the UCMR 5 final release (2026-08-27, 29 PFAS plus lithium), the 2024 six-PFAS rule, the May 2025 rescission announcement, and the PFOA/PFOS 4 ppt with an optional 2031 extension. It graded "final stages of rolling back" partially false, since the rules are proposed (May 18, 2026), comment closed July 20, and no final rule exists. It graded 197M, +23M and the site counts unsupported, since EPA publishes no such totals and EWG's page 403'd. It noted the 83% is a share of sites, not waterways. Strongest framing analysis of the three: UCMR 5 reports detections at 2-20 ppt reporting levels, not exceedances, and EPA's own headline, that 7.8% of systems would exceed the PFOA/PFOS limits, is omitted. It also noted that an EWG model is presented as following from EPA data, and that the "new" findings are nearly three months old.
- **expert (2 Pinocchios).** It matched the figures to EWG's release via secondary outlets, so the fidelity is confirmed but not EWG's method. It confirmed the proposed-only rollback (FR 2026-10085) and the 83%-of-sites point. Key context: NRDC counts 80M+ people above enforceable limits, against the 197M with any detection. ATSDR says "nearly all", and "99%" is EWG's wording.
- **everyman (2 Pinocchios).** Similar conclusions from search summaries: rollback overstated, detection vs above-limit, NRDC 80M+.

## Ranking reasoning

1. **askamerica.** It has the most primary-source verification (EPA rule pages, UCMR 5 release, ATSDR) and the most precise statement of why the headline misleads: EPA's own exceedance figure, and detection at reporting levels vs limits.
2. **expert.** Same core verdict. The NRDC 80M+ comparison is a useful number askamerica lacks, but most of its checks are secondary.
3. **everyman.** Same conclusions, least verification.

What would change the ranking: if the 197M were weighted as "faithfully relayed" (a fidelity call) rather than "unsupported" against EPA, expert's handling would read as more accurate on the headline claim. The two arms would then be close.

## Severity check

askamerica ranks above everyman, so there is no severity flag.

## Rating shape and gap audit

- `claims.json`: `author_claims` (EWG's own voice, since the piece is EWG's release), two `fidelity` claims for figures attributed to other bodies, and `subject_claims` with `speaker: "Scott Faber (EWG)"`. Shape is acceptable. Rating: 1 in both report.html and agent.md.
- Gap: the warehouse has no PFAS / UCMR data (`search_catalog` "pfas" returns no match). With UCMR 5 plus SDWIS population served, the 197M, the site counts and the exceedance share could all be recomputed directly. Filed kenstott/govdata-ops#1438 (`type:sourcing`, `kind:gap`, `schema:environment`, `status:open`).

## Recipe check

Nothing to teach. Once UCMR 5 is loaded, "people served by systems with any detection vs above MCL" is the natural recipe. Not written until the data exists.
