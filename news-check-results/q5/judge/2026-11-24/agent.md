# Judge — News q5 (kffhealthnews-ob-gyn-exodus-maternity-hospital-closures), 2026-11-24

**Question/claim:** KFF Health News (2026-11-24): at least four states restricted malpractice suits since 2025 (central claim); at least 146 hospitals stopped delivering babies or plan to (CHQPR); Idaho lost more than a third of its OB-GYNs; Medicaid paid for about 40% of 2024 births; Utah $450,000 noneconomic cap; $951M default judgment against Steward.

Rank: [askamerica, everyman, expert]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md carry both markers; askamerica received the product's verbatim news prompt, which carries no markers by design. A grep for "askamerica" in the everyman and expert answers (agent.md, agent-report.html) found no hits, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only; secondary press and law commentary; no primary docs opened | Primary documents: Act 124 text, NM HB 99, CDC NVSS, JAMA Netw Open via PMC, CHQPR report, Salt Lake Tribune. Warehouse searched, nothing relevant | Web only; Becker's, OPB, PMC, KFF; older law guides |
| Vintage | 2025-26 | 2025-26 | Medicaid share 2023 (KFF), not 2024 |
| Headline figures | $951M default, Idaho -94/268, Utah cap $450k, 146 "plausible" | 4 laws verified, 40.2% Medicaid, 94/268 (35%), $951M, CHQPR 144 stopped + 4 announced | $951M, Idaho 94/268, Medicaid 41% (2023), 146 not matched |
| Overall grade | 1 Pinocchio | 0 Pinocchios | 1 Pinocchio (provisional) |
| Claims graded | 12 rows (several grouped) | 18 (14 true or mostly true, 4 unsupported) | 3 verified, 1 unmatched, 6 "not verified" |
| Verdict on central claim (four states restricted suits) | Not verified (only Arkansas confirmed) | TRUE, all four named with statute | Not verified; Arkansas guide read as contradicting (older law) |
| Visual | none | report.html with board | dashboard png and report |

## Verification

Central claim. A WebSearch check supports askamerica. Oklahoma SB 453 sets a $500,000 noneconomic cap for injuries on or after Sept. 1, 2025. Arkansas Act 124 (HB 1427, approved Feb. 20, 2025) gives the child until the fifth birthday for childbirth injuries. These match askamerica's cited facts (Act 124 section 4; SB 453 effective Sept. 1, 2025). I did not independently re-open the Utah HB 503 or New Mexico HB 99 text. Everyman could not confirm three of four states. Expert said the Arkansas window was "contradicted by secondary sources", but that was an older guide that predates the 2025 act, so expert's note is a stale-source error, not evidence against the article.

Numbers across arms:
- $951M Steward default: all three agree.
- Idaho 94 of 268: all three agree (everyman also gives 114 left, 20 arrived).
- Medicaid share: askamerica 40.2% of 2024 births (CDC NVSS) matches the article's year. Expert used 41% for 2023 (KFF), a vintage mismatch with the article's 2024. Everyman did not open it.
- 146 hospitals: askamerica read the CHQPR report itself (144 stopped, 4 more announced, 148 total) and graded "mostly true", which is a fair reading of "at least 146". Everyman said "plausible, not confirmed". Expert said "not matched" using rural-only counts of 124 and 139. Neither found the current report.
- Utah $450,000 cap: askamerica and everyman both confirm.

claims.json for askamerica has 18 claims, 1 marked central. Engine-computed rating in report.html: "0 Pinocchios". Stated grade in agent.md: "0 Pinocchios". No mismatch. Claim verdicts in claims.json (14 true or mostly true, 4 unsupported) match agent.md.

Calibration notes. Askamerica graded four attributed or unquantified claims "unsupported" instead of true or false (Pilkington one lawsuit, Doroshow verdicts, Zancanella bills, 12 hours). That is calibrated. Its 0 Pinocchios is slightly generous given the article leans on unchecked attributed claims, but every checkable claim held. Everyman's 1 Pinocchio rests on mostly unverified items, not found errors. Expert's grade rests on 3 verified claims and a defective check of the law claims.

Open item for the orchestrator: no askamerica number needs a warehouse query; the warehouse had nothing relevant.

## Ranking rationale

1. askamerica. Correct central verdict backed by statute text and agency data. Widest coverage (18 claims). Read CHQPR, CDC and JAMA primary sources. Flagged a bad citation (house.gov link) and the Idaho exception omission. Used "unsupported" sparingly and with reasons.
2. everyman. Reasonable coverage (about a dozen claims) and sound framing notes, and it correctly noted that the cap may not reach the default judgment. But it left the central claim and the Medicaid, Arkansas-counties and 2026-ballot claims unverified, with no primary documents.
3. expert. Fewest claims verified. Treated an older secondary guide as contradicting the Arkansas claim. Used 2023 Medicaid data against a 2024 claim. Did not find the CHQPR figure. Provisional grade on thin evidence. The shell and methodology briefing were not used to reach statute text.

What would change the ranking: if the Utah HB 503 or NM HB 99 reads in claims.json were wrong, askamerica's central verdict would weaken. If a live check showed the CHQPR count was not 144 + 4, the "mostly true" grade would need revision. Neither is indicated.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Recipe `fact-check-a-news-article` already covers this workflow (extract every claim, test at the article's own grain). Askamerica followed it and produced the best result. Nothing worth teaching from everyman or expert: both did strictly less. One small point from everyman worth keeping: it noted CHQPR counts shift between rural-only and all-hospital tallies. Askamerica handled this by reading the report itself, so no change needed.

Proposed resolution: d — askamerica ranked first; no actionable cause.

## Corpus gaps stated

- "Catalog searches found no table for malpractice, OB-GYN counts, labor-and-delivery closures or payer of birth (the NCHS state tables carry no Medicaid share). Every claim was therefore checked against primary documents."
- Report text: "The warehouse holds no malpractice, OB-GYN-count, L&D-closure or payer-of-birth tables" (searches: 'medical malpractice payments claims', 'obstetrician gynecologist physicians by state', 'births paid by Medicaid percent').
- "The Commonwealth Fund claim (Alabama, Arkansas and Idaho among the fewest maternity providers) was not verified and is not graded." Reason: Commonwealth Fund returned HTTP 403.
- 1 claim "could not be confirmed" in the lead line; four claims graded UNSUPPORTED because no independent count or total was found (Pilkington lawsuit count, Doroshow "rarely paid", Zancanella bills over $1M, "nearly 12 hours").
