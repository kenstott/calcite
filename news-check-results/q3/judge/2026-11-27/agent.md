# Judge — q3 — CEI, Clyde Wayne Crews, "The regulatory turkey lost weight, but Washington's still gobbling this Thanksgiving" (2026-11-23), 2026-11-27

**Question/claim:** Fact-check the article. Central claims: 2025's 2,441 final rules was the lowest count of all time; 2026 has 2,639 final rules and a 75,742-page Federal Register as of 11/23; only Trump administrations dropped below 3,000; 2024 was a record 106,109 pages with 3,248 rules; significant-rule counts by year; Stephen Miller hailed the agenda as better than Reagan's.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and contain 987654321 (pass); askamerica ran the extension's verbatim news prompt (no fingerprint by design). A grep of the everyman and expert answers for "askamerica" found no match. No unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web search summaries (WebFetch gave only a summary of the article); CEI, Ballotpedia, GW, reginfo | Warehouse fedregister.fr_documents_significance (through 2026-11-23), CEI PDF, CEI Oct 5 post, KATV, whitehouse.gov | federalregister.gov API (counts, max end_page), CEI Oct 5 post |
| Vintage | Mixed; no 2026 YTD data | Current to 2026-11-23 | Current to 2026-11-23 |
| Headline figures | 2,441 and 3,248 supported via CEI only; 2,639/190 "not checkable"; 155 "not confirmed" (Ballotpedia 110) | 3,248 exact; 2025 = 2,442 (CEI 2,441); 2,639 and 190 exact; 166 of 2,238 through 9/30; 2019/2020 significant cells conflict (262, 438 vs 69, 79) | 3,248, 2,441, 2,639, 190 and 75,742 pages verified; 155 vs 189 conflict; 2,946 arithmetic verified |
| Overall Pinocchio | 2 | 1 | 1 |
| Claims graded | 10 rows | 13 (12 author, 1 subject) | 10 lines |
| Central verdict | "Supported, but source is CEI itself"; 2026 counts not checkable | True (lowest of all time; per CEI PDF and warehouse) | Verified (count); "all time" verified only for 1995-2025 |
| Miller claim | Not confirmed; suggests confusion with Fed Governor Miran | Unsupported; title confirmed on whitehouse.gov; no transcript | Not checkable here; no transcript |
| Visual | none | dashboard.png and report.html | agent-dashboard.png and agent-report.html |

## Verification

Central claim, primary source. I queried the federalregister.gov API (document type RULE). 01/01/2026 to 11/23/2026: count 2,639. 01/01/2025 to 12/31/2025: count 2,441. Both match the article. So the central count claim is supported, and expert and everyman's 2,441 are exact. askamerica's warehouse shows 2,442 for 2025, one document off; it said so and still graded the claim true. That is a fair reading.

I did not re-check the 75,742 page figure. Expert reports it as the max end_page of the 11/23/2026 issue. Neither everyman nor askamerica could check it.

Significant counts. Expert and askamerica both found 189 (2025) and 190 (2026) on the Federal Register's flag. CEI's 155 for 2025 does not match that flag. Both flagged it. Everyman used Ballotpedia's 110, a different definition, and left the point open.

2019/2020 table cells (69, 79). Only askamerica caught this. The warehouse flags 262 and 438, and it cites CEI's own PDF (first term average 280 a year). I did not re-run the warehouse query; this is an open item for the orchestrator. The PDF cross-check makes it credible.

Miller remark. No arm found a transcript. askamerica and expert handled it correctly (unsupported / not checkable). Everyman floated "possible confusion with Stephen Miran". That is speculation without evidence; Miller is White House chief of staff per askamerica's whitehouse.gov read.

Everyman errors. It called 2,639 and 190 "not checkable here" when the Federal Register API gives them directly. It also said the 86/34/46 split "did not turn up" and compared it to OIRA's 646 deregulatory actions, a different measure.

askamerica claims.json versus report. The engine's overall rating in report.html is "1 Pinocchio". agent.md says "Rating: 1 Pinocchio". No mismatch. Honesty 88/100, bias +36, 12 author claims graded (7 true, 2 mostly true, 1 mostly false, 2 unsupported). Small over-credit: claim 3 (75,742 pages) graded true although askamerica says the warehouse has no page counts and it never confirmed the page figure. The other counts in that claim were checked.

## Ranking rationale

1. askamerica. Widest coverage and best calibrated. It found the 2019/2020 cell conflict, the 243 Biden January rules omission, and the point that 2026 significant rules already exceed 2025 in CEI's table. It tied each count to warehouse rows and to the CEI PDF. It graded the Q3 split and page table as unsupported with stated reasons. Weak spots: it did not verify the page count, and its warehouse 2025 count is off by one.
2. expert. Strongest on primary evidence: every headline count verified through the Federal Register API, including the 75,742 pages. Missed the table errors for 2019/2020, the Biden-January context, and the extra claims (Franklin, turkeys). Terse list, with a good caveat that "all time" was checked only back to 1995.
3. everyman. Could not reach the primary data, marked checkable claims as not checkable, graded 2 Pinocchios on thin evidence, and speculated on the Miller attribution.

What would change the ranking: if the 2019/2020 warehouse values (262, 438) prove to be a flag-definition artifact rather than a real conflict, askamerica loses its main differentiator and expert would rank first on page-count and exact-count evidence.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Expert found the page-count figure by reading the Federal Register issue's end_page through the public API; askamerica did not reach it and the warehouse has no page field. Recipe `fact-check-a-news-article` exists and covers the general method. Everyman added nothing worth teaching. askamerica did things right (regrouping the Miller claim as a subject claim, citing the PDF for context), and is not a recipe gap. The page-count gap is a data gap, not a pattern issue.

Cause for askamerica not being fully ahead on every claim: (a) data gap in the warehouse. fedregister.fr_documents has no page fields (filed as kenstott/govdata-ops#1600), so a page-count claim cannot be checked from the warehouse.

Proposed resolution: a — add annual Federal Register page counts (end_page by issue) to the fedregister schema, tracked in kenstott/govdata-ops#1600.

## Corpus gaps stated

- "The warehouse holds no page counts (search_catalog 'Federal Register page count annual' returned none)" — claim 6, the adjusted page table. Reason: no page data; federalregister.gov statistics page blocked by CAPTCHA; CEI PDF gives only unadjusted pages.
- "No warehouse column or independent source classifies rules as deregulatory" — claim 9, the Q3 27-to-6 split; graded unsupported.
- Presidential-document rows (240/471/307) vs warehouse PRESDOC (129/176/148): "different universe"; "small-business rows not in the warehouse" — not reconciled.
- The 86/34/46 split: "CEI's subjective classification"; no warehouse column.
- Miller remark: no transcript found in whitehouse.gov videos, web or catalog search; graded unsupported.
- 75,742 pages in claim 3: "Page count 75,742 not in warehouse."
