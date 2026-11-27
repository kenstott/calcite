# Judge — q7 Better Markets (Phillip Basil), "FDIC merger rules threaten survival of community banks and the Main Street economy" (2026-11-23), 2026-11-27

**Question/claim:** Fact-check the press release. Central claims: community banks' share of banking-system assets fell from 45 percent in the early 1990s to 15 percent last year; large banks' share rose from under 20 percent to over 70 percent; the FDIC proposal's timelines trigger automatic merger approvals; the biggest banks lend least to Main Street.

Rank: [askamerica, everyman, expert]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and contain 987654321 (pass); askamerica ran the extension's verbatim news prompt (no fingerprint by design). A grep of the everyman and expert answers for "askamerica" found no match. Neither unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only: law-firm summaries (Skadden, Davis Polk, Sheppard), FDIC 2012/2020 studies | Web (release, comment letter PDF, law firms) plus banking.financials call reports via SQL | Web only: Skadden, HLC, FDIC 2012 study, KC Fed 2003 |
| Vintage / window | FDIC study figures to 2011/2019 | 2010-12-31, 2025-12-31, 2026-06-30 | FDIC figures to 2011 |
| Headline figures | 15% plausible, 12% in 2019 (FDIC study); 27% to 80% for banks over $10B, 1984-2011 | 14.4% (2025-12-31), 13.4% (2026-06-30) community under $10B; 69.9%, 71.8% large over $100B | 14% in 2011; 27% to 80%, 1984-2011 |
| Overall Pinocchio grade | 2 | 0 ("on the checkable numbers") | 1 |
| Claims graded | 8 | 7 (claims.json has 7 entries) | 4 steps, about 6 claims |
| Verdict on central claim (45% to 15%) | Not checkable here; direction supported | True (current end matched); 1992 base unverified | Not confirmed (low confidence) |
| Visual | none | dashboard.png + report.html | agent-dashboard.png + agent-report.html |

## Verification

- Central claim, current end. askamerica's query on banking.financials (community under $10B of total_assets_thousands) gives 14.39% at 2025-12-31 and 13.43% at 2026-06-30. The release's "15 percent last year" rounds from 14.4. The SQL in calls.jsonl (seq 13) and claims.json match. It also used the group's own $10B and $100B cutoffs and said so. I could not open the comment letter PDF (binary in WebFetch) and could not re-run the SQL. A live query is an open item for the orchestrator: confirm 14.39 and 69.94.
- 1990s base. My own web search found no FDIC page giving 45% in 1992. No arm verified it. askamerica said it is not in the warehouse (starts 2010) and rests on Better Markets' FDIC BankFind analysis. That is honest. everyman and expert only found FDIC study figures with different cutoffs and years, and correctly called them directional.
- Automatic approvals. All three agree on the mechanism (de minimis deemed approved in as few as 5 business days; 90/150-day standard windows). expert and everyman state the sharper point: deemed approval is not general. askamerica graded the claim "true" and mentioned the 5-day and 90/150-day windows but did not say the release overgeneralizes. This is the best-supported nuance and askamerica missed it.
- Lending claim. askamerica graded it "unsupported" with a stated reason and said the warehouse has no loan-by-size series (orchestrator confirms: banking.financials has no loan columns, filed as kenstott/govdata-ops#1610). expert said "not checked". everyman said "opinion, unsourced".
- Which arm the evidence supports. The one number any arm could actually reproduce is the current share, and only askamerica reproduced it. Its "true on the checkable numbers" is supported for the current end. A 0 Pinocchio overall is generous: the 1992 endpoints are unverified, the lending claim is unsupported, and the survival headline is unsupported. everyman's 2 and expert's 1 are closer to a calibrated overall grade, but both rest on no reproduced figure.
- askamerica claims.json vs stated grade. report.html shows "0 Pinocchios" and agent.md says "0 Pinocchios". They match. But the headline sentence says claims "about ... bank lending are accurate; 2 claims could not be confirmed". claims.json has 3 true, 2 not checkable, 2 unsupported, including the lending claim graded unsupported. The headline contradicts the body. Flag as an internal mismatch.
- Stale vintage / wrong table. No wrong-table pick. askamerica first used banking.institutions (single snapshot), then moved to banking.financials for a time series. Fine.

## Ranking rationale

1. askamerica. It is the only arm that checked the central numeric claim against primary call-report data and got a value consistent with the release. It read the comment letter, found the group's own definitions and the vintage mismatch (15% vs 13% in the letter), and caught the causal overreach (Riegle-Neal, no survival estimate). Weaknesses: 0 Pinocchio is too lenient, the headline sentence contradicts its own lending grade, and it missed that deemed approval is limited to de minimis deals.
2. everyman. Eight claims, a clear opinion/fact split, a good "misleading or missing context" section (clock starts at substantially complete, Fed and DOJ review remain, community banks may favor speed). It stated the deemed-approval limit. It graded the statistics "not checkable" and used old FDIC study vintages, so it gave no verified number.
3. expert. Correct and calibrated but thinnest: it skipped the lending claim ("not checked"), used 2011 figures, did not check the "last year" value, and did not read the letter. It did state the de minimis point clearly and gave low confidence honestly.

What would change the ranking: if a live query shows the 14.4% or 69.9% values are wrong, askamerica drops below everyman. If the orchestrator judges that a 0 grade with a headline contradicting the body is a worse miss than unverified numbers, everyman would move to first.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

- everyman and expert both stated that deemed approval applies only to de minimis deals. askamerica did not. A fact-check recipe point: when grading "X triggers automatic Y", check whether the mechanism applies to all categories or only one. Existing recipe `fact-check-a-news-article` covers the workflow; a broadened line about scope-of-mechanism checks is warranted there.
- askamerica's 0 grade with unverified base-year endpoints and an unsupported headline suggests the overall-rating guidance in `fact-check-a-news-article` should say that unverified central endpoints and an unsupported headline cap the grade above 0. Warehouse-native method: none needed, this is grading policy.
- The 1992 base is outside the warehouse (banking.financials starts 2010). Reasonable to log as a coverage gap only if historical FDIC call reports (pre-2010) are confirmed sourceable. Do not state it as permanent.
- askamerica is #1, so no cause classification for a loss applies.

Proposed resolution: b — broaden `fact-check-a-news-article` to check scope of "automatic" mechanisms and to cap the overall grade when central endpoints or the headline are unsupported.

## Corpus gaps stated

- "The 1992 base (45%) is not in the warehouse, which starts in 2010 (21.6% at 2010-12)." (agent.md, claim 1)
- "Not verified: all early-1990s shares (45% and under 20%). The warehouse starts in 2010." (agent.md)
- "The warehouse has no loan-by-size series, and I found no independent replication." (agent.md, claim 5; claims.json: "warehouse financials has no loan columns by size"). Orchestrator confirms accurate; filed as kenstott/govdata-ops#1610.
- Graded "not checkable here" in claims.json: "Such provisions can only serve one purpose" (reason: motive and exclusivity of purpose, no evidence can test it); "rubber-stamp process" claim (reason: evaluative judgment, figurative).
