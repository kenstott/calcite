# Judge — news q6 hechinger-ap-enrollment-down-1-4m-teachers-up

**Story:** "School districts with fewer students are delaying painful budget decisions" (AP / The Hechinger Report via ABC News, 2026-11-18). Claims of interest: enrollment down more than 1.4M since the pandemic and one in four schools down 15%+ (Stanford's Dee & Pyne); teacher headcount up 1%; Seattle −8% to below 49,000; SF −10%, $183M contract, ~$139M reserve; 21 state takeovers since 2023; $190B relief.

Rank: [askamerica, expert, everyman]

Judged 2026-11-18 on three answers produced the same day.

## Why
- **askamerica (1st):** engine 0 Pinocchios. Reproduced the article's three national figures independently from NCES CCD in the warehouse (fall 2019 vs fall 2024): enrollment −1.454M (−2.8%); 25.7% of 92,358 matched schools lost 15%+; teacher FTE +1.25% in matched districts. Found Seattle −9.3% (55,986 → 50,773) vs the article's "roughly 8%", and SF −7.4% through 2024 vs "10%". Primary-source confirmation of the $183M UESF contract, the $139M reserve split ($111M board + $28M state-mandated), 21 takeovers, $190B. On resume (see disclosure) graded Dee's "never had more teachers per student in our nation's history" partially false with the warehouse pupil-teacher ratio (lowest 15.159 in 2023–24 within 2013–2024) and Stanford's own "last 15 years".
- **expert (2nd):** 1 Pinocchio. Supported the national figures through coverage of EdWorkingPaper 26-1624 and Chalkbeat; independently caught Dee's "in our nation's history" overreach vs the release's "most in 15 years" and that the release says "over a million". Left Seattle and San Francisco figures unverified.
- **everyman (3rd):** provisional 1 Pinocchio, low confidence; seven tool calls, most numeric claims unchecked, no quotes extracted.

What would change the ranking: discounting everything askamerica produced after the resume, expert's Dee catch is unanswered on askamerica's side — but askamerica's warehouse reproduction of all three headline figures plus the Seattle/SF discrepancies still keeps it first.

## Harness disclosure
askamerica's first delivery extracted no attributed quotes at all (15 claims: 13 author, 2 fidelity) despite three named speakers. I resumed it naming Dee's and Rankin's quotes and the "15 years" question — that is coaching on the Dee point. It added 7 `subject_claims` (Dee ×3, Rankin ×3, Jason Willis ×1).

## Recipe check
No recipe: askamerica already used the warehouse-native route (CCD matched-school panel). The quote-extraction miss is a product guardrail issue already covered by McpServer.java's rule 8 ("A quoted politician's assertion is the politician's"); the instruction exists and was not followed on first pass.

## Shape
Final claims.json: 12 `author_claims`, 2 `fidelity`, 7 `subject_claims`. First delivery wrong (no subject claims); fixed on resume and re-verified from the file.

## Resolution / gaps
Filed kenstott/govdata-ops#1322 (`type:sourcing`, `kind:gap`, `schema:edu`, `status:open`): `edu.f33_district_finance` lacks F-33 Z32 salaries and Z34 employee benefits, so Rankin's "80% of a district's budget" could not be checked from the warehouse. Verified first in the schema YAML (no salary/benefit columns) and no matching issue.

## Severity invariant
Not triggered.
