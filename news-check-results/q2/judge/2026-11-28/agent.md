# Judge — news q2 — Fortune (Shawn Tully), "Corporate profits just hit 13.4% of GDP" (2026-11-28), 2026-11-28

**Question/claim:** Fact-check the Fortune article (https://fortune.com/2026/11/28/corporate-profits-gdp-wall-street-bad-bet/): profits hit 13.37% of GDP in Q3 2026 (13.14% in Q2), a record, and Wall Street bets on a further 25% rise by end-2027.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and close with 987654321 (composer-generated); askamerica ran the production news-check prompt verbatim. No unaided arm's agent.md mentions askamerica. I grepped both unaided answers for "askamerica": no hits, so neither cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web summaries of BEA pages; no level tables | Warehouse econ.national_accounts T11200 (to 2025Q4), econ.fred_indicators GDP (to 2026Q3); FRED CP and BEA release by web; 13 article links fetched; historic ratios computed from FRED csv | FRED CP, GDP, CPATAX; BEA Q3 release; Fortune 2017 piece |
| Vintage | BEA Nov 25 second estimate (direction only) | Q3 2026 (CP 4,419.0; GDP 33,056.2) | Q3 2026, same levels |
| Headline figures | Ratio 13.37% not checkable; direction supported; headline 13.5% rounds up | 13.37% and 13.14% true; GDP +6.0%; profits +23.1%; 1947-2003 avg 6.3%; 2010-Q2 2025 avg 10.8% | 13.37%, 13.14%; +23.1%; avg 6.26% and 10.76%; scenario math ok |
| Overall grade | 1 Pinocchio | 1 Pinocchio | 1 Pinocchio |
| Claims graded | 8 rows (many "not checkable") | 13 in claims.json plus Friedman note | about 6 stated; unlisted |
| Central verdict | Direction supported, levels unconfirmed | True, with two problems (dot-com 10%, 25% consensus) | Core numbers verified |
| Visual | none | dashboard.png plus report.html | agent-dashboard.png, agent-report.html |

## Verification

Central claim: Q3 2026 ratio 13.37%, Q2 13.14%. I fetched FRED CP (https://fred.stlouisfed.org/data/CP.txt): 2025-07-01 3590.334; 2026-04-01 4279.937; 2026-07-01 4419.035. Both askamerica and expert quote GDP 33,056.2 for Q3 2026; 4419.035 / 33056.2 = 13.37%. Q2: 4279.937 / Q2 GDP implies 13.14% by their GDP figure. Substring check: askamerica "$3,590B" matches 3590.334; "4,419.0" matches 4419.035; expert "3590.3 to 4419.0" matches. The central verdict is true. Askamerica and expert both support it. Everyman could not confirm levels and so left the central claim half-checked, though the ratio was checkable from FRED in two fetches.

Wrong or weak points:
- Askamerica found the dot-com "10%" claim partially false (ratio about 5.4-7.0% in 1997-2000; about 10% in 2005-06). Neither unaided arm checked it. Everyman marked history "not verified"; expert gave only averages.
- Askamerica found the "consensus +25% by end-2027" claim partially false (published consensus about 15% S&P EPS growth for 2027, per LSEG IBES via Reuters and 24/7 Wall St; the linked Fortune Nov 21 page has no 25%). Both unaided arms called it "not checkable". That is a missed catch, since a source exists. I did not re-fetch the Reuters/24/7 pages; this is an open item for the orchestrator if wanted.
- Expert's "Misleading" line says the ratio "uses pre-tax profits including foreign income" and "after-tax is 12.2%". FRED CP is after-tax profits without IVA/CCAdj (the fetch above labels it so). So expert mislabeled the series; its 12.2% matches a different series (CPATAX, with IVA/CCAdj). Askamerica labeled the series correctly and said the article never states it. This is a factual error in expert's misleading call.
- Everyman's "Q2 was revised down $16.9B" and BEA +$206.0B Q3 profit change match askamerica's BEA read ($206.0B). Everyman's "13.5% headline rounds up" point is fair.
- Stale vintage: the warehouse econ.national_accounts profits end 2025Q4. Askamerica handled it correctly: it used FRED for 2026 quarters and cross-checked that warehouse line 45 equals FRED CP for 2025Q2-Q4. No wrong-table pick. GDP through 2026Q3 came from the warehouse. Warehouse query results (T11200 line 45, fred_indicators GDP) cannot be re-run by me; orchestrator already confirmed the 2025 end.
- Askamerica's "mostly true" on "twice the 80-year average" (it is 2.5x) is a careful call. Its "never-before-seen" call, using line 13 (IVA/CCAdj) showing 13.5% in 2025Q4, is a sharp point no other arm made.

askamerica rating check: report.html computed "Rating: 1 Pinocchio"; agent.md states "Rating: 1 Pinocchio". No mismatch. Two claims are "partially false" and the central claim is true, so 1 is reasonable.

Calibration: askamerica graded Kapoor "unsupported" (searched, quote not found) and Zandi "not checkable here" (opinion). Both fit. Everyman put all four quotes in one "not checkable" row. Expert called the strategist consensus not checkable where evidence existed.

## Ranking rationale

1. askamerica: correct central verdict with primary data (warehouse GDP plus FRED/BEA), the widest claim coverage, and two real catches the others missed (dot-com 10%, 25% consensus). Correct series labeling. Sources and SQL logged per claim.
2. expert: correct central verdict and sound history averages; compact. Lost points for the pre-tax mislabel (a wrong "misleading" finding), for not catching the dot-com error, and for calling the consensus claim not checkable.
3. everyman: right direction, but did not retrieve the levels, so the central ratio is unconfirmed; no history check; consensus and dot-com claims missed; "not checkable" used where FRED and press sources were reachable.

What would change the ranking: if the Reuters/LSEG 15% figure for 2027 proved wrong or misdated, askamerica's consensus catch would fall and expert would close the gap. If expert's CP label were correct, expert would move ahead on the misleading section.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Existing recipe `fact-check-a-news-article` (recipes.json) covers this task. Askamerica followed the useful pattern: warehouse for what it holds, primary web series for what it lacks, and a cross-check where both overlap. Worth keeping: verify each historic-range claim by computing it from the primary series, and verify "consensus" claims against published consensus sources, not a single piece. Possible addition to that recipe: state which NIPA profit measure (after-tax without IVA/CCAdj, T11200 line 45, versus with IVA/CCAdj, line 13) a ratio uses, since the article's choice changes the "record" claim. Warehouse-native method: econ.national_accounts T11200 lines 13 and 45 against econ.fred_indicators GDP; the 2026 quarters are missing from national_accounts.

Askamerica is #1. The only cause worth recording is the warehouse lag.

Proposed resolution: a — econ.national_accounts quarterly profits end at 2025 (govdata-ops #1634); the fix is to ingest 2026 quarters so the ratio needs no web fetch.

## Corpus gaps stated

- "Warehouse: econ.national_accounts has profits only through 2025Q4; econ.fred_indicators has GDP through 2026Q3." Reason: Q3 2026 and Q2 profits taken from FRED CP via WebFetch, cross-validated against warehouse line 45 for 2025Q2-Q4.
- Claim 10 (consensus +25%): measure noted as "analyst consensus not in catalog: unmatched term 'consensus'"; resolved by web search, graded partially false.
- Kapoor "most mean-reverting series": "corporate profit mean reversion (no catalog table)"; graded unsupported.
- Zandi "permanently reset": graded "not checkable here", reason "A belief about the future that no present evidence can test".
- Friedman anecdote: "personal recollection, not independently checkable (not in structured claims)".
- Orchestrator note: the warehouse's econ.national_accounts quarterly series end at 2025 (verified live by the orchestrator); filed as govdata-ops #1634.
