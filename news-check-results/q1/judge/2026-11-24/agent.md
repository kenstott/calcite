# Judge — News q1 (foxbusiness-hopper-holiday-airfares-decade-high), 2026-11-24

**Question/claim:** Fact-check the Fox Business piece (2026-11-20): holiday airfares are the highest in at least 10 years (Hopper's Hayley Berg); jet fuel about double a year earlier; $4.88/gal April peak, under $3 in summer, $4.90 late October; fuel is 15-30% of flight operating cost.

Rank: [askamerica, everyman, expert]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md carry both markers (123456789 / 987654321); askamerica received the product's verbatim news prompt, which carries no markers by design. A grep for "askamerica" in the everyman and expert answers (agent.md, and expert agent-report.html) found no hits, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Fox syndications of Hopper index, Argus/EIA coverage via web, BLS CPI via Airways Magazine | Web (AP, IATA, airlines.org Argus page, Fox 13/Hopper, BTS/BLS CPI); warehouse searched, only WTI pulled for context | Fox News (Hopper), Skift/Argus, airlines.org, BLS CPI release |
| Vintage | Fuel: 2025 Argus, 2026 YTD; no Nov value | Fuel through Nov 23 ($3.47) | Fuel through Nov 23 ($3.47) |
| Headline figures | Hopper $402 (+31%), $452 (+23%); BLS Sep +22.4%; Argus ATH $4.73 (different series) | Same Hopper and BLS figures; AP $4.88 peak to $2.70 June; IATA +111%; fuel share near one-third | Same Hopper and BLS figures; April avg $4.29, Sep record $4.37, Oct 8 $4.50 |
| Overall Pinocchio | 1 | 1 (own call; engine gave no overall rating) | 1 |
| Claims graded | 10 (3 "not checkable", 1 "not checked") | 6 (1 true, 4 mostly true, 1 unsupported) | 5 bullets, 3 left unverified |
| Central claim verdict | Supported (secondhand) | Mostly true | Supported, medium-high confidence |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

askamerica claim grades in claims.json: 1 true, 1 unsupported, 4 mostly true.

## Verification

Central claim (holiday fares highest in 10+ years). Spot check by web search: Hopper's 2026 Holiday Travel Index reports Thanksgiving domestic round trip $402 (+31%, +$95) and Christmas $452 (+23%, +$85), both at 10-year highs. Forbes, Fortune, Fox Weather and Yahoo repeat it. All three arms agree with this. Going's outlook shows smaller gains (+15%/+16%), a point none of the arms raised. The claim rests on one vendor index; all three arms say so or imply it.

Jet fuel peak $4.88 in April, below $3 in summer. askamerica found the AP report (Argus $4.88 early April, $2.70 June low) and graded it true. everyman marked it "not checkable (partly corroborated)" and cited a different series ($4.73 Gulf Coast). expert cited April average $4.29 and called the timeline "not confirmed, low confidence". askamerica is better evidenced here.

Late-October $4.90. askamerica: unsupported; AP $4.53 (Sep 17), $4.30 (Sep 26), airlines.org $3.47 (Nov 23). expert: Oct 8 $4.50, Nov 23 $3.47. everyman: could not see. Neither checked a late-October point. All three decline to confirm, which is calibrated. Both askamerica and expert found the $3.47 Nov 23 value.

"Double a year ago." askamerica: IATA +111.2% y/y week of Oct 9, AP nearly twice the 2025 average on Sept 26; mostly true, stale now. everyman: plausible, not verified. expert: not verified. askamerica supplies primary-grade evidence and the staleness point.

Fuel 15-30% of cost. askamerica: IATA near one-third in 2026, above the top of the range; mostly true. everyman: "consistent with industry figures", no source retrieved. expert: not verified. askamerica is the only arm with a source.

BLS airline fares Sep 2026 +22.4% y/y and October not collectable: all three report it. Matches.

Claims missed. expert dropped most secondary claims (booking earlier, relief timing, causal link). everyman covered the most claims (10) but left several "not checkable" without trying (booking early, pre-pandemic record low). askamerica did not grade the "book earlier" or 2027 relief claims; it folded the cause into one causal claim. everyman caught the inflation (nominal vs real) caveat and the arithmetic point that a fuel doubling cannot alone explain 23-31% fare rises; askamerica did not.

askamerica engine rating vs stated grade. report.html gives Author's Claims honesty 88/100, Hopper/Berg claims honesty 75/100, and states "too few checkable claims to characterize". The engine did not compute an overall Pinocchio. agent.md states 1 Pinocchio as "my own call". Not a numeric mismatch, but the agent.md headline ("too few to rate; 1 claim could not be confirmed") reads oddly next to a stated 1 Pinocchio grade and six graded claims.

Open item for the orchestrator: askamerica reported "no jet fuel price table and no airfare CPI table" from search_catalog. A live check is needed on whether the transport or econ schemas hold BLS CPI airline fares (CUUR0000SETG01) and EIA jet fuel spot prices; if they do, this is a catalog discovery miss.

## Ranking rationale

1. askamerica. Correct central verdict (mostly true, rests on Hopper alone). Best primary-source evidence: AP for the Argus peak and trough, IATA for the doubling and fuel share, airlines.org for the Nov 23 value. Caught the stale present-tense framing. Calibrated: marked $4.90 unsupported rather than guessing. Weaknesses: only 6 claims, missed the nominal-vs-real and arithmetic points, did not use the warehouse for the claim itself, odd headline wording, no engine overall rating.
2. everyman. Correct central verdict and widest claim coverage with useful framing points (nominal fares, fuel share arithmetic, single vendor). Weaker evidence: marked the $4.88 peak and fuel share without a primary source, and never found the Nov 23 drop, so it missed the staleness point. Several "not checkable" or "not checked" without an attempt.
3. expert. Correct central verdict and found Nov 23 fuel value, but five terse bullets, three left "not verified here". Thin coverage and no primary source for the fuel share or doubling. Did note the fuel is now lower than "surge" implies.

What would change the ranking: if the warehouse holds airline CPI or jet fuel series that askamerica should have used, askamerica loses credit for skipping them. If everyman's nominal-versus-real point proves decisive for the grade, everyman could move above askamerica.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Existing recipe `fact-check-a-news-article` was served (calls.jsonl seq 1) and covers the workflow. Patterns worth adding to it: (1) check whether a price series quoted in the present tense has moved since (askamerica did this, keep it); (2) test nominal versus real framing for "highest in N years" claims, and check magnitude consistency (a cost share of 15-30% cannot explain a larger fare rise by itself), which everyman found and askamerica did not; (3) try the warehouse BLS CPI airline fares and EIA energy tables before declaring "no table". Cause for askamerica not matching everyman on framing: (b) reachable pattern not used. Warehouse-native method: econ or transport schemas for BLS CPI airline fares and EIA jet fuel spot, via search_catalog with the series names (CPI airline fares, kerosene-type jet fuel).

Proposed resolution: b — broaden `fact-check-a-news-article` with a nominal-versus-real and magnitude-consistency check, plus an instruction to try BLS CPI/EIA series names in search_catalog before declaring no table.

## Corpus gaps stated

- "search_catalog found no jet fuel price table and no airfare CPI table (energy, econ and transport were searched)." Reason: catalog search returned none; every claim was graded on outside sources.
- claims.json for the $4.90 claim: "Argus data is paywalled ... $4.90 in late October is plausible but no source found." (Unsupported, not scored.)
- "Hopper's index page was not fetched, only a syndicated Fox local article relaying it, so Hopper figures are checked for fidelity only."
