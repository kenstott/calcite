# Judge — Breitbart, "JD Vance task force has stopped over $6 billion in food stamp fraud", 2026-11-25

**Question/claim:** Fact-check the 2026-11-20 Breitbart article (https://www.breitbart.com/crime/2026/11/20/jd-vance-task-force-has-stopped-over-6-billion-in-food-stamp-fraud/). Central claim: Vance announced that USDA has stopped over $6 billion in SNAP fraud (6,400+ retailers removed, 4,600+ illegal POS devices disabled).

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files carry both composer markers (123456789 at the start, 987654321 at the end); askamerica ran the product's verbatim news-check prompt on the askamerica-news-desktop agent (no composer prompt for that arm, by design). A grep for "askamerica" in the everyman and expert files found no hits; neither unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web search; article via summary; did not fetch Vance post or USDA Nov update | Fetched Vance X post, USDA Sept 18 release, Epoch Times, Breitbart Oct 21, White House Fraud Ledger, GovExec/ProPublica; one warehouse query (fiscal.snap_benefits_by_geography) | Web search summaries; usda.gov 403; DOJ via search only |
| Vintage/window | Sept and Oct 2026 releases | Nov 19 post, Sept 18 release, Oct 21 DOJ, FY2024-25 SNAP benefits | Sept 18, Oct 21 |
| Headline figures | $6B, 6,400, 4,600 "not confirmed"; $5.8B confirmed | $6B post exists, estimate only; 6,400/4,600 true as post; $93.5B FY24, $95.8B FY25 | $6B/6,400/4,600 "low confidence"; $5.8B "not found"; DOJ $42M supported |
| Overall grade | 2 Pinocchios | 0 Pinocchios (engine, author claims only) with framing caveats | 2 Pinocchios |
| Claims graded | 10 | 9 (6 author, 2 speaker, 1 fidelity) | 7 |
| Central verdict | Not confirmed; "stopped" is "potential"; credit to task force unclear | Post is real; figure is an unaudited administration estimate; headline credits "Task Force", post credits USDA | Real post plausible; $6B is unaudited "potential" estimate |
| Visual | none | dashboard.png (SNAP benefits FY24-25) plus report.html | agent-dashboard.png plus report |

## Verification

Primary check. A web search returns Vance's X post (https://x.com/VP/status/2123499556092469306). Its text: ".@USDA has stopped over $6 billion in suspected fraud in the SNAP program through the disqualification of over 6,400 retails and the disabling of over 4,600 illegal point-of-sale devices. These benefits are for Americans who need them, not fraudsters who wish to steal..." Published Thursday Nov 19. So the figures in the article's central claim are accurately relayed. The post says "suspected fraud" and credits USDA, not the task force. The Sept 18 USDA release (Rollins) said "prevented $5.8 billion in potential retailer fraud."

- This supports askamerica, which fetched the post and graded the relay true. It also graded the $6B itself unsupported as independent fact (administration-only source, no method).
- everyman said "I could not retrieve the post" and "Not confirmed" for 6,400 and 4,600. The post is findable by search. This is a miss, not a calibrated gap. everyman also called the Vance quote "not checked."
- expert gave low confidence on the same figures and said $5.8B was "not found." The $5.8B is in the Epoch Times and WJLA coverage of the Sept release, which askamerica fetched.
- DOJ surge: askamerica and expert both support "nearly $42M, 40+ defendants." everyman said "about $40M," a small mismatch, and did not settle it. expert quotes "nearly $42M SNAP" and about $45M intended loss. askamerica verified the $29.7M, $4.4M and $1.09M cases through Breitbart's Oct 21 piece quoting DOJ, not the DOJ page. I did not read the DOJ page. Open item: confirm the three case amounts against justice.gov.
- Kansas City indictment: askamerica graded unsupported (page empty, no search match, stated as not proof of falsity). expert said not checkable. everyman said not checked. All three handled it with similar calibration.
- $100B a year: askamerica ran a warehouse query. $93.5B (FY2024) and $95.8B (FY2025), monthly rows only, and warned that adding annual Total rows doubles it. Matches "roughly $100B." Open item: the orchestrator can rerun the SQL to confirm those two sums. expert said "consistent, not re-pulled." everyman said "not checked."
- Wrong-table or stale picks: none seen.

Engine rating versus stated grade. claims.json has 6 author claims: 5 true and 1 unsupported (not scored). report.html shows Honesty 100/100 and the engine rating of 0 Pinocchios. agent.md states 0 Pinocchios. No mismatch in the final files. The calls log shows an earlier deliver_report refused because the agent had stated 1 Pinocchio against the engine's 0. The files on disk are consistent. Note that the 0 rates the author's own claims only. The speaker claims (Vance's $6B) sit in subject_claims and are not scored toward the article. This explains the gap to the 2 Pinocchios the other arms gave. Reasonable people could grade the piece higher for a headline that credits the "Task Force" and relays the figure without a caveat. askamerica states that caveat in prose. report.html also lists claims 8 and 9 twice in the table, a display duplicate.

## Ranking rationale

1. askamerica. It read the primary source (Vance post) and the Sept USDA release, correctly split the speaker's claims from the author's, and found the key defects: "stopped" is an annualized or potential estimate with no method; the headline credits a task force where the post credits USDA. It added ProPublica context (confirmed recipient fraud about 0.08%, staff cuts) that neither other arm raised in that form. It used one warehouse query for the $100B claim and flagged a double-count trap. Its "not checkable" use was narrow and explained. Weakness: the 0 Pinocchios is engine-forced and may read as lenient on the headline; DOJ amounts rest on a secondary report.
2. expert. Seven claims, sound caveats on "stopped" versus charged amounts (140 times ratio) and unreconciled $10B and $56B claims. Better on the DOJ $42M than everyman. It did not find the Vance post and left the central figures at low confidence. It skipped the SNAP cost check with a plain "consistent."
3. everyman. Ten claims and good skepticism, but it left the central figures "not confirmed" when the source post was one search away, and it left the $42M and $100B items unsettled. It also read the article only through a summary.

What would change the ranking: if the DOJ case amounts or the warehouse SNAP sums fail the orchestrator's live check, askamerica's lead narrows. If the $6B post is judged to deserve 2 Pinocchios on the headline alone, expert and everyman's grade would match the better call, but they would still lack the primary-source evidence.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

- askamerica used the fact-check procedure (find_recipe "fact-check news article", served as fact-check-a-news-article). Good.
- Pattern worth noting: askamerica did a catalog search for "SNAP improper payment error rate" and "SNAP fraud stopped retailers disqualified" and found no fraud-stopped measure. A diagnostic flagged recipe_not_consulted:high on the second catalog call. Cause is a minor ordering issue, not a wrong answer.
- Pattern from expert: compare the headline figure to charged amounts and to other administration totals (ratio check). Existing coverage: the fact-check-a-news-article recipe. I did not grep it for a ratio step; a short added step "compare a stopped/prevented figure with charged and recovered amounts" is a small broadening.
- A warehouse-native check for "fraud stopped" does not exist (fiscal schema holds benefits issued only). Improper-payment or recovery tables, if sourced, would let askamerica check the $6B directly.

Proposed resolution: d — askamerica ranked first; no actionable cause, with an optional small broadening of the fact-check recipe for stopped-versus-charged ratio checks.

## Corpus gaps stated

- "The catalog has no fraud-stopped measure." (Claim: Vance's $6B stopped.) Reason: no warehouse series for SNAP fraud stopped or retailers disqualified; also "no independent government series or audit found; method unpublished."
- Kansas City grand jury indictment of an Illinois store owner, graded unsupported: "DOJ page returned empty content; web search returned only unrelated cases"; "catalog has no indictment data."
- White House Fraud Ledger: "loaded but its figures did not render."
- $6B claim: "USDA release (Sept) gives $5.8B only."
- DOJ surge claims: DOJ page itself not read; checked via Breitbart Oct 21 quoting DOJ.
