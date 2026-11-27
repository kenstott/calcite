# Judge — q9 WDBO/WFTV, "Traffic fatalities drop significantly across Florida, nation" (2026-11-25), 2026-11-27

**Question/claim:** Fact-check the article. Central claims: NHTSA data shows 2,155 Florida traffic deaths so far this year vs nearly 3,000 at the same point last year (834 fewer); Central Florida's six counties fell from 550 to 375 as of Nov 14; US deaths fell 2.5% in H1 2026 to 16,750 from 17,175, VMT up 0.8%, fatality rate near a record low.

Rank: [expert, askamerica, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and contain 987654321 (pass); askamerica ran the extension's verbatim news prompt (no fingerprint by design). askamerica's answer is a same-day rerun after an engine recipe update and is judged on its own merits. A grep for "askamerica" in the everyman and expert directories found nothing. Neither unaided arm cites askamerica.ai. Nothing to discount.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only: NHTSA DOT HS 813 875 via trade press coverage, NSC, NHTSA/DOT Florida 2025 totals, FLHSMV dashboard | NHTSA 813 875 state table (read directly), warehouse `transport.fatal_crashes` (FARS to 2024), trade press | NHTSA 813 875 Tables 1 and 3 (read directly), DOT Florida 2025 total via search |
| Vintage / window | H1 2026 national; Florida full-year 2025 (3,047) | H1 2025 vs H1 2026 state; FARS 2022-2024 | H1 2026 national and state; Florida full-year 2025 |
| Headline figures | 16,750 vs 17,175 true; Florida 2,155 "not verified, doubtful"; county "not checkable" | 16,750 vs 17,175 true; Florida 1,474 to 1,439 (-2.4%); FARS FL 3,138 (2024); county FARS 487 (2024) | 16,750 vs 17,175 true; Florida 1,474 to 1,439 (-2.4%); 2,155 "not verified; implausible" |
| Overall Pinocchio | 3 | 3 (agent.md, stated as own judgment; engine left rating blank) | 2 |
| Claims graded | 5 (4 verdicts plus misleading-framing notes) | 3 in claims.json (1 fidelity, 2 author claims) | 5 (plus an arithmetic note on 834) |
| Verdict on central claim | Not verified, doubtful, likely partial-year vs full-year | "FALSE or misleading" (engine: false) | Not verified, implausible, medium confidence |
| Visual | none | dashboard.png and report.html | agent-dashboard.png and agent-report.html |

## Verification

Central claim, primary source. The orchestrator read NHTSA DOT HS 813 875 Table 3 from the PDF: "Florida 1,474 1,439 -2.4%". My own fetch of the PDF returned only a binary stream I could not extract, so I rely on the orchestrator's reading. Both askamerica and expert quote exactly these figures. Everyman never reached Table 3.

- National claim: 16,750 vs 17,175 is -2.47%. All three arms grade it true and match NHTSA. VMT +0.8% and rate 1.02 vs 1.05 (everyman and expert both give this) agree.
- Florida claim: the article's 2,155 vs "nearly 3,000" (implied 2,989 from the 834 gap, a 28% drop) cannot be reproduced. NHTSA's state table shows -2.4% for H1. This supports the "not supported / overstated" verdict in all three arms. It does not strictly prove the article's year-to-date number false, because the NHTSA table covers H1 and the article says "so far this year" to mid-November. The orchestrator confirms the FARS 2025 national file is not published (404), so the warehouse cannot test 2025 or 2026 and that is source lag.
- askamerica's "FALSE" grade goes further than the evidence. Its own table lists windows tested: H1 -2.4%, FARS 2023 to 2024 -7.0%, article-implied -28%. None of those is the article's window. "Unsupported, implausible" (expert) or "not verified" (everyman) is better calibrated. The engine also marks the claim `errs: toward_thesis`.
- askamerica's "overstate the decline roughly tenfold" compares a mid-November year-to-date figure with an H1 figure. That is the same mismatch it accuses the article of.
- Everyman and expert both found Florida 2025 full-year of 3,047, which shows "nearly 3,000 at this point last year" is high. askamerica fetched the Patch page for it (call 13) but did not use it in the answer.
- Expert noted that 3,000 - 2,155 = 845, not 834. This is consistent with the "nearly 3,000" wording (2,989) and is a minor point, not an error by the article.
- Everyman and expert both caught the slowing national trend (Q1 -5.0%, Q2 -0.2%) and that "significantly" overstates -2.5%. askamerica did not.
- Expert checked the "reckless and impaired driving" attribution and called it not checkable. everyman called it plausible but unverified. askamerica did not extract it as a claim. Missed claim.
- Everyman's "NHTSA/DOT Florida 2025 preliminary data (3,047 vs 3,138 in 2024)" agrees with the warehouse FARS 2024 value of 3,138 that askamerica reports. Consistent.
- County claim: all three could not check 550 to 375. askamerica reports FARS 487 for 2024, which gives context but no 2025 or 2026 value. Graded "unsupported" in claims.json, "UNSUPPORTED" in agent.md. Consistent and calibrated.
- Open item for the orchestrator: the askamerica FARS figures (FL 3,548 / 3,375 / 3,138; six counties 487 in 2024) can only be checked by a live query. The first query attempt errored on a wrong column (`fatals`); the retry with `fatalities` returned rows.

askamerica engine rating vs stated grade. claims.json has no overall rating. report.html carries no Pinocchio rating; its headline reads "too few to rate; 1 claim could not be confirmed". agent.md states 3 Pinocchios as "my own assessment" and says so. This matches the orchestrator note. Not a hidden mismatch, but the lead line of agent.md says "too few to rate" and the body then gives a 3, which reads as mixed. The 3 also sits above expert's 2 for a piece whose national half is fully correct and whose Florida half is unverified rather than disproved.

## Ranking rationale

1. expert. Primary-source evidence (Tables 1 and 3), correct central verdict ("not verified; implausible", medium confidence, with a stated test that would change it), the widest claim coverage (attribution claim, slowing trend, "significantly" framing, arithmetic), and a calibrated 2 Pinocchios. It did not overreach on the Florida window mismatch.
2. askamerica. It reached the same primary table and added warehouse FARS context for the state and counties. Its central direction is right. It loses to expert on calibration (graded the Florida claim "false" against a different window, "tenfold" comparison), on coverage (no attribution claim, no slowing-trend claim; only 3 claims), and on a lead line that says "too few to rate" next to a 3.
3. everyman. Right direction, good use of Florida full-year 3,047, and good calibration, and it caught the slowing trend. It never reached Table 3, so it could not give the decisive -2.4% figure, and its 3 Pinocchios rests on inference about reporting lag. It is above nothing on evidence; it is below askamerica because askamerica found the agency's own state number.

What would change the ranking: a live NHTSA FARS state pull showing 2,155 as a true like-for-like count would make askamerica's "false" wrong and the others' "not verified" safer. If the 2,155 figure were shown to be a genuine partial-year FARS total, all three Pinocchio grades are too high for the arithmetic and too low for the framing, and calibration would decide even more. If askamerica had graded the Florida claim "unsupported" and extracted the attribution and trend claims, it would rank first on its warehouse context.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

- Recipe `fact-check-a-news-article` already covers this (triggers include "nhtsa data" and "traffic fatalities"; requires comparative phrases and attributed statements as separate claims). askamerica followed the recipe (call 2) but extracted only 3 claims and skipped the attribution claim and the "significantly" framing.
- Pattern from expert and everyman not reached by askamerica: a grade of "false" needs evidence for the article's own window. When the primary table covers a different window (H1 vs year-to-date November), the right grade is "unsupported" with the table value as context. The recipe could add one sentence on this. Warehouse-native method: `transport.fatal_crashes` by `state_fips` and `year` for the long run, plus the NHTSA state table via web_fetch for the half-year window, graded "unsupported" unless the windows match.
- Pattern to keep: reading the agency table directly (askamerica and expert both did).
- Cause classification: (b) reachable pattern not used. The recipe covers claim extraction, but askamerica did not extract every claim and did not downgrade a window mismatch.

Proposed resolution: b — broaden `fact-check-a-news-article` to say a claim whose window differs from the primary table's window is graded "unsupported", never "false", and that attributed statements and "significantly" framing are separate claims.

## Corpus gaps stated

- "FARS loaded only through 2024 (487 for these counties); no 2025/2026 county source found." (claims.json, county claim, graded "unsupported")
- "2025 is not loaded." (agent.md, warehouse FARS for Florida; reason: FARS ends at 2024, which the orchestrator confirms is source lag)
- "no 2025 or 2026 county source was found" (agent.md, Central Florida claim)
- "I could not locate a source for 2,155." (agent.md, Florida claim)
- claims.json `why_no_page`: "NSC page 403; wftv pages had no relevant content."
