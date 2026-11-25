# Judge — Newsweek, "Map Shows Where Most Adults Are Living With Their Parents" (Realtor.com data), 2026-11-25

**Question/claim:** Fact-check the Newsweek article (https://www.newsweek.com/map-where-most-adults-living-with-parents-12761153): extract each claim, verify against primary sources, grade on the 0-4 Pinocchio scale. Central claim: 33% of young Americans living with parents is "a record share."

Rank: [askamerica, everyman, expert]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files carry both composer markers (123456789 at the start, 987654321 at the end); askamerica ran the product's verbatim news-check prompt on the askamerica-news-desktop agent (no composer prompt for that arm, by design). A grep of the everyman and expert answers for "askamerica" found no match. No unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only: MBA NewsLink, Fox Business, Freddie Mac, NAR, CRL/NPR | Web (Realtor.com, NAR, Pew, Fox Business) plus warehouse `econ.fred_indicators` for mortgage rate | Web only: MBA NewsLink, NAR, Freddie Mac |
| Vintage | Realtor.com June 2026; PMMS 2026-11-19; NAR 2025 profile | Realtor.com June 2026; PMMS 2026-11-19; NAR 2026 profile | PMMS 2026-11-19; NAR Jul 2024-Jun 2025 |
| Central verdict | "Misleading": count is a record, share is not (33% vs 33.6% in 2020) | "False": share is just below the 33.6% high in 2020 | "Record share is wrong": only the count is a record |
| Overall grade | 2 Pinocchios | 3 Pinocchios (engine-computed) | 2 Pinocchios |
| Claims graded | 14 rows (7 verified, 1 plausible, 1 misleading, 1 unclear, 4 not checked or not checkable groups) | 15 in claims.json (5 true, 1 mostly false, 1 false, 8 unsupported; two are Wachter quotes) | Summary only; about 4 claims confirmed, the rest listed as "not checkable" |
| Visual | None | dashboard.png plus report.html | agent-dashboard.png plus small agent-report.html |

Count note for askamerica: claims.json holds 15 entries (1 false, 1 mostly false, 5 true, 8 unsupported); the headline text says 7 unconfirmed.

## Verification

Central claim. I could not fetch realtor.com directly. A web search of the report's syndications found: 25.2 million adults under 35 in 2025 (record count), about one in three, and 33.0% in 2025 against a pandemic-era peak of 33.6% in 2020 (Burson summary of the report; ABC15 cites 33.9% for 2020, which is higher still). Either way the 2025 share is below the 2020 share, so "record share" is false. All three arms reach this. askamerica labels it "false", the others "misleading"; the verdict is directionally the same. everyman and expert note the count is a record, which is accurate and fair. The central claim check supports all three; askamerica's "false" label is the most exact because the sentence says "record share."

Other numbers, cross-arm:
- Mortgage rate: all three give 7.28% (Freddie Mac PMMS, 2026-11-19). askamerica also pulled it from the warehouse (MORTGAGE30US 7.28%, 2020+ low 2.65%). Match.
- NAR median first-time buyer age 40: everyman cites the 2025 profile (up from 38, 21% first-time share). askamerica cites the 2026 profile (40, "matching all-time high", 22% share). expert says confirmed. askamerica's point that 40 matches rather than sets a new record is the more current reading. Newsweek's "remains a record high" is therefore fine.
- "16 years" to save a down payment: everyman verified it (CRL / Julian Bond Institute, via NPR, 3.5% down on a $335,574 home, twice as long as 2019). askamerica graded it unsupported because the Julian Bond page returned HTTP 403, and did not try the NPR coverage. expert listed it as not checkable. everyman is right here; the others under-searched. Grading it "unsupported" is a calibrated "not found", not a false verdict, so the penalty is small.
- "32.3 percent in the South and 31.7 percent in the Midwest" jump: askamerica caught that the article's next sentence says +1.4 points, so these are levels mislabeled as jumps ("mostly false"). everyman flagged it as "Unclear". expert missed it.
- Home prices more than 30% above seven years ago: askamerica found Realtor.com +34.4% (true). everyman "not checked". expert "not checkable".
- Wachter 30-34 share new high: askamerica found 12.7% in 2025 (11.4% in 2019, 7.1% in 2000), true. The others did not test it.
- Metro and regional figures (NY 41.3%, Northeast 36.9%, Las Vegas +62.7%, Midwest prices, $183K gap, Midwest 25-34 ~15%): none of the arms found a source. All three left them ungraded or "unsupported". Correct use; none graded these true or false.
- Great Depression comparison: askamerica "unsupported", everyman flags it as needing a source and as conflicting with 2020, expert silent.

askamerica engine rating versus stated grade: report.html headline says "somewhat biased and mostly false; 7 claims could not be confirmed". agent.md states "Rating: 3 Pinocchios (engine-computed)", and the orchestrator maps "mostly false" to 3 Pinocchios. No mismatch in the rating. Two small notes: the headline says 7 unconfirmed claims while claims.json has 8 unsupported entries; and 3 Pinocchios is harsh for an article whose headline numbers (25.2M, 33%, 7%+, 40) are true. A 2 would fit the facts. The grade is driven by one false central claim plus the mislabeled 32.3/31.7 sentence. It is a defensible reading of the scale, but it leans high.

Wrong-table or stale checks: none. The only warehouse use was FRED mortgage rate, which is current.

## Ranking rationale

1. askamerica: Widest and most exact claim coverage. It is the only arm to grade the 32.3/31.7 sentence, the 30% price rise and the Wachter statistic, and it labels the central claim precisely. Evidence is primary (Realtor.com, NAR 2026, Pew) plus a warehouse pull for the rate. Its "unsupported" use is honest, with a search trail for each. Weaknesses: missed the NPR copy of the CRL 16-year finding, the 3 Pinocchio grade is on the high side, and the headline count of unconfirmed claims is off by one.
2. everyman: Correct central verdict, a clear per-claim table, and the only arm to verify the 16-year figure. It used honest "not checkable" labels. It missed the home-price check and graded the 32.3/31.7 sentence only as "unclear". The 2 Pinocchio grade is well calibrated.
3. expert: Same central verdict and correct on rate and NAR age, but the answer is a short summary with no per-claim table, so coverage is not visible. It lists the 16-year figure and the home-price rise as not checkable when they were checkable. It missed the 32.3/31.7 contradiction. Its shell and methodology briefing did not show up in the result.

What would change the ranking: if the orchestrator's live check showed the 3 Pinocchio grade to be an engine error (the grade should be 2), askamerica would still lead on coverage, but the margin over everyman would narrow. If everyman's NPR/CRL source for 16 years turned out to be a different study, everyman would lose its one unique win and the gap to expert would shrink.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

- everyman found the NPR coverage of the CRL study that gave the "16 years" figure, after the Julian Bond page was blocked. askamerica hit the 403 and stopped. Existing recipes cover this: `known-official-source-403-try-wayback-before-giving-up` (try Wayback or syndicated copies) and `unfetchable-source-is-unverifiable`. The news recipe `fact-check-a-news-article` already says to web_search the headline for another outlet's copy when the URL fails. The recipe exists; askamerica did not apply it to the report page (it searched for the exact claim only).
- askamerica did something worth keeping: it tested the article's own sentence against the next sentence and caught a level-versus-change error. `fact-check-a-news-article` (test at the article's own grain) covers this.
- No warehouse table holds young-adult co-residence. A warehouse method does not exist for the central claim; the web method is the only route. Census CPS ASEC table AD-1 or ACS tables (B09021, living arrangements) would be a warehouse-native source if the census schema had them. I could not check that from here.

askamerica is #1, so the classification below only applies to the small misses (16-year figure, 3-vs-2 grade).

Proposed resolution: b — apply the existing 403 and syndicated-copy recipes to a blocked secondary report (search the study's title or the press coverage, not only the claim text) before grading a figure "unsupported".

## Corpus gaps stated

- "No warehouse table carries young-adult co-residence. Only the mortgage-rate claim used warehouse data."
- Las Vegas +62.7%: "No Realtor.com metro price page located; catalog has no Las Vegas series."
- Midwest +48.4% / Northeast +50.8%: "No regional price page located."
- $183,000 listing gap: "No regional listing price page located."
- Midwest 25-34 near 15%: "No Midwest cohort series on pages read."
- Northeast 36.9%, down 2.3 points: "Realtor.com report and Pew page carry no 36.9% or 2.3-point figure."
- New York 41.3% and metro list: "Metro list is not on the Realtor.com page or the Pew page."
- Julian Bond "16 years": "Primary page 403; no copy read."
- Wachter Great Depression comparison: "Realtor.com and Pew series begin in 2000; no pre-1940 series opened."
