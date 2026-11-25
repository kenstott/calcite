# Judge — KERA via HPPR, "Texas public school districts suffer historic enrollment decline", 2026-11-25

**Question/claim:** Fact-check the article (https://www.hppr.org/hppr-news/2026-11-19/texas-public-school-districts-suffer-historic-enrollment-decline). Central claim: Texas public school enrollment fell by more than 135,000 this year, the largest one-year drop on record.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files carry both composer markers (123456789 at the start, 987654321 at the end); askamerica ran the product's verbatim news-check prompt on the askamerica-news-desktop agent (no composer prompt for that arm, by design). A grep for "askamerica" in the everyman and expert answers found no hits, so neither unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only (Tribune, Fox, CBS Austin). No TEA document retrieved. WebFetch gave only a summary of the article | Warehouse edu.ccd_districts (2013-2024), census.pep_components_of_change; TEA enroll-2025-26.pdf; Fox 4, Community Impact, Tribune, Houston Public Media read directly | Web plus TEA 2024-25 report; Tribune, Community Impact, Fox 7, Houston Public Media, AP |
| Vintage | Fall 2026 preliminary | Fall 2026 preliminary; warehouse ends 2024-25 | Fall 2026 preliminary; TEA 5,544,255 for 2024-25 |
| Headline figures | 135,000; 122,000 (2020-21); 212,000; 66% | 135,000; -122,592; 5,545,640 (CCD) vs 5,544,255 (TEA); births 364,360 vs 378,664 | 135,000; 122,354; 5,544,255; 368,300 births 2020 |
| Pinocchio grade | 2 | 0 (engine rating from claim scores) | 1 |
| Claims graded | 11 table rows | 18 scored plus 1 unscored source audit | 15 |
| Central verdict | Supported, preliminary | True, central claim, preliminary noted | Supported, preliminary |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

Central claim, spot-checked by web search: Fox 4, Community Impact, CBS Austin, Texas Tribune all report Morath told the SBOE on Nov 18, 2026 that about 135,000 fewer students are enrolled, a record drop, and that it is preliminary (official snapshot in February). The same search shows 66% of districts, a 76,000 prior-year drop (about 212,000 over two years), and Hinojosa vowing to replace Morath. I did not fetch the TEA table itself. All three arms' central verdicts match this.

Number checks:
- 122,000 COVID drop: everyman 122,000 (Fox 4); expert 122,354 (Tribune); askamerica -122,592 (CCD) and TEA 5,371,586, -2.2%. Consistent within series differences.
- 5.5M record: expert and askamerica both cite TEA 5,544,255; askamerica adds CCD 5,545,640. Everyman could not confirm "record".
- 212,000: all three reconstruct it as roughly 76,000 plus 135,000. Match.
- 85% / 89% public-school share: expert found Tribune 85.6% and "nearly 90% in 2015"; askamerica and everyman find no 89% source. Askamerica graded the 89% unsupported; expert graded it roughly supported. Everyman not confirmed.
- 28,000 voucher figure: expert traced a Houston Chronicle estimate of 28,200 but the article attributes it to Houston Public Media. Askamerica read the HPM story and found no 28,000 figure (30% of 120,000+ awards). Everyman found no source. Askamerica's fidelity finding is the sharpest of the three.
- 66% of districts: expert and askamerica both note it is the 2025-26 figure placed beside the 2026-27 loss. Everyman called it supported and missed the mismatch.
- Hinojosa replacing Morath: expert says a pledge, not yet done, and the phrasing reads as settled. Askamerica graded it "confirmed" with no caveat. Everyman called it speculation. Askamerica is the least calibrated here.
- Prosper/Princeton 14%: all three could not verify. Askamerica found Princeton projected about +13.5% (11,297 vs 9,956) and Prosper about 7%, the closest partial evidence.

Askamerica engine rating versus stated grade: report.html says 0 Pinocchios and agent.md says 0 Pinocchios. No mismatch. But the same report lists 4 unsupported claims, an unsupported source attribution, and five "materially misleading" framing items. A 0 is out of step with its own findings.

Grading where no evidence was possible: none of the arms graded the "pattern across the country" quote true or false. Expert and askamerica both marked it not checkable. Everyman did not list it.

## Ranking rationale

1. askamerica. Deepest primary evidence: TEA PDF table, CCD and PEP warehouse series, direct reads of Fox 4, Community Impact, Tribune and HPM. It alone proved the 28,000 attribution is wrong and showed the births explanation is too small for a one-year loss (births recovered to 390,203 by 2025). It also caught the 66% mismatch, preliminary-versus-final, and the omission. Weaknesses: the 0 Pinocchio grade is too lenient, and the Hinojosa claim is graded confirmed without noting it is a pledge.
2. expert. Correct central verdict, 15 claims, good calibration (1 Pinocchio), caught the 66% mislabel and the Hinojosa nuance, used the TEA report. It did not catch that HPM never says 28,000, and it leans on web summaries rather than the warehouse.
3. everyman. Correct central verdict but no TEA primary document, WebFetch summary only, so quotes were not checked verbatim. It missed the 66% mismatch and found no source for 28,000 and 212,000 context beyond Tribune. It states its limits honestly. Its 2 Pinocchios is the most defensible grade given the voucher issue, but the evidence behind it is thinnest.

What would change the ranking: if the orchestrator finds the engine's 0 Pinocchio reflects a scoring defect that a reader would take as endorsing an article with an unsupported attribution, expert could rank first. A live check that edu.ccd_districts values are wrong would also move askamerica down.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Existing recipe `fact-check-a-news-article` covers the method. Askamerica did the patterns expert/everyman did, plus the fidelity audit of the cited source. Two points worth teaching under that recipe: (1) the engine rating should not read 0 when claims are unsupported and the cited source does not contain the quoted figure; (2) a claim that a person "is expected to" do something should be graded on whether it has happened, not on whether it was pledged. Open item for the orchestrator: the CCD values (5,545,640; -122,592) can only be confirmed by a live query.

Proposed resolution: b — broaden `fact-check-a-news-article` so a failed attribution (fidelity) finding and unsupported claims lower the engine rating, and pledged-but-not-done events are graded as partly supported.

## Corpus gaps stated

- "Pattern across the country" quote: "no national K-12 enrollment series in the warehouse."
- Prosper and Princeton ISD growth: "The warehouse ends at 2024-25" and "The TEA district table could not be searched."
- Pre-COVID 89% share: "no source states 89%."
- Immigration fears: "The Texas Tribune offers only advocacy and anecdote" (competing net international migration figures given).
- "Most districts lost millions": "no statewide total found."
- 2025-26 and 2026-27 enrollment: warehouse coverage ends 2024-25 (edu.ccd_districts 2013-2024); TEA totals taken from the TEA PDF instead.
