# Judge: Granite State Report / NHFPI, "NH Community College Enrollment Is Up 6%. A Third Are in High School.", 2026-11-26

**Piece:** https://granitestatereport.com/2026/11/26/nh-community-college-enrollment-dual-enrolled-high-school-students-nhfpi. The task was to fact-check it, extract its claims, and grade it on the 0-4 Pinocchio scale.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. The fingerprint check passed: the everyman and expert prompt.md files carry both composer markers. Askamerica ran the verbatim news-check prompt on askamerica-news-desktop. No unaided arm mentions askamerica.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Article text | Fetch-tool summary only, so claims were paraphrased | Full fetch; 19 claims extracted | Fetched; checked against the NHFPI brief |
| Independent data | None; brief, NHPR and Keene Sentinel only | Warehouse `edu.ipeds_fall_enrollment` for the 7 CCSNH colleges: fall headcount -37.2%, 2014 to 2023 | None beyond the brief, NHPR and UNH; did not query IPEDS |
| Grade | 1 (borderline 0) | 0 (engine-computed) | 1 |
| Framing flag | 12-month count vs fall count, single-source reliance | Headline pairs +6% with the one-third share although no 2014-15 dual count exists; Promise paraphrase graded mostly true | Headline leads with the flattering 12-month measure while fall is -34.9% |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

- Arithmetic: 9,364 / 24,741 = 37.85% and 24,741 - 9,364 = 15,377. All three arms got this right.
- The warehouse figure is independent evidence. Askamerica's -37.2% fall decline for 2014-2023 comes from IPEDS, not from the brief. It sits close to the brief's -34.9% for 2014-2024, and the gap fits a one-year window difference. No unaided arm reached any federal data.
- The 12-month and dual-enrollment counts behind the headline come from IPEDS EFFY, which the warehouse does not hold. All three arms could only check these against the brief. That is a real corpus gap, now filed as govdata-ops #1540.
- Rating shape: askamerica's first publish put every claim in `author_claims`. After a resume it was regrouped to author 10, fidelity 6 and subject 3 (NHPR and NHFPI as speakers). I verified the regrouped claims.json directly. The engine's rating stayed at 0.

## Ranking rationale

1. **askamerica** checked the most claims (19) and is the only arm that tested the central trend against federal data. It also caught a real paraphrase drift in the article's Promise-eligibility wording, which no other arm graded. It treats the fall-versus-12-month divergence as context the article itself discloses, not as a misleading claim. That is defensible, because the piece reports both measures.
2. **expert** has the strongest framing critique: the headline foregrounds the flattering measure. Its numbers come from the brief, not from IPEDS. It caught the brief's internal "$100,000" endnote inconsistency, and askamerica did not.
3. **everyman** reached the same conclusions working from summaries rather than the article text, so its claim wording is paraphrased.

What could change the order: if the headline-framing point is weighted as heavily as a misstated figure, expert's 1-Pinocchio call is arguably the better-calibrated grade, and expert and askamerica would be close to a tie.

## Recipe check

Nothing new to teach through a recipe. Askamerica already used the warehouse table it had. The missing measure (EFFY 12-month and dual enrollment) is a data gap, filed as #1540.
