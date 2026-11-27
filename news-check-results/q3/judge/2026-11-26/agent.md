# Judge: Rep. Scott Peters press release, "Peters, Whitesides, Crank Urge Inclusion of Forestry Bill in Annual NDAA Package", 2026-11-26

**Piece:** https://scottpeters.house.gov/press-releases/peters-whitesides-crank-urge-inclusion-of-forestry-bill-in-annual-ndaa-package. The release is dated 2026-10-23, which falls outside the default 7-day discovery window. I kept it because its season-to-date wildfire figures are still the current season's numbers. Each persona was asked to fact-check the piece, extract the claims and grade it on 0-4 Pinocchios.

Rank: [expert, askamerica, everyman]

All three personas ran fresh today. The fingerprint check passed: the everyman and expert prompt.md files carry both composer markers. Askamerica ran the verbatim news-check prompt on askamerica-news-desktop. No unaided arm mentions askamerica.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Text read | WebFetch summary of the release only | Release in full; the linked letter only in part | Release and the 28-member letter, quotes verified verbatim |
| Fire stats | NIFC through Oct 30 (67,457 fires / 9.39M acres); noted that the release matches an earlier snapshot | NIFC 10/23 sitrep matches exactly; the warehouse WFIGS table agrees on acres but counts fewer incidents | NIFC through Oct 23: 65,796 fires / 9,048,486 acres (132% / 141%); 1,614 personnel |
| Bill status | H.R. 471 passed the House 1/23/2025; the Save Our Sequoias Act sponsor is Fong, so "Peters led" is overstated | House passages confirmed from the warehouse bill table; Senate S.1462 not passed | Vote tallies (279-141; voice vote); no Senate floor vote |
| Nuance caught | JEC cost range is a Democratic-majority estimate; the $3.75 figure is fuel treatments only | $3.75 graded mostly true (fuel treatments); the military-readiness opinion marked not checkable | "600 homes" were 600 structures; preparedness level dropped to 1 by Oct 14 (omitted); 48 days at PL5 unconfirmed; JEC and UC Davis caveats |
| Grade | 1 | 0 (engine-computed) | 1 |
| Defects | Did not read the text verbatim | The sequoia "one-fifth since 2015" claim graded **true**, citing Peters's own March 2026 release as corroboration (circular) and NPS's 13-19%, which sits below one-fifth, with no 2015 anchor found | Dashboard tiles (17 claims) disagree with the prose count (16) |

## Verification

- The fire statistics in all three arms trace to NIFC IMSR/YTD pages. Askamerica and expert both pin the release's numbers to the Oct 23 snapshot. Everyman compared against Oct 30 but correctly said the release matched an earlier snapshot.
- Sequoia claim: askamerica's own evidence (NPS, 13-19% of large sequoias killed in 2020-21) does not reach "one-fifth" and does not support "since 2015". Its other corroboration is the claimant's own earlier release, which is not independent. By the engine's own rubric this should have been "mostly true" with an `inaccuracy`. This is a grading slip, not a data gap.
- Claim shape: split present (8 author_claims, 2 subject_claims with speaker "Letter signers"). The release is the office's own voice, so author_claims is right for most claims. No regrouping needed.

## Ranking rationale

1. **expert** read both the release and the letter. It verified the quotes verbatim and surfaced the most material context: the structures-versus-homes wording, the omitted drop in preparedness level, and the unconfirmed 48-day PL5 claim. Its 1-Pinocchio grade is the better-calibrated call for an advocacy piece with several stretched figures. Its dashboard/prose count mismatch is a presentation defect, not a substantive one.
2. **askamerica** got the vintage exactly right, used warehouse tables (WFIGS fire incidents, bills) to check acres and bill status, and correctly scoped the $3.75 claim. It checked fewer claims than expert and graded the sequoia claim true on circular evidence. That pulled its rating to 0 where 1 was warranted.
3. **everyman** reached reasonable conclusions and caught the Fong sponsorship point, which askamerica missed. It worked from a summary rather than the text and compared against a later NIFC snapshot.

The askamerica-everyman gap is narrow. Everyman caught two points askamerica graded too generously (Fong, sequoia). Askamerica wins on exact-vintage verification and warehouse-backed bill and fire checks. If the sequoia and sponsor grading errors were weighted above data verification, the two would swap. That would be a severity-invariant defect, so I record the call as close.

## Recipe check

The candidate lesson is **do not corroborate a claim with the claimant's own earlier statement.** Askamerica graded the sequoia figure true partly because Peters's March 2026 release used the same wording. No recipe or McpServer instruction currently names this. This is the first observed instance, and the loss to expert is mainly breadth (expert read the letter). Under the daily-eval discipline (below-expert losses need a repeat instance before a recipe), I am logging it here rather than writing a recipe today. No data gap is involved, so nothing is filed.
