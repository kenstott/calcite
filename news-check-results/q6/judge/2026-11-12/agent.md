# q6 judge: Industrial Cyber "Bridewell reports 80% surge in global ransomware victims as manufacturing emerges as top target in Q3 2026"

Rank: [everyman, expert, askamerica]

Judged 2026-11-12. The article returns HTTP 403, so its body was not read by any arm or by me. All three graded claims reconstructed from search summaries. I fetched Bridewell's report directly (WebFetch succeeded) and searched for other trackers.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| everyman | 1 | Bridewell report; Comparitech Q3 2026; article blocked |
| expert | 1 | Bridewell report; ransomware.live API pull; GuidePoint, ReliaQuest, Comparitech, Point Wild; article blocked |
| askamerica | 1 | Bridewell report; CyberMaxx, ReliaQuest, cinchops; warehouse searched (no ransomware table); article blocked |

## Assessment
- **Correct verdict: 1 Pinocchio.** The headline figures match Bridewell exactly, so there is no factual error, but "victims" are leak-site postings and the Q3 2025 base was the series low, so an unqualified "80% surge" and "emerges as top target" lack context.
  - **Verified directly in Bridewell:** 2,889 victims; +80% from 1,609; +26% from Q2 2,299 in the chart; 111 groups (+14% from 97 in Q2); manufacturing 474 (16.4%) first, then professional services 392, technology 363, healthcare 285; Qilin 362, Thegentlemen 360, Storm 78 (new); Coveware average $1,025,278, median $400,000, payment rate 28%.
  - **Bridewell internal flaws:** its analyst note says "approximately 30% higher than Q2", which is really the rise over the six-quarter baseline of 2,229. Q2 manufacturing 274 is 11.9% of 2,299, not the stated 12.4%. Whether the article repeats either is unknown.
  - **Other trackers (my search):** Comparitech 2,627 attacks with manufacturing 478; NCC 2,865; NordLayer 2,894; GuidePoint 2,760 victims from 112 groups. Totals are in the same range as Bridewell's 2,889 and none contradicts it. I could not confirm a Q3-specific sector ranking from ReliaQuest or GuidePoint, so claims about those are unverified by me.
- **everyman (1st):** correct rating, broadest correct claim coverage.
  - It checked ten claims, all consistent with Bridewell, including the 111 groups, the new groups, the Clop spike and the Coveware payment direction.
  - It disclosed the access limit and listed unchecked items (quotes, OT commentary).
  - It got the 26% versus "30%" point right as a caveat in Bridewell's own text.
  - It flagged the leak-site proxy problem, the low Q3 2025 base and the group-count inflation.
  - Comparitech's 478 manufacturing figure agrees with what I found.
- **expert (2nd):** correct rating, strong independent checks, one mis-framing.
  - Its ransomware.live pull and the GuidePoint/Comparitech cross-checks are useful, and it marked Coveware as not checkable.
  - Error: it says "~30% over Q2" is "not supported by Bridewell (+26%)" and attributes it to the article as reported. Bridewell's own text contains the 30%, so this is a source inconsistency, not an unsupported article claim.
  - Its own pull showing Q3 2025 at 1,398 (+108%) and Thegentlemen at 395 is unverifiable here and is presented with less weight than it deserves; the conclusion that totals drift with backfill is reasonable.
  - Thinner on the manufacturing-lead and Clop context than everyman.
- **askamerica (3rd):** correct rating, but weaker on handling what cannot be verified.
  - Its opening sentence ("biased and mostly accurate") is not a clean verdict statement.
  - It labels the 30% claim "PARTIALLY FALSE" while admitting the error is Bridewell's, which charges the article for a source flaw.
  - It cites ReliaQuest as saying professional services led and that 93% of growth came from 43 new groups, while expert says ReliaQuest agrees manufacturing is first. I could not retrieve a ReliaQuest Q3 2026 result, so that claim is unverified by me and the two arms contradict each other. It also relies on CyberMaxx and cinchops, which I could not confirm.
  - Positive: it noted that the warehouse has no ransomware leak-site table and said so, and it flagged leak-site proxy limits.

## Recipe check
Partly. A recipe for vendor-report-relay stories would help askamerica:
- **Separate the vendor report from the outlet.** Errors inside the vendor report (the 30% note, the 12.4% share) should be labeled as source flaws, not article falsehoods, especially when the article body was never read.
- **Do not cite a third-party tracker's finding unless the page was actually fetched.** The ReliaQuest sector claim conflicts with another arm's reading.
- **Cross-tracker comparisons:** totals differ by methodology (2,627 to 2,894), so a differing count is not a contradiction.
- Warehouse coverage: no ransomware leak-site table exists, so the web was the right route.

## Severity invariant
**Triggered:** askamerica ranks below everyman. All three gave 1 Pinocchio, matching the correct verdict, so the ranking turns on accuracy of findings and handling of the unverifiable, where askamerica charged the article with a source flaw and made a ReliaQuest claim that conflicts with another arm.

## Defects
- askamerica: attributed Bridewell's own "30% above Q2" inconsistency to the article and labeled it PARTIALLY FALSE.
- askamerica: ReliaQuest sector ranking claim is unconfirmed and contradicted by another arm.
- expert: same attribution of the 30% figure to the article rather than to Bridewell's text.
