# q16 judge: Newsweek "Rollins: Americans 'good with' surging beef prices" (Andrew Stanton)

Rank: [askamerica, expert, everyman]

Judged 2026-11-11. I read the Newsweek text through WebFetch (summarizing), pulled BLS series through api.bls.gov, and checked the executive order dates against DTN, USDA press release, NPR and Drovers search results. The AAA diesel figure and the USDA spokesperson's 10% demand claim could not be verified by me or by any arm.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| askamerica | 1 | Newsweek full text; BLS via warehouse; CPA report read directly; DTN Aug 21 and Sept 4; Agri-Pulse; NASS history |
| expert | 1 | BLS API; USDA NASS catl0126; USDA press release; Al Jazeera, GMA; Newsweek via summary |
| everyman | 1 | Newsweek via summary only; secondary outlets; no primary source |

## Assessment
- **Correct verdict: 1 Pinocchio.** Newsweek's figures are accurate (BLS ground beef, herd, CPA, NCBA). One factual error is Newsweek's own: it says "Earlier in August, Trump signed two executive orders", but they were signed Sept 4, 2026. It also calls the import plan ground beef when the quota covers lean trimmings. The lede lets a stale Rollins figure and an unsupported "most Americans" line stand. This is a minor inaccuracy, not a pattern of shading.
- **BLS APU0000FC1101 (verified via API):** Aug 2026 7.158, Jul 7.116, Aug 2025 6.631, Sep 7.062, Oct 7.068. Record 7.158 in Aug is consistent with the series. Article's $7.16 is correct. Rollins's "$6.50" matches Jul 2025 (6.504), so it is about a year stale; the article quotes it without noting the gap. Ground beef 100% beef was 6.923 in Aug.
- **Herd 86.2M and CPA:** consistent (NASS Jan 1 2026 about 86.15M; CPA report read directly by askamerica).
- **Executive orders:** Sept 4, 2026 (USDA, DTN, NPR agree). The article's "August" is wrong. Aug 21 was the tariff-rate quota announcement, later a proclamation on lean trimmings, 300,000 t, 90 days.
- **AAA diesel $6.5107 and USDA "almost 10%" demand:** unverified by me. askamerica found no support for the 10% figure and a roughly 0.5% yearly rise; that is a spokesperson claim quoted accurately, not an article error.
- **askamerica (1st):** the most findings, correct rating.
  - It caught the executive order date error with a primary-ish source (DTN Sept 4), the trimmings-vs-ground-beef wording, the stale $6.50, and read the CPA report directly.
  - It separated Rollins's errors from the author's and handled unverifiable items as unsupported, not false.
  - Weaknesses: it graded the "executive orders" date claim as flatly FALSE without the White House text; the NASS history work was extra and the 2026 herd was not warehouse-verified. Its headline said "mostly accurate" while listing an author error, which is consistent with 1 Pinocchio.
- **expert (2nd):** correct rating, accurate BLS and NASS confirmation, and it flagged the Sept 4 vs August date as a likely error. It did not verify CPA or the 75% drought figure, did not note the stale $6.50 Rollins figure or the trimmings wording, and checked diesel against the wrong window. Compact but thinner.
- **everyman (3rd):** correct rating but weakest process. It read only a summary, left the executive orders, CPA, herd and diesel unchecked, and so missed the date error that is the article's one clear fault. Its 1 Pinocchio rests on headline framing, not a found error. It did note the proclamation-vs-waiver wording.

## Recipe check
Mostly applicable. Procedure lessons:
- For "signed on date X" claims, match the date against the issuing agency or trade-press report; this surfaced the one author error.
- When a quoted official gives a number, compare it with the series for the stated month; this surfaced the stale $6.50.
- A "demand grew almost 10%" claim needs the metric defined (per-capita, volume, expenditure) before it can be called checkable.
- Wording check: waiver vs quota change, ground beef vs trimmings.

## Severity invariant
**Not triggered.** askamerica (1) is equal to everyman (1) and matches the correct verdict, and askamerica ranks above everyman.
