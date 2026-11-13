# q6 judge: CFR "The 2026 Election by the Numbers" (James Lindsay, Nov 10, 2026)

Rank: [askamerica, expert, everyman]

Judged 2026-11-13. Verified by fetching the CFR article text (WebFetch, summarized but with verbatim quotes on the contested sentences) and by search coverage (AP-via-Cox, Brookings, IBTimes, Newsweek/CAWP, States Newsroom, Fox/CNN governor reports). No primary source (Clerk of the House, CAWP final table, certified returns) was reachable.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| askamerica | 1 | CFR article (cut off before the last paragraphs), Ballotpedia, House History, Roll Call/NYT age sources, warehouse officials.house_race_results |
| expert | 1 | CFR article, AP, NBC, Cook, CNN/CAWP, UF Election Lab, NCSL |
| everyman | 2 | Secondary coverage only; never obtained the article text |

## Assessment
- **Correct verdict: 1 Pinocchio.** The core claims (Democrats won both chambers, 54-44 Senate with Alaska and Mississippi uncalled, 254/165 House with 16 uncalled, about 13-point House margin, 49.1% turnout, 8.8-9 poll average) match contemporaneous reporting. Counts that moved afterward (AP 255/169, Mississippi later called) are stale, not false. The article does contain a few small slips, which keeps it off 0:
  - **Youngest senator: wrong.** The article says Democrat Jon Ossoff "will be thirty-seven." Ossoff (born Feb 1987) turns 39 in Jan 2027 and 40 in Feb 2027. A 37-year-old (Talarico) would be the youngest. The name is wrong and the age is wrong for him.
  - **Texas governorship: wrong as worded.** "First Democrat to win ... in thirty-two years." Richards last won in 1990 (36 years); 32 years counts from her 1995 exit.
  - **1894: 127 vs 125.** Sources differ slightly (House History 218 to 93 gives 125). Trivial.
  - **"Thirteenth time in sixteen tries":** the 2000-2028 count is 15 elections, a small counting looseness.
  - **159 women:** unverified. Post-election CAWP-based reporting I could find says "at least 155" (record over 152). The record claim holds. I could not confirm 159, so it is a projection discrepancy, not a finding of falsity.
  - **13-point margin and "second since 1942":** Brookings gives 13 points; one outlet says "largest since 1976" (tentative). Early, uncertified, plausible.
  - **Governors "at least 28":** consistent with early tallies (28-20 with Kansas and Alaska uncalled; first majority in 16 years matches the 2010 baseline).
  - **Senate +9 / Alaska and Mississippi "poised":** forward-looking and flagged by the article itself as uncalled. Net +7 flips was the called count at the time, so +9 depends on those two.
- **askamerica (1st):** best findings and right rating.
  - It was the only arm to catch the Ossoff error (and named the correct youngest senator), the Texas 32-versus-36-year error, the 127/125 discrepancy, and the 12-of-14 and 13-of-16 counts.
  - It handled the uncalled-race framing properly (Alaska a 637-vote lead with ballots outstanding) and the early-tally caveat on 13 points.
  - It disclosed that the fetch cut off before the last paragraphs and listed what it did not grade.
  - Weaknesses: left the 159 women claim ungraded and the governor count unconfirmed; "Mostly true" labels on the Texas and "thirteenth" items are generous.
- **expert (2nd):** right rating, thinner findings.
  - It correctly separated stale (AP 255/169) from false and handled the uncalled races well.
  - It marked 159 women "confirmed (CNN/CAWP)" and governors "confirmed (NCSL)"; my search could only support 155 and early 28-20 tallies, so those confirmations are overstated.
  - It missed the Ossoff and Texas errors, and put average age and 12-of-14 under "not checkable."
  - Terse format with little explanation of reasoning.
- **everyman (3rd):** rating one step high and weakest evidence.
  - It never read the article verbatim and graded mostly "not checkable here."
  - It raised the 155-vs-159 discrepancy and the Alaska doubt (reasonable), but called the net +9 and the Alaska characterization problems on weak footing, and its 2-Pinocchio rating rests on inability to verify, not on findings against the piece.
  - It caught none of the real errors (Ossoff, Texas, 1894).

## Recipe check
Partly. A procedure for election-roundup pieces would help:
- Check every named-person or age/tenure claim against a biography source (the Ossoff and Texas errors were pure arithmetic or identity checks).
- Distinguish "uncalled at publication" from "false" and tag later-called seats as stale.
- For record or rank claims, require a primary table (CAWP, Clerk) before marking confirmed; a secondary projection is not confirmation.
- Not assessed from the arm files: whether find_recipe ran before grading.

## Severity invariant
**Not triggered:** askamerica ranks first, above everyman. Its rating (1) matches the correct verdict; everyman's (2) does not.
