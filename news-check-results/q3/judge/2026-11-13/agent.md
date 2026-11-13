# q3 judge: yourNEWS "Homicides Drop 15.2% Across Major U.S. Cities as Violent Crime Declines Continue in 2026"

Rank: [expert, everyman, askamerica]

The story ran Nov 12, 2026, on the MCCA January-September 2026 survey. Judged 2026-11-13. I fetched both MCCA PDFs directly and extracted their text locally. The article text itself was not read verbatim by any arm or by me; the checks quote its figures.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| expert | 2 | Both MCCA PDFs (original and Updated), crimeindex.org, Crime Forecast post, NY1 NYPD report |
| everyman | 1 | Axios-derived coverage only; MCCA PDF unreadable; noticed an "Updated" Nov 12 listing on the MCCA site but did not open it |
| askamerica | 1 | Original MCCA PDF (Nov 9) only; White House release; warehouse not usable |

## Assessment
- **Correct verdict: 2 Pinocchios.** The headline direction (big-city homicides falling sharply) is right, but the piece's totals and its New York City claim come from a superseded release.
  - **Two MCCA versions exist.** The site homepage now lists only the Updated report (dated Nov 12, 2026); the original (Nov 9, `...Jan-to-Sept.pdf`) is still reachable. Footnote 3 of the Updated PDF reads "New York City PD information updated November 12, 2026."
  - **Original (article's source):** homicide 3,394 vs 4,000 (-15.2%), rape -3.5%, robbery 62,939 to 57,209 (-9.1%), aggravated assault -3.4%. NYC row lists 239 and 192 in the 2026/2025 columns, i.e. 2026 = 239, 2025 = 192 (an increase).
  - **Updated (current):** homicide 3,347 vs 4,047 (-17.3%), rape -3.8%, robbery 55,786 vs 64,362 (-13.3%), aggravated assault 188,912 vs 196,774 (-4.0%). NYC row is 192 (2026) and 239 (2025), a 19.7% decline. The 47-case swing equals the whole homicide total difference (4,047 - 4,000).
  - **NYPD's own figures agree with the Updated version.** NYPD reported 230 murders through October 2026, down 15.7% (203 through September, 50 fewer than 2025). The article's "NYC homicides rose 192 to 239" is false; NYC is falling.
  - **Robbery and combined totals:** the article's -9.1% robbery and -4.9% combined (284,382 to 270,560; difference 13,822) match the original only. Current robbery is -13.3%. The combined total moves to about -6.3%.
  - **"547 agencies, murders down about 16.6% through August":** the figure traces to the Crime Index (Jeff Asher / AH Datalytics) as quoted in a Crime Forecast Substack post. That post says "16.6% in the Crime Index" but gives no agency count; the 547 count was taken by expert from crimeindex.org and I did not independently confirm it. The Crime Index figure is real, not fabricated. The 16.6% coincidence with MCCA midyear robbery (-16.6%) is only a coincidence.
  - **Severity reasoning:** the original release was what MCCA had published when the outlet wrote, so the numbers are faithful to a real source. But the NYC "increase" contradicts NYPD's public data available at publication, and the piece's headline figure is now superseded. That is more than a minor problem but short of a fabrication: 2, not 3.
  - Other items: 66 of 67 agencies, Houston missing, preliminary data, Memphis 156 to 99, Las Vegas 64 to 79, Canada -21.3%, midyear -17.2%: all true. "Under Trump" framing implies a policy link the MCCA report does not make; the decline began before 2025.
- **expert (1st):** correct verdict and the only complete finding.
  - Found the Updated report, its changed totals, and the swapped NYC columns, and cross-checked NYC against NYPD. Traced the 547/16.6% claim to the Crime Index.
  - Weaknesses: did not check the White House credit claim; the 547 count is only from the index site.
- **everyman (2nd):** rated 1, one off, and wrongly marked NYC "supported".
  - Its strength is that it noticed MCCA lists an "Updated" report dated Nov 12 and flagged that figures might differ, which is the pointer to the real defect. It also said the 547 claim looks mixed up with the 16.6% robbery figure, which is wrong in cause but correctly flagged as unsupported.
  - It confirmed only through secondary coverage and did not open the Updated PDF.
- **askamerica (3rd):** rated 1 and presented a wrong verification as certain.
  - It labelled NYC 192 to 239, robbery and the combined total "verified true against the MCCA PDF" using the superseded Nov 9 file, and never noticed an update exists. Its engine score of 93 "honest" is wrong for this piece.
  - It correctly flagged "nationwide" overreach, the "under Trump" framing, and the unverified 547 claim.
  - It missed the update that everyman at least flagged, and it did not check NYC against NYPD.

## Recipe check
Yes, a recipe would have helped. For any "report X says" claim:
- Check the publisher's landing page for an "Updated", "revised" or "corrected" version before grading figures, and compare both versions.
- Cross-check any outlier row (here the largest city moving opposite to the national trend) against the agency's own published figures.
- For secondary-index claims (Crime Index, agency counts), search the index author's posts before marking unsupported.
- Whether askamerica consulted find_recipe is not visible in its output.

## Severity invariant
**Triggered.** askamerica ranks below everyman (rank 3 vs 2). Both gave 1 Pinocchio against a correct 2, but askamerica asserted a stale-source verification as fact and missed the update, while everyman flagged that a revision exists.

## Defects
- askamerica: graded against the original MCCA PDF without checking the MCCA site for a revised release; reported NYC increase as verified.
- askamerica: no outlier cross-check against NYPD figures for the one large city moving against trend.
