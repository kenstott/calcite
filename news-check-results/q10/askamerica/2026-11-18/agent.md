# The Ag Optimus article's claims about COT fund positioning, lean hogs and grain and cattle prices are balanced and mostly accurate; 1 claim could not be confirmed.

Article: https://agoptimus.com/grain-livestock-cot-report-november-17-2026/ (fetched successfully)

**Rating: 1 Pinocchio.** No figure I could test was contradicted. The point comes from basis differences in some "lowest since" dates and a large share of the piece that I could not verify.

## Central claim: true
Lean hog managed money net short of 51,084 contracts is the largest in the Futures-and-Options Combined series.
- CFTC Socrata combined data, fetched directly: Nov. 10 is 79,486 long less 130,570 short = -51,084. Nov. 3 is 72,010 less 122,502 = -50,492 (the "prior record"). Open interest is 481,857. All match the article.
- The warehouse table cftc.cot_disaggregated_futures holds the futures-only report. Hog net short there is -62,236 on Nov. 10, the largest in 2006-2026 (next is -59,072 on Nov. 3).
- Not confirmed: the pre-2022 history of the combined series and the 20,278 July 2007 low.

## Other claims
- Corn managed money flipped from net short 22,900 to 62,311. Warehouse (futures-only): +8,795 to -22,628, last at or below that level on June 30. Mostly true; the article says June 23, a basis difference.
- Soybean net long lowest since Aug. 11: true on the warehouse (last as low Aug. 11).
- Live cattle "lowest since Apr. 30, 2024", feeders "since Sept. 29", wheat commercials "since Jan. 27": direction matches. Futures-only dates are Jan. 30, 2024, Oct. 13 and Feb. 10. I did not resolve this basis difference. Mostly true.
- Wheat funds the only buyers: true (-87,126 to -81,050 futures-only).
- Corn harvest 81% (Nov. 8) and 88% (Nov. 15), Iowa 72% to 84%: true (ag.nass_crop_progress).
- The publisher's own COT portal (reports.agoptimus.com/cot-report) shows the same six net positions. That is not independent evidence. The other three linked pages (ethanol, livestock slaughter, cattle on feed) were fetched and hold no figures the article depends on.

## Not verified (graded unsupported, not false)
CME settlements and spreads, export sales, cash bids, storage-cost schedules, K-State and Iowa State benchmarks, screwworm counts, NOPA crush, and summit timing. No warehouse table carries them and I did not retrieve the primary sources (CME, USDA AMS and FAS, K-State, Iowa State) in this run.

## Misleading framing
Nothing materially misleading. One wording issue: the corn subhead says funds "doubled their short", but 22,900 to 62,311 is about 2.7 times; the body states the figures correctly. The piece separates Tuesday positioning from Friday prices, labels opinion, and shows both CME and USDA/Pro Farmer sources for Monday.

Report scores for the author's claims: honesty 85 (mostly honest), bias -17 (no consistent lean). These describe this article only.

Report: http://127.0.0.1:61444/a/64743fecb1c18aef6b0d00907dbb2e06.html
