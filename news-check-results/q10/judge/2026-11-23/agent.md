# Judge - Q10 (Al Jazeera, "Anthropic raises $81.3bn in biggest IPO of all time", 2026-11-19)

**Task:** fact-check every claim and grade on the 0-4 Pinocchio scale.

Rank: [askamerica, expert, everyman]

Fingerprints pass for all three. Ground truth for the central claim was checked at judge time (see "Ground truth the judge found"). `sec.stock_prices` ends 2026-05-29 (govdata-ops #1457), so no persona could test the Nvidia or Nasdaq moves in the warehouse.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Grade | 1 Pinocchio | 0 Pinocchios (engine-computed; agent's own pre-engine call was 1) | 2 Pinocchios |
| Claims graded | 12 rows, plus "not checked" for Asian markets; S&P "half", Gartner, "12th biggest" absent | 13 claims in claims.json; 8 true, 1 mostly true, 3 unsupported, 2 not checkable (opinion) | about 6 items; no S&P, Gartner, Asian markets, 12th rank |
| SpaceX | "final total rose", no figure | $75bn initial, $85.7bn final, CNBC and NPR | $85.7bn final, SpaceX IR primary |
| Record framing | "holds for the initial offering" | flag 1: holds only against $75bn; notes Anthropic's own option worth about $12.2bn (CNBC live) | "misleading", base-vs-base only; says the record holds "if Anthropic's own greenshoe ... exceeds it" but does not check |
| Primary vs secondary | "about $79.4bn to Anthropic" | 956m company plus 23m stockholder shares (NPR) | about $79.4bn to Anthropic, rest secondary |
| Co-founders sold | "nothing confirming" | true, CNBC (7 co-founders at 3.38m each) | not addressed |
| Nvidia >30%, Nasdaq -7.6% | not confirmed; Nvidia 12-19%, no Nasdaq source | unsupported; Nvidia 11.0% below 6 Nov (StockTitan), no Nasdaq source; warehouse queried, 2026 rows absent | not exact; Nvidia 23-35% below 6 Oct peak, Nasdaq over 10% below peak; "window-dependent" |
| Valuation | $1.5tn, fully diluted caveat | $1.5tn at IPO price; above $2tn after debut | $1.5tn confirmed |
| Warehouse use | none | find_recipe, search_catalog x4, 3 sec.stock_prices queries, all documented | none |

## Ground truth the judge found

- WebSearch on the $79.4bn figure: the arithmetic holds. 956m company shares at $83 is $79.35bn. 23m selling-stockholder shares at $83 is $1.9bn. The total is $81.3bn. The expert's and everyman's "about $79.4bn to Anthropic" is correct, and the selling stockholders' 23.6m shares match askamerica's "seven co-founders at 3.38m each" (CNBC), so the co-founder claim is true and everyman's "partly unverified" is the weaker read.
- Anthropic's closing release (https://www.anthropic.com/news/ipo-close, published 2026-11-20, fetched at judge time): underwriters fully exercised the 147m-share option; gross proceeds are about $93.5bn (1,127,000,000 shares at $83), of which about $91.6bn went to Anthropic; the private placement of about $26.0bn brings the total to about $119.5bn. $93.5bn is above SpaceX's $85.7bn final. The article's headline claim ("largest IPO in history") is therefore true on a final-vs-final basis. All three personas ran on 2026-11-23, after this release, and none fetched it.

## Ranking rationale

**1. askamerica.** Best grade calibration and best coverage. It graded all 13 claims, including the S&P "half", Gartner, Asian-market closes and the "12th biggest" rank, which the other two skipped. Its 0 Pinocchios fits the ground truth: the core claims are true and the record claim holds on final figures. It flagged the exact issue that decides the record claim (Anthropic's own option, about $12.2bn, which would restore the lead) even though it did not fetch the closing release. The central claim is graded "mostly true", not false. Nasdaq/Nvidia: it queried `sec.stock_prices` (seq 3, 6, 7, 10, 11), found no 2026 rows, said so, and fell back to StockTitan (11.0% on 12 Nov). That is an honest "unsupported" with the window issue named in the claim reason. Weak points: the framing flags and the three unsupported claims sit beside a 0, and the agent's own pre-engine grade was 1. It never ran the one search that settles the record (the closing release).

**2. expert.** Correct primary sourcing (SpaceX IR for $85.7bn), correct $79.4bn primary/secondary split, and the best handling of the Nvidia/Nasdaq window issue: it saw that the article's 30% and 7.6% depend on the baseline date, giving 23-35% from the 6 Oct peak and Nasdaq over 10% from peak, so the article's numbers are neither confirmed nor contradicted. That is more useful than askamerica's single 11% datapoint. It is third on calibration: 2 Pinocchios rests on "biggest ever holds only base-vs-base", yet its own sentence says the claim stands if Anthropic's greenshoe exceeds SpaceX's, and it never checked. It was fully exercised and does exceed. Coverage is thinner (about 6 items, no Gartner, S&P, Asian markets, rank).

**3. everyman.** 1 Pinocchio is the closest grade to the ground truth, but the work behind it is the thinnest. It gives no SpaceX final figure, treats the co-founder claim as unverified when CNBC states it, found no Nasdaq source, and marks Asian markets "not checked". The Nvidia 12-19% range is a real finding but covers only the days after the resignation. Calibration beats expert's; coverage and evidence do not.

**What would change the ranking:** expert versus everyman is a close call. If the judge weighted grade closeness to the true outcome over evidence quality, everyman (1) would pass expert (2). askamerica would drop to second only if grading were scored on agreement with the agent's own flags, because its 0 sits beside three framing flags and three unsupported claims.

## Severity invariant

askamerica ranks above everyman. No defect flag. The connector contributed: find_recipe served `fact-check-a-news-article`, the warehouse queries showed the Nasdaq/Nvidia data is missing rather than guessed, and the claim table is more complete. Honest note: the leading edge over everyman comes from claim coverage and the recipe's claim-extraction rule, not from warehouse data (the warehouse could not test the market-move claims).

## Recipe check

Existing recipe `fact-check-a-news-article` (recipes.json) was served at seq 1 and already covers claim extraction, "record" phrases as separate claims, primary-source grading, and projections. It has no rule for a record or size claim whose deal had a pending option or later close. All three personas left the record claim at "holds only base-vs-base" while the closing release, available three days before the run, settled it in the article's favour. askamerica came closest (it named the $12.2bn option) but graded without fetching the release.

Evidence type: the gap is shown by askamerica's own run plus the judge's search; the two unaided personas share it. The pattern (find the later closing release) is reachable by web_fetch but was not reached, so this is a real gap, not an already-reached behaviour.

**Resolution (one):** recipe update, not a new recipe. Add one rule to `fact-check-a-news-article`: when a record or "largest" claim compares a figure that has a pending option (greenshoe, overallotment, upsizing) or a later closing, MUST web_fetch the issuer's closing release dated after pricing and grade the claim on final figures, naming both bases. Triggers to add: "greenshoe", "overallotment", "biggest IPO", "largest IPO", "record IPO". It is warehouse-independent because no table carries IPO proceeds (search_catalog "Anthropic IPO proceeds" returned nothing relevant at seq 12), and the body should say so.
