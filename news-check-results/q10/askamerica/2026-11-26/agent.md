The White House release "Gas Prices Plunge Ahead of Thanksgiving" (Nov. 24, 2026): claims about the national average, the three-week drop and Trump's prediction are very biased and mostly accurate.

**Rating: 1 Pinocchio** (the engine's rating from the claim scores).

**Grouping.** Relayed figures and headlines are now in `fidelity`: Bloomberg's "lowest since mid-March", CNN's "biggest three-week drop since 2008", and the local headlines (Birmingham, Missouri, Arkansas, Connecticut, Georgia, Maine, Minnesota/Iowa; DFW, Oklahoma City, Philadelphia, New Mexico, Utah, Lynchburg; Cincinnati). The White House's own voice stays in `author_claims`. "Just as Trump predicted" is the `central` claim, and "delivering just what he promised" is the causal claim. The server also required each relayed figure to stand as an author claim, so those appear in both groups.

**Warehouse check** (energy.eia_retail_gasoline_prices, weekly US regular, through 2026-11-23):
- The latest price is $3.642, the lowest since the week of Mar 9 ($3.502). The eight-month claim is true.
- The three-week change is -$0.511. Only Oct-Nov 2008 windows are larger since 2000. The claim is true.
- Gas is about 19% above last year ($3.642 vs $3.061).

**Graded against their sources**
- "Just as Trump predicted": mostly true on direction only. The Sept. 9 remark was about oil prices after the election, not pump prices.
- "Delivering just what he promised: real relief": mostly false. CNN and Bloomberg credit seasonal demand, Hormuz supply recovery and no war re-escalation, and the release omits the year-ago comparison.
- Local headlines: matched their stories, with two caveats. Lynchburg "$3.21" is the lowest station price (AAA metro $3.47). Cincinnati "plummet overnight" is a 6-cent one-day drop.

**Limits**
- Bloomberg returned a 403, so I read its copy at ttnews.com.
- Fox 8 (403) and Argus Leader (402) were blocked.
- The Fox 32 Chicago and CBS Boston pages were video-only.
- Ten states' headlines were not verified beyond the headline.
- The server restarted mid-session. I reran the queries and refetched the release and its links, so they are on record.

Report: /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q10/askamerica/2026-11-26/report.html