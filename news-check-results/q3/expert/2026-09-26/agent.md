# Q3 (expert) fact-check record — 2026-09-26

## Claim under review
(1) Total IE spending, 2026 cycle: $1,452,997,175 across 881 candidates.
(2) Four-week window (Aug 24–Sep 14, 2026): $328,333,233.
(3) Talarico (TX Senate) largest single target in that window: $59,564,521 / 17 committees / $2,086,549 support / $57,477,973 oppose.

## Step 1 — Locate the source of the three headline figures
**Answer:** All three figures trace to exactly one place found in research: thepoliticalgroup.com/research/pac-spend-2026 ("PAC Spend Tracker 2026"). Repeated WebSearch queries phrased differently all returned the identical sentences, confirming a single underlying page rather than independent corroboration.
**Key values:** $1,452,997,175 / 881 candidates; $328,333,233 (Aug 24–Sep 14); Talarico $59,564,521, 17 committees, $2,086,549 support, $57,477,973 oppose.
**Sources:** https://www.thepoliticalgroup.com/research/pac-spend-2026
**Confidence:** High that this is the origin of the numbers; the page itself states its methodology plainly (see Step 2).

## Step 2 — Determine whether the source is primary (FEC.gov) or secondary
**Answer:** It is a secondary, third-party aggregator, not an FEC.gov aggregation/report page. The page's own text states the figures come from "the Federal Election Commission's public API: Schedule E independent expenditure lines (processed reports plus 24 and 48 hour notices, deduplicated)," and adds the caveats that "the newest week is partial until filings catch up" and "the FEC's processed totals can trail the newest filings by days to weeks." This is exactly the unvetted-itemized-rows scenario the task brief warns about — it is a raw sum of Schedule E line items, not a number FEC.gov itself has published as an official aggregate.
**Key values:** Source = raw Schedule E API sum (deduplicated), not FEC.gov's own statistical summary.
**Sources:** https://www.thepoliticalgroup.com/research/pac-spend-2026
**Confidence:** High (self-disclosed methodology).

## Step 3 — Attempt direct verification against FEC.gov / FEC API
**Answer:** Could not independently verify the exact figures. FEC.gov's browse-data pages (fec.gov/data/independent-expenditures/, fec.gov/data/elections/senate/TX/2026/, fec.gov/data/candidate/S6TX00479/) are JavaScript single-page apps; static fetch returned only the empty page shell, no populated totals. The FEC's public API (api.open.fec.gov) was rate-limited on the shared DEMO_KEY for the entire session (repeated attempts over ~10 minutes all returned OVER_RATE_LIMIT); no personal API key is available in this environment, so I could not query Schedule E directly to reproduce or refute the tracker's sums.
**Key values:** n/a — verification attempt was blocked, not a data point.
**Sources:** api.open.fec.gov (blocked); fec.gov/data/independent-expenditures (JS shell only)
**Confidence:** N/A (inconclusive by tool limitation, not by finding contrary data).

## Step 4 — Sanity-check the cycle total's magnitude against FEC's own published trend
**Answer:** FEC.gov's own Statistical Summary series (an official FEC.gov page, not itemized data) gives cumulative IE totals for congressional races in the 2025–2026 cycle at three earlier checkpoints: $16M through Jun 30 2025 (6-month), $56.4M through Dec 31 2025 (12-month), and $252.1M through Mar 31 2026 (15-month). Growth from $252M in March to a claimed $1.45B by September is a ~5.8x increase over ~6 months. That is directionally consistent with the well-known pattern of IE spending concentrating heavily in the final months before a federal election, so the order of magnitude is plausible, but FEC has not (as of this research) published an official statistical summary covering the September 2026 period, so the $1.45B figure itself remains unconfirmed against any FEC.gov aggregate — only against a secondary tracker.
**Key values:** $16M (Jun 2025) → $56.4M (Dec 2025) → $252.1M (Mar 2026) → $1.45B claimed (Sep 2026, secondary source only).
**Sources:** https://www.fec.gov/updates/statistical-summary-of-12-month-campaign-activity-of-the-2025-2026-election-cycle/ ; https://www.fec.gov/updates/statistical-summary-of-15-month-campaign-activity-of-the-2025-2026-election-cycle/
**Confidence:** Medium (trend is plausible; absolute figure not confirmed).

## Step 5 — Cross-check the Talarico/TX Senate support-vs-oppose direction against independent news reporting
**Answer:** Multiple independent, credible news outlets (Texas Tribune, Axios, CNBC, The Hill, TPR, WBAP) confirm the *direction* claimed — that outside spending opposing Talarico (i.e., supporting Paxton) sharply escalated in September 2026 and dwarfs support for Talarico in that period:
- MAGA Inc.: $10M ad buy announced ~Sept 5–7, split evenly support-Paxton/oppose-Talarico (≈$5M opposing Talarico) (Axios, The Hill, TPR).
- America PAC (Musk-backed): ~$2.4M, mostly opposing Talarico ($1.36M digital opposing Talarico vs. $775K supporting Paxton) (Yahoo/AP wire).
- Lone Star Liberty PAC (pro-Paxton): "more than $15 million" on ads beginning Sept 1; cumulative to date "over $19 million" by Sept 22 (Texas Tribune).
- Senate Leadership Fund: committed $100M, announced ~Sept 22 — after the window's Sept 14 end date, so largely not yet reflected in the Aug 24–Sep 14 window.
- Talarico's own outside support (Lone Star Rising PAC): ~$3M in September, well below the opposition totals.
Summing the *identifiable, named* opposition PAC spend that clearly falls inside Aug 24–Sep 14 (Lone Star Liberty's early-September buy, MAGA Inc.'s Sept 5–7 buy, America PAC) lands in the roughly $20–30M range from named PACs alone — below the claimed $57.5M "oppose" figure, but the claimed figure also includes an unspecified number of the other 17 committees not named in these articles, so the totals are not strictly contradictory, just not reconcilable to the dollar with what mainstream reporting names explicitly.
**Key values:** MAGA Inc. $10M (Sept 5–7, half opposing); America PAC ~$2.4M; Lone Star Liberty "$15M+" ads from Sept 1, "$19M+" cumulative by Sept 22; Senate Leadership Fund $100M (announced Sept 22, mostly outside window); Lone Star Rising (pro-Talarico) ~$3M in September.
**Sources:** https://www.texastribune.org/2026/09/22/texas-paxton-ads-100-million-senate-race-talarico/ ; https://www.axios.com/2026/09/05/trump-magainc-paxton-talarico-texas-senate ; https://www.tpr.org/government-politics/2026-09-07/maga-inc-cavalry-arrives-for-paxton-in-texas-senate-as-trumps-pac-places-10-million-ad-buy ; https://www.yahoo.com/news/politics/articles/musk-backed-pac-adds-2-210023016.html ; https://www.texastribune.org/2026/07/20/texas-senate-talarico-paxton-fundraising-spending-money-donors/
**Confidence:** Medium-High on direction (opposition >> support, and Talarico's race is a major national IE target); Low on the exact dollar figures and the "17 committees" count, which appear nowhere outside the single tracker.

## Overall assessment
- Claim (1) — total $1,452,997,175 / 881 candidates: **Unverified.** Traces to one secondary aggregator's raw, deduplicated Schedule E API sum, not an FEC.gov published aggregate. Order of magnitude is plausible against FEC's own (older) trend data, but the specific figure could not be confirmed against FEC.gov or a second independent source.
- Claim (2) — $328,333,233 in the four-week window: **Unverified**, same single-source issue as (1).
- Claim (3) — Talarico/TX Senate largest target at $59,564,521 / 17 committees / $2,086,549 support / $57,477,973 oppose: **Direction confirmed independently** (opposition spending against Talarico clearly surged and dominates in Sept 2026, per multiple named-PAC news reports), but the **exact dollar amounts and committee count are unverified** — they appear only on the one secondary tracker page, and named-PAC reporting from mainstream outlets does not, by itself, sum to the claimed opposition total for the stated window.

**n (candidates referenced in cycle total):** 881, per the single tracker source — not independently confirmed.
**Time period:** 2025–2026 election cycle; four-week window Aug 24–Sep 14, 2026; as-of date for cycle total Sep 21, 2026 (per tracker).
