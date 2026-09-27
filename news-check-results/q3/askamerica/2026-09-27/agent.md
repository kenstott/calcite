# Fact-check: "What's Going On With Your Power Bill and What Ohio's Candidates for Governor Will Do About It" (Cleveland Scene / Ohio Capital Journal)

**Overall rating: 1 Pinocchio** — essentially accurate reporting with one minor date error and a real, worth-flagging asymmetry in tone, but no false or fabricated facts.

Full report (HTML with dashboard, saved locally): `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q3/askamerica/2026-09-27/report.html`

## Summary

Every specific, checkable number in this piece that I could verify by directly fetching and reading the actual primary document — not a summary of it — matched exactly:

- **Ohio Auditor Keith Faber's PUCO performance audit**: "more than doubled since 2001" and "34% in the past five years." Verified by fetching the audit PDF directly: it states electricity prices rose 116% from CY2001 to CY2026 (year-to-date), and 34% from CY2022 to CY2026 (YTD). Both figures are exact. The only soft spot: the 34% span is really ~4 elapsed years (2022→2026), which the article rounds up to "five years" — a defensible but generous framing.
- **PJM Independent Market Monitor's $29.4 billion data-center capacity-cost figure**: verified word-for-word by fetching Monitoring Analytics' July 2026 Market Monitor Report PDF directly — page 3 states the exact figure and 46.2% share.
- **Ohio Power Siting Board solar denials (8 projects, 1.29 GW)**: verified by fetching OPSB's own official Solar_Map_and_Stats.pdf — its "Denied Solar Facilities" table lists exactly 8 cases totaling 1,293 MW.
- AskAmerica's own `energy.eia_electricity_prices` table independently corroborates the underlying trend (Ohio residential price rose from 9.34 to 15.99 cents/kWh, 2006–2024), and `energy.pjm_capacity_auction_prices` corroborates the scale of the PJM capacity-price spike (RTO clearing price $28.92/MW-day in 2024/25 to $329.17 in 2026/27).

**One directly-confirmed factual error**: the article says Ohio Senate Bill 52 (which lets local officials block wind/solar projects) "became law in 2022." It actually took effect October 9, 2021. The 8-project/1.29 GW denial count is still exactly right (all denials occurred from October 2022 onward), so the substance survives — only the enactment year is wrong.

**Claims corroborated by web search but not independently fetched from a primary document** (marked "not checkable here" in the formal grading, out of an abundance of provenance caution, though nothing found contradicts them): FirstEnergy's Distribution Modernization Rider collecting ~$457.7M (article: "almost half a billion") and its 2019 Ohio Supreme Court reversal; PIPP's current 175%-of-poverty eligibility threshold that Acton proposes raising to 200%; and Sen. Rob McColley's sponsorship of SB 52.

**Not independently reproduced** (no AskAmerica table covers this, and I did not re-derive it from a raw dataset): the $16 billion Ohio transmission pass-through (2017–2025), the $10+ billion PJM-supplemental-process share of it (from cleveland.com), the $1.4 trillion nationwide transmission buildout figure, and PUCO's interactive city-by-city bill dashboard (28.1% Cleveland/Ashtabula increase, -0.4% Cincinnati). These are reported here as the article's own sourced figures, not verified against a queryable dataset.

## What's worth flagging beyond individual facts

**Asymmetric scrutiny of the two candidates.** Ashley Brown, a single credentialed source (former PUCO commissioner), is quoted extensively and skeptically about Ramaswamy's free-power pledge ("doomed to fail," "not a serious plan," "completely destroys the market") while the same source's assessment of Acton's data-center cost-recovery plan is used almost entirely to validate it ("a commonsense remedy"). Both sets of quotes appear accurately rendered, and the article transparently notes Ramaswamy's campaign didn't respond to requests for detail — a legitimate reported fact, not editorializing. But relying on one expert to adjudicate both plans, quoted very differently in tone toward each, can read to a reader as a broader consensus than a single source represents. This is a shading-of-emphasis issue, not a fabricated fact, and is the main reason for docking a Pinocchio rather than rating this a clean 0.

## Method

Extracted every checkable factual claim and quote from the article. Searched AskAmerica's catalog (`search_catalog`) for matching tables — found `energy.eia_electricity_prices` and `energy.pjm_capacity_auction_prices`, queried both directly. For claims with no matching AskAmerica table (PUCO's dashboard, PJM IMM analysis, OPSB case records, Ohio Supreme Court/FirstEnergy history, SB 52 legislative history), fetched the article's own cited primary-source PDFs/pages directly via `web_fetch`, or — where a fetch was blocked (HTTP 403 on one Ohio Capital Journal URL) or not attempted — used WebSearch and labeled those claims accordingly rather than certifying them at the same confidence as the directly-fetched ones. Each graded claim was independently scored via the `score_claim` tool before publishing.

Report file paths:
- Local HTML report (session-only link, dies with this process; permanent copy on disk): `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q3/askamerica/2026-09-27/report.html`
