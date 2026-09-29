# Fact-Check: NYT "Dark Money" 2026 Midterms Review

**Overall verdict: Pinocchio rating 0 (fidelity) / 0 (claims accuracy).** Every specifically checkable figure in the NYT's Aug 31, 2026 dark-money review (as relayed by Political Wire and the Brennan Center) holds up, several confirmed to the exact dollar against primary sources.

## Claim-by-claim

**1. MAGA Inc. cash-on-hand trajectory ($294M → $400M+) — TRUE, verified to the dollar against FEC.gov primary data.**
AskAmerica's own `fec.committee_summaries` table had null `cash_end` for this committee (a real warehouse gap for C00892471), so verification moved to FEC.gov's OpenFEC API directly. Confirmed:
- $294,416,079.21 cash on hand as of the Jan 2026 "Post-Special 2025" filing (coverage ending 2025-12-22) — exact match to the article's "$294M."
- $403,450,026.85 as of July 31, 2026 — exact match to the widely-cited "$403.5M" figure (CNBC, Election Law Blog).
- $415,776,043.10 as of the latest filing (through Aug 31, 2026) — confirms "more than $400 million."
- The trajectory is monotonically increasing month-over-month across 2026 ($294.4M → $356.5M → $382.4M → $403.45M → $415.8M) — real fundraising growth, not two conflicting snapshots, exactly as the article frames it.

**2. First Amendment LLC, Wisconsin (~$13M total, ~$9.3M vs. Crowley) — TRUE, but outside FEC scope.**
No such entity is FEC-registered — correctly so, since Wisconsin's governor's race is a state contest, not federal. Independently corroborated via four Wisconsin outlets (WisPolitics, Urban Milwaukee, Hoodline, Wisconsin Public Radio), all reporting the identical breakdown: ~$13M total, ~$9.3M against Crowley, ~$3.7M on legislative races, funded by an undisclosed donor base under Wisconsin's express-advocacy LLC loophole.

**3. Working Families Party National PAC, Delaware ($455K, six candidates, no donor disclosure) — TRUE, but outside FEC scope.**
WFP National PAC is FEC-registered federally (C00606962), but this specific $455K spend was a Delaware state filing, not a federal one. Confirmed via Spotlight Delaware's direct reporting on the filing, including the regulatory nuance that WFP said Delaware regulators told them state-level disclosure wasn't required since they file separately with the FEC.

**4. The headline "$1 billion in dark money" figure — graded "not checkable here."**
This is the NYT's own proprietary tally (ad-tracking data + campaign-finance filings + tax records), not reproducible from any single public dataset — dark money is by definition undisclosed, so no FEC or Form 990 table can sum it into a real-time total. `search_catalog` confirmed no table aggregates this measure. The quote itself ("about $1 billion... almost certainly a low-end estimate") was verified verbatim via Political Wire and the Brennan Center's newsletter (both directly fetched), so the *attribution* is accurate even though the underlying number can't be independently recomputed. The order of magnitude is plausible against the Brennan Center's own separately published finding of $1.9 billion in dark money for the full 2024 presidential cycle (historically larger than a midterm).

## What's misleading even though the facts check out
Nothing found is materially misleading. The "real growth, not a contradiction" framing for the MAGA Inc. cash-on-hand figures is, if anything, understated — the primary FEC data shows a smooth, near-linear month-over-month build across 2026, which strengthens rather than complicates the article's account.

## Scope note
Wisconsin's gubernatorial race and Delaware's legislative races are state contests governed by state disclosure law, so First Amendment LLC's and the WFP National PAC's state-level spending had to be verified against state-level reporting rather than FEC tables — a genuine scope boundary of the `fec` schema, not a data gap.

## Sources
- [Political Wire: How Dark Money Is Washing Over the 2026 Election](https://politicalwire.com/2026/08/31/how-dark-money-is-washing-over-the-2026-election/) (relaying NYT, Aug 31, 2026)
- [Brennan Center: $1 Billion in Secret Cash Is Reshaping the Midterms](https://www.brennancenter.org/our-work/analysis-opinion/1-billion-secret-cash-reshaping-midterms)
- [Brennan Center: Dark money hit record high of $1.9 billion in 2024 federal races](https://www.brennancenter.org/our-work/research-reports/dark-money-hit-record-high-19-billion-2024-federal-races)
- [OpenFEC committee/C00892471/reports API](https://api.open.fec.gov/v1/committee/C00892471/reports/) (FEC.gov primary data, live-fetched 2026-09-29)
- [WisPolitics: New group pumps $13 million into Wisconsin races](https://www.wispolitics.com/2026/new-group-pumps-13-million-into-wisconsin-races-to-knock-dems-including-9-3-million-opposing-crowley/)
- [Urban Milwaukee: New GOP-Aligned Group Drops Millions Against Crowley](https://urbanmilwaukee.com/2026/09/24/new-gop-aligned-group-drops-millions-against-crowley-democrats/)
- [Wisconsin Public Radio: New dark money GOP group dominates Wisconsin political spending](https://www.wpr.org/news/new-dark-money-gop-group-dominates-wisconsin-political-spending)
- [Hoodline: Virginia Group Spends $13M Against Wisconsin Democrats](https://hoodline.com/2026/09/virginia-group-pours-13m-into-wisconsin-races-to-sink-crowley-democrats/)
- [Spotlight Delaware: National progressive group reveals $455K Delaware campaign, but donors unclear](https://spotlightdelaware.org/2026/08/31/national-progressive-group-reveals-455k-delaware-campaign-but-donors-unclear/)
- [CNBC: Trump MAGA Inc. $403 million war chest spending in 2026 midterms](https://www.cnbc.com/2026/09/18/trump-election-ad-spend-maga-inc.html)
- [Election Law Blog: MAGA Inc reaches $400M with no spending to boost candidates](https://electionlawblog.org/2026/maga-inc-reaches-400m-with-no-spending-to-boost-candidates/)

Full report with dashboard published via `publish_report`; artifacts saved to `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q2/askamerica/2026-09-29/`.
