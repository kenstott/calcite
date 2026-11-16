The CNN State of the Union (Nov 15, 2026) transcript's claims about the FAA ground stop, air traffic control funding and the midterms are balanced and mostly accurate.

**Rating: 1 Pinocchio.**

The piece is an interview transcript. I fetched it directly and extracted the claims from it. I ran search_catalog for each measure first. The warehouse holds Senate returns only through 2024 and has no 2026 results, no DOT/FAA obligation data and no Arkansas program data. Most grades therefore rest on news and primary pages I fetched.

**Verified as true or mostly true**
- Colom beat Hyde-Smith by 4,669 votes and is the first Black senator from Mississippi since Reconstruction (Fox 5 Atlanta). Warehouse baseline: in 2024 Mississippi's Senate vote was R 763,420 vs D 451,981, so a 2026 Democratic win is a large swing (officials.senate_election_results).
- Congress appropriated $12.5B for air traffic control in the July 2025 reconciliation law (Roll Call, AOPA).
- Duffy asked for $50B (Wired). Wired also confirms the five-hour ground stop was the worst nationwide ground stop since 9/11, and the September GAO warning on speed over cost. Bash's "only the second since 9/11" count is not stated in any source I read, and the GAO wording differs slightly from Wired's quote. Both are graded mostly true.
- Sanders's "fourth tax cut since 2023" is confirmed (top rate 3.9% to 3.7%, Arkansas Advocate). I did not compute her "25 percent" figure.
- Van Hollen's challenge to Schumer is described by AP as "considering" a challenge as of the week after the election, so mostly true.

**Problems**
- **Duffy, "no money in the $12.5B for new telecom architecture":** partially false. $4.75B of the $12.5B is designated for telecom modernization (AOPA), and the copper-to-fiber work is already funded from it. Only a wholly new architecture or software may be unfunded.
- **Van Hollen, "only spent one-third":** mostly false. Using the figures read on air, one-third matches obligations ($4.3B of $12.5B), not spending ($1B, about 8%).
- **Bash, a Democratic-era shutdown "in 2007, 2008":** mostly false. There was no federal shutdown in those years. The federal shutdowns were 1995-96, 2013, Jan 2018, 2018-19 and 2025. Only state shutdowns (Michigan, Pennsylvania 2007) appear.

**Unsupported (not shown false)**
- The $4.3B obligated / $1B spent figures. No DOT, FAA or congressional release carrying them turned up, and Duffy disputed the framing, not the numbers.
- Sanders's "one of two states" paid maternity leave and $50M in teacher bonuses. The Democrat-Gazette budget story had neither, and the Axios page returned 403.

**Materially misleading**
- Both sides spin the same funding fight. Duffy overstates what the $12.5B excludes, and Van Hollen conflates obligated and spent money. Neither correction changes the underlying fact that the $12.5B is far from fully spent and that more funding is being sought.

**Limitations**
- The Dec 11 funding deadline and the Dec 1 Senate Democratic leadership vote date could not be confirmed.
- My first warehouse query excluded null-candidate (write-in) rows and returned nothing. The reported totals come from a rerun without that exclusion, so nothing was dropped from them.

Report page (saved copy): /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q2/askamerica/2026-11-16/report.html

Sources:
- [Fox 5 Atlanta](https://www.fox5atlanta.com/news/scott-colom-wins-senate-mississippi)
- [Roll Call](https://rollcall.com/2026/04/21/more-air-traffic-control-modernization-funding-needed-duffy-says/)
- [AOPA](https://aopa.org/news-and-media/all-news/2025/july/10/where-is-the-12-5-billion-for-atc-modernization-going)
- [Wired](https://www.wired.com/story/the-faa-nationwide-ground-stop-is-over/)
- [Philadelphia Inquirer](https://www.inquirer.com/business/faa-smart-ai-system-ground-stop-20261116.html)
- [Arkansas Advocate](https://arkadelphian.com/2026/05/06/arkansas-advocate-state-lawmakers-approve-192m-tax-cut-legislation-end-session/)
- [Wikipedia: Government shutdowns in the United States](https://en.wikipedia.org/wiki/Government_shutdowns_in_the_United_States)
- [AP via Scripps](https://www.scrippsnews.com/politics/congress/van-hollen-considers-challenge-to-schumer-for-senate-democratic-leader)
- [Arkansas Democrat-Gazette](https://www.arkansasonline.com/news/2026/nov/06/sanders-proposed-state-budget-includes-increased/)
