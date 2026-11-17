The Register article's claims about 2026 CVE volume, exploitation speed and AI are biased and accurate; 1 claim could not be confirmed.

Rating: 0 Pinocchios (engine-computed from the claim grades).

Article: https://www.theregister.com/security/2026/11/13/this-years-bug-count-just-passed-100k/5309882

Verified true against primary sources
- 100,222 CVEs counted by VulnCheck on Nov 13, more than double 2025 (baselines 48,150 to 49,972). Warehouse NVD table: 2024 40,704; 2025 49,972; 2026 75,986 loaded (NVD lags, so VulnCheck's broader count is what gets past 100K).
- 1,363 KEVs, about 1.4% (1,363/100,222 = 1.36%).
- Median CVE-to-KEV time 120 days (2025) to 80 days (1H 2026), per VulnCheck's State of Exploitation 1H-2026.
- CrowdStrike: 88% of PoC exploitation within 48 hours (via the Register's Aug 3 report; CrowdStrike pages were navigation shells only).
- Microsoft 1,094 and 1,474 CVEs; 14 Copeland XWEB EVO CVEs from VulnCheck with two seen exploited; CISA BOD 26-04 issued June 10, 2026 (revokes BOD 19-02 and 22-01); curl's Oct 26 policy and its January bounty shutdown.

Problems found
- Bressers' "literally minutes later": VulnCheck's own post says the NVD record went live 20:18 UTC Nov 12 and the first honeypot hit was 22:00 UTC, 1h40m later. Partly false.
- "Percentage exploited remains relatively consistent": VulnCheck says the KEV-to-CVE ratio fell from 2.7% (2H 2023) to 1.4% (1H 2026). Mostly true, stable only recently.
- "Window has closed even further in the second half": no 2H measurement found; unsupported.
- "AI is driving a majority" (Bressers): unsupported. VulnCheck's own report tracks only 1,061 AI-attributed CVEs, calls the impact real but modest, and ties growth partly to CNA scale-up.
- Open VSX customers "AWS, IBM, Google, Cursor, Windsurf": Eclipse's release names AWS, Google and Cursor; IBM Bob and Windsurf are ecosystem platforms. Mostly true.
- "Iranian hackers ... across Europe": France attributed the October French campaign to Iran on Nov 9; later Belgian/Irish/Swedish incidents lack attribution found here.
- "We have a data problem now": opinion, not checkable.

Misleading framing
The piece leans on one vendor (also a CNA and KEV seller) for both the count and the AI-cause story, and omits the main conclusion of the VulnCheck report it cites: AI-found bugs are not more exploited, and hype exceeds evidence. A stricter grader could reasonably dock the piece for that and for the "minutes" quote.

Not independently verified: VulnCheck's live 100,222 and 1,363 (live vendor database; consistent with its 1H report and press copies).

Report saved: /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q9/askamerica/2026-11-17/report.html