# Fact-check: The Register, "This year's bug count just passed 100k" (2026-11-13)

Method: WebFetch of the article (returned a summary, not verbatim text, so the quote wording is not checked), then web searches. I did not open primary documents (VulnCheck PDFs, cisa.gov, curl.se). Verification below rests on search-result summaries and secondary coverage.

## Claims
| # | Claim | Verdict |
|---|---|---|
| 1 | 2026 CVE count passed 100,000 (VulnCheck: 100,222) | Supported by secondary coverage (WebProNews and others citing VulnCheck). Primary page not opened. |
| 2 | More than double the 2025 total | Roughly supported. 2025 baselines vary: about 48,150 (CVE.icu) versus "over 40,000" (VulnCheck). Either way about 2x or more. |
| 3 | AI-driven bug hunting is the main cause | The article's and VulnCheck's framing. Causal attribution is plausible but not proven. VulnCheck's H1 report attributes only 1,061 vulnerabilities to AI-assisted discovery, so "mainly AI" is not demonstrated by that figure. |
| 4 | About 1.4% (1,363) of 2026 CVEs are in VulnCheck KEV | Supported (matches search summary). |
| 5 | Median time from CVE publication to exploitation fell from 120 days (2025) to 80 days (H1 2026) | Supported by VulnCheck H1 2026 report (per search summary). The report says "to KEV", not "to exploitation", a minor wording difference. |
| 6 | CrowdStrike: 88% of public-PoC exploitation Jan-Jun occurred within 48 hours | Not checked here (CrowdStrike source not found in my searches). |
| 7 | Microsoft: 1,094 flaws in November Patch Tuesday, 1,474 in October | Not checked here. |
| 8 | VulnCheck disclosed 14 root-level flaws in Copeland refrigeration controllers, 2 under active exploitation, with honeypot probing within minutes | Not checked here. |
| 9 | CISA BOD 26-04 issued June 2026, ranks patching by exposure, exploitation, and automatability, not CVSS alone | Supported. BOD 26-04 was issued June 10, 2026 and moves from CVSS-based to risk-based (SSVC) deadlines. |
| 10 | Bressers (the article's source) says the industry has a "data problem" and that organizations are reluctant to move first | Quote not verified (article text not retrieved verbatim; no independent source found). |
| 11 | Eclipse Foundation launched a paid managed tier for Open VSX in April | Not checked here. |
| 12 | Stenberg ended the curl bug bounty earlier this year | Supported. It ended at the end of January 2026 (The Register, 2026-01-21). |
| 13 | From Oct 26 curl publishes low and medium severity reports in its public GitHub repo | Supported in substance (Stenberg: such reports go to the public GitHub repo from Oct 26). Only one search; details not verified. |

## Rating: 1 Pinocchio (mostly accurate)
Every claim I could check holds up. Several are unchecked, not wrong.

## Misleading or missing context
- The headline count is a VulnCheck tally. NVD, MITRE, and CVE.org counts differ, and so does the 2025 baseline. "More than double" depends on the source.
- A higher CVE count reflects more reporting, not necessarily more danger. The article's own 1.4% exploitation figure supports this. The headline "bug count" can read as an alarm.
- The "AI-driven" causation is stated more firmly than the evidence (about 1,061 AI-attributed vulnerabilities in H1, about 14 exploited) supports.
- The 120 to 80 day figure is time to KEV listing. It is partly affected by VulnCheck's own detection and catalog growth.
- BOD 26-04 binds only federal civilian agencies.
