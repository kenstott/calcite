# Fact-check: The Register, 'This year's bug count just passed 100K'

**Grade: 1 Pinocchio (some shading), borderline 2.** Core facts hold; peripheral errors and causal overreach.

## Summary
- 100K: NVD 100,416 pub 1 Jan-13 Nov 2026; 2.01x 2025 (49,972). Verified.
- Verified: CrowdStrike 88%/48h; Panda 24h; VulnCheck 120->80d; BOD 26-04 (10 Jun); Eclipse launch 21 Apr, 99.95% SLA; curl 26 Oct policy; bounty ended 31 Jan.
- Wrong: Microsoft Oct 1,474 (sources 1,407-1,416); IBM/Windsurf not Open VSX managed customers (AWS, Google, Cursor); 'all 14 make-me-root'; 'minutes' (actual 1h40m, 7h23m); BOD replaced KEV-based, not CVSS-only, deadlines.
- Misleading: AI stated as cause (VulnCheck counts 1,061 AI-attributed vulns in 1H; CrowdStrike says pattern predates frontier AI); median-time-to-exploit is censored; 1.4% is VulnCheck-catalog specific (CISA KEV 2026: 297, 0.30%).
- Not checkable: live 100,222 count; exact 1,363 KEV; 2H 'closed further'; Bressers quotes.

## Claims
- 2026 CVEs passed 100,000 on Fri 13 Nov: **Verified**. NVD: 100,416 published 1 Jan-13 Nov 2026 (n=100,416). 8,819 by 17 Nov.
- VulnCheck counted 100,222 at ~4pm UTC: **Not checkable**. Live VulnCheck count not archived; NVD figure within 0.2%.
- More than double 2025's total: **Verified**. NVD 2025: 49,972 (2.01x); VulnCheck 2025 48,150 per secondary press (2.08x).
- Microsoft added 1,094 flaws Tuesday (10 Nov): **Verified w/ caveat**. Tenable/Redmondmag/CybersecurityNews 1,093; CyberInsider 1,094; BleepingComputer 1,235. Counting-method spread.
- 1,474 the month before: **Incorrect**. October Patch Tuesday: 1,407 (Tenable) to 1,416 (PCWorld); BleepingComputer 1,411 plus 348 earlier out-of-band (about 1,759 total). No source gives 1,474.
- 1.4% (1,363) of 2026 CVEs in VulnCheck KEV: **Verified w/ caveat**. VulnCheck 1H report: KEV-to-CVE ratio 1.4%; 1,363/100,222=1.36%. Exact 1,363 not accessible. 'Consistent' is loose: 1.9% 1H25, 2.7% peak 2H23.
- Exploitation percentage steady: **Verified w/ caveat**. Different catalog gives different rate: CISA KEV adds in 2026 = 297 (0.30%).
- Bressers: AI drives a majority of CVE growth: **Not checkable**. Expert opinion. Supported by timing (Sep-Oct 3.6x) but VulnCheck itself counts only 1,061 explicitly AI-attributed vulns in 1H 2026.
- Patch-to-exploit window shrinking 'also thanks to AI': **Incorrect**. Shrinking is supported; AI as cause is not. CrowdStrike: pattern 'predates the emergence of frontier AI models'.
- CrowdStrike: 88% of PoC exploitation within 48h, Jan-Jun: **Verified**. CrowdStrike 2026 Threat Hunting Report, exact wording. Share of observed exploitation, not of CVEs.
- Vault Panda / Genesis Panda attacked within 24h: **Verified w/ caveat**. Report: within 24h of public disclosure of a critical web-app vuln (one case, not a rate).
- VulnCheck median 120 days (2025) to 80 (1H 2026): **Verified w/ caveat**. VulnCheck 1H-2026 report. Median among exploited CVEs; 2026 cohort is right-censored.
- Window closed further in 2H 2026: **Not checkable**. No published 2H data found.
- VulnCheck disclosed 14 Copeland vulns 'on Friday': **Verified w/ caveat**. 14 IDs confirmed; NVD records went live Thu 12 Nov 20:18 UTC, write-up Fri 13 Nov.
- All 14 are 'make-me-root': **Incorrect**. VulnCheck describes 1 direct unauthenticated root, several root via setuid/authenticated paths; not all 14.
- Two under active exploitation: **Verified**. CVE-2026-124014 and CVE-2026-124010; other 12 no hits.
- Honeypot hits 'literally minutes later': **Incorrect**. First hit 1h40m (124014), 7h23m (124010) after NVD publication.
- Iranian hackers abused such devices, food spoiled, fires, across Europe: **Verified w/ caveat**. France attributes Oct 2026 attack to IRGC unit Shahid Kaveh, ~700 French sites, fires reported; Europe-wide only via Sekoia second wave. VulnCheck says its canary hits are not evidence of that campaign.
- CISA directive in June: **Verified**. BOD 26-04 issued 10 Jun 2026, revokes BOD 19-02 and 22-01.
- Replaces policies based 'solely on CVSS': **Incorrect**. Prior flat deadlines were KEV-driven, not CVSS-driven; CVSS does not set BOD 26-04 timelines either. Also omits 4th criterion (technical impact); Phase III runs 180 days; applies to federal agencies.
- Eclipse launched managed tier in April: **Verified**. Announced 21 Apr 2026.
- 99.95% uptime SLA with service credits: **Verified w/ caveat**. Press release 'SLA'; MSA: 'commercially reasonable efforts' monthly target.
- Customers include AWS, IBM, Google, Cursor, Windsurf: **Incorrect**. Press release names AWS, Google, Cursor. IBM (Bob) and Windsurf only appear as platforms that use Open VSX.
- Stenberg cites 'avalanche of vulnerability reports': **Verified**. Stenberg: 'entirely triggered by the current avalanche of vulnerability reports'.
- As of 26 Oct low/medium reports go to public GitHub; high/critical private: **Verified**. curl policy post; uncertain-severity reports may still go to HackerOne for triage.
- Stenberg already shut bug bounty this year, AI submissions: **Verified**. Ended 31 Jan 2026 (announced 21 Jan).
- Bressers quotes (scares the bejesus, data problem, alligator pool): **Not checkable**. Given only to The Register interview; no transcript.
- Developers drowning / foundations crushed by AI scraping: **Verified w/ caveat**. Consistent with curl and Eclipse evidence; general assertion by Bressers, no figures.
- Rules updated 'last month': **Verified**. Posted 20 Oct 2026.

**Confidence:** high on counts and quotes checked; medium on interpretive points.

## Sources
- The Register article: https://www.theregister.com/security/2026/11/13/this-years-bug-count-just-passed-100k/5309882
- NVD CVE API 2.0 (pubStartDate counts, queried 17 Nov 2026): https://services.nvd.nist.gov/rest/json/cves/2.0
- CISA KEV catalog JSON: https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json
- VulnCheck State of Exploitation 1H-2026: https://www.vulncheck.com/blog/state-of-exploitation-1h-2026
- VulnCheck, Copeland XWEB EVO write-up: https://www.vulncheck.com/blog/copeland-xweb-evo-unauthenticated-root
- CrowdStrike 2026 Threat Hunting Report: https://www.crowdstrike.com/en-us/blog/crowdstrike-2026-threat-hunting-report/
- CISA BOD 26-04: https://www.cisa.gov/news-events/directives/bod-26-04-prioritizing-security-updates-based-risk
- CISA, Patch Smarter, Not Harder: https://www.cisa.gov/news-events/news/patch-smarter-not-harder
- Eclipse Foundation press release, Open VSX Managed Registry: https://newsroom.eclipse.org/news/announcements/eclipse-foundation-launches-open-vsx-managed-registry-0
- Open VSX Managed Registry terms: https://managed.open-vsx.org/terms/
- Daniel Stenberg, curl security policy post (Oct 2026): https://daniel.haxx.se/blog/2026/10/20/
- The Register, curl maintainers go public (20 Oct 2026): https://www.theregister.com/security/2026/10/20/swamped-curl-maintainers-take-low-level-flaws-public-to-survive-vulnerability-avalanche/5304198
- BleepingComputer, Oct 2026 Patch Tuesday: https://www.bleepingcomputer.com/news/microsoft/microsoft-october-2026-patch-tuesday-fixes-1-411-flaws-3-zero-days/
- BleepingComputer, Nov 2026 Patch Tuesday: https://www.bleepingcomputer.com/news/microsoft/microsoft-november-2026-patch-tuesday-fixes-1-235-flaws-outlook-zero-day/
- Tenable, Nov 2026 Patch Tuesday: https://www.tenable.com/blog/microsoft-november-2026-patch-tuesday-addresses-1093-cves-cve-2026-124194
- PCWorld, Oct 2026 Patch Tuesday: https://www.pcworld.com/article/3260668/microsoft-breaks-record-fixing-1416-security-flaws-update-windows-now.html
- BleepingComputer, France blames Iranian hackers: https://www.bleepingcomputer.com/news/security/france-blames-iranian-hackers-for-supermarket-attacks-that-sparked-fires/
- Sekoia, The cold chain on fire: https://www.sekoia.com/blog/the-cold-chain-on-fire-unpatched-danfoss-controllers-exploited
