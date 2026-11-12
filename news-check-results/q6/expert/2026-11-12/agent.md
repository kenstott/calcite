# Fact-check: Industrial Cyber on Bridewell Q3 2026 ransomware report

**Verdict: 1 Pinocchio (mostly accurate).** Limitation: industrialcyber.co returned HTTP 403 (Cloudflare challenge) to WebFetch and curl, so I could not read the article itself. I checked the article's claims as reproduced in search-engine summaries of it, against Bridewell's primary report (https://www.bridewell.com/insights/blogs/detail/the-state-of-ransomware-q3-2026, 3 Nov 2026) and independent data.

## Summary of findings
The headline holds: Bridewell counted 2,889 leak-site victims in Q3 2026 versus 1,609 in Q3 2025 (+79.6%, rounds to 80%), with manufacturing the top sector at 474 victims (16.4%; 474/2,889 = 16.4%).
- Inconsistency: the article reportedly says Q3 was about 30% above Q2; Bridewell's own report says +26% (2,299 to 2,889 = +25.7%). GuidePoint says +21%, ReliaQuest +30% (listings).
- Bridewell's Q2 manufacturing share (12.4%, 274) does not match its Q2 total (274/2,299 = 11.9%). Internal inconsistency in the source.
- Misleading framing: counts are leak-site postings, not attacks; active groups rose 29% (86 to 111), and ReliaQuest attributes much of manufacturing's jump to Qilin and The Gentlemen.
- Not independently verifiable: Coveware ransom-payment figures (avg $1,025,278, median $400,000, 28% payment rate) as cited by Bridewell.

## Claim table
| Claim | Result |
|---|---|
| 2,889 victims Q3 2026 | Confirmed: Bridewell; ransomware.live API pull 2,902 (my dedup, by discovered date) |
| +80% vs 1,609 Q3 2025 | Arithmetic correct. My pull of ransomware.live shows 1,398 for Q3 2025 (+108%), so base differs by backfill; GuidePoint +75% |
| ~30% over Q2 | Not supported by Bridewell (+26%); close to ReliaQuest 30% |
| 111 active groups, up from 86 | Bridewell; my pull 106 groups |
| Manufacturing 474 (16.4%) top sector | Confirmed; my pull 475; GuidePoint, ReliaQuest, Point Wild agree manufacturing is #1 |
| Qilin 362, Thegentlemen 360 | Qilin 362 exact in my pull; Thegentlemen 395 in my pull (differs) |
| US 1,002 (34.7%) | Arithmetic correct; not independently recounted |
| Coveware payment figures | Not checkable here (primary Coveware report not retrieved) |

## Sources
Bridewell report; ransomware.live API v2; GuidePoint GRIT Q3 2026; ReliaQuest Q3 2026 spotlight; Comparitech Q3 2026 roundup; Point Wild Q3 2026.
Confidence: medium (article not read directly; counts drift with ransomware.live backfill).
