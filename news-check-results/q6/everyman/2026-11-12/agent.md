# Fact-check: Industrial Cyber, "Bridewell reports 80% surge in global ransomware victims as manufacturing emerges as top target in Q3 2026"

## Access limits
- The article itself returned HTTP 403 to WebFetch, and the web archive is blocked. I could not read its body or quotes.
- Its claims below come from a search-engine summary of the article, so they are second-hand. I checked each against Bridewell's own report and an independent tracker.
- Any claim or attributed quote in the article that is not listed here is NOT CHECKED. That includes any quotes from Bridewell staff and any OT/ICS commentary.
- Primary source: Bridewell, "The State of Ransomware Q3 2026" (BCON Collective blog, dated 3 Nov 2026), https://www.bridewell.com/insights/blogs/detail/the-state-of-ransomware-q3-2026

## Claims
| # | Claim (as reported) | Verdict | Evidence |
|---|---|---|---|
| 1 | 2,889 publicly disclosed victims in Q3 2026 | TRUE | Bridewell: 2,889 leak-site postings (July 970, Aug 1,064, Sep 855). |
| 2 | Up about 80% year on year (from 1,609 in Q3 2025) | TRUE | Bridewell: 1,609 to 2,889, +80%. |
| 3 | About 26% above Q2 2026 (2,299) | TRUE, with a caveat | Bridewell's table says +26%. Its own text says "approximately 30% higher" than Q2, an internal inconsistency in the source. |
| 4 | 111 active groups, a record | TRUE | Bridewell: 111 groups, up from 86 a year earlier and 97 in Q2. It is the highest in its six-quarter series (86, 86, 83, 81, 97, 111). "Record" holds only within that window. |
| 5 | Storm and Direwolf emerged as new groups | TRUE | Bridewell: Storm 78 victims, Direwolf 60, both flagged new. |
| 6 | Safepay, LockBit5 and Clop grew sharply month on month | TRUE | Safepay 12 to 32 (Aug to Sep). LockBit5 12 to 21. Clop 1 in July to 45 in August. |
| 7 | Manufacturing is the top target | TRUE | Bridewell: manufacturing 474 victims, 16.4%, ahead of professional services (392) and technology (363). Healthcare is fourth (285). Bridewell's Q2 manufacturing figure was 274. |
| 8 | 13 sectors affected | TRUE | Bridewell: 13 sectors. |
| 9 | Moderate-confidence assessment that elevated activity continues into Q4 | TRUE | Bridewell: "with moderate confidence... likely to persist into Q4". |
| 10 | Independent corroboration of manufacturing as top target | SUPPORTED | Comparitech's Q3 2026 roundup: 2,627 attacks (+61% year on year), manufacturing 478 (+22% vs Q2, +52% year on year). It is a different dataset, and its growth rates differ from Bridewell's, but the direction agrees. |

## Minor source flaws (Bridewell, not necessarily the article)
- The Q2 manufacturing share is given as 12.4%, but 274 of 2,299 is about 11.9%.
- The top-10 share is given as 48% (1,401 victims), but 48% of 2,889 is about 1,387.

## Misleading-context flags
1. "Victims" means names posted on criminal leak sites, not confirmed attacks or successful intrusions. Leak-site counts are shaped by which groups post, so a jump from 86 to 111 groups can inflate the count. The 80% rise measures posting behaviour as much as underlying attack volume.
2. The Q3 2025 baseline (1,609) was the lowest quarter in the series, so the year-on-year percentage is flattered. The six-quarter baseline average is 2,229, and Q3 2026 is about 30% above it. Q4 2025 and Q1 2026 were already 2,437 and 2,417.
3. A single group, Clop, posted 45 victims in one month, off its mass exploitation of a PTC Windchill/FlexPLM flaw (CVE-2026-12569). This is a one-off spike and part of what drives the manufacturing ranking.
4. In the same Bridewell data, the ransom payment rate rose (28% vs 19%) while the average payment fell 45%. Coverage focused on volume can miss that.
5. Manufacturing's lead was already established by other trackers, such as ZeroFox in Q2. "Emerges as top target" suggests a new development, but it is a continuing pattern.

## Overall rating
**1 Pinocchio (minor omissions of context).** Every checkable figure matches Bridewell's primary report. The caveats are about interpretation (leak-site counts, a low baseline), not factual error. This rating is provisional because the article body could not be read, so unseen quotes and claims are not covered.
