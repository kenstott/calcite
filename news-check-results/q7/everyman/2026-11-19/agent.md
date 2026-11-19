# Fact-check: Paubox "HIPAA Breach Report for November 2026"

Rating: 1 Pinocchio (mostly accurate; small verification gaps and one framing issue).

Method: WebFetch of the article (summarized by the tool, not verbatim), WebSearch for corroboration. The HHS OCR breach portal is a dynamic app and returned no records through WebFetch, so the raw HHS rows could not be checked directly.

## Claims and verdicts
| Claim | Verdict |
|---|---|
| iRhythm Technologies was the largest October breach, 8,179,772 people | Supported. SecurityWeek reports the HHS portal lists 8,179,772, matching iRhythm's earlier Oregon AG filing. Cause: social engineering of third-party-hosted business apps. |
| Central Ohio Primary Care Physicians second, 1,351,650 | Largely supported. Multiple outlets (Becker's, HIPAA Journal, ClaimDepot) say about 1.4M; the exact figure was not found in an accessible source. COPC discovered the incident Aug 25, 2026. |
| 34 breaches, 10,116,362 individuals in October | Not checkable here (HHS portal inaccessible). Internally consistent: 8,179,772 + 1,351,650 = 9,531,422, leaving about 585k for the other 32. |
| Network server: 10,106,558 people, 27 breaches (22 in 2025); prior-year 1,419,860 | Not checkable here. Plausible given the two largest breaches. |
| EMR 64,728 (20,081 in 2025); Other 34,828 | Not checkable here. |
| Email 4 breaches (6 in 2025, 15 in 2024) | Not checkable here. |
| Network server "most popular attack vector" | Supported by the article's own counts; opinion-level framing. |

## Issues
- Category counts (27 + 5 + 4 = 36) exceed the stated 34 total. Either categories overlap or the numbers are inconsistent; the article does not explain.
- Network server people-affected (10,106,558) is nearly the full total (10,116,362), so the "highest in five years" framing is driven by two incidents. It reflects a few mega-breaches, not a broad trend.
- "Network server" is an HHS location-of-breach field. The article's "attack vector" label is loose.
- Paubox sells email security, and the report is a marketing blog. It still cites HHS data and links the portal.
- Reported-in-October data does not mean the breaches occurred in October (COPC was discovered in August).
- No attributed quotes beyond the article's own phrase "remain the most popular attack vector for bad actors."

Overall: core headline facts check out; aggregate statistics could not be independently verified.
