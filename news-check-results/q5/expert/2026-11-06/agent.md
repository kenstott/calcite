# Fact-check: ABC News, "Ahead of midterms, backlash grows over soaring electricity bills" (Schulze, Oct 29, 2026)

Overall: **1 Pinocchio** (mostly accurate; one framing issue, two claims unverified).

Caveat: WebFetch returned a model summary of the article, not verbatim text, so the claim list below is from that summary and may miss claims or exact wording.

## Claims
| # | Claim | Verdict | Evidence |
|---|---|---|---|
| 1 | Residential electricity costs up ~5% nationally over the past year (EIA) | Supported, with a window caveat | EIA API v2 retail-sales, sector RES, average price in cents/kWh, US: July 2026 vs July 2025 = +5.3%. August 2026 vs August 2025 = 17.57 to 18.16, +3.4%. The 5% figure matches July only. September 2026 data was not yet published (0 rows). |
| 2 | Electricity costs rose 16% in Maine | Supported (price, July) | ME residential price July 2026 vs July 2025 = +16.2%. August = +14.1%. |
| 3 | Electricity costs rose 12% in Ohio | Supported (price, July) | OH July YoY = +12.2%. August = +10.4%. |
| 4 | 5 of the 10 states with the largest increases host competitive Senate races | Plausible, not fully checkable | July top 10 by YoY price: HI, NH, ME, NY, NC, MD, OH, MI, IL, ID. Those with commonly rated competitive Senate races: ME, OH, NC, MI, NH, which gives 5. "Competitive" is a judgment and the analysis itself is not published, so I treat this as consistent, not confirmed. The ranking is by price, not by bill. |
| 5 | Maine race: Collins vs. Troy Jackson | Supported | Jackson became the Democratic nominee July 25 after Platner withdrew; the race is rated a toss-up (Fox-affiliate explainer, news search). |
| 6 | Ohio race: Husted vs. Sherrod Brown | Consistent with my knowledge; not re-searched. | |
| 7 | ABC/Post/Ipsos poll: 57% say utilities unaffordable, up from 45% in February | Half verified | The February 45% is confirmed (poll fielded Feb 12-17, 2026, n=2,589). I could not find the October 57% topline, so it is not checkable here. |
| 8 | Causes: aging grid upgrades, regulation, demand incl. data centers | General, qualitative | Consistent with the UCLA Loudoun study, which attributes bill increases mainly to transmission costs. |
| 9 | Data centers: companies pledge to cover own costs; evidence on bill impact is mixed | Fair characterization | No contrary evidence found. |
| 10 | Oct 20 Loudoun County meeting; resident's bill rose from ~$250 to ~$850 | Not checkable | I found no source for the quote or the figures. Search did confirm Loudoun has an active data-center pause debate. This is an anecdote about one household, not a typical bill. |
| 11 | DOE tips: power strips, seal gaps, water heater at 120F, warm-water washing | Not checked in depth | These are standard DOE Energy Saver advice, and 120F is DOE's long-standing recommendation. Low risk. |

## Materially misleading or imprecise
- The article's "costs" are average residential price per kWh (EIA), not monthly bills. Price is not the bill; usage and weather differ. The Loudoun $250 to $850 anecdote (+240%) is far outside the price data, which is likely a mix of usage and rates.
- The figures are July, a single month, while later data (August) are lower: ME +14.1%, OH +10.4%, US +3.4%. The piece says "past year" without naming the month. The rounded 5%, 16% and 12% match July only.
- The state-ranking and Senate-race overlap is correlation. It does not show that electricity prices drive these races.
- The poll's affordability question is about perception, not measured prices.

## Method and confidence
- Source: EIA API v2 `electricity/retail-sales`, `sectorid=RES`, `price` field, monthly, 62 geographies per month (50 states, DC, US, regional aggregates). I filtered to 2-letter state codes for the rankings. Confidence in the numbers is high; confidence in the article text is medium (summary only).
- Not done: could not retrieve the Oct 2026 poll topline, the ABC state analysis, or the Loudoun meeting record.
