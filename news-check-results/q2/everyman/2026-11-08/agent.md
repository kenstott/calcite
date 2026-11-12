# Fact-check: Medical Daily, "Overdose Deaths Climbed in 430 U.S. Counties While the National Toll Fell 12% to Nearly 68,000" (Nov 5, 2026, Joseph James)

## Verdict: 1 Pinocchio (mostly accurate, minor framing issues)

I could not check the county-level numbers against CDC county data. I could verify them only through Axios's published analysis. The national figures I checked against the CDC dataset directly. Medical Daily's page fetched fine. The Axios page returned 403, so for Axios I relied on a search-result summary of its article.

## Claim-by-claim

| # | Claim | Verdict | Source and confidence |
|---|---|---|---|
| 1 | Overdose deaths fell about 12% nationally, to nearly 68,000 (12 months ending March 2026 vs March 2025) | True | CDC VSRR dataset xkb8-kh2a. Reported: 67,576 vs 77,064, a 12.3% drop. Predicted: 68,080 vs 77,306, an 11.9% drop. Axios's "about 9,500 fewer" matches (77,064 minus 67,576 is 9,488). High confidence. |
| 2 | MedicalDaily earlier put the national figure at 67,798 deaths, down 12.3% | Not checkable here | The 12.3% matches the reported-count change. 67,798 matches neither the reported (67,576) nor predicted (68,080) value I pulled. It may come from a different data vintage. Low confidence. |
| 3 | Deaths rose in 430 counties and fell in 842 | Supported, with a source caveat | The Axios analysis of CDC provisional data, per a search snippet. It also says 352 counties were unchanged. I did not recompute this from CDC county data. Medium confidence. |
| 4 | Several neighboring counties in Arizona and New Mexico saw increases | Supported by Axios snippet only | Medium-low confidence. |
| 5 | Rio Arriba County, NM, had about 173 deaths per 100,000, the highest calculable rate | Plausible, unverified | The Axios snippet confirms Rio Arriba had the highest calculable rate. I could not confirm the 173 figure. Medium-low confidence. |
| 6 | CDC withholds counts of 1-9 deaths, so trends are unavailable for nearly half of counties | True | CDC suppression rule, and Axios says the same. High confidence. |
| 7 | Ohio: of 42 counties with a measurable trend, 30 fell, 9 rose, 3 were flat | Not checkable here | It comes from Axios Cincinnati, which I did not reach. The numbers sum correctly (30+9+3 = 42). |
| 8 | Butler County OH fell 30% (104 to 73); Hamilton County fell 27% (200 to 147) | Arithmetic correct | 73/104 is -29.8%, and 147/200 is -26.5%, which rounds to 27%. The underlying counts are unverified. |
| 9 | Campbell County KY rose 31%, from 13 to 17 | Arithmetic correct, misleading as stated | 17/13 is +30.8%. A 4-death change is the small-count noise the article itself warns about. |
| 10 | Butler County coroner data show a peak of 236 in 2017 and 87 in 2025 | Not checkable here | It comes from local coroner data that I did not access. |
| 11 | The CDC data are provisional and small counts swing widely | True | Stated on the CDC dashboard. High confidence. |
| 12 | The CDC cites naloxone access, treatment expansion and drug-supply changes as drivers | Plausible, unverified | Axios attributes this to the CDC. Medium confidence. |
| 13 | A University of Arizona researcher pointed to potent fentanyl and meth, and long drive times to rural treatment, per Arkansas Public Media (May 2026) | Not checkable here | I did not check the secondary source. |
| 14 | Quote from Grant Baldwin, CDC Division of Overdose Prevention, about useful data guiding next steps | Not checkable here | It is a paraphrase, not a direct quote. The attribution was not verified. |
| 15 | Safety guidance: naloxone is over the counter, SAMHSA Helpline 1-800-662-4357, 988, buprenorphine and methadone reduce death risk | True | Well-established public-health facts. High confidence. |

## Materially misleading or notable points

- **Headline framing.** The headline, "US overdose deaths rise in 430 counties," leads with the minority trend. Nationally deaths fell 12%, and in the analysis 842 counties fell against 430 that rose. The body corrects this, so the headline is technically accurate but selective.
- **Selection effect.** The 430 versus 842 comparison covers only counties where trends can be calculated, about half of all counties. It leaves out the smallest counties.
- **Secondary reporting.** Nearly all county data is aggregated from Axios and Axios Cincinnati, not from the CDC. The article's attribution is honest, but readers cannot check the county figures from the piece itself.
- **Date discrepancy.** The page shows both Nov 08 and Nov 05 timestamps, with no explanation.
- **Newer data available.** The CDC dataset already has a May 2026 period (66,478 predicted). The article's March figures are slightly stale, though accurate for the period it names.

## Grade: 1 Pinocchio
The national figures, which I verified against the CDC, are accurate. The county claims rest on a credible secondary analysis I could not independently confirm. The only issues are headline emphasis and unreproduced local numbers.

Sources: [CDC VSRR dataset xkb8-kh2a](https://data.cdc.gov/resource/xkb8-kh2a.json), [CDC dashboard](https://www.cdc.gov/nchs/nvss/vsrr/drug-overdose-data.htm), [Axios, Nov 3 2026 (403 on fetch; read via search snippet)](https://axios.com/2026/11/03/overdose-deaths-falling-cdc), [Medical Daily article](https://www.medicaldaily.com/us-overdose-deaths-rise-430-counties-480874).
