**The Fortune article's claims about Ron Johnson's Stop Scaring Seniors Act, the 22% Social Security cut and CRFB's $190 trillion estimate are accurate; 1 claim could not be confirmed.**

**Rating: 0 Pinocchios** (engine rating from the claim scores). The framing caveats below are context, not factual errors.

Article: Fortune, Nov 19 2026, Nick Lichtenberg. It was fetched directly with no 403.

## Claim checks
| Claim | Verdict | Basis |
|---|---|---|
| Retirement trust fund under six years from depletion; cut about 22% | True | Warehouse `econ.social_security_trustees_projections`: 2026 Trustees OASI depletion 2032, 78% payable. The Senate memo also says 2032. |
| CRFB: $190T added over 75 years, real, 165% of GDP (central) | True | CRFB Aug 2026 testimony and the Nov 19 release. Nominal figure is $840T. |
| About $500 a month per average beneficiary, per CRFB | True | Stated in Fortune's Aug 14 piece. I did not read CRFB's own page. |
| Roughly 70 million beneficiaries | Mostly true | Warehouse `health.ssa_oasdi_county`: 63.8M in the 50 states, Dec 2025. DC and territories are absent from the table. |
| CRFB's Nov 2025 analysis: rates +2.6 points, 10-year 4% to 6.6%, mortgages 6.3% to 8.9% | True as reported | InvestmentNews, Nov 17 2025. |
| Senate Budget report projects a $511B Social Security deficit in 2032 | True | Chairman's report p.9. The $511B is 21% of a $2.4T federal deficit. |
| Johnson's "sons and daughters" and "spent to pay current beneficiaries" quotes | True | Same report, p.4. |
| July 2011 letter from Johnson and 12 freshman GOP senators on "scare seniors" | True | Milwaukee Journal Sentinel copy on Johnson's site. |
| Fox interview Nov 7, "swamp won," Calley Means | True | Newser, Raw Story and Political Wire. I did not read the Fox transcript. |
| MacGuineas quotes ($190T, 165% of GDP, "cynical pander") | True | Verbatim in the CRFB release. |
| "Spook the bond market" | Not checkable | A prediction no data can test. |
| TD Securities: $41.1T debt limit binds in January, out of room about Sept 2027 | Unsupported | No TD note found. The CRS confirms the $41.1T limit. Warehouse `econ.federal_debt` shows $40.71T on 2026-11-20. NTU put headroom at $777B as of Oct 30. |

## Framing caveats
- The headline's "borrow $190 trillion" presents a 75-year, inflation-adjusted CRFB figure as the cost of the bill. CRFB's number is for general-revenue funding of Social Security's shortfall generally, not a score of the bill's text. The bill PDF is image-only, so I could not read it. The body does state "inflation-adjusted" and "75 years", but the headline drops both.
- The 2.6-point rate shock comes from CRFB's Nov 2025 analysis, which used about $150T and 130% of GDP. It is not the $190T scenario. The 10-year yield was 4.92% on 2026-11-20 (warehouse `econ.fred_indicators`, DGS10), so the 4% starting point is stale.
- Johnson's argument that the trust fund is an accounting construct gets one paragraph, and CRFB's framing leads.

Report: /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q6/askamerica/2026-11-25/report.html