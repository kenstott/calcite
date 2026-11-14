The Nevada governor's "eleven straight months" job-growth release is mostly accurate on payroll data but mislabels its measure and overstates its causal and investment claims are biased and accurate; 2 claims could not be confirmed.

Rating: 0 Pinocchios, from the claim scores (honesty 90, "honest"; bias 50, "biased"). Scores describe this article only.

Article: https://gov.nv.gov/press-releases/nevada-leads-nation-in-job-growth-for-eleven-straight-months (fetched OK, June 23, 2026; linked DETR PDF fetched OK; BLS laus.nr0.htm and the DETR statewide report returned HTTP 403, so BLS figures were recomputed from warehouse tables).

Claim results
1. (Central) Nevada led the nation in YoY job growth for eleven consecutive months: mostly true. Warehouse econ.state_industry (BLS SM payroll, current revised NSA vintage): Nevada ranked #1 every month July 2025 through May 2026 (11 months); Idaho led June 2025. Caveat: on LAUS household employment (the page the release links), Nevada led only Jan-Mar 2026; South Carolina led April-May (May: SC 2.65% vs NV 1.97%).
2. Total employment grew 1.8%, highest in America: mostly true. Payroll measure ranks first (warehouse revised NSA 2.0%, next West Virginia 1.23%); household measure does not.
3. Surpassed 1.6 million jobs for the first time: true. First month at or above 1,600K in the loaded data is Sep 2025 (1,603.5K); May 2026 1,617.1K.
4. Policies helped attract more than $6 billion private investment: unsupported. No independent tally; the governor's April 2026 statement said over $5 billion, and a $6 billion figure elsewhere is a five-year projection of incentivized capital investment. Causal attribution untested.
5. Unemployment comparisons (NV 5.2%; DC 6.1, CA 5.3, WA/OR 5.2, CT/DE/MI/IL 5.1): true, exact match in econ.regional_employment, May 2026.
6. North Carolina followed at 1.2%: true as the as-published figure; the revised NSA warehouse shows WV 1.23%, NC 1.01% (vintage/seasonal-adjustment difference).
7. Led in Private 2.2%, Information 7.8%, Prof/Business Services 6.0%, Health/Education 4.6%, etc.: unsupported (not verified; industry query returned nothing, primary DETR report blocked with 403).
8. Schmidt (DETR): 11 months in a row, strong private sector, softer leisure/hospitality: true on the streak.
9. Lombardo quote: opinion, not checkable.

Materially misleading framing: the streak is true for nonfarm payroll jobs but the release calls it "total employment" and links the household-survey page, where it fails for April and May. The $6 billion investment figure is the governor's own and unstable across his releases, and the policy-to-growth causation is asserted, not demonstrated. The leisure/hospitality softness DETR concedes is absent from the governor's text.

Data notes: first-pass self-joins fanned out duplicate rows (does not affect per-state ratios; final figures re-run with DISTINCT); annual-average rows (M13) were left out of the SM queries.

Report page (local): http://127.0.0.1:52646/a/ce3a1d5c88687bf9cff02b48cb43d1e9.html ; saved at /Users/kennethstott/.askamerica/reports/20261114-091729-the-nevada-governor-s-eleven-straight-mo.html
