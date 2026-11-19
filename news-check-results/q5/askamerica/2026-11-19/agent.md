The CRFB CEA Projections Tracker article's claims about GDP, debt and deficits versus CEA projections are accurate.

Rating: 0 Pinocchios.

Report page (local): http://127.0.0.1:54763/a/ce475981dbdb03630d84504da9f12ff7.html (report_id 20261119-072108-the-crfb-cea-projections-tracker-article)

## Warehouse checks (AskAmerica data first)
- Deficit: econ.fred_indicators FYFSD shows FY2025 -$1.775T and FY2026 -$1.992T. Article: $1.8T and "nearly $2.0T". True.
- Deficit share: FYFSGDA188S FY2025 is -5.75%, which matches the article's 5.8%. The 6.2% for FY2026 is CRFB's own estimate (not loaded in the warehouse); CRFB's linked post and sheet agree.
- Real GDP growth: fiscal-year average of econ.real_gdp_growth gives +2.5% (FY2025) and +2.23% (FY2026). Article: 2.5% and 2.2%. True.
- Debt: econ.federal_debt shows debt held by the public of $32.43T at 2026-09-30. CRFB estimates $32.3T, or 100% of GDP. Consistent; it is an estimate.
- Primary deficit: econ.mts_outlays_by_function net interest ($970B FY2025, $1,052B FY2026) subtracted from the deficit gives $805B and $941B. Matches the article.
- Revenue: total outlays minus deficit gives $5.23T (FY2025) and $5.40T (FY2026). Matches the article's $5.2T and $5.4T.

## Primary sources
- The CEA chart book and the White House release confirm 94% debt/GDP and 3.2% deficit/GDP in 2034, and a primary surplus by 2034.
- CRFB's tracker Google Sheet reproduces the CEA values ($645B, $660B, -$35B primary deficits; revenue $5.26T and $5.55T) and the CBO baseline figures (116% debt, $2.8T deficit, $915B primary deficit, $7.7T revenue).
- The Fed's Sept 2026 SEP shows a 2.4% median for 2027 and 2.0% longer run, as the article says.
- CRFB's other posts confirm the Supreme Court IEEPA ruling.

## Not verified or only partly verified
- CBO pages and the CBO monthly review returned HTTP 403. CBO figures were checked through CRFB's sheet (the same publisher as the article) and the warehouse, not read from CBO directly.
- OMB debt, revenue and deficit-share projections (102% peak, 98%, $8.9T, 3.6%) were not found in the fetched OMB appendix text.
- The "$1.8T dynamic" CEA estimate and CBO's $4.2T were not found in any fetched text.
- The "70% of tariffs replaced" figure was not found; CRFB's linked post says "less than 60%" of lost IEEPA revenue, with a later note citing a revised CBO estimate.
- The CEA 4% growth and 97% debt figures appeared only in a summarized fetch of the White House banner and in CRFB's sheet, not in text the engine read directly.

## Framing flags
- CEA values are hand-measured from chart images. The article discloses this in footnote 1 but states the figures to the billion in the body. For example, the White House banner gives a $1.8T 2026 deficit versus CRFB's $1.7T reading.
- "Well short" on revenue overstates the gap: revenue was 0.5% below CEA in FY2025 and 2.6% below in FY2026.
- Part of the miss comes from events after CEA's projection (the tariff ruling and inflation). The article itself warns that missing projections does not prove the policy analysis was wrong.
- Typos: "FY 2017 Budget" should be FY 2027, and "5.2% in 2026 and 2026" is garbled.
- CRFB is a fiscal-hawk advocacy group, and the tracker, sheet and most "actual" links are its own work.