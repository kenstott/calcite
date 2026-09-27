# Verdict: Crude oil claim TRUE; natural gas claim STALE VINTAGE (one release-cycle out of date)

## Summary
- **Crude oil claim** ("13.8 mb/d in 2026, surpassing 13.7 mb/d record in 2025") is **accurate**. It matches EIA's September 2026 STEO exactly.
- **Natural gas claim** ("122.5 Bcf/d in 2026, surpassing 118.5 Bcf/d record in 2025") uses **EIA's August 2026 STEO figures**, not the September 2026 STEO the article names as its source. The actual September 2026 STEO revised these to **123.0 Bcf/d (2026)** and **118.4 Bcf/d (2025)**.

## What the corpus has (and doesn't have)
`energy.eia_crude_oil_production` and `energy.eia_natural_gas_production` (askamerica `energy` schema) carry EIA API v2 **actual** monthly production — not EIA's STEO forecast series. So this corpus can verify actuals and trend direction, but the STEO's own forecast numbers had to be checked against EIA's primary STEO document directly (fetched live).

## Computed actuals (askamerica)
**Crude oil** — `SELECT production_year, AVG(production_volume) FROM energy.eia_crude_oil_production WHERE eia_area_code='NUS' AND fuel_type='Crude Oil' AND production_unit='MBBL/D' GROUP BY production_year`
- 2024: 13,267 MBBL/D (13.27 mb/d)
- 2025 (full year): 13,660 MBBL/D (**13.66 mb/d**, rounds to the claim's cited 13.7 record)
- 2026 (Jan–Jun, only months loaded): 13,712 MBBL/D (13.71 mb/d) — already above the 2025 average, consistent with a full-year ~13.8

**Marketed natural gas** — summed monthly MMCF from `energy.eia_natural_gas_production` where `eia_area_code='NUS' AND process_code='VGM'` (VGM = "Marketed Production"), converted to Bcf/d (÷1000 for Bcf, ÷~30.4 days/month):
- 2024: ~113.3 Bcf/d
- 2025 (full year): ~118.3 Bcf/d (matches both STEO vintages closely)
- 2026 (Jan–Jun): ~121.6 Bcf/d, trending toward but below either STEO's full-year 2026 forecast

## Verifying the forecast numbers against the primary source
Fetched `https://www.eia.gov/outlooks/steo/pdf/steo_full.pdf` directly (confirmed "September 2026," EIA completed modeling September 3, 2026):
- **Table 4a** (Petroleum): "U.S. total crude oil production" = 13.66 (2025), **13.83** (2026), 14.26 (2027) million b/d — matches the claim and EIA's Sept 9, 2026 press release (press592.php) exactly.
- **Table 5a** (Natural Gas): "U.S. total marketed natural gas production" = 118.4 (2025), **123.0** (2026), 127.6 (2027) Bcf/d — **not** 118.5/122.5.

Traced 122.5/118.5 to EIA's "Today in Energy" article published **August 12, 2026** (eia.gov/todayinenergy/detail.php?id=67944), which states verbatim it is reporting "our August 2026 Short-Term Energy Outlook (STEO)" forecast. Between August and September, EIA revised the 2026 marketed-gas forecast up 0.5 Bcf/d and the 2025 figure down 0.1 Bcf/d.

## Fact-check ratings (score_claim / Washington Post scale)
- Crude oil claim: **TRUE**, 0 Pinocchios.
- Natural gas claim: **STALE VINTAGE** (real, correctly-quoted EIA numbers, but from the wrong monthly edition of the same recurring report — direction and rough magnitude are still correct). Overall report rating: 1 Pinocchio.

## Report
Published report (dashboard + full narrative + citations): see `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q9/askamerica/2026-09-26/report.html`

## Sources
- EIA September 2026 STEO (Tables 4a, 5a): https://www.eia.gov/outlooks/steo/pdf/steo_full.pdf
- EIA Press Release, Sept 9, 2026: https://www.eia.gov/pressroom/releases/press592.php
- EIA Today in Energy, Aug 12, 2026 (source of the stale 122.5/118.5 figures): https://www.eia.gov/todayinenergy/detail.php?id=67944
- askamerica: `energy.eia_crude_oil_production`, `energy.eia_natural_gas_production`
