# Verdict: The FBI's 2024-to-2025 crime decline claim is confirmed by independent computation; two historical superlatives are not checkable in this corpus

## Summary

Using `crime.cde_offenses` (FBI Crime Data Explorer, summarized offense rates by state/month), I independently computed national-level year-over-year changes for calendar year 2024 vs. calendar year 2025 across all 51 states + DC. **Every checkable numeric claim in the CBS/FBI article is confirmed**, matching within roughly half a percentage point or less:

| Offense | 2024 est. count | 2025 est. count | Computed % change | Claimed % change |
|---|---|---|---|---|
| Violent crime | 1,242,777 | 1,127,691 | **-9.26%** | -9.3% |
| Murder | 17,363 | 14,308 | **-17.6%** | -18.1% |
| Robbery | 209,700 | 170,493 | **-18.70%** | -18.5% |
| Aggravated assault | 881,425 | 817,458 | **-7.26%** | -7.2% |
| Property crime | 6,035,622 | 5,302,985 | **-12.14%** | -12.4% |

**2025 murder rate**: 14,308 estimated homicides / 341,784,857 average 2025 population × 100,000 = **4.19 per 100,000**, close to the claimed 4.1.

## What is NOT checkable, and why

The claims that the 9.3% violent-crime decline is "the largest year-to-year decline since FBI estimations began in 1936" and that the murder rate is "tied with 1955 and 1956 for the lowest rate since FBI estimations began" **cannot be verified from this corpus**. `crime.cde_offenses` and every other crime table here have a coverage window of 2010–2026 (both declared and observed) — no table reaches back to 1936, 1955, or 1956. This is a genuine data-availability gap, not evidence the claim is wrong.

## Method

- Table: `crime.cde_offenses` — one row per state × offense × month, with `offense_rate` (per 100k, per-month) and `population`. There is no single "national" row, so I built a national estimate by summing `offense_rate * population / 100000` across all 51 `state_abbr` values and all 12 months of each calendar year.
- Coverage check first: both years have all 51 jurisdictions × 12 months present, average `population_coverage_pct` 96.5% (2024) / 96.2% (2025), stable month-to-month (95–97% range) in both years — no anomalous low-coverage month distorting either total.
- SQL used, e.g.: `SELECT "year", offense_code, SUM(offense_rate*population/100000.0) AS est_count FROM crime.cde_offenses WHERE offense_code IN ('violent-crime','homicide','robbery','aggravated-assault','property-crime') AND "year" IN ('2024','2025') GROUP BY "year", offense_code`

A separate table, `crime.cde_trends`, carries a single live rolling-12-month snapshot (currently "June 2025 – May 2026", refreshed Sept 15, 2026) of similar-looking FBI trend percentages (Violent Crime -9.0%, Murder -18.7%, Aggravated Assault -6.2%, Property Crime -12.4%). I checked it but did **not** use it as evidence: (1) its `trend_pct` column was flagged by this session's own query diagnostics as a high-severity "broken_field" issue (values out-of-domain for the column), and (2) it's a rolling trailing-12-month window that keeps moving forward monthly, not a fixed calendar-year 2024-vs-2025 comparison — a different measurement than the article's claim. Every figure in this verdict comes solely from the independently-aggregated `crime.cde_offenses` calendar-year computation.

## Why the small residual gaps (0.05–0.5 points)

Plausibly a methodology difference, not a data problem: the FBI's official "First Look" release uses its own national estimation procedure across ~17,000+ agencies; this corpus's `cde_offenses` table separately estimates each state's rate, and I summed state-rate×population products — a slightly different aggregation path that won't reproduce the FBI's number to the decimal point even from the same underlying submissions. Direction and rough magnitude agree in every category.

## Verdict rating: 0 Pinocchios

Every checkable figure in the article's central claim is confirmed within a fraction of a percentage point. The two historical superlatives (since-1936, tied-with-1955/56) are outside this corpus's 2010–2026 window and are graded "not checkable here" rather than false.

## Report

Published report (local link, dies when this session ends): http://127.0.0.1:51087/a/57a5e56fa044723e3a402c480e4711b7.html
Saved to: `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q2/askamerica/2026-09-26/report.html`

Source article checked: https://www.cbsnews.com/news/fbi-crime-data-violent-crime-murder-white-house/
