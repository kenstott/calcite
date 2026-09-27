## Verdict: Not checkable — from this corpus or from NIFC's own current public archive

**Claim under test:** Through August 2026, the U.S. has recorded 52,253 total wildfires burning 8,238,284 acres, with 9 deaths reported; as of April 10, 2026, YTD acreage stood at 1,707,778 acres, 231% of the ten-year average.

### What I checked

1. **`disasters.wildfire_perimeters`** — the corpus's only NIFC-sourced wildfire table (built from NIFC's WFIGS Interagency Perimeters feed, with `fire_year`, `gis_acres`/`final_acres`, cause, ownership). Query:
```sql
SELECT fire_year, COUNT(*) AS fire_count, SUM(COALESCE(final_acres, gis_acres)) AS total_acres
FROM disasters.wildfire_perimeters
WHERE fire_year BETWEEN 2015 AND 2026
GROUP BY fire_year ORDER BY fire_year;
```
Result: 2018 (1 row), 2019 (2), 2020 (2,805 fires / 1,410,366 acres), 2021 (3,461 / 730,189), 2022 (3,729 / 1,424,127), 2023 (**2 fires / 783 acres**). A follow-up `SELECT MAX(fire_year), MIN(fire_year), COUNT(*) FROM disasters.wildfire_perimeters` confirms **max fire_year = 2023**, 10,000 total rows. **There are zero rows for 2024, 2025, or 2026.** This table is a stale, partially-refreshed snapshot, not a live NIFC feed — it cannot produce a 2026 fire count, acreage, or a usable 10-year baseline.

2. **`disasters.storm_events`** filtered to `event_type = 'Wildfire'` does reach 2026 (223 episodes, 2 deaths recorded so far), but this is NOAA's local Storm Events Database — a structurally different, much smaller-scale measure (individual local storm reports, no acreage column) that cannot substitute for NIFC's incident-level fire/acreage tally the claim cites.

3. **NIFC's own live statistics page**, fetched directly (`https://www.nifc.gov/fire-information/statistics/wildfires`), per this corpus's own research-first practice of going to the primary source when the warehouse has a confirmed gap. It publishes one row per year, 1983–2025, and **has not yet posted a 2026 row** as of this check. Its five most recent years: 2025 (77,850 fires / 5,131,474 acres), 2024 (64,897 / 8,924,884), 2023 (56,580 / 2,693,910), 2022 (68,988 / 7,577,183), 2021 (58,985 / 7,125,643). The 2016–2025 average is **63,872 fires/year and 7,054,337 acres/year** — a genuine annual baseline, but not the date-matched, within-year cumulative ("year-to-date through April 10") series the claim's 231%-of-ten-year-average figure requires, and this page carries no such series.

### Conclusion

Neither this corpus nor NIFC's own public archive can confirm or refute the claim's specific 2026 figures. This is a **coverage gap** (our table is stuck at 2023; NIFC's own year-end table hasn't posted 2026 yet and doesn't carry a YTD-by-date series at all) — not a contradiction of the claim. The claim's overall magnitude (tens of thousands of fires, single-digit millions of acres) is well within NIFC's historical 1983–2025 range, so it is not implausible, but "plausible in magnitude" is not the same as verified. Actually confirming these numbers would require NIFC/NICC's current-year situation reports, which were not reachable in this session.

**Pinocchios: 0** — no contradiction was found; the gap is on the data-availability side (both this corpus and, at the time of the check, NIFC's own public annual archive), not evidence the article's numbers are wrong.

### Tables/queries cited
- `disasters.wildfire_perimeters` (schema `disasters`) — SQL above
- `disasters.storm_events` — `SELECT event_type, "year", COUNT(*), SUM(deaths_direct+deaths_indirect) FROM disasters.storm_events WHERE event_type ILIKE '%wildfire%' AND "year">=2020 GROUP BY event_type,"year"`
- NIFC "Wildfires and Acres" page (nifc.gov/fire-information/statistics/wildfires), fetched live 2026-09-26

### Report
Published report (full narrative, dashboard, claim table, sources): saved to `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q7/askamerica/2026-09-26/report.html` via `publish_report`. Local link (dies with this session): `http://127.0.0.1:49511/a/9d78c7300dc006726883ca5805bbd0f6.html`
