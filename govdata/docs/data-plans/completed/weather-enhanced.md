# Weather Schema Expansion Plan (`weather-schema.yaml`)

## Strategic Context

The existing weather schema (6 tables: NWS stations/alerts, NOAA CDO monthly/annual summaries,
EPA annual AQI) was designed for weather research. This expansion targets **cross-correlated
research** — making weather a joinable dimension across SEC, economic, agricultural, health,
education, and geographic schemas.

The central problem: current tables are station-level with no county-level aggregation, and
monthly/annual granularity prevents correlation with daily economic events. These gaps are
addressed in phases ordered by cross-schema research value.

---

## Current Tables (Baseline)

| Table | Source | Granularity | Auth |
|---|---|---|---|
| `nws_stations` | weather.gov API | static | none |
| `nws_alerts` | weather.gov API | snapshot | none |
| `cdo_stations` | NOAA CDO API | static | token |
| `cdo_monthly_summaries` | NOAA CDO API | monthly/station | token |
| `cdo_annual_summaries` | NOAA CDO API | annual/station | token |
| `epa_annual_aqi` | EPA AQS API | annual/county | key |

---

## Phase 1 — County Bridge (Prerequisite for All Cross-Schema Joins)

**Goal:** Make weather joinable to every other schema on `county_fips`. Without this,
weather data cannot join to SEC filings, BLS wages, IPEDS institutions, FEC contributions,
or census demographics. This is the highest-leverage single addition.

### New Table: `ghcnd_stations_with_county`

Station metadata enriched with `county_fips` via reverse geocoding from FIPS gazetteer.
One row per station. Enables all station-to-county joins downstream.

**Source:** NOAA GHCN-Daily station inventory (ghcnd-stations.txt) + Census FIPS gazetteer
**Auth:** None
**Update:** Annual (station list is stable)

| Column | Type | Description |
|---|---|---|
| station_id | VARCHAR | GHCND station ID (e.g., `USW00094846`) |
| station_name | VARCHAR | Station name |
| state_fips | VARCHAR | 2-digit state FIPS |
| county_fips | VARCHAR | 5-digit county FIPS (nearest county centroid) |
| latitude | DOUBLE | Station latitude |
| longitude | DOUBLE | Station longitude |
| elevation_m | DOUBLE | Elevation in meters |
| data_start_year | INTEGER | First year of observations |
| data_end_year | INTEGER | Last year of observations |
| distance_to_county_centroid_km | DOUBLE | Distance used for nearest-county assignment |

**Implementation note:** Use Census TIGER county centroid file for nearest-county assignment
(Euclidean distance in lat/lon is sufficient at county granularity). No spatial library needed.

---

## Phase 2 — NOAA GHCN-Daily (Daily Station Observations)

**Goal:** Daily temperature, precipitation, and snow at station level. Foundational for
correlating weather with daily economic events: earnings releases, SEC 8-K filings,
commodity price moves, retail sales, traffic incidents.

### New Table: `ghcnd_daily`

**Source:** NOAA NCEI GHCN-Daily bulk CSV — `https://www.ncei.noaa.gov/pub/data/ghcn/daily/`
**Auth:** None (anonymous FTP/HTTP)
**Partitioned by:** `year`, `state_fips`
**Primary key:** `(station_id, date)`
**Volume:** ~40M rows/year for US stations; load 2000–present

| Column | Type | Description |
|---|---|---|
| station_id | VARCHAR | GHCND station ID |
| state_fips | VARCHAR | From `ghcnd_stations_with_county` |
| county_fips | VARCHAR | From `ghcnd_stations_with_county` |
| date | DATE | Observation date |
| tmax_c | DOUBLE | Max temperature (°C); NULL if missing |
| tmin_c | DOUBLE | Min temperature (°C); NULL if missing |
| tavg_c | DOUBLE | Average temperature (°C); NULL or derived |
| prcp_mm | DOUBLE | Precipitation (mm) |
| snow_mm | DOUBLE | Snowfall (mm) |
| snwd_mm | DOUBLE | Snow depth (mm) |
| awnd_ms | DOUBLE | Average wind speed (m/s) |
| tmax_flag | VARCHAR | Quality flag for tmax (blank = passed all checks) |
| tmin_flag | VARCHAR | Quality flag for tmin |
| prcp_flag | VARCHAR | Quality flag for prcp |

**Implementation notes:**
- GHCN-Daily distributes one file per station (`.dly` fixed-width format). Use the
  `by_year/` subset which reorganizes by year — one CSV per year, far easier to ingest.
- Filter to US stations only (station IDs starting with `US`).
- Quality flags: only load rows where flag is blank or `Z` (removed flagged observations).
- Partition by `year` + `state_fips` for efficient temporal slicing.

### New Derived Table: `weather_daily_by_county`

**Goal:** Pre-aggregate GHCN-Daily to county level so downstream joins are simple
`county_fips` equi-joins rather than spatial operations.

Aggregation: average of all stations in county weighted by inverse distance to county
centroid, or simple mean when distance data is not available.

**Source:** Derived from `ghcnd_daily` JOIN `ghcnd_stations_with_county`
**Partitioned by:** `year`, `county_fips`
**Primary key:** `(county_fips, date)`

| Column | Type | Description |
|---|---|---|
| county_fips | VARCHAR | 5-digit county FIPS |
| date | DATE | Observation date |
| year | INTEGER | Partition column |
| tmax_c | DOUBLE | County-average daily max temperature |
| tmin_c | DOUBLE | County-average daily min temperature |
| tavg_c | DOUBLE | County-average daily mean temperature |
| prcp_mm | DOUBLE | County-average precipitation |
| snow_mm | DOUBLE | County-average snowfall |
| station_count | INTEGER | Number of stations contributing |
| coverage_pct | DOUBLE | Fraction of county area within 50km of a reporting station |

**Cross-schema research enabled:**
```sql
-- SEC filings on days with extreme weather in company's home county
SELECT fm.company_name, fm.filing_type, fm.filed_date,
       w.tmax_c, w.prcp_mm
FROM sec.filing_metadata fm
JOIN ipeds_institutions i ON ...   -- or use company HQ county from another table
JOIN weather.weather_daily_by_county w
  ON w.county_fips = i.county_fips AND w.date = fm.filed_date
WHERE w.tmax_c > 38 OR w.prcp_mm > 50;

-- BLS employment changes correlated with monthly precipitation anomalies
SELECT b.area_fips, b.year, b.month, b.avg_weekly_wage,
       AVG(w.prcp_mm) AS monthly_prcp
FROM econ.county_wages b
JOIN weather.weather_daily_by_county w
  ON w.county_fips = b.area_fips
  AND YEAR(w.date) = b.year AND MONTH(w.date) = b.month
GROUP BY b.area_fips, b.year, b.month, b.avg_weekly_wage;
```

---

## Phase 3 — NOAA Storm Events

> **Relocated:** `storm_events` now lives in the **`disasters`** schema alongside FEMA
> declarations and NIFC wildfire perimeters. See [disasters data plan](disasters.md) for
> the full table definition, column list, and worker assignment.
>
> Cross-schema joins to `weather.weather_daily_by_county` still work — use
> `disasters.storm_events` as the join source.

---

## Phase 4 — USDA/NOAA Drought Monitor

**Goal:** Weekly county-level drought severity going back to 2000. Drought is a slow-moving
signal that correlates with agricultural yields, water utility revenue, wildfire ignition,
and migration patterns (census/ACS population flows).

### New Table: `drought_monitor_weekly`

**Source:** National Drought Monitor — `https://droughtmonitor.unl.edu/DmData/GISData.aspx`
Weekly shapefile or CSV export, available since 2000-01-04.
**Auth:** None
**Partitioned by:** `year`, `state_fips`
**Primary key:** `(county_fips, week_date)`

| Column | Type | Description |
|---|---|---|
| county_fips | VARCHAR | 5-digit county FIPS |
| state_fips | VARCHAR | State FIPS (partition) |
| week_date | DATE | Tuesday of the week (drought monitor release day) |
| year | INTEGER | Year (partition) |
| none_pct | DOUBLE | % of county area with no drought (D-None) |
| d0_pct | DOUBLE | % Abnormally Dry |
| d1_pct | DOUBLE | % Moderate Drought |
| d2_pct | DOUBLE | % Severe Drought |
| d3_pct | DOUBLE | % Extreme Drought |
| d4_pct | DOUBLE | % Exceptional Drought |
| dsci | DOUBLE | Drought Severity and Coverage Index (0–500 composite) |

**Implementation note:** `dsci = 0.1×(d0+d1+d2+d3+d4) + 0.1×(d1+d2+d3+d4) + ...` — NOAA
formula; compute during ETL to avoid storing a redundant derived field in source form.

**Cross-schema research enabled:**
```sql
-- Water utility financial stress vs. drought severity
SELECT d.county_fips, d.year, AVG(d.dsci) AS avg_drought_index,
       f.operating_revenue, f.net_income
FROM weather.drought_monitor_weekly d
JOIN econ.utility_financials f           -- if added to econ schema
  ON f.county_fips = d.county_fips AND f.year = d.year
WHERE d.dsci > 200
GROUP BY d.county_fips, d.year, f.operating_revenue, f.net_income;
```

---

## Phase 5 — Wildfire and Smoke

> **Wildfire perimeters relocated:** `wildfire_perimeters` (NIFC WFIGS) now lives in the
> **`disasters`** schema. See [disasters data plan](disasters.md).
> The smoke table (`hms_smoke_daily`) remains in the `weather` schema — it is a continuous
> atmospheric observation, not a discrete incident record.

### New Table: `hms_smoke_daily`

Smoke extent from NOAA Hazard Mapping System satellite detection. Daily coverage by county.

**Source:** NOAA HMS Smoke Product — `https://www.ospo.noaa.gov/Products/land/hms.html`
Shapefile → county intersection. Available 2005–present.
**Auth:** None
**Partitioned by:** `year`, `state_fips`
**Primary key:** `(county_fips, date)`

| Column | Type | Description |
|---|---|---|
| county_fips | VARCHAR | County FIPS |
| state_fips | VARCHAR | State FIPS |
| date | DATE | Observation date |
| year | INTEGER | Year (partition) |
| smoke_coverage_pct | DOUBLE | % of county covered by any smoke |
| heavy_smoke_pct | DOUBLE | % of county covered by heavy smoke |
| medium_smoke_pct | DOUBLE | % of county covered by medium smoke |
| light_smoke_pct | DOUBLE | % of county covered by light smoke |

**Implementation note:** HMS data is gridded; county intersection requires spatial join
during ETL. Pre-compute county fractions offline; store as flat CSV before ingestion.

---

## Phase 6 — NOAA Climate Normals (Anomaly Baseline)

**Goal:** 30-year climate normals (1991–2020) by station. Without a baseline, raw daily
observations cannot identify anomalies. Adding normals enables "X standard deviations above
normal" queries critical for detecting extremes rather than just raw values.

### New Table: `climate_normals_monthly`

**Source:** NOAA NCEI Climate Normals 1991–2020 — bulk CSV
**Auth:** None
**Primary key:** `(station_id, month)`

| Column | Type | Description |
|---|---|---|
| station_id | VARCHAR | GHCND station ID |
| county_fips | VARCHAR | From `ghcnd_stations_with_county` |
| month | INTEGER | 1–12 |
| normal_tmax_c | DOUBLE | 30-year normal daily max temperature |
| normal_tmin_c | DOUBLE | 30-year normal daily min temperature |
| normal_tavg_c | DOUBLE | 30-year normal daily mean temperature |
| normal_prcp_mm | DOUBLE | 30-year normal daily precipitation |
| normal_snow_mm | DOUBLE | 30-year normal daily snowfall |
| tmax_stddev | DOUBLE | Standard deviation of daily tmax |
| tmin_stddev | DOUBLE | Standard deviation of daily tmin |
| prcp_stddev | DOUBLE | Standard deviation of daily prcp |

**Cross-schema research enabled:**
```sql
-- Temperature anomaly z-score for any county, any date
SELECT w.county_fips, w.date, w.tmax_c,
       n.normal_tmax_c,
       (w.tmax_c - n.normal_tmax_c) / NULLIF(n.tmax_stddev, 0) AS tmax_zscore
FROM weather.weather_daily_by_county w
JOIN weather.climate_normals_monthly n
  ON w.county_fips = n.county_fips AND MONTH(w.date) = n.month;
```

---

## Phase Summary

| Phase | Tables Added | Auth Needed | Cross-Schema Value |
|---|---|---|---|
| 1 — County Bridge | `ghcnd_stations_with_county` | none | **Unlocks all other phases** |
| 2 — GHCN-Daily | `ghcnd_daily`, `weather_daily_by_county` | none | SEC, BLS, FEC, census daily joins |
| 3 — Storm Events | *moved → `disasters` schema* | — | See [disasters plan](disasters.md) |
| 4 — Drought Monitor | `drought_monitor_weekly` | none | Agriculture, water, migration |
| 5 — Wildfire/Smoke | `hms_smoke_daily` (perimeters → `disasters`) | none | Health, air quality, insurance |
| 6 — Climate Normals | `climate_normals_monthly` | none | Anomaly detection baseline |

All sources are freely available without API keys or authentication. Total new tables in weather schema: 5.

---

## Join Architecture

```
geo.counties (county_fips)
    └── ghcnd_stations_with_county (county_fips)
            └── ghcnd_daily (station_id, date)
                    └── weather_daily_by_county (county_fips, date)  ← primary join anchor
                            ├── sec.filing_metadata (county_fips + filed_date)
                            ├── econ.county_wages (area_fips + year/month)
                            ├── edu.ccd_districts (county_fips + year)
                            ├── census.acs_* (county_fips + year)
                            └── health.* (county_fips + year)

geo.counties (county_fips)
    ├── drought_monitor_weekly (county_fips, week_date)
    └── hms_smoke_daily (county_fips, date)

    [cross-schema — disasters schema]
    ├── disasters.storm_events (county_fips, begin_date)
    └── disasters.wildfire_perimeters (county_fips, fire_year)

ghcnd_stations_with_county (county_fips)
    └── climate_normals_monthly (station_id, month)
```

---

## Implementation Notes

- **Phase 1 must precede all others.** The county bridge is the prerequisite join key.
- **GHCN-Daily volume.** The `by_year/` bulk files are ~1GB compressed per year for all
  global stations. Filter to `station_id LIKE 'US%'` to reduce to ~200MB/year.
- **Storm Events damage parsing.** NCEI encodes `1.5M` as the string `"1.50M"`. ETL must
  parse these to integer dollars: `K` × 1000, `M` × 1,000,000, `B` × 1,000,000,000.
- **Drought Monitor cadence.** Released every Tuesday. Partitioning by year is sufficient;
  week_date is the temporal key. Do not attempt to align to Monday calendar weeks.
- **HMS Smoke spatial join.** The hardest ETL in the set — requires offline shapefile
  intersection. Recommend computing county fraction coverage in Python (geopandas) and
  storing as CSV before YAML-driven ingestion.
- **Climate normals are static.** The 1991–2020 normals will not change until NOAA releases
  the next 30-year period (~2031). No incremental update logic needed.
- **Worker assignment.** Phase 1–2 can share a worker (station bridge is small, GHCN-Daily
  is bulk annual). Phase 3–4 can share a worker. Phase 5 requires separate worker due to
  HMS spatial preprocessing. Phase 6 is single-pass load.
