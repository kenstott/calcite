# Weather Schema Documentation

## Overview

The `weather` schema provides access to U.S. weather and atmospheric data from NOAA, EPA, and
USDA. It covers NWS observation stations and active alerts, NOAA CDO station inventories and
monthly/annual climate summaries, EPA air quality index data, GHCND daily observations with
county linkage, drought monitor weekly assessments, HMS satellite smoke polygons and daily
smoke coverage, and 30-year climate normals.

The schema is served by `WeatherSchemaFactory` via `GovDataSchemaFactory` and is driven by
`weather-schema.yaml`.

---

## Tables

| Table | Description | Primary source | Cadence |
|---|---|---|---|
| `nws_stations` | NWS observation station inventory: ID, name, lat/lon, elevation, timezone, forecast zone, county zone | NWS API `/stations?state={state}` | Annual |
| `nws_alerts` | Active NWS weather alerts: event type, severity, certainty, urgency, headline, description, onset/expiry, affected zones | NWS API `/alerts?area={state}` | Daily |
| `cdo_stations` | NOAA CDO station inventory: ID, name, lat/lon, elevation, data coverage fraction, min/max data date | NOAA CDO API v2 `/stations` | Annual |
| `cdo_monthly_summaries` | NOAA CDO monthly climate summaries (GSOM dataset): station, month, datatype (TAVG/TMAX/TMIN/PRCP/SNOW), value, quality flags | NOAA CDO API v2 `/data?datasetid=GSOM` | Annual |
| `cdo_annual_summaries` | NOAA CDO annual climate summaries (GSOY dataset): station, datatype, annual aggregate values | NOAA CDO API v2 `/data?datasetid=GSOY` | Annual |
| `epa_annual_aqi` | EPA annual air quality index summary by county: median AQI, max AQI, days with good/moderate/unhealthy/hazardous AQI, days by pollutant | EPA AQS annual AQI bulk CSV | Annual |
| `epa_daily_aqi` | EPA daily air quality index by county: AQI value, defining pollutant, AQI category | EPA AQS daily AQI bulk CSV | Daily |
| `ghcnd_stations_with_county` | GHCND station inventory enriched with county FIPS via spatial join | NOAA GHCND station list + geo crosswalk | Annual |
| `ghcnd_daily` | GHCND daily climate observations: TMAX, TMIN, PRCP, SNOW, SNWD, and additional elements per station | NOAA GHCND bulk download | Daily |
| `drought_monitor_weekly` | USDA/NDMC weekly drought monitor: county-level drought classification (D0-D4) percentages | USDA Drought Monitor weekly CSV | Weekly |
| `hms_smoke_daily` | NOAA HMS daily satellite-derived smoke coverage: county-level light/medium/heavy smoke-day flags | NOAA HMS smoke product | Daily |
| `hms_smoke_polygons` | NOAA HMS smoke polygon geometries: centroid lat/lon, area, density category, satellite source | NOAA HMS smoke shapefiles | Daily |
| `climate_normals_monthly` | NOAA 1991–2020 30-year climate normals by station and month: mean TMAX/TMIN/PRCP/SNOW and standard deviations | NOAA Climate Normals bulk | Static |
| `weather_daily_by_county` | GHCND daily observations aggregated to county level: county-median TMAX/TMIN/PRCP/SNOW | Derived from `ghcnd_daily` + `ghcnd_stations_with_county` | Daily |

---

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `GOVDATA_PARQUET_DIR` | Root Parquet directory |
| `NOAA_CDO_TOKEN` | NOAA Climate Data Online API token (register at ncdc.noaa.gov/cdo-web/token) |

---

## Known DQ Warnings

- `ghcnd_daily.year` and `hms_smoke_daily.year` columns are partition keys — `all_same_value` DQ check fires as expected for single-year partition writes.

---

## Sample Queries

```sql
-- States with most days of unhealthy AQI (latest year)
SELECT state_name, county_name,
       days_unhealthy + days_unhealthy_sensitive + days_very_unhealthy + days_hazardous AS bad_air_days
FROM weather.epa_annual_aqi
WHERE year = (SELECT MAX(year) FROM weather.epa_annual_aqi)
ORDER BY bad_air_days DESC
LIMIT 20;

-- Weekly drought severity trend for a state (latest 12 weeks)
SELECT week_date, state_abbr,
       AVG(d4_extreme) AS avg_pct_exceptional_drought
FROM weather.drought_monitor_weekly
WHERE state_abbr = 'TX'
ORDER BY week_date DESC
LIMIT 12;

-- County-level temperature trend (annual average max temp)
SELECT year, county_fips,
       AVG(tmax_avg) AS avg_daily_tmax_c
FROM weather.weather_daily_by_county
WHERE county_fips LIKE '06%'  -- California
GROUP BY year, county_fips
ORDER BY year DESC, county_fips;

-- 30-year normal vs recent actual temperature comparison
SELECT d.station_id, d.year,
       AVG(d.tmax) AS recent_avg_tmax,
       n.tmax_normal,
       AVG(d.tmax) - n.tmax_normal AS anomaly_c
FROM weather.ghcnd_daily d
JOIN weather.climate_normals_monthly n
  ON d.station_id = n.station_id AND d.month = n.month
WHERE d.year >= 2020
GROUP BY d.station_id, d.year, n.tmax_normal
ORDER BY anomaly_c DESC
LIMIT 20;
```
