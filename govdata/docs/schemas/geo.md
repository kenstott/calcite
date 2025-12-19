# GEO Schema Documentation

## Overview

The GEO schema provides geographic boundary data and spatial relationships from the U.S. Census Bureau's TIGER/Line database and HUD's USPS ZIP Code Crosswalk files. This schema serves as the geographic foundation for cross-domain analysis, enabling location-based joins with SEC, ECON, and CENSUS data.

## Architecture Note: FileSchema Delegation

The GEO schema operates as a **declarative data pipeline** that:
1. **Downloads** geographic boundary files from Census and HUD
2. **Transforms** shapefiles and crosswalks to optimized Parquet format
3. **Enriches** tables with metadata, comments, and internal/external foreign keys
4. **Delegates** actual query execution to the FileSchema adapter

This means you get the benefits of:
- **Automatic boundary updates** - Latest TIGER/Line and HUD data
- **Spatial operations** - Geometry columns for geographic analysis
- **Cross-domain foundation** - Links SEC companies and ECON regions to geography
- **Format flexibility** - Handles shapefiles, DBF, and CSV transparently

## Tables

### State and Territory Tables

#### `states`
State boundaries and metadata - the primary geographic reference table.

Primary key: `state_fips`
Unique keys: `state_code`, `state_name`

| Column | Type | Description |
|--------|------|-------------|
| state_fips | VARCHAR | 2-digit FIPS code (e.g., "06" for California) |
| state_code | VARCHAR | Geographic identifier (GEOID) for the state |
| state_name | VARCHAR | Full state name (e.g., "California") |
| state_abbr | VARCHAR | 2-letter postal abbreviation (e.g., "CA") |
| land_area | DOUBLE | Land area in square meters |
| water_area | DOUBLE | Water area in square meters |
| geometry | VARCHAR | WKT representation of state boundary polygon |

**Key Role**: Bridges between FIPS codes (used internally in GEO) and state abbreviations (used by SEC/ECON).

### County and Local Area Tables

#### `counties`
County boundaries and metadata for all 3,000+ U.S. counties and county equivalents.

Primary key: `county_fips`
Foreign key: `state_fips` → `states.state_fips`

| Column | Type | Description |
|--------|------|-------------|
| county_fips | VARCHAR | 5-digit FIPS code (state + county, e.g., "06037" for Los Angeles County) |
| state_fips | VARCHAR | 2-digit state FIPS (FK → states.state_fips) |
| county_name | VARCHAR | Full county name (e.g., "Los Angeles County") |
| county_code | VARCHAR | Geographic identifier (GEOID) for the county |
| land_area | DOUBLE | Land area in square meters |
| water_area | DOUBLE | Water area in square meters |
| geometry | VARCHAR | WKT representation of county boundary polygon |

#### `places`
Census designated places including cities, towns, and villages.

Primary key: `place_fips`
Foreign key: `state_fips` → `states.state_fips`

| Column | Type | Description |
|--------|------|-------------|
| place_fips | VARCHAR | 7-digit FIPS code (state + place code, e.g., "0644000" for Los Angeles city) |
| state_fips | VARCHAR | 2-digit state FIPS (FK → states.state_fips) |
| place_name | VARCHAR | Name of incorporated place or census-designated place |
| place_type | VARCHAR | Classification of place (e.g., "city", "town", "CDP") |
| geometry | VARCHAR | WKT representation of place boundary polygon |

### Census Geographic Units

#### `census_tracts`
Census tract boundaries for detailed demographic analysis - small statistical subdivisions averaging 4,000 inhabitants.

Primary key: `tract_fips`
Foreign key: `county_fips` → `counties.county_fips`

| Column | Type | Description |
|--------|------|-------------|
| tract_fips | VARCHAR | 11-digit census tract FIPS code (state + county + tract) |
| state_fips | VARCHAR | 2-digit state FIPS code |
| county_fips | VARCHAR | 5-digit county FIPS (FK → counties.county_fips) |
| tract_name | VARCHAR | Census tract number (e.g., "4201.02") |
| land_area | DOUBLE | Land area in square meters |
| water_area | DOUBLE | Water area in square meters |
| geometry | VARCHAR | WKT representation of tract boundary polygon |

#### `block_groups`
Census block groups - smallest geography for which census sample data is published (600-3,000 people).

Primary key: `block_group_fips`
Foreign key: `tract_fips` → `census_tracts.tract_fips`

| Column | Type | Description |
|--------|------|-------------|
| block_group_fips | VARCHAR | 12-digit block group FIPS (state + county + tract + block group) |
| state_fips | VARCHAR | 2-digit state FIPS code |
| county_fips | VARCHAR | 5-digit county FIPS code |
| tract_fips | VARCHAR | 11-digit tract FIPS (FK → census_tracts.tract_fips) |
| land_area | DOUBLE | Land area in square meters |
| water_area | DOUBLE | Water area in square meters |
| geometry | VARCHAR | WKT representation of block group boundary |

#### `zctas`
ZIP Code Tabulation Areas - census approximation of USPS ZIP Code delivery areas.

Primary key: `zcta`

| Column | Type | Description |
|--------|------|-------------|
| zcta | VARCHAR | 5-digit ZCTA code approximating USPS ZIP Code |
| land_area | DOUBLE | Land area in square meters |
| water_area | DOUBLE | Water area in square meters |
| geometry | VARCHAR | WKT representation of ZCTA boundary polygon |

### Metropolitan Statistical Areas

#### `cbsa`
Core Based Statistical Areas (metropolitan and micropolitan areas).

Primary key: `cbsa_fips`
Unique key: `cbsa_name`

| Column | Type | Description |
|--------|------|-------------|
| cbsa_fips | VARCHAR | 5-digit CBSA code |
| cbsa_name | VARCHAR | Metropolitan area name |
| metro_micro | VARCHAR | LSAD code indicating "Metropolitan" or "Micropolitan" |
| land_area | DOUBLE | Land area in square meters |
| water_area | DOUBLE | Water area in square meters |
| geometry | VARCHAR | WKT representation of CBSA boundary polygon |

### Congressional and School Districts

#### `congressional_districts`
U.S. Congressional district boundaries for the House of Representatives.

Primary key: `cd_fips`
Foreign key: `state_fips` → `states.state_fips`

| Column | Type | Description |
|--------|------|-------------|
| cd_fips | VARCHAR | 4-digit congressional district code (state + district number) |
| state_fips | VARCHAR | 2-digit state FIPS (FK → states.state_fips) |
| cd_name | VARCHAR | Congressional district name or number |
| land_area | DOUBLE | Land area in square meters |
| water_area | DOUBLE | Water area in square meters |
| geometry | VARCHAR | WKT representation of district boundary polygon |

#### `school_districts`
Elementary, secondary, and unified school district boundaries.

Primary key: `sd_lea`
Foreign key: `state_fips` → `states.state_fips`

| Column | Type | Description |
|--------|------|-------------|
| sd_lea | VARCHAR | Local Education Agency (LEA) code |
| state_fips | VARCHAR | 2-digit state FIPS (FK → states.state_fips) |
| sd_name | VARCHAR | School district name |
| sd_type | VARCHAR | District type (elementary, secondary, or unified) |
| land_area | DOUBLE | Land area in square meters |
| water_area | DOUBLE | Water area in square meters |
| geometry | VARCHAR | WKT representation of district boundary polygon |

### Demographic Summary Tables

#### `population_demographics`
Population counts and demographic characteristics by geography from Census/ACS.

Primary key: `(geo_id, year)`
Foreign key: `(geo_id, year)` → `census.acs_population.(geoid, year)`

| Column | Type | Description |
|--------|------|-------------|
| geo_id | VARCHAR | Census geographic identifier (state or county FIPS) |
| year | INTEGER | Census survey year |
| total_population | INTEGER | Total population count |
| male_population | INTEGER | Male population count |
| female_population | INTEGER | Female population count |
| state_fips | VARCHAR | 2-digit state FIPS code |
| county_fips | VARCHAR | 5-digit county FIPS code (nullable) |

#### `housing_characteristics`
Housing unit counts, occupancy, tenure, and structural characteristics.

Primary key: `(geo_id, year)`
Foreign key: `(geo_id, year)` → `census.acs_housing.(geoid, year)`

| Column | Type | Description |
|--------|------|-------------|
| geo_id | VARCHAR | Census geographic identifier |
| year | INTEGER | Census survey year |
| total_housing_units | INTEGER | Total number of housing units |
| occupied_units | INTEGER | Number of occupied housing units |
| vacant_units | INTEGER | Number of vacant housing units |
| median_home_value | DOUBLE | Median home value in dollars |
| state_fips | VARCHAR | 2-digit state FIPS code |
| county_fips | VARCHAR | 5-digit county FIPS code (nullable) |

#### `economic_indicators`
Income, poverty, employment, and economic characteristics by geography.

Primary key: `(geo_id, year)`
Foreign key: `(geo_id, year)` → `census.acs_income.(geoid, year)`

| Column | Type | Description |
|--------|------|-------------|
| geo_id | VARCHAR | Census geographic identifier |
| year | INTEGER | Census survey year |
| median_household_income | DOUBLE | Median household income in dollars |
| per_capita_income | DOUBLE | Per capita income in dollars |
| labor_force | INTEGER | Total civilian labor force count |
| employed | INTEGER | Number of employed persons |
| unemployed | INTEGER | Number of unemployed persons |
| state_fips | VARCHAR | 2-digit state FIPS code |
| county_fips | VARCHAR | 5-digit county FIPS code (nullable) |

### ZIP Code Crosswalk Tables (HUD USPS)

#### `zip_county_crosswalk`
ZIP code to county mapping with allocation ratios.

Primary key: `(zip, county_fips)`
Foreign key: `county_fips` → `counties.county_fips`

| Column | Type | Description |
|--------|------|-------------|
| zip | VARCHAR | 5-digit ZIP code |
| county_fips | VARCHAR | 5-digit county FIPS (FK → counties.county_fips) |
| res_ratio | DOUBLE | Ratio of residential addresses in ZIP that are in this county |
| bus_ratio | DOUBLE | Ratio of business addresses in ZIP that are in this county |
| oth_ratio | DOUBLE | Ratio of other addresses in ZIP that are in this county |
| tot_ratio | DOUBLE | Ratio of total addresses in ZIP that are in this county |

#### `zip_cbsa_crosswalk`
ZIP code to metropolitan area mapping.

Primary key: `(zip, cbsa_code)`
Foreign key: `cbsa_code` → `cbsa.cbsa_fips`

| Column | Type | Description |
|--------|------|-------------|
| zip | VARCHAR | 5-digit ZIP code |
| cbsa_code | VARCHAR | 5-digit CBSA code |
| res_ratio | DOUBLE | Ratio of residential addresses in ZIP that are in this CBSA |
| bus_ratio | DOUBLE | Ratio of business addresses in ZIP that are in this CBSA |
| oth_ratio | DOUBLE | Ratio of other addresses in ZIP that are in this CBSA |
| tot_ratio | DOUBLE | Ratio of total addresses in ZIP that are in this CBSA |

#### `tract_zip_crosswalk`
Census tract to ZIP code mapping.

Primary key: `(tract_fips, zip)`
Foreign key: `tract_fips` → `census_tracts.tract_fips`

| Column | Type | Description |
|--------|------|-------------|
| tract_fips | VARCHAR | 11-digit census tract FIPS code |
| zip | VARCHAR | 5-digit ZIP code |
| res_ratio | DOUBLE | Ratio of residential addresses in tract that are in this ZIP |
| bus_ratio | DOUBLE | Ratio of business addresses in tract that are in this ZIP |
| oth_ratio | DOUBLE | Ratio of other addresses in tract that are in this ZIP |
| tot_ratio | DOUBLE | Ratio of total addresses in tract that are in this ZIP |

## Foreign Key Relationships

### Geographic Hierarchy (Internal)
1. `counties.state_fips` → `states.state_fips`
2. `places.state_fips` → `states.state_fips`
3. `census_tracts.county_fips` → `counties.county_fips`
4. `block_groups.tract_fips` → `census_tracts.tract_fips`
5. `congressional_districts.state_fips` → `states.state_fips`
6. `school_districts.state_fips` → `states.state_fips`
7. `zip_county_crosswalk.county_fips` → `counties.county_fips`
8. `zip_cbsa_crosswalk.cbsa_code` → `cbsa.cbsa_fips`
9. `tract_zip_crosswalk.tract_fips` → `census_tracts.tract_fips`

### Cross-Schema Relationships (To CENSUS)
10. `population_demographics.(geo_id, year)` → `census.acs_population.(geoid, year)`
11. `housing_characteristics.(geo_id, year)` → `census.acs_housing.(geoid, year)`
12. `economic_indicators.(geo_id, year)` → `census.acs_income.(geoid, year)`

### Cross-Domain Relationships (Incoming from other schemas)
- SEC: `filing_metadata.state_of_incorporation` → `states.state_abbr`
- ECON: `regional_employment.state_fips` → `states.state_fips`
- ECON: `regional_income.geo_fips` → `states.state_fips`
- ECON: `state_industry.state_fips` → `states.state_fips`
- ECON: `state_wages.state_fips` → `states.state_fips`
- ECON: `county_qcew.area_fips` → `counties.county_fips`

### Complete Reference
For a comprehensive view of all relationships including the complete ERD diagram, cross-schema query examples, and detailed FK implementation status, see the **[Schema Relationships Guide](relationships.md)**.

## Common Query Patterns

### Geographic Aggregation
```sql
-- Companies by state with geographic context
SELECT
    s.state_name,
    s.state_abbr,
    s.land_area / 1000000 as land_area_sq_km,
    COUNT(DISTINCT f.cik) as company_count
FROM geo.states s
LEFT JOIN sec.filing_metadata f ON s.state_abbr = f.state_of_incorporation
GROUP BY s.state_name, s.state_abbr, s.land_area
ORDER BY company_count DESC;
```

### ZIP Code Analysis
```sql
-- Metropolitan area ZIP code coverage
SELECT
    c.cbsa_name,
    c.metro_micro,
    COUNT(DISTINCT z.zip) as zip_codes,
    SUM(z.res_ratio) as residential_weight
FROM geo.zip_cbsa_crosswalk z
JOIN geo.cbsa c ON z.cbsa_code = c.cbsa_fips
GROUP BY c.cbsa_name, c.metro_micro
ORDER BY residential_weight DESC;
```

### County Queries
```sql
-- Find all counties in a state
SELECT
    c.county_name,
    c.county_fips,
    c.land_area / 1000000 as area_sq_km
FROM geo.counties c
JOIN geo.states s ON c.state_fips = s.state_fips
WHERE s.state_abbr = 'CA'
ORDER BY c.county_name;
```

### Cross-Domain Geographic Analysis
```sql
-- Economic indicators by company incorporation state
SELECT
    s.state_name,
    COUNT(DISTINCT f.cik) as companies,
    AVG(e.median_household_income) as avg_income,
    SUM(e.labor_force) as total_labor_force
FROM geo.states s
LEFT JOIN sec.filing_metadata f ON s.state_abbr = f.state_of_incorporation
LEFT JOIN geo.economic_indicators e ON s.state_fips = e.state_fips
WHERE e.year = 2023
GROUP BY s.state_name
ORDER BY companies DESC;
```

## Data Sources

- **Census TIGER/Line**: Annual boundary files and geographic relationships
- **HUD USPS Crosswalk**: Quarterly ZIP code to geographic unit mappings
- **Census ACS**: American Community Survey demographic estimates

## Configuration Options

### Model Configuration
```json
{
  "name": "geo",
  "type": "custom",
  "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
  "operand": {
    "dataSource": "geo",
    "cacheDirectory": "/path/to/geo-cache",
    "autoDownload": true,
    "tigerYear": 2024,
    "includeDemographics": true,
    "spatialIndexing": true
  }
}
```

## Update Schedule

| Dataset | Update Frequency | Typical Release |
|---------|-----------------|-----------------|
| TIGER/Line | Annual | December |
| HUD Crosswalk | Quarterly | End of quarter + 45 days |
| Census Demographics | Annual | September |

## Special Considerations

### State Identifier Formats
- **state_fips**: 2-digit FIPS codes (06, 48) used internally in GEO schema
- **state_abbr**: 2-letter codes (CA, TX) used by SEC and some ECON tables
- The `states` table provides the mapping between both formats

### ZIP Code Limitations
- ZIP codes don't align with census boundaries
- Use ZCTAs for census data approximations
- HUD crosswalk provides allocation ratios for splitting ZIP data across geographies

### Spatial Operations
- Geometry columns contain WKT (Well-Known Text) representations
- Coordinate system: WGS84 (EPSG:4326)
- Use DuckDB spatial extension for ST_* functions when enabled
