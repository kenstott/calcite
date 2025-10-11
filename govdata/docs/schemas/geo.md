# GEO Schema Documentation

## Overview

The GEO schema provides geographic boundary data and spatial relationships from the U.S. Census Bureau's TIGER/Line database and HUD's USPS ZIP Code Crosswalk files. This schema serves as the geographic foundation for cross-domain analysis, enabling location-based joins with SEC and ECON data.

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

#### `tiger_states`
State boundaries and metadata - the primary geographic reference table.

Primary key: `state_fips`
Unique key: `state_code`

| Column | Type | Description |
|--------|------|-------------|
| state_fips | VARCHAR | 2-digit FIPS code (e.g., "06" for California) |
| state_code | VARCHAR | 2-letter state code (e.g., "CA" for California) |
| state_name | VARCHAR | Full state name |
| boundary | GEOMETRY | State boundary polygon |
| land_area | DECIMAL | Land area in square meters |
| water_area | DECIMAL | Water area in square meters |

**Key Role**: Bridges between FIPS codes (used internally in GEO) and 2-letter codes (used by SEC/ECON).

### County and Local Area Tables

#### `tiger_counties`
County boundaries and metadata.

Primary key: `county_fips`

| Column | Type | Description |
|--------|------|-------------|
| county_fips | VARCHAR | 5-digit FIPS code (state + county) |
| state_fips | VARCHAR | 2-digit state FIPS (FK → tiger_states.state_fips) |
| county_name | VARCHAR | County name |
| boundary | GEOMETRY | County boundary polygon |
| land_area | DECIMAL | Land area in square meters |
| water_area | DECIMAL | Water area in square meters |

#### `census_places`
Cities, towns, and census-designated places.

Primary key: `(place_code, state_code)`

| Column | Type | Description |
|--------|------|-------------|
| place_code | VARCHAR | Census place code |
| state_code | VARCHAR | 2-letter state code (FK → tiger_states.state_code) |
| place_name | VARCHAR | Place name |
| population | INTEGER | Population count |
| median_income | DECIMAL | Median household income |
| boundary | GEOMETRY | Place boundary polygon |

### Census Geographic Units

#### `tiger_census_tracts`
Census tract boundaries for detailed demographic analysis.

Primary key: `tract_code`

| Column | Type | Description |
|--------|------|-------------|
| tract_code | VARCHAR | 11-digit census tract code |
| county_fips | VARCHAR | County FIPS (FK → tiger_counties.county_fips) |
| tract_name | VARCHAR | Tract identifier |
| boundary | GEOMETRY | Tract boundary polygon |
| population | INTEGER | Population count |
| housing_units | INTEGER | Number of housing units |

#### `tiger_block_groups`
Census block groups - subdivision of tracts.

Primary key: `block_group_code`

| Column | Type | Description |
|--------|------|-------------|
| block_group_code | VARCHAR | Block group identifier |
| tract_code | VARCHAR | Census tract (FK → tiger_census_tracts.tract_code) |
| block_group_num | VARCHAR | Block group number |
| boundary | GEOMETRY | Block group boundary |
| population | INTEGER | Population count |

### Metropolitan Statistical Areas

#### `tiger_cbsa`
Core Based Statistical Areas (metropolitan and micropolitan areas).

Primary key: `cbsa_code`

| Column | Type | Description |
|--------|------|-------------|
| cbsa_code | VARCHAR | 5-digit CBSA code |
| cbsa_title | VARCHAR | Metropolitan area name |
| metro_micro | VARCHAR | "Metropolitan" or "Micropolitan" |
| boundary | GEOMETRY | CBSA boundary polygon |
| population | INTEGER | Total population |

#### `tiger_csa`
Combined Statistical Areas (groups of CBSAs).

Primary key: `csa_code`

| Column | Type | Description |
|--------|------|-------------|
| csa_code | VARCHAR | 3-digit CSA code |
| csa_title | VARCHAR | Combined area name |
| boundary | GEOMETRY | CSA boundary polygon |

### ZIP Code Tables (HUD USPS Crosswalk)

#### `hud_zip_county`
ZIP code to county mapping with allocation ratios.

Primary key: `zip`

| Column | Type | Description |
|--------|------|-------------|
| zip | VARCHAR | 5-digit ZIP code |
| state_fips | VARCHAR | State FIPS (FK → tiger_states.state_fips) |
| county_fips | VARCHAR | County FIPS (FK → tiger_counties.county_fips) |
| res_ratio | DECIMAL | Residential address ratio |
| bus_ratio | DECIMAL | Business address ratio |
| oth_ratio | DECIMAL | Other address ratio |
| tot_ratio | DECIMAL | Total address ratio |

#### `hud_zip_tract`
ZIP code to census tract mapping.

Primary key: `(zip, tract)`

| Column | Type | Description |
|--------|------|-------------|
| zip | VARCHAR | 5-digit ZIP code |
| state_fips | VARCHAR | State FIPS (FK → tiger_states.state_fips) |
| tract | VARCHAR | Census tract code |
| county_fips | VARCHAR | County FIPS (FK → tiger_counties.county_fips) |
| res_ratio | DECIMAL | Residential ratio |
| bus_ratio | DECIMAL | Business ratio |
| oth_ratio | DECIMAL | Other ratio |
| tot_ratio | DECIMAL | Total ratio |

#### `hud_zip_cbsa`
ZIP code to metropolitan area mapping.

Primary key: `zip`

| Column | Type | Description |
|--------|------|-------------|
| zip | VARCHAR | 5-digit ZIP code |
| state_fips | VARCHAR | State FIPS (FK → tiger_states.state_fips) |
| cbsa_code | VARCHAR | CBSA code |
| cbsa_title | VARCHAR | Metropolitan area name |
| res_ratio | DECIMAL | Residential ratio |
| bus_ratio | DECIMAL | Business ratio |
| oth_ratio | DECIMAL | Other ratio |
| tot_ratio | DECIMAL | Total ratio |

#### `hud_zip_cbsa_div`
ZIP code to CBSA division mapping.

Primary key: `zip`

| Column | Type | Description |
|--------|------|-------------|
| zip | VARCHAR | 5-digit ZIP code |
| cbsa_div | VARCHAR | CBSA division code |
| cbsa | VARCHAR | Parent CBSA (FK → tiger_cbsa.cbsa_code) |
| res_ratio | DECIMAL | Residential ratio |
| bus_ratio | DECIMAL | Business ratio |
| oth_ratio | DECIMAL | Other ratio |
| tot_ratio | DECIMAL | Total ratio |

#### `hud_zip_congressional`
ZIP code to congressional district mapping.

Primary key: `(zip, congressional_district)`

| Column | Type | Description |
|--------|------|-------------|
| zip | VARCHAR | 5-digit ZIP code |
| state_code | VARCHAR | State code (FK → tiger_states.state_code) |
| congressional_district | VARCHAR | District number |
| res_ratio | DECIMAL | Residential ratio |
| bus_ratio | DECIMAL | Business ratio |
| oth_ratio | DECIMAL | Other ratio |
| tot_ratio | DECIMAL | Total ratio |

### Additional Geographic Tables

#### `tiger_zctas`
ZIP Code Tabulation Areas (census approximation of ZIP codes).

Primary key: `zcta_code`

| Column | Type | Description |
|--------|------|-------------|
| zcta_code | VARCHAR | 5-digit ZCTA code |
| boundary | GEOMETRY | ZCTA boundary polygon |
| population | INTEGER | Population count |
| housing_units | INTEGER | Housing units |

## Foreign Key Relationships

### Geographic Hierarchy (11 internal FKs)
1. `tiger_counties.state_fips` → `tiger_states.state_fips`
2. `census_places.state_code` → `tiger_states.state_code`
3. `hud_zip_county.county_fips` → `tiger_counties.county_fips`
4. `hud_zip_county.state_fips` → `tiger_states.state_fips`
5. `hud_zip_tract.county_fips` → `tiger_counties.county_fips`
6. `hud_zip_tract.state_fips` → `tiger_states.state_fips`
7. `hud_zip_cbsa.state_fips` → `tiger_states.state_fips`
8. `tiger_census_tracts.county_fips` → `tiger_counties.county_fips`
9. `tiger_block_groups.tract_code` → `tiger_census_tracts.tract_code`
10. `hud_zip_cbsa_div.cbsa` → `tiger_cbsa.cbsa_code`
11. `hud_zip_congressional.state_code` → `tiger_states.state_code`

### Cross-Domain Relationships (Incoming)
- SEC: `filing_metadata.state_of_incorporation` → `tiger_states.state_code`
- ECON: `regional_employment.state_code` → `tiger_states.state_code`
- ECON: `regional_income.geo_fips` → `tiger_states.state_fips`

### Complete Reference
For a comprehensive view of all relationships including the complete ERD diagram, cross-schema query examples, and detailed FK implementation status, see the **[Schema Relationships Guide](relationships.md)**.

## Common Query Patterns

### Geographic Aggregation
```sql
-- Companies by state with geographic context
SELECT
    s.state_name,
    s.state_code,
    s.land_area / 1000000 as land_area_sq_km,
    COUNT(DISTINCT f.cik) as company_count
FROM tiger_states s
LEFT JOIN sec.filing_metadata f ON s.state_code = f.state_of_incorporation
GROUP BY s.state_name, s.state_code, s.land_area
ORDER BY company_count DESC;
```

### ZIP Code Analysis
```sql
-- Metropolitan area economic activity
SELECT
    c.cbsa_title,
    c.metro_micro,
    COUNT(DISTINCT z.zip) as zip_codes,
    SUM(z.res_ratio) as residential_weight
FROM hud_zip_cbsa z
JOIN tiger_cbsa c ON z.cbsa_code = c.cbsa_code
GROUP BY c.cbsa_title, c.metro_micro
ORDER BY residential_weight DESC;
```

### Spatial Joins
```sql
-- Find all counties in a state
SELECT
    c.county_name,
    c.county_fips,
    c.land_area / 1000000 as area_sq_km
FROM tiger_counties c
JOIN tiger_states s ON c.state_fips = s.state_fips
WHERE s.state_code = 'CA'
ORDER BY c.county_name;
```

### Cross-Domain Geographic Analysis
```sql
-- Economic indicators by company incorporation state
SELECT
    s.state_name,
    COUNT(DISTINCT f.cik) as companies,
    AVG(e.unemployment_rate) as avg_unemployment,
    AVG(i.per_capita_value) as avg_income
FROM tiger_states s
LEFT JOIN sec.filing_metadata f ON s.state_code = f.state_of_incorporation
LEFT JOIN econ.regional_employment e ON s.state_code = e.state_code
LEFT JOIN econ.regional_income i ON s.state_fips = i.geo_fips
WHERE e.area_type = 'state'
  AND i.metric = 'Per Capita Personal Income'
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
    "hudQuarter": "Q3",
    "includeDemographics": true,
    "spatialIndexing": true
  }
}
```

### Performance Optimization

1. **Spatial Indexing**: Enabled by default for geometry columns
2. **Boundary Simplification**: Configurable tolerance for faster rendering
3. **Caching**: Geographic data changes annually, cache aggressively
4. **Join Strategies**: Use state_code/state_fips columns for cross-domain joins

## Update Schedule

| Dataset | Update Frequency | Typical Release |
|---------|-----------------|-----------------|
| TIGER/Line | Annual | December |
| HUD Crosswalk | Quarterly | End of quarter + 45 days |
| Census Demographics | Annual | September |

## Special Considerations

### State Code vs FIPS Code
- **state_code**: 2-letter codes (CA, TX) used by SEC and some ECON tables
- **state_fips**: 2-digit FIPS (06, 48) used internally in GEO schema
- `tiger_states` table provides the mapping between both formats

### ZIP Code Limitations
- ZIP codes don't align with census boundaries
- Use ZCTA for census data approximations
- HUD crosswalk provides allocation ratios for splitting ZIP data

### Spatial Operations
- Geometry columns support ST_ spatial functions
- Coordinate system: WGS84 (EPSG:4326)
- Boundaries simplified to 0.0001 degree tolerance by default
