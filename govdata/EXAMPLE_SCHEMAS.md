# Example Schemas Using DuckDB Conversion Extensions

This document provides complete, working examples of schemas that leverage DuckDB conversion extensions (quackformers, spatial, h3, fts, excel) for advanced data transformation.

## Example 1: Economic Indicators with Embeddings

**Use Case**: FRED economic indicators with semantic search on series descriptions

**Extensions Used**: quackformers (embeddings)

```json
{
  "partitionedTables": [{
    "name": "fred_indicators",
    "pattern": "type=fred_indicators/series={series}/year={year}/fred_indicators.parquet",
    "columns": [
      {"name": "series_id", "type": "varchar"},
      {"name": "date", "type": "date"},
      {"name": "value", "type": "double"},
      {"name": "series_name", "type": "varchar"},
      {"name": "series_notes", "type": "varchar"},
      {
        "name": "year",
        "type": "integer",
        "expression": "EXTRACT(YEAR FROM date)",
        "comment": "Partition key - year extracted from date"
      },
      {
        "name": "series_name_embedding",
        "type": "array<double>",
        "expression": "CASE WHEN length(series_name) > 10 THEN embed(series_name)::FLOAT[384] ELSE NULL END",
        "comment": "Semantic embedding for series name (only if substantial)"
      },
      {
        "name": "notes_embedding",
        "type": "array<double>",
        "expression": "CASE WHEN length(series_notes) > 100 THEN embed(series_notes)::FLOAT[384] ELSE NULL END",
        "comment": "Semantic embedding for series notes (only for detailed descriptions)"
      },
      {
        "name": "yoy_change",
        "type": "double",
        "expression": "(value - lag(value, 12) OVER (PARTITION BY series_id ORDER BY date)) / NULLIF(lag(value, 12) OVER (PARTITION BY series_id ORDER BY date), 0)",
        "comment": "Year-over-year percentage change"
      }
    ]
  }]
}
```

**Query Examples**:
```sql
-- Find series similar to "consumer price inflation"
SELECT series_id, series_name,
       COSINE_SIMILARITY(series_name_embedding, embed('consumer price inflation')) as relevance
FROM fred_indicators
WHERE series_name_embedding IS NOT NULL
GROUP BY series_id, series_name, series_name_embedding
ORDER BY relevance DESC
LIMIT 10;

-- Analyze inflation trends
SELECT series_id, date, value, yoy_change
FROM fred_indicators
WHERE series_id = 'CPIAUCSL'
  AND year >= 2020
ORDER BY date;
```

---

## Example 2: Census Geographic Data with Spatial & H3

**Use Case**: Census tracts with geospatial indexing and computed areas

**Extensions Used**: spatial (GIS), h3 (hex indexing)

```json
{
  "partitionedTables": [{
    "name": "census_tracts",
    "pattern": "type=census_tracts/year={year}/state={state}/census_tracts.parquet",
    "columns": [
      {"name": "geoid", "type": "varchar"},
      {"name": "name", "type": "varchar"},
      {"name": "population", "type": "integer"},
      {"name": "aland", "type": "bigint"},
      {"name": "awater", "type": "bigint"},
      {"name": "lat", "type": "double"},
      {"name": "lon", "type": "double"},
      {
        "name": "state_fips",
        "type": "varchar",
        "expression": "SUBSTR(geoid, 1, 2)",
        "comment": "State FIPS code from GEOID"
      },
      {
        "name": "county_fips",
        "type": "varchar",
        "expression": "SUBSTR(geoid, 1, 5)",
        "comment": "County FIPS code from GEOID"
      },
      {
        "name": "area_sq_km",
        "type": "double",
        "expression": "CAST(aland AS DOUBLE) / 1000000",
        "comment": "Land area in square kilometers"
      },
      {
        "name": "water_pct",
        "type": "double",
        "expression": "CAST(awater AS DOUBLE) / NULLIF(CAST(aland + awater AS DOUBLE), 0) * 100",
        "comment": "Percentage of area that is water"
      },
      {
        "name": "h3_index_res7",
        "type": "varchar",
        "expression": "h3_latlng_to_cell(lat, lon, 7)",
        "comment": "H3 hex index at resolution 7 (~5.16 km² per cell)"
      },
      {
        "name": "h3_index_res9",
        "type": "varchar",
        "expression": "h3_latlng_to_cell(lat, lon, 9)",
        "comment": "H3 hex index at resolution 9 (~0.105 km² per cell)"
      },
      {
        "name": "point_geometry",
        "type": "varchar",
        "expression": "ST_AsText(ST_Point(lon, lat))",
        "comment": "WKT point geometry for tract centroid"
      }
    ]
  }]
}
```

**Query Examples**:
```sql
-- Aggregate population by H3 neighborhood
SELECT
  h3_index_res7,
  COUNT(*) as tract_count,
  SUM(population) as total_population,
  AVG(area_sq_km) as avg_area_sq_km,
  AVG(water_pct) as avg_water_pct
FROM census_tracts
WHERE year = 2020
GROUP BY h3_index_res7
HAVING total_population > 10000
ORDER BY total_population DESC;

-- Find tracts in a specific county
SELECT geoid, name, population, area_sq_km
FROM census_tracts
WHERE county_fips = '36061'  -- New York County (Manhattan)
  AND year = 2020
ORDER BY population DESC;
```

---

## Example 3: BLS Employment Data with FTS

**Use Case**: Bureau of Labor Statistics employment data with full-text search

**Extensions Used**: fts (full-text search), Excel (reading BLS Excel files)

```json
{
  "partitionedTables": [{
    "name": "bls_employment",
    "pattern": "type=bls_employment/industry={industry}/year={year}/bls_employment.parquet",
    "columns": [
      {"name": "series_id", "type": "varchar"},
      {"name": "period", "type": "varchar"},
      {"name": "year", "type": "integer"},
      {"name": "value", "type": "double"},
      {"name": "industry_code", "type": "varchar"},
      {"name": "industry_name", "type": "varchar"},
      {"name": "area_code", "type": "varchar"},
      {"name": "area_name", "type": "varchar"},
      {
        "name": "month",
        "type": "integer",
        "expression": "CASE WHEN period LIKE 'M%' THEN CAST(SUBSTR(period, 2) AS INTEGER) ELSE NULL END",
        "comment": "Month number extracted from period"
      },
      {
        "name": "quarter",
        "type": "integer",
        "expression": "CASE WHEN period LIKE 'M%' THEN CEIL(CAST(SUBSTR(period, 2) AS INTEGER) / 3.0) ELSE NULL END",
        "comment": "Quarter (1-4) derived from month"
      },
      {
        "name": "search_text",
        "type": "varchar",
        "expression": "industry_name || ' ' || area_name",
        "comment": "Combined text for full-text search"
      },
      {
        "name": "mom_change",
        "type": "double",
        "expression": "value - lag(value, 1) OVER (PARTITION BY series_id ORDER BY year, month)",
        "comment": "Month-over-month change in employment"
      },
      {
        "name": "yoy_change",
        "type": "double",
        "expression": "value - lag(value, 12) OVER (PARTITION BY series_id ORDER BY year, month)",
        "comment": "Year-over-year change in employment"
      }
    ]
  }]
}
```

**Setup FTS Index** (after data load):
```sql
-- Create full-text search index on combined search text
PRAGMA create_fts_index('bls_employment', 'series_id', 'search_text');
```

**Query Examples**:
```sql
-- Full-text search for specific industries and areas
SELECT series_id, industry_name, area_name, year, value, score
FROM (
  SELECT *,
         fts_main_bls_employment.match_bm25(series_id, 'healthcare New York') AS score
  FROM bls_employment
) WHERE score IS NOT NULL
ORDER BY score DESC
LIMIT 20;

-- Employment trends in tech industries
SELECT year, month,
       SUM(value) as total_employment,
       SUM(mom_change) as total_mom_change,
       SUM(yoy_change) as total_yoy_change
FROM bls_employment
WHERE industry_name LIKE '%Computer%'
   OR industry_name LIKE '%Software%'
   OR industry_name LIKE '%Information Technology%'
GROUP BY year, month
ORDER BY year, month;
```

---

## Example 4: SEC Company Facts with Embeddings & Text Search

**Use Case**: SEC XBRL company facts with semantic search on risk factors

**Extensions Used**: quackformers (embeddings), fts (text search)

```json
{
  "partitionedTables": [{
    "name": "company_facts",
    "pattern": "type=company_facts/cik={cik}/fiscal_year={fiscal_year}/company_facts.parquet",
    "columns": [
      {"name": "cik", "type": "varchar"},
      {"name": "fiscal_year", "type": "integer"},
      {"name": "fiscal_period", "type": "varchar"},
      {"name": "entity_name", "type": "varchar"},
      {"name": "business_description", "type": "varchar"},
      {"name": "risk_factors", "type": "varchar"},
      {"name": "revenue", "type": "double"},
      {"name": "net_income", "type": "double"},
      {"name": "total_assets", "type": "double"},
      {
        "name": "profit_margin",
        "type": "double",
        "expression": "net_income / NULLIF(revenue, 0)",
        "comment": "Profit margin (net income / revenue)"
      },
      {
        "name": "business_profile",
        "type": "varchar",
        "expression": "entity_name || ' | ' || COALESCE(business_description, '')",
        "comment": "Combined business profile for search"
      },
      {
        "name": "business_embedding",
        "type": "array<double>",
        "expression": "CASE WHEN length(business_description) > 100 THEN embed(business_description)::FLOAT[384] ELSE NULL END",
        "comment": "Semantic embedding of business description"
      },
      {
        "name": "risk_embedding",
        "type": "array<double>",
        "expression": "CASE WHEN length(risk_factors) > 100 THEN embed(risk_factors)::FLOAT[384] ELSE NULL END",
        "comment": "Semantic embedding of risk factors"
      },
      {
        "name": "size_category",
        "type": "varchar",
        "expression": "CASE " +
                     "WHEN total_assets >= 1000000000 THEN 'Large Cap' " +
                     "WHEN total_assets >= 100000000 THEN 'Mid Cap' " +
                     "WHEN total_assets >= 10000000 THEN 'Small Cap' " +
                     "ELSE 'Micro Cap' END",
        "comment": "Company size category based on total assets"
      }
    ]
  }]
}
```

**Query Examples**:
```sql
-- Find companies with similar business models
SELECT cik, entity_name,
       COSINE_SIMILARITY(business_embedding,
                        (SELECT business_embedding FROM company_facts
                         WHERE cik = '0000320193' LIMIT 1)) as similarity
FROM company_facts
WHERE business_embedding IS NOT NULL
  AND fiscal_year = 2023
  AND cik != '0000320193'
ORDER BY similarity DESC
LIMIT 10;

-- Find companies with similar risk profiles
SELECT cik, entity_name,
       COSINE_SIMILARITY(risk_embedding, embed('supply chain disruption pandemic')) as risk_similarity
FROM company_facts
WHERE risk_embedding IS NOT NULL
  AND fiscal_year = 2023
ORDER BY risk_similarity DESC
LIMIT 20;

-- Financial analysis by size category
SELECT size_category,
       COUNT(*) as company_count,
       AVG(revenue) as avg_revenue,
       AVG(profit_margin) as avg_profit_margin
FROM company_facts
WHERE fiscal_year = 2023
  AND fiscal_period = 'FY'
GROUP BY size_category
ORDER BY avg_revenue DESC;
```

---

## Example 5: Real Estate Data with Spatial & H3

**Use Case**: Property listings with geospatial analysis

**Extensions Used**: spatial (GIS), h3 (hex indexing)

```json
{
  "partitionedTables": [{
    "name": "property_listings",
    "pattern": "type=property_listings/state={state}/year={year}/property_listings.parquet",
    "columns": [
      {"name": "listing_id", "type": "varchar"},
      {"name": "address", "type": "varchar"},
      {"name": "city", "type": "varchar"},
      {"name": "state", "type": "varchar"},
      {"name": "zipcode", "type": "varchar"},
      {"name": "lat", "type": "double"},
      {"name": "lon", "type": "double"},
      {"name": "price", "type": "double"},
      {"name": "sqft", "type": "integer"},
      {"name": "bedrooms", "type": "integer"},
      {"name": "bathrooms", "type": "double"},
      {"name": "listing_date", "type": "date"},
      {
        "name": "price_per_sqft",
        "type": "double",
        "expression": "price / NULLIF(sqft, 0)",
        "comment": "Price per square foot"
      },
      {
        "name": "h3_block",
        "type": "varchar",
        "expression": "h3_latlng_to_cell(lat, lon, 11)",
        "comment": "H3 index at block level (resolution 11)"
      },
      {
        "name": "h3_neighborhood",
        "type": "varchar",
        "expression": "h3_latlng_to_cell(lat, lon, 7)",
        "comment": "H3 index at neighborhood level (resolution 7)"
      },
      {
        "name": "point_geom",
        "type": "varchar",
        "expression": "ST_AsText(ST_Point(lon, lat))",
        "comment": "Point geometry for property location"
      },
      {
        "name": "year",
        "type": "integer",
        "expression": "EXTRACT(YEAR FROM listing_date)",
        "comment": "Partition key - year from listing date"
      }
    ]
  }]
}
```

**Query Examples**:
```sql
-- Average price per sqft by neighborhood
SELECT
  h3_neighborhood,
  COUNT(*) as listing_count,
  AVG(price) as avg_price,
  AVG(price_per_sqft) as avg_price_per_sqft,
  AVG(sqft) as avg_sqft
FROM property_listings
WHERE year = 2024
  AND state = 'CA'
GROUP BY h3_neighborhood
HAVING listing_count >= 5
ORDER BY avg_price DESC;

-- Find properties near a specific location
WITH target_location AS (
  SELECT h3_latlng_to_cell(37.7749, -122.4194, 11) as target_h3
)
SELECT p.listing_id, p.address, p.price, p.price_per_sqft
FROM property_listings p, target_location t
WHERE h3_get_base_cell(p.h3_block) = h3_get_base_cell(t.target_h3)
  AND h3_distance(p.h3_block, t.target_h3) <= 5  -- Within 5 hex cells
  AND year = 2024
ORDER BY price;
```

---

## Example 6: Multi-Extension Integration

**Use Case**: Comprehensive urban analytics combining all extensions

**Extensions Used**: quackformers, spatial, h3, fts

```json
{
  "partitionedTables": [{
    "name": "urban_metrics",
    "pattern": "type=urban_metrics/metro_area={metro}/year={year}/urban_metrics.parquet",
    "columns": [
      {"name": "location_id", "type": "varchar"},
      {"name": "metro_area", "type": "varchar"},
      {"name": "lat", "type": "double"},
      {"name": "lon", "type": "double"},
      {"name": "population", "type": "integer"},
      {"name": "median_income", "type": "double"},
      {"name": "employment_rate", "type": "double"},
      {"name": "description", "type": "varchar"},
      {"name": "year", "type": "integer"},
      {
        "name": "h3_cell_res7",
        "type": "varchar",
        "expression": "h3_latlng_to_cell(lat, lon, 7)",
        "comment": "Neighborhood-level H3 cell"
      },
      {
        "name": "h3_cell_res9",
        "type": "varchar",
        "expression": "h3_latlng_to_cell(lat, lon, 9)",
        "comment": "Block-level H3 cell"
      },
      {
        "name": "point_wkt",
        "type": "varchar",
        "expression": "ST_AsText(ST_Point(lon, lat))",
        "comment": "WKT point geometry"
      },
      {
        "name": "description_embedding",
        "type": "array<double>",
        "expression": "CASE WHEN length(description) > 50 THEN embed(description)::FLOAT[384] ELSE NULL END",
        "comment": "Semantic embedding for similarity search"
      },
      {
        "name": "economic_score",
        "type": "double",
        "expression": "(median_income / 100000) * employment_rate",
        "comment": "Composite economic health score"
      },
      {
        "name": "search_text",
        "type": "varchar",
        "expression": "metro_area || ' ' || description",
        "comment": "Combined text for full-text search"
      }
    ]
  }]
}
```

**Multi-Extension Query Examples**:
```sql
-- Find similar neighborhoods using embeddings AND spatial proximity
WITH target AS (
  SELECT description_embedding, h3_cell_res7
  FROM urban_metrics
  WHERE location_id = 'SF_SOMA'
  LIMIT 1
)
SELECT
  u.location_id,
  u.metro_area,
  COSINE_SIMILARITY(u.description_embedding, t.description_embedding) as semantic_similarity,
  h3_distance(u.h3_cell_res7, t.h3_cell_res7) as spatial_distance,
  u.economic_score
FROM urban_metrics u, target t
WHERE u.description_embedding IS NOT NULL
  AND u.location_id != 'SF_SOMA'
  AND year = 2024
ORDER BY semantic_similarity DESC, spatial_distance
LIMIT 20;

-- Spatial aggregation with economic metrics
SELECT
  h3_cell_res7,
  COUNT(*) as location_count,
  SUM(population) as total_population,
  AVG(median_income) as avg_income,
  AVG(employment_rate) as avg_employment,
  AVG(economic_score) as avg_economic_score
FROM urban_metrics
WHERE year = 2024
GROUP BY h3_cell_res7
HAVING total_population > 5000
ORDER BY avg_economic_score DESC;
```

---

## Best Practices

### 1. Conditional Embedding Generation

Always check text length before generating embeddings:
```json
{
  "expression": "CASE WHEN length(text) > 100 THEN embed(text)::FLOAT[384] ELSE NULL END"
}
```

**Why**: Short text provides little semantic value and wastes computation.

### 2. Multi-Resolution H3 Indexing

Include multiple resolution levels for different analysis scales:
```json
{
  "name": "h3_neighborhood",
  "expression": "h3_latlng_to_cell(lat, lon, 7)"
},
{
  "name": "h3_block",
  "expression": "h3_latlng_to_cell(lat, lon, 9)"
}
```

### 3. Computed Metrics in Conversion

Calculate derived metrics during conversion rather than at query time:
```json
{
  "name": "profit_margin",
  "expression": "net_income / NULLIF(revenue, 0)"
}
```

**Why**: Compute once during conversion, use many times in queries.

### 4. Graceful Degradation

Design schemas to work even if extensions fail to load:
```json
{
  "expression": "CASE WHEN h3_latlng_to_cell IS NOT NULL THEN h3_latlng_to_cell(lat, lon, 7) ELSE NULL END"
}
```

### 5. Performance Tuning

For large datasets with embeddings:
- Limit embedding generation to important fields
- Use partitioning to process in chunks
- Consider time-based partitioning (year/month)

---

## Testing Your Schema

### 1. Validate Schema JSON

```bash
# Check JSON syntax
cat schema.json | python -m json.tool
```

### 2. Test Extension Availability

```sql
-- Check which extensions are available
SELECT * FROM duckdb_extensions();
```

### 3. Test Expression Columns

```sql
-- Verify expression syntax before using in schema
SELECT
  embed('test text')::FLOAT[384] as embedding,
  h3_latlng_to_cell(40.7128, -74.0060, 7) as h3_index,
  ST_AsText(ST_Point(-74.0060, 40.7128)) as wkt;
```

### 4. Monitor Conversion Performance

```bash
# Enable logging to monitor conversion time
tail -f logs/govdata-adapter.log | grep "conversion"
```

---

## Version History

- **Phase 6 (2025-11-10)**: Created comprehensive example schemas
- Demonstrates all conversion extensions (quackformers, spatial, h3, fts, excel)
- Provides 6 complete working examples with queries
