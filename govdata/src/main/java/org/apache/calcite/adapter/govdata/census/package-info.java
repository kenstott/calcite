/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * U.S. Census Bureau demographic and socioeconomic data adapter for Apache Calcite.
 *
 * <p>This package provides comprehensive access to Census data including:
 * <ul>
 *   <li>American Community Survey (ACS) - Annual demographic estimates</li>
 *   <li>Decennial Census - Complete population counts every 10 years</li>
 *   <li>Economic Census - Business statistics every 5 years</li>
 *   <li>Population Estimates Program - Annual population updates</li>
 * </ul>
 *
 * <p>The adapter exposes Census data as SQL-queryable tables with proper geographic
 * keys enabling joins with the GEO schema for spatial analysis and with ECON/SEC
 * schemas for demographic-economic correlations.
 *
 * <h2>Key Features</h2>
 * <ul>
 *   <li>20+ demographic and socioeconomic subject tables</li>
 *   <li>Support for multiple geographic levels (state, county, place, tract)</li>
 *   <li>Time-partitioned data for trend analysis</li>
 *   <li>Margin of error columns for statistical accuracy</li>
 *   <li>Cross-schema foreign key relationships</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * -- Corporate headquarters demographics
 * SELECT
 *     s.company_name,
 *     c.median_household_income,
 *     c.bachelor_degree_pct,
 *     c.unemployment_rate
 * FROM SEC.filing_metadata s
 * JOIN GEO.tiger_counties g ON s.county = g.county_name
 * JOIN CENSUS.acs_income c ON g.county_fips = c.geoid
 * WHERE s.fiscal_year = 2023
 *   AND c.year = 2023;
 *
 * -- Population-weighted economic indicators
 * SELECT
 *     e.indicator_name,
 *     SUM(e.value * p.total_population) / SUM(p.total_population) as weighted_avg
 * FROM ECON.regional_indicators e
 * JOIN CENSUS.acs_population p ON e.fips = p.geoid
 * GROUP BY e.indicator_name;
 * }</pre>
 *
 * <h2>Configuration</h2>
 * <p>The Census schema requires the following environment variables:
 * <ul>
 *   <li>{@code CENSUS_API_KEY} - Free API key from census.gov</li>
 *   <li>{@code GOVDATA_CACHE_DIR} - Local cache directory</li>
 *   <li>{@code GOVDATA_PARQUET_DIR} - Parquet storage directory</li>
 * </ul>
 *
 * <p>Model configuration example:
 * <pre>{@code
 * {
 *   "schemas": [{
 *     "name": "CENSUS",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *     "operand": {
 *       "schemaType": "CENSUS",
 *       "autoDownload": true,
 *       "startYear": 2019,
 *       "endYear": 2023
 *     }
 *   }]
 * }
 * }</pre>
 */
package org.apache.calcite.adapter.govdata.census;