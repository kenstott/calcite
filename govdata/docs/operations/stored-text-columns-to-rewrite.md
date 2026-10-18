# Stored-text columns to rewrite (data device)

**Scope:** data work for the device that runs govdata's ETL against `govdata-parquet-v1`. Nothing
here is to be run from a development machine.

## Why

Each table below declares a column as an integer (or a date) but stores it in its Iceberg table
as text. A `year` partition is the usual case: the writer typed a partition column only from the
table's `materialize.partition.columnDefinitions`, which named none, so it wrote the hive path
value (`year=2023`) as a string.

Two code changes, released together, deal with this:

- **Read side.** The DuckDB engine now serves each stored-text integer partition column as its
  declared type and rewrites filters on it to the stored text, so partition pruning is kept.
  Readers already see `INTEGER`; the files are unchanged. This covers partition columns only:
  the four date columns of `health.fda_drug_shortages_history` are data columns, and readers see
  them as text until that table is rewritten.
- **Write side.** A table's materialize block now takes the partition types the table declares
  (`partitions.columnDefinitions`). A new table is written with them (GOV-075).

## What the rewrite involves

Loads of the tables below keep running. A table that already stores a column as text keeps
writing it as text, and logs one WARN per table and column naming both types, "listed in
stored-text-columns-to-rewrite.md". Each table switches to the declared type when it is purged
and reprocessed, since the reprocess creates it anew.

For each table:

1. Read its distinct values first (column given below). Each must be a plain integer: a 4-digit
   year, a month, or a quarter.
2. If any value is a range or period label (`2019-2023`, `FY2023`, `2023Q1`), stop. That column
   gets a descriptive name instead (`acs_period`, `fiscal_period`), with `start_year`/`end_year`
   integers if they are useful. That is a schema change, made in this repo first.
3. Otherwise purge and reprocess the table.
4. Check it with a short download through the bundle: the column is `INTEGER` and the values are
   unchanged.

Every `year` here declares `storedDigits: 4`; `fedregister.fr_significance.month` declares
`storedDigits: 2`. The read side relies on that until the table is rewritten: a stored value of
another width would compare wrongly.

**Check values** marks a table whose `observedCoverage` does not show that every year value is a
plain year. Either it records no coverage, or its years are not contiguous. Check those before
rewriting.

## Tables

### banking

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| consumer_complaints | year | INTEGER | string | 2,606,880 | 2011–2026, 16 years |  |
| cra_small_business_lending | year | INTEGER | string | 979,623 | 2009–2023, 11 years, gaps | check values (coverage not contiguous) |
| failures | year | INTEGER | string | 384 | 2010–2026, 14 years, gaps | check values (coverage not contiguous) |
| financials | year | INTEGER | string | 386,056 | 2010–2026, 17 years |  |
| history | year | INTEGER | string | 571,736 | 1934–2026, 93 years |  |
| sod | year | INTEGER | string | 2,898,836 | 1994–2026, 33 years |  |

### census

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| abs_characteristics | year | INTEGER | string | 204 | 2017–2023, 4 years, gaps | check values (coverage not contiguous) |
| acs1_income | year | INTEGER | string | 989 | 2005–2024, 19 years, gaps | check values (coverage not contiguous) |
| acs1_population | year | INTEGER | string | 988 | 2005–2024, 19 years, gaps | check values (coverage not contiguous) |
| acs_age | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_commuting | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_congressional_district | year | INTEGER | string | 5,681 |  | check values (no observed coverage) |
| acs_county_migration_flows | year | INTEGER | string | 5,552,941 |  | check values (no observed coverage) |
| acs_disability | year | INTEGER | string | 42,546 | 2012–2023, 12 years |  |
| acs_earnings_by_education | year | INTEGER | string | 780 | 2010–2023, 14 years |  |
| acs_education | year | INTEGER | string | 42,546 | 2012–2023, 12 years |  |
| acs_employment | year | INTEGER | string | 45,819 | 2011–2023, 13 years |  |
| acs_health_insurance | year | INTEGER | string | 42,546 | 2012–2023, 12 years |  |
| acs_household_type | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_housing | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_housing_tenure | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_housing_tenure_by_age | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_housing_year_built_detail | year | INTEGER | string | 32,728 | 2015–2023, 9 years |  |
| acs_income | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_income_by_nativity | year | INTEGER | string | 784 | 2010–2023, 14 years |  |
| acs_income_distribution | year | INTEGER | string | 49,092 | 2010–2023, 14 years |  |
| acs_industry | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_industry_by_nativity | year | INTEGER | string | 784 | 2010–2023, 14 years |  |
| acs_internet | year | INTEGER | string | 26,184 | 2017–2023, 7 years |  |
| acs_language | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_marital_status | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_migration | year | INTEGER | string | 49,092 | 2010–2023, 14 years |  |
| acs_nativity | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_nativity_by_education | year | INTEGER | string | 780 | 2010–2023, 14 years |  |
| acs_occupation | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_population | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_poverty | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_race_ethnicity | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_vehicle_access | year | INTEGER | string | 49,092 | 2010–2023, 14 years |  |
| acs_veterans | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_veterans_by_period | year | INTEGER | string | 52,365 | 2009–2023, 15 years |  |
| acs_veterans_employment | year | INTEGER | string | 49,092 | 2010–2023, 14 years |  |
| bds_dynamics | year | INTEGER | string | 1,581 | 1993–2023, 31 years |  |
| cbp_establishments | year | INTEGER | string | 8,297,538 | 2017–2023, 7 years |  |
| cps_voting_supplement | year | INTEGER | string | 765 | 1996–2024, 15 years, gaps | check values (coverage not contiguous) |
| decennial_housing | year | INTEGER | string | 9,817 | 2000–2020, 3 years, gaps | check values (coverage not contiguous) |
| decennial_population | year | INTEGER | string | 9,817 | 2000–2020, 3 years, gaps | check values (coverage not contiguous) |
| economic_census | year | INTEGER | string | 1,731,888 | 2017–2022, 2 years, gaps | check values (coverage not contiguous) |
| nonemployer_statistics | year | INTEGER | string | 8,183,108 | 2012–2023, 9 years, gaps | check values (coverage not contiguous) |
| pep_components_of_change | year | INTEGER | string | 50,284 |  | check values (no observed coverage) |
| pep_population | year | INTEGER | string | 16,368 | 2019–2023, 5 years |  |
| sahie_insurance | year | INTEGER | string | 57,483 | 2006–2023, 18 years |  |
| saipe_poverty | year | INTEGER | string | 73,436 | 1995–2024, 23 years, gaps | check values (coverage not contiguous) |
| uscis_i765_employment_authorization | year | INTEGER | string | 1,508 | 2025–2026, 2 years |  |

### crime

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| bjs_ncvs_household | year | INTEGER | string | 81,536 | 2008–2024, 17 years |  |
| bjs_ncvs_household_pop | year | INTEGER | string | 1,669,337 | 2008–2024, 17 years |  |
| bjs_ncvs_personal | year | INTEGER | string | 20,878 | 2008–2024, 17 years |  |
| bjs_ncvs_personal_pop | year | INTEGER | string | 2,302,736 | 2008–2024, 17 years |  |
| cde_offenses | year | INTEGER | string | 102,000 | 2010–2026, 17 years |  |

### cyber_vuln

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| advisories | year | INTEGER | string | 101 | 2024–2026, 3 years |  |
| vulnerabilities | quarter | INTEGER | string | 400,579 | 1999–2026, 28 years | check values |
| vulnerabilities | year | INTEGER | string | 400,579 | 1999–2026, 28 years |  |
| vulnerability_cpes | quarter | INTEGER | string | 2,911,622 | 1999–2026, 28 years | check values |
| vulnerability_cpes | year | INTEGER | string | 2,911,622 | 1999–2026, 28 years |  |
| vulnerability_cwes | quarter | INTEGER | string | 381,796 | 1999–2026, 28 years | check values |
| vulnerability_cwes | year | INTEGER | string | 381,796 | 1999–2026, 28 years |  |

### disasters

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| disaster_declarations | year | INTEGER | string | 70,430 | 1953–2026, 74 years |  |
| hazard_mitigation_projects | year | INTEGER | string | 31,039 | 2010–2025, 16 years |  |
| nfip_claims | year | INTEGER | string | 2,755,527 | 1978–2026, 49 years |  |
| nfip_policies | year | INTEGER | string | 2,415,000 | 2010–2026, 15 years, gaps | check values (coverage not contiguous) |
| public_assistance_projects | year | INTEGER | string | 405,843 | 2010–2026, 17 years |  |
| storm_events | year | INTEGER | string | 1,427,137 | 2004–2026, 23 years |  |

### econ

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| county_qcew | year | INTEGER | string | 55,034,569 | 2007–2025, 19 years |  |
| county_wages | year | INTEGER | string | 55,034,569 | 2007–2025, 19 years |  |
| fdi_activities | year | INTEGER | string | 146,894 | 2008–2024, 17 years |  |
| fdi_direct_investment | year | INTEGER | string | 66,240 | 2008–2025, 18 years |  |
| federal_debt | year | INTEGER | string | 4,464 | 2009–2026, 18 years |  |
| fred_indicators | year | INTEGER | string | 128,245 | 1986–2026, 41 years |  |
| iip_positions | year | INTEGER | string | 8,538 | 2008–2025, 18 years |  |
| industry_gdp | year | INTEGER | string | 58,644 | 2008–2025, 18 years |  |
| ita_data | year | INTEGER | string | 308 | 1998–2025, 28 years |  |
| jolts_regional | year | INTEGER | string | 278,000 | 2009–2026, 18 years |  |
| labor_productivity | year | INTEGER | string | 420 | 2009–2026, 18 years |  |
| metro_cpi | year | INTEGER | string | 2,015 | 2009–2026, 18 years |  |
| metro_food_cpi | year | INTEGER | string | 3,234 | 2010–2026, 17 years |  |
| metro_wages | year | INTEGER | string | 10,073,699 | 2007–2025, 19 years |  |
| mts_outlays_by_function | year | INTEGER | string | 4,554 | 2015–2026, 12 years |  |
| national_accounts | year | INTEGER | string | 85,837 | 1998–2025, 28 years |  |
| national_wages | year | INTEGER | string | 77,832 | 2009–2025, 17 years |  |
| regional_cpi | year | INTEGER | string | 848 | 2009–2026, 18 years |  |
| regional_food_cpi | year | INTEGER | string | 800 | 2010–2026, 17 years |  |
| regional_income | year | INTEGER | string | 27,311,411 | 2007–2025, 19 years |  |
| state_wages | year | INTEGER | string | 2,635,282 | 2007–2025, 19 years |  |
| treasury_yields | year | INTEGER | string | 3,532 | 2009–2026, 18 years |  |
| wage_growth | year | INTEGER | string | 639 | 2009–2026, 18 years |  |

### energy

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| eia_capacity_changes | year | INTEGER | string | 345,484 | 2015–2025, 11 years |  |
| eia_coal_mines | year | INTEGER | string | 42,476 | 2007–2025, 19 years |  |
| eia_crude_oil_imports | year | INTEGER | string | 215,316 | 2010–2026, 17 years |  |
| eia_crude_oil_production | year | INTEGER | string | 20,724 | 2006–2026, 21 years |  |
| eia_electricity_generation | year | INTEGER | string | 3,356,386 | 2010–2026, 17 years |  |
| eia_electricity_prices | year | INTEGER | string | 7,440 | 2006–2025, 20 years |  |
| eia_petroleum_stocks | year | INTEGER | string | 151,294 | 2010–2026, 17 years |  |
| eia_power_plants | year | INTEGER | string | 314,042 | 2012–2023, 12 years |  |
| eia_refinery_operations | year | INTEGER | string | 320,357 | 2006–2026, 21 years |  |
| eia_service_territory | year | INTEGER | string | 141,873 | 2012–2023, 12 years |  |
| eia_utility_annual | year | INTEGER | string | 37,649 | 2012–2023, 12 years |  |
| eia_utility_reliability | year | INTEGER | string | 11,208 | 2013–2023, 11 years |  |
| pjm_capacity_auction_prices | year | INTEGER | string | 93 | 2021–2026, 6 years |  |

### environment

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| streamflow | year | INTEGER | string | 56,940,350 | 2010–2026, 17 years |  |
| tri_releases | year | INTEGER | string | 1,384,639 | 2008–2024, 17 years |  |
| water_quality_samples | year | INTEGER | string | 97,714,662 | 2010–2026, 16 years, gaps | check values (coverage not contiguous) |

### fec

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| candidate_committee_linkages | year | INTEGER | string | 62,762 | 2010–2026, 9 years, gaps | check values (coverage not contiguous) |
| candidate_summaries | year | INTEGER | string | 37,509 | 2010–2026, 9 years, gaps | check values (coverage not contiguous) |
| candidates | year | INTEGER | string | 65,930 | 2010–2026, 9 years, gaps | check values (coverage not contiguous) |
| committee_contributions | year | INTEGER | string | 4,497,325 | 2010–2026, 9 years, gaps | check values (coverage not contiguous) |
| committee_summaries | year | INTEGER | string | 110,282 | 2010–2026, 9 years, gaps | check values (coverage not contiguous) |
| committees | year | INTEGER | string | 157,043 | 2010–2026, 9 years, gaps | check values (coverage not contiguous) |
| communication_costs | year | INTEGER | string | 25,716 | 2010–2026, 9 years, gaps | check values (coverage not contiguous) |
| electioneering_communications | year | INTEGER | string | 1,711 | 2010–2026, 9 years, gaps | check values (coverage not contiguous) |
| independent_expenditures | year | INTEGER | string | 690,569 | 2010–2026, 9 years, gaps | check values (coverage not contiguous) |
| individual_contributions | year | INTEGER | string | 274,437,240 | 2010–2026, 9 years, gaps | check values (coverage not contiguous) |
| intercommittee_transactions | year | INTEGER | string | 57,920,158 | 2010–2026, 8 years, gaps | check values (coverage not contiguous) |
| operating_expenditures | year | INTEGER | string | 15,999,156 | 2010–2026, 8 years, gaps | check values (coverage not contiguous) |

### fedregister

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| fr_document_text | month | INTEGER | string | 487,301 | 2010–2026, 17 years | check values |
| fr_document_text | year | INTEGER | string | 487,301 | 2010–2026, 17 years |  |
| fr_documents | month | INTEGER | string | 482,396 | 2010–2026, 17 years | check values |
| fr_documents | year | INTEGER | string | 482,396 | 2010–2026, 17 years |  |
| fr_significance | month | INTEGER | string | 488,226 | 2010–2026, 17 years | check values |
| fr_significance | year | INTEGER | string | 488,226 | 2010–2026, 17 years |  |

### fiscal

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| broadband_caf_deployment_locations | year | INTEGER | string | 4,800,271 | 2015–2025, 11 years |  |
| broadband_high_cost_disbursements | year | INTEGER | string | 1,048,660 | 2016–2026, 11 years |  |
| county_migration_flows | year | INTEGER | string | 954,222 | 2012–2022, 11 years |  |
| entitlement_spending_by_state | year | INTEGER | string | 2,479 | 2009–2025, 17 years |  |
| exempt_org_990 | year | INTEGER | string | 3,539,766 | 2024–2025, 2 years |  |
| fac_audits_historic | year | INTEGER | string | 723,457 |  | check values (no observed coverage) |
| fac_federal_awards_historic | year | INTEGER | string | 9,392,323 |  | check values (no observed coverage) |
| govt_finance_by_unit | year | INTEGER | string | 10,881,346 | 2012–2024, 13 years |  |
| irs_gross_collections_by_state_year | year | INTEGER | string | 392 | 2019–2025, 7 years |  |
| omb_apportionments | year | INTEGER | string | 318,227 | 2023–2026, 3 years, gaps | check values (coverage not contiguous) |
| soi_income_by_county | year | INTEGER | string | 328,864 | 2011–2023, 13 years |  |
| soi_income_by_zip | year | INTEGER | string | 166,131 | 2022–2022, 1 years |  |
| ssa_benefits_by_geography | year | INTEGER | string | 269,019 | 2017–2023, 7 years |  |
| ssa_benefits_by_geography_acs | year | INTEGER | string | 51,533 | 2009–2024, 16 years |  |
| state_corporate_income_tax_collections | year | INTEGER | string | 510 | 2016–2025, 10 years |  |
| usaspending_assistance | year | INTEGER | string | 129,166,763 |  | check values (no observed coverage) |
| usaspending_by_agency | year | INTEGER | string | 981 | 2017–2025, 9 years |  |
| usaspending_by_county | year | INTEGER | string | 54,953 | 2009–2025, 17 years |  |
| usaspending_by_district | year | INTEGER | string | 7,497 | 2009–2025, 17 years |  |
| usaspending_by_state | year | INTEGER | string | 952 | 2009–2025, 17 years |  |
| usaspending_contract_recipients_by_district | year | INTEGER | string | 392,564 | 2017–2025, 9 years |  |
| usaspending_recipients_by_district | year | INTEGER | string | 396,709 | 2017–2025, 9 years |  |

### fisheries

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| mrip_recreational_catch | year | INTEGER | string | 239,424 | 2009–2025, 17 years |  |
| mrip_recreational_effort | year | INTEGER | string | 13,655 | 2009–2025, 17 years |  |
| noaa_commercial_landings | year | INTEGER | string | 229,828 | 1950–2024, 75 years |  |

### geo

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| block_groups | year | INTEGER | string | 3,363,454 | 2010–2025, 16 years |  |
| cbsa | year | INTEGER | string | 14,108 | 2010–2025, 15 years, gaps | check values (coverage not contiguous) |
| cd_zip_crosswalk | year | INTEGER | string | 639,682 | 2010–2023, 14 years |  |
| census_tracts | year | INTEGER | string | 1,169,345 | 2010–2025, 16 years |  |
| congressional_districts | year | INTEGER | string | 1,776 | 2021–2024, 4 years |  |
| counties | year | INTEGER | string | 51,729 | 2010–2025, 16 years |  |
| county_subdivisions | year | INTEGER | string | 570,250 | 2010–2025, 16 years |  |
| county_zip_crosswalk | year | INTEGER | string | 731,205 | 2010–2023, 14 years |  |
| gazetteer_counties | year | INTEGER | string | 45,092 | 2012–2025, 14 years |  |
| gazetteer_places | year | INTEGER | string | 429,461 | 2012–2025, 14 years |  |
| gazetteer_zctas | year | INTEGER | string | 467,251 | 2012–2025, 14 years |  |
| places | year | INTEGER | string | 484,210 | 2010–2025, 16 years |  |
| pumas | year | INTEGER | string | 33,358 | 2012–2025, 14 years |  |
| ruca_codes | year | INTEGER | string | 85,528 | 2020–2020, 1 years |  |
| rural_urban_continuum | year | INTEGER | string | 3,235 | 2023–2023, 1 years |  |
| school_districts | year | INTEGER | string | 174,302 | 2010–2025, 16 years |  |
| state_legislative_lower | year | INTEGER | string | 76,618 | 2010–2025, 16 years |  |
| state_legislative_upper | year | INTEGER | string | 31,290 | 2010–2025, 16 years |  |
| states | year | INTEGER | string | 892 | 2010–2025, 16 years |  |
| tract_zip_crosswalk | year | INTEGER | string | 2,355,514 | 2010–2023, 14 years |  |
| tribal_areas | year | INTEGER | string | 13,471 | 2010–2025, 16 years |  |
| urban_areas | year | INTEGER | string | 5,288 | 2024–2025, 2 years |  |
| voting_districts | year | INTEGER | string | 829,906 | 2012–2025, 7 years, gaps | check values (coverage not contiguous) |
| watersheds_huc12 | year | INTEGER | string | 1,752,139 | 2009–2025, 14 years, gaps | check values (coverage not contiguous) |
| watersheds_huc2 | year | INTEGER | string | 374 | 2009–2025, 17 years |  |
| watersheds_huc4 | year | INTEGER | string | 4,165 | 2009–2025, 17 years |  |
| watersheds_huc8 | year | INTEGER | string | 41,752 | 2009–2025, 17 years |  |
| zctas | year | INTEGER | string | 236,537 | 2020–2025, 6 years |  |
| zip_cbsa_crosswalk | year | INTEGER | string | 653,911 | 2010–2023, 14 years |  |
| zip_cd_crosswalk | year | INTEGER | string | 639,682 | 2010–2023, 14 years |  |
| zip_county_crosswalk | year | INTEGER | string | 731,205 | 2010–2023, 14 years |  |
| zip_tract_crosswalk | year | INTEGER | string | 2,355,514 | 2010–2023, 14 years |  |

### health

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| fda_drug_shortages_history | change_date | date | string | 191,536 | 2019–2026, 8 years | check values |
| fda_drug_shortages_history | discontinued_date | date | string | 191,536 | 2019–2026, 8 years | check values |
| fda_drug_shortages_history | initial_posting_date | date | string | 191,536 | 2019–2026, 8 years | check values |
| fda_drug_shortages_history | update_date | date | string | 191,536 | 2019–2026, 8 years | check values |
| ssa_oasdi_county | year | INTEGER | string | 49,344 | 2009–2025, 17 years |  |

### housing

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| building_permits | year | INTEGER | string | 33,381 | 2015–2025, 11 years |  |
| building_permits_place | year | INTEGER | string | 340,592 | 2009–2025, 17 years |  |
| fair_market_rents | year | INTEGER | string | 47,663 | 2017–2026, 10 years |  |
| hmda_applicant_demographics | year | INTEGER | string | 59,904 | 2018–2025, 8 years |  |
| hmda_lar | year | INTEGER | string | 136,591,817 | 2018–2025, 8 years |  |
| hmda_loans | year | INTEGER | string | 19,968 | 2018–2025, 8 years |  |
| hud_subsidized_county | year | INTEGER | string | 62,356 | 2022–2025, 4 years |  |
| income_limits | year | INTEGER | string | 500 | 2017–2026, 10 years |  |
| income_limits_county | year | INTEGER | string | 9,527 | 2024–2026, 2 years, gaps | check values (coverage not contiguous) |

### lands

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| nps_visitation | year | INTEGER | string | 119,443 | 1999–2025, 27 years |  |
| pilt_county_payments | year | INTEGER | string | 63,217 | 1999–2026, 28 years |  |
| timber_sales | year | INTEGER | string | 241,232 | 1999–2026, 28 years |  |

### law

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| scotus_docket_entries | year | INTEGER | string | 26,536 | 2017–2025, 9 years |  |
| scotus_dockets | year | INTEGER | string | 576 | 2017–2025, 9 years |  |
| scotus_reports_cases | year | INTEGER | string | 645 | 2010–2018, 9 years |  |
| scotus_slip_opinions | year | INTEGER | string | 585 | 2017–2025, 9 years |  |

### patents

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| patent_claims | year | INTEGER | string | 80,700,233 | 2010–2025, 16 years |  |
| patent_grants | year | INTEGER | string | 4,865,652 | 2010–2025, 16 years |  |
| patent_summaries | year | INTEGER | string | 4,865,183 | 2010–2025, 16 years |  |

### research

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| nih_award_projects | year | INTEGER | string | 1,236,084 | 2010–2026, 17 years |  |
| nih_publications | year | INTEGER | string | 1,008,494 | 2022–2026, 5 years |  |
| nsf_award_projects | year | INTEGER | string | 201,273 | 2010–2026, 17 years |  |

### transport

| Table | Column | Declared | Stored | Rows | Observed coverage | Before rewriting |
|---|---|---|---|---|---|---|
| airline_ontime | year | INTEGER | string | 106,363,320 | 2009–2025, 17 years |  |
| bridges | year | INTEGER | string | 9,494,201 | 2013–2025, 13 years |  |
| fatal_crashes | year | INTEGER | string | 560,749 | 2008–2024, 17 years |  |
| pavement_roughness | year | INTEGER | string | 728 | 2023–2024, 2 years |  |
| rail_service_performance | year | INTEGER | string | 1,469,654 | 2017–2026, 10 years |  |
| t100_segments | year | INTEGER | string | 7,144,555 | 2007–2025, 19 years |  |
| transit_ridership | year | INTEGER | string | 297,946 | 2007–2025, 19 years |  |
| vehicle_registrations | year | INTEGER | string | 561 | 2014–2024, 11 years |  |

221 columns in 212 tables; 46 to check before rewriting.

## A table with fewer columns than it declares

`health.cdc_county_injury_mortality` declares 13 columns in `health-schema.yaml`. The seed
catalog's view and `iceberg-schema-cache.json` record the same 13:

`geoid`, `name`, `st_geoid`, `st_name`, `intent`, `year`, `period`, `count_sup`, `rate`,
`rate_m`, `rate_m_ci`, `ttm_date_range`, `type`

The published table scans 12. On DuckDB 1.4, every read of the 13-column view failed with
`Binder Error: table "unnamed_subquery" has 12 columns available but 13 columns specified`.
Calcite now recreates that view over the live table and retries (#601), so reads of the
remaining columns work. The question for this device:

1. Which declared column does the published table lack? Read the column list of its current
   snapshot.
2. If that column belongs in the table, reprocess the table so it carries all 13.
3. If it does not, it is removed from `health-schema.yaml` in this repo.

In both cases the seed catalog and the schema cache take the 13 or 12 columns at the next seed
build (`govdata/scripts/build-seed.sh`), which reads them from the live table.
