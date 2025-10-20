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
package org.apache.calcite.adapter.govdata;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Comprehensive business definition comments for government data tables and columns.
 *
 * <p>This class provides standardized business definitions for SEC and GEO
 * government data tables that are exposed through JDBC metadata operations.
 * Comments explain the business meaning and usage of each table and column.
 */
public final class TableCommentDefinitions {

  private TableCommentDefinitions() {
    // Utility class
  }

  // =========================== SEC SCHEMA COMMENTS ===========================

  /** SEC schema table comments */
  private static final Map<String, String> SEC_TABLE_COMMENTS = new HashMap<>();

  /** SEC schema column comments */
  private static final Map<String, Map<String, String>> SEC_COLUMN_COMMENTS = new HashMap<>();

  static {
    // financial_line_items table
    SEC_TABLE_COMMENTS.put("financial_line_items",
        "Financial statement line items extracted from SEC XBRL filings. Each row represents a single "
        + "financial concept (e.g., Revenue, Assets, Liabilities) with its value for a specific reporting "
        + "period and context. Data is sourced from 10-K annual and 10-Q quarterly filings.");

    Map<String, String> financialLineItemsCols = new HashMap<>();
    financialLineItemsCols.put("cik", "Central Index Key - SEC's unique 10-digit identifier for registered entities");
    financialLineItemsCols.put("filing_type", "Type of SEC filing (10-K for annual reports, 10-Q for quarterly reports)");
    financialLineItemsCols.put("year", "Fiscal year of the filing (YYYY format)");
    financialLineItemsCols.put("filing_date", "Date when the filing was submitted to SEC (YYYY-MM-DD format)");
    financialLineItemsCols.put("concept", "XBRL concept name representing the financial metric (e.g., 'Revenues', 'Assets', 'NetIncomeLoss')");
    financialLineItemsCols.put("context_ref", "Reference to the XBRL context defining the reporting entity, period, and dimensions");
    financialLineItemsCols.put("value", "Numeric value of the financial concept (typically in USD)");
    financialLineItemsCols.put("label", "Human-readable label for the concept from XBRL taxonomy");
    financialLineItemsCols.put("units", "Unit of measurement (typically 'USD' for monetary values, 'shares' for share counts)");
    financialLineItemsCols.put("decimals", "Number of decimal places for rounding the value (-3 means thousands, -6 means millions)");
    financialLineItemsCols.put("accession_number", "Unique SEC document identifier (NNNNNNNNNN-NN-NNNNNN format)");
    financialLineItemsCols.put("period_start", "Start date of the reporting period (YYYY-MM-DD format)");
    financialLineItemsCols.put("period_end", "End date of the reporting period (YYYY-MM-DD format)");
    SEC_COLUMN_COMMENTS.put("financial_line_items", financialLineItemsCols);

    // filing_metadata table
    SEC_TABLE_COMMENTS.put("filing_metadata",
        "Core filing information and company metadata for SEC submissions. Contains one row per filing "
        + "with essential details about the submitting entity, filing type, dates, and document references. "
        + "This table serves as the master record for linking detailed financial data.");

    Map<String, String> filingMetadataCols = new HashMap<>();
    filingMetadataCols.put("cik", "Central Index Key - SEC's unique 10-digit identifier for the filing entity");
    filingMetadataCols.put("accession_number", "Unique SEC document identifier in NNNNNNNNNN-NN-NNNNNN format");
    filingMetadataCols.put("filing_type", "Type of SEC filing (10-K, 10-Q, 8-K, etc.)");
    filingMetadataCols.put("filing_date", "Date when the filing was submitted to SEC (YYYY-MM-DD format)");
    filingMetadataCols.put("company_name", "Legal name of the reporting entity as registered with SEC");
    filingMetadataCols.put("fiscal_year", "Company's fiscal year for this filing (YYYY format)");
    filingMetadataCols.put("fiscal_year_end", "Company's fiscal year end date (MM-DD format, e.g., '12-31')");
    filingMetadataCols.put("period_of_report", "End date of the reporting period covered by this filing (YYYY-MM-DD format)");
    filingMetadataCols.put("acceptance_datetime", "Date and time when SEC accepted the filing (YYYY-MM-DD HH:MM:SS format)");
    filingMetadataCols.put("primary_document", "Filename of the primary filing document (e.g., 'form10k.htm')");
    filingMetadataCols.put("file_size", "Size of the filing in bytes");
    filingMetadataCols.put("state_of_incorporation", "Two-letter state code where the entity is incorporated (e.g., 'DE', 'CA')");
    SEC_COLUMN_COMMENTS.put("filing_metadata", filingMetadataCols);

    // insider_transactions table
    SEC_TABLE_COMMENTS.put("insider_transactions",
        "Insider trading transactions reported on SEC Forms 3, 4, and 5. Tracks stock purchases, sales, "
        + "and other equity transactions by company officers, directors, and 10% shareholders. Each row "
        + "represents a single transaction with details about the insider, transaction type, and ownership changes.");

    Map<String, String> insiderTransactionsCols = new HashMap<>();
    insiderTransactionsCols.put("cik", "CIK of the company (issuer) whose securities were traded");
    insiderTransactionsCols.put("filing_type", "Type of insider form (3 for initial, 4 for changes, 5 for annual)");
    insiderTransactionsCols.put("year", "Year of the filing (YYYY format)");
    insiderTransactionsCols.put("filing_date", "Date when the insider form was filed with SEC (YYYY-MM-DD format)");
    insiderTransactionsCols.put("transaction_id", "Unique identifier for this transaction within the filing");
    insiderTransactionsCols.put("insider_cik", "CIK of the insider (officer/director) making the transaction");
    insiderTransactionsCols.put("insider_name", "Full name of the insider (officer, director, or beneficial owner)");
    insiderTransactionsCols.put("insider_title", "Title or position of the insider (CEO, CFO, Director, etc.)");
    insiderTransactionsCols.put("transaction_date", "Date when the transaction occurred (YYYY-MM-DD format)");
    insiderTransactionsCols.put("transaction_code", "SEC transaction code (P=Purchase, S=Sale, A=Grant, etc.)");
    insiderTransactionsCols.put("security_title", "Type of security traded (Common Stock, Stock Option, etc.)");
    insiderTransactionsCols.put("shares_transacted", "Number of shares or units involved in the transaction");
    insiderTransactionsCols.put("price_per_share", "Price per share for the transaction (in USD)");
    insiderTransactionsCols.put("total_value", "Total dollar value of the transaction (shares * price)");
    insiderTransactionsCols.put("shares_owned_following", "Total shares owned by insider after this transaction");
    insiderTransactionsCols.put("ownership_type", "Direct (D) or Indirect (I) ownership of the securities");
    SEC_COLUMN_COMMENTS.put("insider_transactions", insiderTransactionsCols);

    // footnotes table
    SEC_TABLE_COMMENTS.put("footnotes",
        "Financial statement footnotes and disclosures extracted from SEC filings. Contains the textual "
        + "explanations, accounting policies, and supplementary information that accompany financial statements. "
        + "Each row represents a single footnote or disclosure section.");

    Map<String, String> footnotesCols = new HashMap<>();
    footnotesCols.put("cik", "Central Index Key of the filing entity");
    footnotesCols.put("filing_type", "Type of SEC filing containing this footnote");
    footnotesCols.put("year", "Fiscal year of the filing");
    footnotesCols.put("accession_number", "SEC document identifier for the filing");
    footnotesCols.put("footnote_id", "Unique identifier for this footnote within the filing");
    footnotesCols.put("footnote_title", "Title or heading of the footnote section");
    footnotesCols.put("footnote_text", "Full text content of the footnote");
    footnotesCols.put("footnote_type", "Category of footnote (accounting policy, subsequent events, etc.)");
    footnotesCols.put("table_reference", "Financial statement table that this footnote relates to");
    footnotesCols.put("sequence_number", "Order of this footnote within the filing");
    SEC_COLUMN_COMMENTS.put("footnotes", footnotesCols);

    // filing_contexts table
    SEC_TABLE_COMMENTS.put("filing_contexts",
        "XBRL context metadata defining reporting periods, business segments, and dimensional qualifiers for financial data. "
        + "Each context element links financial facts to their specific reporting context including entity, time period, and dimensions. "
        + "Essential for understanding the scope and applicability of XBRL facts in SEC filings.");

    Map<String, String> filingContextsCols = new HashMap<>();
    filingContextsCols.put("cik", "Central Index Key of the filing entity");
    filingContextsCols.put("filing_type", "Type of SEC filing (10-K, 10-Q, 8-K)");
    filingContextsCols.put("year", "Fiscal year of the filing");
    filingContextsCols.put("accession_number", "SEC accession number uniquely identifying this filing");
    filingContextsCols.put("filing_date", "Date when the filing was submitted to SEC (ISO 8601 format)");
    filingContextsCols.put("context_id", "Unique identifier for this context element within the XBRL instance");
    filingContextsCols.put("entity_identifier", "Entity identifier, typically the CIK number");
    filingContextsCols.put("entity_scheme", "Entity identifier scheme URI (e.g., 'http://www.sec.gov/CIK')");
    filingContextsCols.put("period_start", "Start date of the reporting period for duration facts (ISO 8601 format, YYYY-MM-DD)");
    filingContextsCols.put("period_end", "End date of the reporting period for duration facts (ISO 8601 format, YYYY-MM-DD)");
    filingContextsCols.put("period_instant", "Instant date for point-in-time facts like balance sheet items (ISO 8601 format, YYYY-MM-DD)");
    filingContextsCols.put("segment", "Segment dimension information as XML fragment defining business segments, products, or geographic areas");
    filingContextsCols.put("scenario", "Scenario dimension information as XML fragment for forecasts, budgets, or alternative scenarios");
    filingContextsCols.put("type", "Table type identifier used by DuckDB catalog system");
    SEC_COLUMN_COMMENTS.put("filing_contexts", filingContextsCols);

    // mda_sections table
    SEC_TABLE_COMMENTS.put("mda_sections",
        "Management Discussion and Analysis (MD&A) sections extracted from 10-K and 10-Q filings. Contains management's narrative "
        + "about company performance, financial condition, results of operations, liquidity, capital resources, and business outlook. "
        + "Organized by Item sections and subsections with paragraph-level granularity.");

    Map<String, String> mdaSectionsCols = new HashMap<>();
    mdaSectionsCols.put("cik", "Central Index Key of the filing entity");
    mdaSectionsCols.put("filing_type", "Type of SEC filing (typically 10-K or 10-Q for MD&A content)");
    mdaSectionsCols.put("year", "Fiscal year of the filing");
    mdaSectionsCols.put("accession_number", "SEC accession number uniquely identifying this filing");
    mdaSectionsCols.put("filing_date", "Date when the filing was submitted to SEC (ISO 8601 format)");
    mdaSectionsCols.put("section", "Item section identifier from SEC form (e.g., 'Item 7' for MD&A in 10-K, 'Item 2' in 10-Q)");
    mdaSectionsCols.put("subsection", "Subsection name within the item (e.g., 'Overview', 'Results of Operations', 'Liquidity and Capital Resources')");
    mdaSectionsCols.put("section_id", "Unique identifier for this section within the filing");
    mdaSectionsCols.put("paragraph_number", "Sequential paragraph number within the subsection for ordering");
    mdaSectionsCols.put("paragraph_text", "Full text content of the paragraph");
    mdaSectionsCols.put("footnote_refs", "Comma-separated list of footnote references cited in this paragraph");
    mdaSectionsCols.put("type", "Table type identifier used by DuckDB catalog system");
    SEC_COLUMN_COMMENTS.put("mda_sections", mdaSectionsCols);

    // xbrl_relationships table
    SEC_TABLE_COMMENTS.put("xbrl_relationships",
        "XBRL calculation and presentation linkbase relationships showing how financial concepts relate to each other in SEC filings. "
        + "Defines parent-child hierarchies, calculation roll-ups (e.g., Assets = Current Assets + Non-Current Assets), "
        + "and presentation ordering. Essential for reconstructing financial statement structure and validating calculations.");

    Map<String, String> xbrlRelationshipsCols = new HashMap<>();
    xbrlRelationshipsCols.put("cik", "Central Index Key of the filing entity");
    xbrlRelationshipsCols.put("filing_type", "Type of SEC filing (10-K, 10-Q, 8-K)");
    xbrlRelationshipsCols.put("year", "Fiscal year of the filing");
    xbrlRelationshipsCols.put("accession_number", "SEC accession number uniquely identifying this filing");
    xbrlRelationshipsCols.put("filing_date", "Date when the filing was submitted to SEC (ISO 8601 format)");
    xbrlRelationshipsCols.put("relationship_id", "Unique identifier for this relationship within the filing");
    xbrlRelationshipsCols.put("linkbase_type", "Type of linkbase defining this relationship (presentation, calculation, definition, label)");
    xbrlRelationshipsCols.put("arc_role", "Arc role URI defining relationship semantics (e.g., parent-child, summation-item, general-special)");
    xbrlRelationshipsCols.put("from_concept", "Source concept name in the relationship (parent or sum concept)");
    xbrlRelationshipsCols.put("to_concept", "Target concept name in the relationship (child or component concept)");
    xbrlRelationshipsCols.put("weight", "Calculation weight: +1.0 for addition, -1.0 for subtraction in summation relationships");
    xbrlRelationshipsCols.put("order", "Presentation order number for display sequencing (lower numbers appear first)");
    xbrlRelationshipsCols.put("preferred_label", "Preferred label role for presentation (e.g., periodStartLabel, periodEndLabel, totalLabel)");
    xbrlRelationshipsCols.put("type", "Table type identifier used by DuckDB catalog system");
    SEC_COLUMN_COMMENTS.put("xbrl_relationships", xbrlRelationshipsCols);

    // earnings_transcripts table
    SEC_TABLE_COMMENTS.put("earnings_transcripts",
        "Quarterly earnings information extracted from 8-K filings. Contains earnings press releases, conference call transcripts, "
        + "prepared remarks, and Q&A sessions. Includes speaker attribution for conference call sections. "
        + "Valuable for sentiment analysis, management tone, and forward-looking statements.");

    Map<String, String> earningsTranscriptsCols = new HashMap<>();
    earningsTranscriptsCols.put("cik", "Central Index Key of the filing entity");
    earningsTranscriptsCols.put("filing_type", "Type of SEC filing (typically 8-K for earnings announcements)");
    earningsTranscriptsCols.put("year", "Fiscal year of the filing");
    earningsTranscriptsCols.put("accession_number", "SEC accession number uniquely identifying this filing");
    earningsTranscriptsCols.put("filing_date", "Date when the filing was submitted to SEC (ISO 8601 format)");
    earningsTranscriptsCols.put("exhibit_number", "Exhibit number within the 8-K filing (e.g., '99.1' for earnings press release)");
    earningsTranscriptsCols.put("section_type", "Section type: 'prepared_remarks' for scripted statements, 'qa_session' for Q&A portion");
    earningsTranscriptsCols.put("paragraph_number", "Sequential paragraph number within the section for ordering");
    earningsTranscriptsCols.put("paragraph_text", "Full text content of the paragraph");
    earningsTranscriptsCols.put("speaker_name", "Name of the speaker (for Q&A sections and prepared remarks with attribution)");
    earningsTranscriptsCols.put("speaker_role", "Role or title of the speaker (e.g., 'CEO', 'CFO', 'Analyst', 'Investor Relations')");
    earningsTranscriptsCols.put("type", "Table type identifier used by DuckDB catalog system");
    SEC_COLUMN_COMMENTS.put("earnings_transcripts", earningsTranscriptsCols);

    // stock_prices table
    SEC_TABLE_COMMENTS.put("stock_prices",
        "Daily stock price data including open, high, low, close, adjusted close, and trading volume for SEC-reporting companies. "
        + "Historical equity market data sourced from financial data providers. Adjusted close accounts for stock splits, "
        + "dividends, and other corporate actions. Enables financial analysis, returns calculation, and market correlation studies.");

    Map<String, String> stockPricesCols = new HashMap<>();
    stockPricesCols.put("ticker", "Stock ticker symbol in uppercase (e.g., 'AAPL', 'MSFT', 'GOOGL')");
    stockPricesCols.put("cik", "Central Index Key of the company whose stock is traded");
    stockPricesCols.put("date", "Trading date in ISO 8601 format (YYYY-MM-DD)");
    stockPricesCols.put("open", "Opening price for the trading day in USD");
    stockPricesCols.put("high", "Highest price reached during the trading day in USD");
    stockPricesCols.put("low", "Lowest price reached during the trading day in USD");
    stockPricesCols.put("close", "Closing price for the trading day in USD");
    stockPricesCols.put("adj_close", "Adjusted closing price accounting for splits, dividends, and distributions in USD");
    stockPricesCols.put("volume", "Trading volume in number of shares traded during the day");
    SEC_COLUMN_COMMENTS.put("stock_prices", stockPricesCols);

    // vectorized_blobs table
    SEC_TABLE_COMMENTS.put("vectorized_blobs",
        "Vector embeddings and enriched text representations of SEC filing content for semantic search and AI/ML applications. "
        + "Contains text chunks from footnotes, MD&A paragraphs, and other narrative sections with their vector embeddings. "
        + "Enriched text includes added financial context and relationships. Optional table enabled via configuration.");

    Map<String, String> vectorizedBlobsCols = new HashMap<>();
    vectorizedBlobsCols.put("cik", "Central Index Key of the filing entity");
    vectorizedBlobsCols.put("filing_type", "Type of SEC filing (10-K, 10-Q, 8-K)");
    vectorizedBlobsCols.put("year", "Fiscal year of the filing");
    vectorizedBlobsCols.put("accession_number", "SEC accession number uniquely identifying this filing");
    vectorizedBlobsCols.put("vector_id", "Unique identifier for this vector embedding");
    vectorizedBlobsCols.put("original_blob_id", "Identifier of the original text blob before vectorization");
    vectorizedBlobsCols.put("blob_type", "Type of text blob: 'footnote', 'mda_paragraph', 'concept_group', 'insider_remark', 'insider_footnote'");
    vectorizedBlobsCols.put("filing_date", "Date when the filing was submitted to SEC (ISO 8601 format)");
    vectorizedBlobsCols.put("original_text", "Original text content before enrichment");
    vectorizedBlobsCols.put("enriched_text", "Text after adding financial context, related concepts, and cross-references");
    vectorizedBlobsCols.put("embedding",
        "Vector embedding for semantic search across SEC filing narratives including MD&A, footnotes, and earnings transcripts. "
        + "[VECTOR dimension=384 provider=local model=tf-idf source_table_col=source_table source_id_col=source_id]");
    vectorizedBlobsCols.put("parent_section", "Parent section identifier (e.g., 'Item 7', 'Note 1 - Summary of Significant Accounting Policies')");
    vectorizedBlobsCols.put("relationships", "JSON string of relationships to other blobs for graph-based navigation");
    vectorizedBlobsCols.put("financial_concepts", "Comma-separated list of XBRL concept names referenced in this text");
    vectorizedBlobsCols.put("tokens_used", "Number of tokens consumed for this embedding (for API cost tracking)");
    vectorizedBlobsCols.put("token_budget", "Token budget allocated for this blob's embedding generation");
    vectorizedBlobsCols.put("type", "Table type identifier used by DuckDB catalog system");
    SEC_COLUMN_COMMENTS.put("vectorized_blobs", vectorizedBlobsCols);
  }

  // =========================== GEO SCHEMA COMMENTS ===========================

  /** GEO schema table comments */
  private static final Map<String, String> GEO_TABLE_COMMENTS = new HashMap<>();

  /** GEO schema column comments */
  private static final Map<String, Map<String, String>> GEO_COLUMN_COMMENTS = new HashMap<>();

  static {
    // tiger_states table
    GEO_TABLE_COMMENTS.put("tiger_states",
        "U.S. state boundaries and metadata from Census TIGER/Line shapefiles. Includes geographic "
        + "and demographic attributes for all 50 states, District of Columbia, Puerto Rico, and other "
        + "U.S. territories. Updated annually by the U.S. Census Bureau.");

    Map<String, String> tigerStatesCols = new HashMap<>();
    tigerStatesCols.put("state_fips", "2-digit Federal Information Processing Standards (FIPS) code for the state");
    tigerStatesCols.put("state_code", "2-letter USPS state abbreviation (e.g., 'CA', 'NY', 'TX')");
    tigerStatesCols.put("state_name", "Full official state name (e.g., 'California', 'New York', 'Texas')");
    tigerStatesCols.put("state_abbr", "2-letter state abbreviation (same as state_code)");
    tigerStatesCols.put("region", "Census region (1=Northeast, 2=Midwest, 3=South, 4=West)");
    tigerStatesCols.put("division", "Census division within region (more granular than region)");
    tigerStatesCols.put("land_area", "Land area in square meters (excludes water bodies)");
    tigerStatesCols.put("water_area", "Water area in square meters (lakes, rivers, coastal waters)");
    tigerStatesCols.put("total_area", "Total area in square meters (land + water)");
    tigerStatesCols.put("latitude", "Geographic center latitude in decimal degrees");
    tigerStatesCols.put("longitude", "Geographic center longitude in decimal degrees");
    GEO_COLUMN_COMMENTS.put("tiger_states", tigerStatesCols);

    // tiger_counties table
    GEO_TABLE_COMMENTS.put("tiger_counties",
        "U.S. county boundaries from Census TIGER/Line shapefiles. Counties are the primary legal "
        + "divisions of states and serve as the basis for many statistical and administrative functions. "
        + "Includes parishes (Louisiana), boroughs (Alaska), and independent cities.");

    Map<String, String> tigerCountiesCols = new HashMap<>();
    tigerCountiesCols.put("state_fips", "2-digit FIPS code of the state containing this county");
    tigerCountiesCols.put("county_fips", "5-digit FIPS code for the county (state + 3-digit county code)");
    tigerCountiesCols.put("county_name", "Official county name (e.g., 'Los Angeles', 'Cook', 'Harris')");
    tigerCountiesCols.put("county_type", "Type of county subdivision (County, Parish, Borough, Independent City)");
    tigerCountiesCols.put("state_name", "Name of the state containing this county");
    tigerCountiesCols.put("land_area", "Land area in square meters");
    tigerCountiesCols.put("water_area", "Water area in square meters");
    tigerCountiesCols.put("total_area", "Total area in square meters (land + water)");
    tigerCountiesCols.put("latitude", "Geographic center latitude in decimal degrees");
    tigerCountiesCols.put("longitude", "Geographic center longitude in decimal degrees");
    tigerCountiesCols.put("population", "Total population from most recent decennial census");
    GEO_COLUMN_COMMENTS.put("tiger_counties", tigerCountiesCols);

    // census_places table
    GEO_TABLE_COMMENTS.put("census_places",
        "Census designated places including incorporated cities, towns, villages, and census designated "
        + "places (CDPs). These are statistical geographic entities used by the Census Bureau for "
        + "data collection and tabulation. Includes population and housing unit counts.");

    Map<String, String> censusPlacesCols = new HashMap<>();
    censusPlacesCols.put("place_fips", "5-digit FIPS place code within the state");
    censusPlacesCols.put("place_name", "Official name of the place (city, town, village, or CDP)");
    censusPlacesCols.put("state_fips", "2-digit FIPS code of the state containing this place");
    censusPlacesCols.put("state_code", "2-letter state abbreviation");
    censusPlacesCols.put("state_name", "Full state name");
    censusPlacesCols.put("place_type", "Type of place (city, town, village, CDP, borough, etc.)");
    censusPlacesCols.put("county_fips", "5-digit FIPS code of the primary county containing this place");
    censusPlacesCols.put("population", "Total population from most recent decennial census");
    censusPlacesCols.put("housing_units", "Total number of housing units from most recent census");
    censusPlacesCols.put("land_area", "Land area in square meters");
    censusPlacesCols.put("water_area", "Water area in square meters");
    censusPlacesCols.put("latitude", "Geographic center latitude in decimal degrees");
    censusPlacesCols.put("longitude", "Geographic center longitude in decimal degrees");
    GEO_COLUMN_COMMENTS.put("census_places", censusPlacesCols);

    // hud_zip_county table
    GEO_TABLE_COMMENTS.put("hud_zip_county",
        "HUD USPS ZIP code to county crosswalk mapping. Maps 5-digit ZIP codes to counties with "
        + "allocation ratios for residential, business, and other address types. Essential for "
        + "geographic analysis of ZIP-based data. Updated quarterly by HUD.");

    Map<String, String> hudZipCountyCols = new HashMap<>();
    hudZipCountyCols.put("zip", "5-digit USPS ZIP code");
    hudZipCountyCols.put("county_fips", "5-digit FIPS county code that contains addresses in this ZIP");
    hudZipCountyCols.put("state_fips", "2-digit FIPS code of the state");
    hudZipCountyCols.put("state_code", "2-letter state abbreviation");
    hudZipCountyCols.put("county_name", "Official county name");
    hudZipCountyCols.put("res_ratio", "Ratio of residential addresses in this ZIP-county combination (0.0 to 1.0)");
    hudZipCountyCols.put("bus_ratio", "Ratio of business addresses in this ZIP-county combination (0.0 to 1.0)");
    hudZipCountyCols.put("oth_ratio", "Ratio of other address types in this ZIP-county combination (0.0 to 1.0)");
    hudZipCountyCols.put("tot_ratio", "Total ratio for this ZIP-county combination (sum of res + bus + oth)");
    hudZipCountyCols.put("usps_zip_pref_city", "USPS preferred city name for this ZIP code");
    hudZipCountyCols.put("usps_zip_pref_state", "USPS preferred state abbreviation for this ZIP code");
    GEO_COLUMN_COMMENTS.put("hud_zip_county", hudZipCountyCols);

    // tiger_zctas table
    GEO_TABLE_COMMENTS.put("tiger_zctas",
        "ZIP Code Tabulation Areas (ZCTAs) from Census TIGER/Line files. Statistical entities "
        + "that represent ZIP Code service areas for statistical analysis. More stable than actual "
        + "ZIP codes and better suited for census data linkage. Include population and housing counts.");

    Map<String, String> tigerZctasCols = new HashMap<>();
    tigerZctasCols.put("zcta5", "5-digit ZIP Code Tabulation Area code");
    tigerZctasCols.put("zcta5_name", "ZCTA name (usually 'ZCTA5 XXXXX')");
    tigerZctasCols.put("state_fips", "Primary state FIPS code for this ZCTA");
    tigerZctasCols.put("state_code", "Primary state 2-letter abbreviation");
    tigerZctasCols.put("land_area", "Land area in square meters");
    tigerZctasCols.put("water_area", "Water area in square meters");
    tigerZctasCols.put("total_area", "Total area in square meters (land + water)");
    tigerZctasCols.put("intpt_lat", "Internal point latitude (geographic center)");
    tigerZctasCols.put("intpt_lon", "Internal point longitude (geographic center)");
    tigerZctasCols.put("population", "Total population from most recent decennial census");
    tigerZctasCols.put("housing_units", "Total housing units from most recent census");
    tigerZctasCols.put("aland_sqmi", "Land area in square miles");
    tigerZctasCols.put("awater_sqmi", "Water area in square miles");
    GEO_COLUMN_COMMENTS.put("tiger_zctas", tigerZctasCols);

    // tiger_census_tracts table
    GEO_TABLE_COMMENTS.put("tiger_census_tracts",
        "Census tracts from TIGER/Line files. Small statistical subdivisions of counties with "
        + "populations typically between 1,200 and 8,000 people. Primary geography for detailed "
        + "demographic analysis and American Community Survey (ACS) data tabulation.");

    Map<String, String> tigerTractsCols = new HashMap<>();
    tigerTractsCols.put("tract_geoid", "11-digit census tract Geographic Identifier (GEOID)");
    tigerTractsCols.put("state_fips", "2-digit state FIPS code");
    tigerTractsCols.put("county_fips", "3-digit county FIPS code within state");
    tigerTractsCols.put("tract_code", "6-digit tract code within county");
    tigerTractsCols.put("tract_name", "Tract name (usually numeric identifier)");
    tigerTractsCols.put("namelsad", "Name and Legal/Statistical Area Description");
    tigerTractsCols.put("mtfcc", "MAF/TIGER Feature Class Code");
    tigerTractsCols.put("funcstat", "Functional status of the tract");
    tigerTractsCols.put("land_area", "Land area in square meters");
    tigerTractsCols.put("water_area", "Water area in square meters");
    tigerTractsCols.put("intpt_lat", "Internal point latitude");
    tigerTractsCols.put("intpt_lon", "Internal point longitude");
    tigerTractsCols.put("population", "Total population");
    tigerTractsCols.put("housing_units", "Total housing units");
    tigerTractsCols.put("aland_sqmi", "Land area in square miles");
    tigerTractsCols.put("awater_sqmi", "Water area in square miles");
    GEO_COLUMN_COMMENTS.put("tiger_census_tracts", tigerTractsCols);

    // tiger_block_groups table
    GEO_TABLE_COMMENTS.put("tiger_block_groups",
        "Census block groups from TIGER/Line files. Statistical divisions of census tracts, "
        + "typically containing 600-3,000 people. Smallest geography for American Community Survey "
        + "(ACS) sample data and essential for neighborhood-level demographic analysis.");

    Map<String, String> tigerBlockGroupsCols = new HashMap<>();
    tigerBlockGroupsCols.put("bg_geoid", "12-digit block group Geographic Identifier");
    tigerBlockGroupsCols.put("state_fips", "2-digit state FIPS code");
    tigerBlockGroupsCols.put("county_fips", "3-digit county FIPS code");
    tigerBlockGroupsCols.put("tract_code", "6-digit census tract code");
    tigerBlockGroupsCols.put("blkgrp", "1-digit block group number within tract");
    tigerBlockGroupsCols.put("namelsad", "Name and Legal/Statistical Area Description");
    tigerBlockGroupsCols.put("mtfcc", "MAF/TIGER Feature Class Code");
    tigerBlockGroupsCols.put("funcstat", "Functional status of the block group");
    tigerBlockGroupsCols.put("land_area", "Land area in square meters");
    tigerBlockGroupsCols.put("water_area", "Water area in square meters");
    tigerBlockGroupsCols.put("intpt_lat", "Internal point latitude");
    tigerBlockGroupsCols.put("intpt_lon", "Internal point longitude");
    tigerBlockGroupsCols.put("population", "Total population");
    tigerBlockGroupsCols.put("housing_units", "Total housing units");
    tigerBlockGroupsCols.put("aland_sqmi", "Land area in square miles");
    tigerBlockGroupsCols.put("awater_sqmi", "Water area in square miles");
    GEO_COLUMN_COMMENTS.put("tiger_block_groups", tigerBlockGroupsCols);

    // tiger_cbsa table
    GEO_TABLE_COMMENTS.put("tiger_cbsa",
        "Core Based Statistical Areas (CBSAs) from TIGER/Line files. Geographic entities associated "
        + "with urban cores of 10,000+ population. Includes Metropolitan Statistical Areas (50,000+) "
        + "and Micropolitan Statistical Areas (10,000-49,999). Essential for urban/regional analysis.");

    Map<String, String> tigerCbsaCols = new HashMap<>();
    tigerCbsaCols.put("cbsa_code", "5-digit Core Based Statistical Area code");
    tigerCbsaCols.put("cbsa_name", "CBSA name (e.g., 'New York-Newark-Jersey City, NY-NJ-PA')");
    tigerCbsaCols.put("namelsad", "Name and Legal/Statistical Area Description");
    tigerCbsaCols.put("lsad", "Legal/Statistical Area Description code");
    tigerCbsaCols.put("memi", "Metropolitan/Micropolitan indicator");
    tigerCbsaCols.put("mtfcc", "MAF/TIGER Feature Class Code");
    tigerCbsaCols.put("land_area", "Land area in square meters");
    tigerCbsaCols.put("water_area", "Water area in square meters");
    tigerCbsaCols.put("intpt_lat", "Internal point latitude");
    tigerCbsaCols.put("intpt_lon", "Internal point longitude");
    tigerCbsaCols.put("cbsa_type", "Metropolitan or Micropolitan designation");
    tigerCbsaCols.put("population", "Total population");
    tigerCbsaCols.put("aland_sqmi", "Land area in square miles");
    tigerCbsaCols.put("awater_sqmi", "Water area in square miles");
    GEO_COLUMN_COMMENTS.put("tiger_cbsa", tigerCbsaCols);

    // hud_zip_cbsa_div table
    GEO_TABLE_COMMENTS.put("hud_zip_cbsa_div",
        "HUD crosswalk mapping ZIP codes to CBSA Divisions. CBSA Divisions are Metropolitan "
        + "Divisions within large Metropolitan Statistical Areas, enabling sub-metropolitan analysis "
        + "for major urban areas like New York, Los Angeles, Chicago.");

    Map<String, String> hudZipCbsaDivCols = new HashMap<>();
    hudZipCbsaDivCols.put("zip", "5-digit ZIP code");
    hudZipCbsaDivCols.put("cbsadiv", "CBSA Division code");
    hudZipCbsaDivCols.put("cbsadiv_name", "CBSA Division name");
    hudZipCbsaDivCols.put("cbsa", "Parent CBSA code");
    hudZipCbsaDivCols.put("cbsa_name", "Parent CBSA name");
    hudZipCbsaDivCols.put("res_ratio", "Proportion of residential addresses in this geographic relationship");
    hudZipCbsaDivCols.put("bus_ratio", "Proportion of business addresses in this geographic relationship");
    hudZipCbsaDivCols.put("oth_ratio", "Proportion of other addresses in this geographic relationship");
    hudZipCbsaDivCols.put("tot_ratio", "Total proportion of all addresses in this geographic relationship");
    hudZipCbsaDivCols.put("usps_city", "USPS-assigned city name for the ZIP code");
    hudZipCbsaDivCols.put("state_code", "2-letter state abbreviation");
    GEO_COLUMN_COMMENTS.put("hud_zip_cbsa_div", hudZipCbsaDivCols);

    // hud_zip_congressional table
    GEO_TABLE_COMMENTS.put("hud_zip_congressional",
        "HUD crosswalk mapping ZIP codes to Congressional Districts. Essential for political "
        + "analysis, constituent services, and connecting corporate/demographic data to "
        + "political representation. Updated for 119th Congress district boundaries.");

    Map<String, String> hudZipCongressionalCols = new HashMap<>();
    hudZipCongressionalCols.put("zip", "5-digit ZIP code");
    hudZipCongressionalCols.put("cd", "Congressional District number within state");
    hudZipCongressionalCols.put("cd_name", "Congressional District name");
    hudZipCongressionalCols.put("state_cd", "State-Congressional District code (SSDD format)");
    hudZipCongressionalCols.put("res_ratio", "Proportion of residential addresses in this district");
    hudZipCongressionalCols.put("bus_ratio", "Proportion of business addresses in this district");
    hudZipCongressionalCols.put("oth_ratio", "Proportion of other addresses in this district");
    hudZipCongressionalCols.put("tot_ratio", "Total proportion of all addresses in this district");
    hudZipCongressionalCols.put("usps_city", "USPS-assigned city name for the ZIP code");
    hudZipCongressionalCols.put("state_code", "2-letter state abbreviation");
    hudZipCongressionalCols.put("state_name", "Full state name");
    GEO_COLUMN_COMMENTS.put("hud_zip_congressional", hudZipCongressionalCols);

    // tiger_congressional_districts table
    GEO_TABLE_COMMENTS.put("tiger_congressional_districts",
        "U.S. Congressional district boundaries for the House of Representatives from Census TIGER/Line shapefiles. "
        + "Updated after each decennial census reapportionment and subsequent state-level redistricting. "
        + "Includes 435 congressional districts plus delegates from District of Columbia and territories. "
        + "Essential for political analysis, election forecasting, and demographic studies of congressional constituencies.");

    Map<String, String> tigerCongressionalDistrictsCols = new HashMap<>();
    tigerCongressionalDistrictsCols.put("geoid", "Geographic identifier for the congressional district");
    tigerCongressionalDistrictsCols.put("district_fips", "Congressional district FIPS code (state FIPS + district number)");
    tigerCongressionalDistrictsCols.put("state_fips", "2-digit state FIPS code");
    tigerCongressionalDistrictsCols.put("district_number", "District number within the state (00 for at-large districts)");
    tigerCongressionalDistrictsCols.put("name", "Congressional district name (e.g., 'Congressional District 1', 'At Large')");
    tigerCongressionalDistrictsCols.put("state_name", "Full state name");
    tigerCongressionalDistrictsCols.put("land_area", "Land area in square meters");
    tigerCongressionalDistrictsCols.put("water_area", "Water area in square meters");
    tigerCongressionalDistrictsCols.put("total_area", "Total area in square meters (land + water)");
    tigerCongressionalDistrictsCols.put("latitude", "Geographic center latitude in decimal degrees");
    tigerCongressionalDistrictsCols.put("longitude", "Geographic center longitude in decimal degrees");
    GEO_COLUMN_COMMENTS.put("tiger_congressional_districts", tigerCongressionalDistrictsCols);

    // tiger_school_districts table
    GEO_TABLE_COMMENTS.put("tiger_school_districts",
        "Elementary, secondary, and unified school district boundaries from Census TIGER/Line files. "
        + "Covers public school districts that provide education services funded by local property taxes and state aid. "
        + "Updated annually by Census Bureau in coordination with state education agencies. "
        + "Useful for education research, demographic analysis, and property tax studies.");

    Map<String, String> tigerSchoolDistrictsCols = new HashMap<>();
    tigerSchoolDistrictsCols.put("geoid", "Geographic identifier for the school district");
    tigerSchoolDistrictsCols.put("district_id", "Unique school district identifier");
    tigerSchoolDistrictsCols.put("state_fips", "2-digit state FIPS code");
    tigerSchoolDistrictsCols.put("district_type", "District type: 'Elementary', 'Secondary', or 'Unified' (K-12)");
    tigerSchoolDistrictsCols.put("name", "Official school district name");
    tigerSchoolDistrictsCols.put("state_name", "Full state name");
    tigerSchoolDistrictsCols.put("land_area", "Land area in square meters");
    tigerSchoolDistrictsCols.put("water_area", "Water area in square meters");
    tigerSchoolDistrictsCols.put("total_area", "Total area in square meters (land + water)");
    GEO_COLUMN_COMMENTS.put("tiger_school_districts", tigerSchoolDistrictsCols);

    // population_demographics table
    GEO_TABLE_COMMENTS.put("population_demographics",
        "Population counts and demographic characteristics by geography from Census Bureau and American Community Survey. "
        + "Includes total population, gender distribution, and age demographics aggregated to various geographic levels "
        + "(state, county, place, tract). Annual estimates updated between decennial censuses. "
        + "Essential for demographic trend analysis, population projections, and market sizing.");

    Map<String, String> populationDemographicsCols = new HashMap<>();
    populationDemographicsCols.put("geo_id", "Census geographic identifier (state or county FIPS code)");
    populationDemographicsCols.put("year", "Survey year for this demographic data");
    populationDemographicsCols.put("total_population", "Total population count");
    populationDemographicsCols.put("male_population", "Male population count");
    populationDemographicsCols.put("female_population", "Female population count");
    populationDemographicsCols.put("state_fips", "2-digit state FIPS code");
    populationDemographicsCols.put("county_fips", "5-digit county FIPS code (state + county)");
    GEO_COLUMN_COMMENTS.put("population_demographics", populationDemographicsCols);

    // housing_characteristics table
    GEO_TABLE_COMMENTS.put("housing_characteristics",
        "Housing unit counts, occupancy status, tenure, and valuation by geography from American Community Survey 5-year estimates. "
        + "Provides comprehensive housing inventory including occupied vs. vacant units and median home values. "
        + "Updated annually with 5-year rolling averages for statistical reliability. "
        + "Critical for housing market analysis, affordability studies, and urban planning.");

    Map<String, String> housingCharacteristicsCols = new HashMap<>();
    housingCharacteristicsCols.put("geo_id", "Census geographic identifier (state or county FIPS code)");
    housingCharacteristicsCols.put("year", "Survey year (end year of 5-year estimate period)");
    housingCharacteristicsCols.put("total_housing_units", "Total number of housing units (occupied + vacant)");
    housingCharacteristicsCols.put("occupied_units", "Number of occupied housing units (households)");
    housingCharacteristicsCols.put("vacant_units", "Number of vacant housing units");
    housingCharacteristicsCols.put("median_home_value", "Median home value for owner-occupied units in dollars");
    housingCharacteristicsCols.put("state_fips", "2-digit state FIPS code");
    housingCharacteristicsCols.put("county_fips", "5-digit county FIPS code (state + county)");
    GEO_COLUMN_COMMENTS.put("housing_characteristics", housingCharacteristicsCols);

    // economic_indicators table
    GEO_TABLE_COMMENTS.put("economic_indicators",
        "Economic and income statistics by geography including median household income, per capita income, "
        + "and labor force metrics from American Community Survey. Provides insights into regional economic conditions, "
        + "income inequality, and employment patterns. Updated annually with 5-year estimates for stability. "
        + "Essential for economic development, policy analysis, and socioeconomic research.");

    Map<String, String> economicIndicatorsCols = new HashMap<>();
    economicIndicatorsCols.put("geo_id", "Census geographic identifier (state or county FIPS code)");
    economicIndicatorsCols.put("year", "Survey year (end year of 5-year estimate period)");
    economicIndicatorsCols.put("median_household_income", "Median household income in dollars");
    economicIndicatorsCols.put("per_capita_income", "Per capita income in dollars");
    economicIndicatorsCols.put("labor_force", "Total civilian labor force count");
    economicIndicatorsCols.put("employed", "Number of employed persons");
    economicIndicatorsCols.put("unemployed", "Number of unemployed persons");
    economicIndicatorsCols.put("state_fips", "2-digit state FIPS code");
    economicIndicatorsCols.put("county_fips", "5-digit county FIPS code (state + county)");
    GEO_COLUMN_COMMENTS.put("economic_indicators", economicIndicatorsCols);

    // zip_county_crosswalk table (maps to hud_zip_county in parquet)
    GEO_TABLE_COMMENTS.put("zip_county_crosswalk",
        "ZIP Code to County crosswalk file mapping ZIP codes to counties with residential and business address ratios. "
        + "Source: HUD USPS ZIP Code Crosswalk Files updated quarterly. Maps each ZIP code to all overlapping counties "
        + "with allocation ratios for apportioning ZIP-level data to counties. Essential for geographic data integration "
        + "when datasets use different geographic units.");

    Map<String, String> zipCountyCrosswalkCols = new HashMap<>();
    zipCountyCrosswalkCols.put("zip", "5-digit USPS ZIP code");
    zipCountyCrosswalkCols.put("county_fips", "5-digit county FIPS code that contains addresses in this ZIP");
    zipCountyCrosswalkCols.put("res_ratio", "Ratio of residential addresses in this ZIP-county combination (0.0 to 1.0)");
    zipCountyCrosswalkCols.put("bus_ratio", "Ratio of business addresses in this ZIP-county combination (0.0 to 1.0)");
    zipCountyCrosswalkCols.put("oth_ratio", "Ratio of other address types in this ZIP-county combination (0.0 to 1.0)");
    zipCountyCrosswalkCols.put("tot_ratio", "Total ratio for this ZIP-county combination (sum of res + bus + oth)");
    zipCountyCrosswalkCols.put("city", "USPS preferred city name for this ZIP code");
    zipCountyCrosswalkCols.put("state", "2-letter state abbreviation");
    GEO_COLUMN_COMMENTS.put("zip_county_crosswalk", zipCountyCrosswalkCols);

    // zip_cbsa_crosswalk table (maps to hud_zip_cbsa in parquet)
    GEO_TABLE_COMMENTS.put("zip_cbsa_crosswalk",
        "ZIP Code to Core Based Statistical Area (CBSA) crosswalk mapping ZIP codes to metropolitan and micropolitan areas. "
        + "Source: HUD USPS ZIP Code Crosswalk Files. CBSAs represent economically integrated regions with substantial commuting ties. "
        + "Includes allocation ratios for apportioning ZIP-level data to CBSAs. "
        + "Critical for metropolitan area analysis, urban economics research, and regional market segmentation.");

    Map<String, String> zipCbsaCrosswalkCols = new HashMap<>();
    zipCbsaCrosswalkCols.put("zip", "5-digit USPS ZIP code");
    zipCbsaCrosswalkCols.put("cbsa_fips", "CBSA FIPS code (5-digit identifier for metro/micro areas)");
    zipCbsaCrosswalkCols.put("res_ratio", "Ratio of residential addresses in this ZIP-CBSA combination (0.0 to 1.0)");
    zipCbsaCrosswalkCols.put("bus_ratio", "Ratio of business addresses in this ZIP-CBSA combination (0.0 to 1.0)");
    zipCbsaCrosswalkCols.put("oth_ratio", "Ratio of other address types in this ZIP-CBSA combination (0.0 to 1.0)");
    zipCbsaCrosswalkCols.put("tot_ratio", "Total ratio for this ZIP-CBSA combination (sum of res + bus + oth)");
    zipCbsaCrosswalkCols.put("city", "USPS preferred city name for this ZIP code");
    zipCbsaCrosswalkCols.put("state", "2-letter state abbreviation");
    GEO_COLUMN_COMMENTS.put("zip_cbsa_crosswalk", zipCbsaCrosswalkCols);

    // tract_zip_crosswalk table (maps to hud_zip_tract in parquet)
    GEO_TABLE_COMMENTS.put("tract_zip_crosswalk",
        "Census tract to ZIP Code crosswalk for relating census geography to postal geography. "
        + "Source: HUD USPS ZIP Code Crosswalk Files. Enables integration of census demographic data (organized by tracts) "
        + "with business/consumer data (organized by ZIP codes). Includes residential and business address allocation ratios. "
        + "Essential for demographic segmentation, market analysis, and socioeconomic research using ZIP-based datasets.");

    Map<String, String> tractZipCrosswalkCols = new HashMap<>();
    tractZipCrosswalkCols.put("zip", "5-digit USPS ZIP code");
    tractZipCrosswalkCols.put("tract_fips", "11-digit census tract FIPS code (state + county + tract)");
    tractZipCrosswalkCols.put("res_ratio", "Ratio of residential addresses in this tract-ZIP combination (0.0 to 1.0)");
    tractZipCrosswalkCols.put("bus_ratio", "Ratio of business addresses in this tract-ZIP combination (0.0 to 1.0)");
    tractZipCrosswalkCols.put("oth_ratio", "Ratio of other address types in this tract-ZIP combination (0.0 to 1.0)");
    tractZipCrosswalkCols.put("tot_ratio", "Total ratio for this tract-ZIP combination (sum of res + bus + oth)");
    tractZipCrosswalkCols.put("city", "USPS preferred city name for this ZIP code");
    tractZipCrosswalkCols.put("state", "2-letter state abbreviation");
    GEO_COLUMN_COMMENTS.put("tract_zip_crosswalk", tractZipCrosswalkCols);
  }

  // =========================== CENSUS SCHEMA COMMENTS ===========================

  /** CENSUS schema table comments */
  private static final Map<String, String> CENSUS_TABLE_COMMENTS = new HashMap<>();

  /** CENSUS schema column comments */
  private static final Map<String, Map<String, String>> CENSUS_COLUMN_COMMENTS = new HashMap<>();

  static {
    // population_estimates table
    CENSUS_TABLE_COMMENTS.put("population_estimates",
        "Annual population estimates between decennial censuses from the Census Bureau Population Estimates Program. "
        + "Includes components of population change (births, deaths, migration) for states and counties. "
        + "Updated annually providing intercensal estimates and postcensal estimates. Essential for understanding "
        + "population trends, demographic projections, and allocation of federal funds.");

    Map<String, String> populationEstimatesCols = new HashMap<>();
    populationEstimatesCols.put("geoid", "Census geographic identifier (state or county FIPS code)");
    populationEstimatesCols.put("state_fips", "2-digit state FIPS code");
    populationEstimatesCols.put("population_estimate", "Total population estimate for the year");
    CENSUS_COLUMN_COMMENTS.put("population_estimates", populationEstimatesCols);

    // decennial_population table
    CENSUS_TABLE_COMMENTS.put("decennial_population",
        "Decennial Census total population counts and basic demographics from complete enumeration every 10 years (2000, 2010, 2020). "
        + "Most accurate population count providing official counts for congressional apportionment and redistricting. "
        + "Includes population by race, sex, and age groups at detailed geographic levels. Constitutionally mandated and serves as "
        + "baseline for all intercensal estimates.");

    Map<String, String> decennialPopulationCols = new HashMap<>();
    decennialPopulationCols.put("geoid", "Census geographic identifier");
    decennialPopulationCols.put("state_fips", "2-digit state FIPS code");
    decennialPopulationCols.put("county_fips", "5-digit county FIPS code (state + county)");
    decennialPopulationCols.put("total_population", "Total population count from decennial census");
    decennialPopulationCols.put("white_alone", "Population identifying as White alone");
    decennialPopulationCols.put("black_african_american_alone", "Population identifying as Black or African American alone");
    decennialPopulationCols.put("asian_alone", "Population identifying as Asian alone");
    decennialPopulationCols.put("male_population", "Male population count");
    decennialPopulationCols.put("female_population", "Female population count");
    CENSUS_COLUMN_COMMENTS.put("decennial_population", decennialPopulationCols);

    // decennial_demographics table
    CENSUS_TABLE_COMMENTS.put("decennial_demographics",
        "Detailed demographic characteristics from Decennial Census including age, sex, race, ethnicity, and household relationships. "
        + "Provides comprehensive demographic breakdowns for redistricting, civil rights enforcement, and demographic research. "
        + "Updated every 10 years with complete population enumeration.");

    Map<String, String> decennialDemographicsCols = new HashMap<>();
    decennialDemographicsCols.put("geoid", "Census geographic identifier");
    decennialDemographicsCols.put("state_fips", "2-digit state FIPS code");
    decennialDemographicsCols.put("total_population", "Total population count");
    decennialDemographicsCols.put("white_alone", "Population identifying as White alone");
    decennialDemographicsCols.put("black_african_american_alone", "Population identifying as Black or African American alone");
    decennialDemographicsCols.put("asian_alone", "Population identifying as Asian alone");
    decennialDemographicsCols.put("male_population", "Male population count");
    decennialDemographicsCols.put("female_population", "Female population count");
    CENSUS_COLUMN_COMMENTS.put("decennial_demographics", decennialDemographicsCols);

    // decennial_housing table
    CENSUS_TABLE_COMMENTS.put("decennial_housing",
        "Housing unit counts and occupancy status from Decennial Census. Includes total housing units, occupied units, "
        + "vacant units, and tenure information. Updated every 10 years providing comprehensive housing inventory for "
        + "housing policy, urban planning, and market analysis.");

    Map<String, String> decennialHousingCols = new HashMap<>();
    decennialHousingCols.put("geoid", "Census geographic identifier");
    decennialHousingCols.put("state_fips", "2-digit state FIPS code");
    decennialHousingCols.put("total_housing_units", "Total number of housing units");
    decennialHousingCols.put("occupied_housing_units", "Number of occupied housing units (households)");
    decennialHousingCols.put("vacant_housing_units", "Number of vacant housing units");
    CENSUS_COLUMN_COMMENTS.put("decennial_housing", decennialHousingCols);

    // acs_population table
    CENSUS_TABLE_COMMENTS.put("acs_population",
        "American Community Survey population demographics including age, sex, race, and ethnicity breakdowns. "
        + "Annual 5-year estimates providing detailed demographic characteristics between decennial censuses. "
        + "Enables demographic analysis, trend identification, and population projections at detailed geographic levels.");

    Map<String, String> acsPopulationCols = new HashMap<>();
    acsPopulationCols.put("geoid", "Census geographic identifier");
    acsPopulationCols.put("state_fips", "2-digit state FIPS code");
    acsPopulationCols.put("county_fips", "5-digit county FIPS code (state + county)");
    acsPopulationCols.put("total_population", "Total population estimate from ACS");
    acsPopulationCols.put("male_population", "Male population estimate");
    acsPopulationCols.put("female_population", "Female population estimate");
    acsPopulationCols.put("white_alone", "Population identifying as White alone");
    acsPopulationCols.put("black_african_american_alone", "Population identifying as Black or African American alone");
    acsPopulationCols.put("asian_alone", "Population identifying as Asian alone");
    CENSUS_COLUMN_COMMENTS.put("acs_population", acsPopulationCols);

    // acs_demographics table
    CENSUS_TABLE_COMMENTS.put("acs_demographics",
        "Detailed demographic characteristics from American Community Survey including household composition, family structure, "
        + "marital status, and birth/fertility data. Annual 5-year estimates enable demographic trend analysis and social research.");

    Map<String, String> acsDemographicsCols = new HashMap<>();
    acsDemographicsCols.put("geoid", "Census geographic identifier");
    acsDemographicsCols.put("state_fips", "2-digit state FIPS code");
    acsDemographicsCols.put("total_population", "Total population estimate");
    acsDemographicsCols.put("male_population", "Male population estimate");
    acsDemographicsCols.put("female_population", "Female population estimate");
    CENSUS_COLUMN_COMMENTS.put("acs_demographics", acsDemographicsCols);

    // acs_income table
    CENSUS_TABLE_COMMENTS.put("acs_income",
        "Household and family income statistics from American Community Survey including median household income, per capita income, "
        + "income distribution by brackets, and poverty status. Annual 5-year estimates critical for economic analysis, "
        + "income inequality research, and means-tested program eligibility determination.");

    Map<String, String> acsIncomeCols = new HashMap<>();
    acsIncomeCols.put("geoid", "Census geographic identifier");
    acsIncomeCols.put("state_fips", "2-digit state FIPS code");
    acsIncomeCols.put("median_household_income", "Median household income in dollars");
    CENSUS_COLUMN_COMMENTS.put("acs_income", acsIncomeCols);

    // acs_poverty table
    CENSUS_TABLE_COMMENTS.put("acs_poverty",
        "Poverty statistics from American Community Survey including population below poverty level, poverty rates by age groups, "
        + "and ratio of income to poverty level. Annual 5-year estimates essential for poverty research, program planning, "
        + "and federal fund allocation for anti-poverty programs.");

    Map<String, String> acsPovertyCols = new HashMap<>();
    acsPovertyCols.put("geoid", "Census geographic identifier");
    acsPovertyCols.put("state_fips", "2-digit state FIPS code");
    acsPovertyCols.put("below_poverty_level", "Population below federal poverty threshold");
    acsPovertyCols.put("total_population_poverty_determined", "Total population for whom poverty status was determined");
    acsPovertyCols.put("poverty_rate", "Calculated poverty rate (below_poverty_level / total_population_poverty_determined)");
    CENSUS_COLUMN_COMMENTS.put("acs_poverty", acsPovertyCols);

    // acs_employment table
    CENSUS_TABLE_COMMENTS.put("acs_employment",
        "Employment status, labor force participation, unemployment rates, and work status by demographic groups from American Community Survey. "
        + "Annual 5-year estimates provide detailed labor market analysis, workforce development insights, and economic planning data.");

    Map<String, String> acsEmploymentCols = new HashMap<>();
    acsEmploymentCols.put("geoid", "Census geographic identifier");
    acsEmploymentCols.put("state_fips", "2-digit state FIPS code");
    acsEmploymentCols.put("employed_population", "Number of employed persons in civilian labor force");
    acsEmploymentCols.put("unemployed_population", "Number of unemployed persons actively seeking work");
    acsEmploymentCols.put("labor_force_participation", "Total civilian labor force (employed + unemployed)");
    acsEmploymentCols.put("unemployment_rate", "Calculated unemployment rate (unemployed_population / labor_force_participation)");
    CENSUS_COLUMN_COMMENTS.put("acs_employment", acsEmploymentCols);

    // acs_education table
    CENSUS_TABLE_COMMENTS.put("acs_education",
        "Educational attainment levels from less than high school to graduate degrees from American Community Survey. "
        + "Includes school enrollment and field of study. Annual 5-year estimates critical for education policy, "
        + "workforce development, and socioeconomic research.");

    Map<String, String> acsEducationCols = new HashMap<>();
    acsEducationCols.put("geoid", "Census geographic identifier");
    acsEducationCols.put("state_fips", "2-digit state FIPS code");
    acsEducationCols.put("population_25_and_over", "Population 25 years and over (base for attainment rates)");
    acsEducationCols.put("high_school_graduate", "Population with high school diploma or equivalent");
    acsEducationCols.put("bachelor_degree_or_higher", "Population with bachelor's degree or higher");
    CENSUS_COLUMN_COMMENTS.put("acs_education", acsEducationCols);

    // acs_housing table
    CENSUS_TABLE_COMMENTS.put("acs_housing",
        "Housing characteristics from American Community Survey including occupancy status, tenure (owner/renter), housing value, "
        + "rent costs, number of rooms, and year structure built. Annual 5-year estimates essential for housing market analysis, "
        + "affordability studies, and urban planning.");

    Map<String, String> acsHousingCols = new HashMap<>();
    acsHousingCols.put("geoid", "Census geographic identifier");
    acsHousingCols.put("state_fips", "2-digit state FIPS code");
    acsHousingCols.put("county_fips", "5-digit county FIPS code (state + county)");
    acsHousingCols.put("total_housing_units", "Total number of housing units");
    acsHousingCols.put("occupied_housing_units", "Number of occupied housing units (households)");
    acsHousingCols.put("vacant_housing_units", "Number of vacant housing units");
    acsHousingCols.put("median_home_value", "Median home value for owner-occupied units in dollars");
    CENSUS_COLUMN_COMMENTS.put("acs_housing", acsHousingCols);

    // acs_housing_costs table
    CENSUS_TABLE_COMMENTS.put("acs_housing_costs",
        "Detailed housing costs from American Community Survey including median home values, median rent, housing cost burden "
        + "(percentage of income spent on housing), and mortgage status. Annual 5-year estimates critical for affordability analysis, "
        + "housing policy, and cost-of-living comparisons.");

    Map<String, String> acsHousingCostsCols = new HashMap<>();
    acsHousingCostsCols.put("geoid", "Census geographic identifier");
    acsHousingCostsCols.put("state_fips", "2-digit state FIPS code");
    acsHousingCostsCols.put("median_home_value", "Median home value for owner-occupied units in dollars");
    acsHousingCostsCols.put("median_gross_rent", "Median gross rent for renter-occupied units in dollars per month");
    CENSUS_COLUMN_COMMENTS.put("acs_housing_costs", acsHousingCostsCols);

    // acs_commuting table
    CENSUS_TABLE_COMMENTS.put("acs_commuting",
        "Commuting patterns from American Community Survey including means of transportation to work, travel time to work, "
        + "departure time, and vehicles available. Annual 5-year estimates essential for transportation planning, infrastructure "
        + "investment, and understanding workforce mobility patterns.");

    Map<String, String> acsCommutingCols = new HashMap<>();
    acsCommutingCols.put("geoid", "Census geographic identifier");
    acsCommutingCols.put("state_fips", "2-digit state FIPS code");
    acsCommutingCols.put("mean_travel_time_to_work", "Mean travel time to work in minutes");
    acsCommutingCols.put("drove_alone_to_work", "Number of workers who drove alone to work");
    acsCommutingCols.put("public_transportation_to_work", "Number of workers who used public transportation");
    CENSUS_COLUMN_COMMENTS.put("acs_commuting", acsCommutingCols);

    // acs_health_insurance table
    CENSUS_TABLE_COMMENTS.put("acs_health_insurance",
        "Health insurance coverage status from American Community Survey including types of coverage (private, public, Medicare, Medicaid) "
        + "by age groups and employment status. Annual 5-year estimates essential for health policy analysis, ACA impact assessment, "
        + "and understanding healthcare access disparities.");

    Map<String, String> acsHealthInsuranceCols = new HashMap<>();
    acsHealthInsuranceCols.put("geoid", "Census geographic identifier");
    acsHealthInsuranceCols.put("state_fips", "2-digit state FIPS code");
    acsHealthInsuranceCols.put("total_population_health_insurance", "Total population for whom health insurance status was determined");
    acsHealthInsuranceCols.put("health_insurance_coverage", "Population with any health insurance coverage");
    acsHealthInsuranceCols.put("no_health_insurance", "Population without health insurance coverage");
    CENSUS_COLUMN_COMMENTS.put("acs_health_insurance", acsHealthInsuranceCols);

    // acs_language table
    CENSUS_TABLE_COMMENTS.put("acs_language",
        "Language spoken at home and English proficiency levels from American Community Survey. Includes detailed language categories "
        + "for population 5 years and over. Annual 5-year estimates essential for language access planning, education services, "
        + "and understanding linguistic diversity.");

    Map<String, String> acsLanguageCols = new HashMap<>();
    acsLanguageCols.put("geoid", "Census geographic identifier");
    acsLanguageCols.put("state_fips", "2-digit state FIPS code");
    acsLanguageCols.put("total_population_language", "Total population 5 years and over");
    acsLanguageCols.put("speak_english_only", "Population that speaks only English at home");
    acsLanguageCols.put("speak_language_other_than_english", "Population that speaks a language other than English at home");
    CENSUS_COLUMN_COMMENTS.put("acs_language", acsLanguageCols);

    // acs_disability table
    CENSUS_TABLE_COMMENTS.put("acs_disability",
        "Disability status by age group and type of disability from American Community Survey including hearing, vision, cognitive, "
        + "ambulatory, self-care, and independent living difficulties. Annual 5-year estimates critical for ADA compliance, "
        + "accessibility planning, and disability services allocation.");

    Map<String, String> acsDisabilityCols = new HashMap<>();
    acsDisabilityCols.put("geoid", "Census geographic identifier");
    acsDisabilityCols.put("state_fips", "2-digit state FIPS code");
    acsDisabilityCols.put("with_disability", "Population with one or more disabilities");
    acsDisabilityCols.put("no_disability", "Population with no disability");
    CENSUS_COLUMN_COMMENTS.put("acs_disability", acsDisabilityCols);

    // acs_veterans table
    CENSUS_TABLE_COMMENTS.put("acs_veterans",
        "Veteran status and characteristics from American Community Survey including period of service, disability rating, "
        + "and demographic profile of veteran population. Annual 5-year estimates essential for VA services planning, "
        + "veterans benefits administration, and military demographic research.");

    Map<String, String> acsVeteransCols = new HashMap<>();
    acsVeteransCols.put("geoid", "Census geographic identifier");
    acsVeteransCols.put("state_fips", "2-digit state FIPS code");
    acsVeteransCols.put("veteran_status", "Civilian population 18 years and over who are veterans");
    acsVeteransCols.put("nonveteran_population", "Civilian population 18 years and over who are not veterans");
    CENSUS_COLUMN_COMMENTS.put("acs_veterans", acsVeteransCols);

    // acs_migration table
    CENSUS_TABLE_COMMENTS.put("acs_migration",
        "Geographic mobility and migration patterns from American Community Survey including residence 1 year ago, domestic migration flows, "
        + "and foreign-born population statistics. Annual 5-year estimates essential for understanding population redistribution, "
        + "urbanization trends, and migration policy analysis.");

    Map<String, String> acsMigrationCols = new HashMap<>();
    acsMigrationCols.put("geoid", "Census geographic identifier");
    acsMigrationCols.put("state_fips", "2-digit state FIPS code");
    acsMigrationCols.put("moved_within_same_county", "Population that moved within the same county in past year");
    acsMigrationCols.put("moved_from_different_state", "Population that moved from a different state in past year");
    CENSUS_COLUMN_COMMENTS.put("acs_migration", acsMigrationCols);

    // acs_occupation table
    CENSUS_TABLE_COMMENTS.put("acs_occupation",
        "Detailed occupation categories and industry of employment from American Community Survey. Cross-tabulated with demographic "
        + "characteristics for workforce analysis. Annual 5-year estimates critical for labor market analysis, workforce development, "
        + "and occupational projections.");

    Map<String, String> acsOccupationCols = new HashMap<>();
    acsOccupationCols.put("geoid", "Census geographic identifier");
    acsOccupationCols.put("state_fips", "2-digit state FIPS code");
    acsOccupationCols.put("total_employed_population", "Total civilian employed population 16 years and over");
    acsOccupationCols.put("management_occupations", "Population employed in management, business, science, and arts occupations");
    acsOccupationCols.put("service_occupations", "Population employed in service occupations");
    CENSUS_COLUMN_COMMENTS.put("acs_occupation", acsOccupationCols);

    // economic_census table
    CENSUS_TABLE_COMMENTS.put("economic_census",
        "Economic Census data conducted every 5 years providing detailed statistics on business establishments, employment, payroll, "
        + "and sales by NAICS industry. Comprehensive survey of all business establishments critical for business planning, "
        + "economic development, and understanding industry structure.");

    Map<String, String> economicCensusCols = new HashMap<>();
    economicCensusCols.put("geoid", "Census geographic identifier");
    economicCensusCols.put("state_fips", "2-digit state FIPS code");
    economicCensusCols.put("naics_code", "NAICS industry classification code (2-6 digit)");
    economicCensusCols.put("establishments", "Number of business establishments in this industry");
    economicCensusCols.put("employment", "Total employment in this industry");
    economicCensusCols.put("annual_payroll", "Total annual payroll in thousands of dollars");
    CENSUS_COLUMN_COMMENTS.put("economic_census", economicCensusCols);

    // county_business_patterns table
    CENSUS_TABLE_COMMENTS.put("county_business_patterns",
        "Annual County Business Patterns data showing number of establishments, employment, and payroll by NAICS industry at county level. "
        + "Provides annual economic snapshots between economic censuses. Essential for business demographics, industry analysis, "
        + "and regional economic development planning.");

    Map<String, String> countyBusinessPatternsCols = new HashMap<>();
    countyBusinessPatternsCols.put("geoid", "Census geographic identifier");
    countyBusinessPatternsCols.put("state_fips", "2-digit state FIPS code");
    countyBusinessPatternsCols.put("naics_code", "NAICS industry classification code (2-6 digit)");
    countyBusinessPatternsCols.put("establishments", "Number of business establishments in this industry");
    countyBusinessPatternsCols.put("employment", "Total employment in this industry (mid-March payroll)");
    countyBusinessPatternsCols.put("annual_payroll", "Total annual payroll in thousands of dollars");
    CENSUS_COLUMN_COMMENTS.put("county_business_patterns", countyBusinessPatternsCols);
  }

  // =========================== ECON SCHEMA COMMENTS ===========================

  /** ECON schema table comments */
  private static final Map<String, String> ECON_TABLE_COMMENTS = new HashMap<>();

  /** ECON schema column comments */
  private static final Map<String, Map<String, String>> ECON_COLUMN_COMMENTS = new HashMap<>();

  static {
    // employment_statistics table
    ECON_TABLE_COMMENTS.put("employment_statistics",
        "U.S. employment and unemployment statistics from the Bureau of Labor Statistics (BLS). "
        + "Contains monthly data on unemployment rates, labor force participation, job openings, "
        + "and employment levels by sector. Essential for economic analysis and labor market trends.");

    Map<String, String> employmentStatsCols = new HashMap<>();
    employmentStatsCols.put("date", "Observation date (first day of month for monthly data)");
    employmentStatsCols.put("series_id", "BLS series identifier (e.g., 'LNS14000000' for unemployment rate)");
    employmentStatsCols.put("series_name", "Human-readable description of the data series");
    employmentStatsCols.put("value", "Metric value (percentage for rates, thousands for employment counts)");
    employmentStatsCols.put("unit", "Unit of measurement (percent, thousands, index)");
    employmentStatsCols.put("seasonally_adjusted", "Whether data is seasonally adjusted (true/false)");
    employmentStatsCols.put("percent_change_month", "Percent change from previous month");
    employmentStatsCols.put("percent_change_year", "Year-over-year percent change");
    employmentStatsCols.put("category", "Major category (Employment, Unemployment, Labor Force)");
    employmentStatsCols.put("subcategory", "Detailed subcategory (Manufacturing, Services, etc.)");
    ECON_COLUMN_COMMENTS.put("employment_statistics", employmentStatsCols);

    // inflation_metrics table
    ECON_TABLE_COMMENTS.put("inflation_metrics",
        "Consumer and Producer Price Index data from BLS tracking inflation across different "
        + "categories of goods and services. Includes CPI-U for urban consumers, CPI-W for wage earners, "
        + "and PPI for producer prices. Critical for understanding inflation trends and purchasing power.");

    Map<String, String> inflationMetricsCols = new HashMap<>();
    inflationMetricsCols.put("date", "Observation date for the index value");
    inflationMetricsCols.put("index_type", "Type of price index (CPI-U, CPI-W, PPI, etc.)");
    inflationMetricsCols.put("item_code", "BLS item code for specific good/service category");
    inflationMetricsCols.put("item_name", "Description of item or category (All items, Food, Energy, etc.)");
    inflationMetricsCols.put("index_value", "Index value with base period = 100");
    inflationMetricsCols.put("percent_change_month", "Month-over-month percent change");
    inflationMetricsCols.put("percent_change_year", "Year-over-year percent change (inflation rate)");
    inflationMetricsCols.put("area_code", "Geographic area code (U.S., regions, or metro areas)");
    inflationMetricsCols.put("area_name", "Geographic area name");
    inflationMetricsCols.put("seasonally_adjusted", "Whether data is seasonally adjusted");
    ECON_COLUMN_COMMENTS.put("inflation_metrics", inflationMetricsCols);

    // wage_growth table
    ECON_TABLE_COMMENTS.put("wage_growth",
        "Average earnings and compensation data by industry and occupation from BLS. "
        + "Tracks hourly and weekly earnings, employment cost index, and wage growth trends. "
        + "Essential for understanding labor cost pressures and income trends across sectors.");

    Map<String, String> wageGrowthCols = new HashMap<>();
    wageGrowthCols.put("date", "Observation date");
    wageGrowthCols.put("series_id", "BLS series identifier for wage data");
    wageGrowthCols.put("industry_code", "NAICS industry classification code");
    wageGrowthCols.put("industry_name", "Industry description");
    wageGrowthCols.put("occupation_code", "SOC occupation classification code");
    wageGrowthCols.put("occupation_name", "Occupation description");
    wageGrowthCols.put("average_hourly_earnings", "Average hourly earnings in dollars");
    wageGrowthCols.put("average_weekly_earnings", "Average weekly earnings in dollars");
    wageGrowthCols.put("employment_cost_index", "Employment cost index with base = 100");
    wageGrowthCols.put("percent_change_year", "Year-over-year percent change in earnings");
    ECON_COLUMN_COMMENTS.put("wage_growth", wageGrowthCols);

    // regional_employment table
    ECON_TABLE_COMMENTS.put("regional_employment",
        "State and metropolitan area employment statistics from BLS Local Area Unemployment Statistics "
        + "(LAUS). Provides unemployment rates, employment levels, and labor force participation by "
        + "geographic region. Critical for regional economic analysis and geographic comparisons.");

    Map<String, String> regionalEmploymentCols = new HashMap<>();
    regionalEmploymentCols.put("date", "Observation date");
    regionalEmploymentCols.put("area_code", "Geographic area code (FIPS or MSA code)");
    regionalEmploymentCols.put("area_name", "Geographic area name");
    regionalEmploymentCols.put("area_type", "Type of area (state, MSA, county)");
    regionalEmploymentCols.put("state_code", "Two-letter state abbreviation");
    regionalEmploymentCols.put("unemployment_rate", "Unemployment rate as percentage");
    regionalEmploymentCols.put("employment_level", "Number of employed persons");
    regionalEmploymentCols.put("labor_force", "Total labor force size");
    regionalEmploymentCols.put("participation_rate", "Labor force participation rate as percentage");
    regionalEmploymentCols.put("employment_population_ratio", "Employment to population ratio");
    ECON_COLUMN_COMMENTS.put("regional_employment", regionalEmploymentCols);

    // treasury_yields table
    ECON_TABLE_COMMENTS.put("treasury_yields",
        "Daily U.S. Treasury yield curve rates from Treasury Direct API. Provides yields for "
        + "various maturities from 1-month to 30-year securities. Essential benchmark for risk-free "
        + "rates, corporate bond pricing, and monetary policy analysis.");

    Map<String, String> treasuryYieldsCols = new HashMap<>();
    treasuryYieldsCols.put("date", "Observation date (YYYY-MM-DD)");
    treasuryYieldsCols.put("maturity_months", "Maturity period in months (1, 3, 6, 12, 24, 60, 120, 360)");
    treasuryYieldsCols.put("maturity_label", "Human-readable maturity (1M, 3M, 2Y, 10Y, 30Y)");
    treasuryYieldsCols.put("yield_percent", "Yield rate as annual percentage");
    treasuryYieldsCols.put("yield_type", "Type of yield (nominal, real/TIPS, average)");
    treasuryYieldsCols.put("source", "Data source (Treasury Direct)");
    ECON_COLUMN_COMMENTS.put("treasury_yields", treasuryYieldsCols);

    // federal_debt table
    ECON_TABLE_COMMENTS.put("federal_debt",
        "U.S. federal debt statistics from Treasury Fiscal Data API. Tracks daily debt levels "
        + "including debt held by public, intragovernmental holdings, and total outstanding debt. "
        + "Critical for fiscal policy analysis and understanding government financing needs.");

    Map<String, String> federalDebtCols = new HashMap<>();
    federalDebtCols.put("date", "Observation date");
    federalDebtCols.put("debt_type", "Type of debt measurement (Total, Held by Public, Intragovernmental)");
    federalDebtCols.put("amount_billions", "Debt amount in billions of dollars");
    federalDebtCols.put("holder_category", "Category of debt holder");
    federalDebtCols.put("debt_held_by_public", "Portion held by public in billions");
    federalDebtCols.put("intragovernmental_holdings", "Portion held by government accounts in billions");
    ECON_COLUMN_COMMENTS.put("federal_debt", federalDebtCols);

    // world_indicators table
    ECON_TABLE_COMMENTS.put("world_indicators",
        "International economic indicators from World Bank for major economies. Includes GDP, "
        + "inflation, unemployment, population, and government debt for G20 countries. Enables "
        + "international comparisons and global economic analysis.");

    Map<String, String> worldIndicatorsCols = new HashMap<>();
    worldIndicatorsCols.put("country_code", "ISO 3-letter country code (USA, CHN, DEU, etc.)");
    worldIndicatorsCols.put("country_name", "Full country name");
    worldIndicatorsCols.put("indicator_code", "World Bank indicator code (e.g., NY.GDP.MKTP.CD)");
    worldIndicatorsCols.put("indicator_name", "Indicator description");
    worldIndicatorsCols.put("year", "Observation year");
    worldIndicatorsCols.put("value", "Indicator value");
    worldIndicatorsCols.put("unit", "Unit of measurement (USD, percent, persons)");
    worldIndicatorsCols.put("scale", "Scale factor if applicable");
    ECON_COLUMN_COMMENTS.put("world_indicators", worldIndicatorsCols);

    // fred_indicators table
    ECON_TABLE_COMMENTS.put("fred_indicators",
        "Federal Reserve Economic Data (FRED) time series covering 800,000+ economic indicators. "
        + "Includes comprehensive coverage of Fed policy tools (EFFR, DFEDTARU, DFEDTARL), financial "
        + "conditions indices (NFCI, STLFSI), complete Treasury yield curve (DGS30, DGS5, DGS2, "
        + "T10Y2Y, T10Y3M), credit market spreads (BAMLH0A0HYM2, BAMLC0A1CAAAEY), international "
        + "exchange rates, commodity prices, and economic activity measures. Standardized format "
        + "across all federal data sources for comprehensive economic analysis.");

    Map<String, String> fredIndicatorsCols = new HashMap<>();
    fredIndicatorsCols.put("series_id", "FRED series identifier (DFF, GDP, UNRATE, etc.)");
    fredIndicatorsCols.put("series_name", "Human-readable series description");
    fredIndicatorsCols.put("date", "Observation date");
    fredIndicatorsCols.put("value", "Observation value");
    fredIndicatorsCols.put("units", "Unit of measurement");
    fredIndicatorsCols.put("frequency", "Data frequency (D=Daily, W=Weekly, M=Monthly, Q=Quarterly, A=Annual)");
    fredIndicatorsCols.put("seasonal_adjustment", "Seasonal adjustment method if applicable");
    fredIndicatorsCols.put("last_updated", "Timestamp when series was last updated");
    ECON_COLUMN_COMMENTS.put("fred_indicators", fredIndicatorsCols);

    // gdp_components table
    ECON_TABLE_COMMENTS.put("gdp_components",
        "Detailed GDP components from Bureau of Economic Analysis (BEA) NIPA tables. Breaks down "
        + "GDP into personal consumption, investment, government spending, and net exports. Provides "
        + "granular view of economic activity drivers and sectoral contributions to growth.");

    Map<String, String> gdpComponentsCols = new HashMap<>();
    gdpComponentsCols.put("table_id", "BEA NIPA table identifier");
    gdpComponentsCols.put("line_number", "Line number within NIPA table");
    gdpComponentsCols.put("line_description", "Component description (e.g., Personal Consumption, Fixed Investment)");
    gdpComponentsCols.put("series_code", "BEA series code");
    gdpComponentsCols.put("year", "Observation year");
    gdpComponentsCols.put("value", "Component value in billions of dollars");
    gdpComponentsCols.put("units", "Unit of measurement (typically billions of dollars)");
    gdpComponentsCols.put("frequency", "Data frequency (A=Annual, Q=Quarterly, M=Monthly)");
    ECON_COLUMN_COMMENTS.put("gdp_components", gdpComponentsCols);

    // regional_income table
    ECON_TABLE_COMMENTS.put("regional_income",
        "State and regional personal income statistics from BEA Regional Economic Accounts. "
        + "Includes total personal income, per capita income, and population by state. Essential "
        + "for understanding regional economic disparities and income trends across states.");

    Map<String, String> regionalIncomeCols = new HashMap<>();
    regionalIncomeCols.put("geo_fips", "Geographic FIPS code");
    regionalIncomeCols.put("geo_name", "Geographic area name (state or region)");
    regionalIncomeCols.put("metric", "Metric type (Total Income, Per Capita Income, Population)");
    regionalIncomeCols.put("line_code", "BEA line code");
    regionalIncomeCols.put("line_description", "Detailed description of the metric");
    regionalIncomeCols.put("year", "Observation year");
    regionalIncomeCols.put("value", "Metric value (dollars or persons)");
    regionalIncomeCols.put("units", "Unit of measurement");
    ECON_COLUMN_COMMENTS.put("regional_income", regionalIncomeCols);

    // state_gdp table
    ECON_TABLE_COMMENTS.put("state_gdp",
        "State-level GDP statistics from BEA Regional Economic Accounts (SAGDP2N table). "
        + "Provides both total GDP and per capita real GDP by state across all NAICS industry sectors. "
        + "Essential for understanding state economic output, productivity differences, and regional "
        + "economic performance comparisons across the United States.");

    Map<String, String> stateGdpCols = new HashMap<>();
    stateGdpCols.put("geo_fips", "FIPS code for state (e.g., '06' for California)");
    stateGdpCols.put("geo_name", "State name");
    stateGdpCols.put("line_code", "BEA line code (1=Total GDP, 2=Per capita GDP)");
    stateGdpCols.put("line_description", "Description of the metric (All industry total or Per capita real GDP)");
    stateGdpCols.put("year", "Year of the GDP measurement");
    stateGdpCols.put("value", "GDP value (in millions for total, in chained dollars for per capita)");
    stateGdpCols.put("units", "Unit of measurement");
    ECON_COLUMN_COMMENTS.put("state_gdp", stateGdpCols);

    // trade_statistics table
    ECON_TABLE_COMMENTS.put("trade_statistics",
        "Detailed U.S. export and import statistics from BEA NIPA Table T40205B. Provides "
        + "comprehensive breakdown of goods and services trade by category including foods, "
        + "industrial supplies, capital goods, automotive, and consumer goods. Includes calculated "
        + "trade balances for matching export/import pairs. Essential for trade policy analysis.");

    Map<String, String> tradeStatsCols = new HashMap<>();
    tradeStatsCols.put("table_id", "BEA NIPA table identifier (T40205B)");
    tradeStatsCols.put("line_number", "Line number within the NIPA table");
    tradeStatsCols.put("line_description", "Detailed description of trade category");
    tradeStatsCols.put("series_code", "BEA series code for this trade component");
    tradeStatsCols.put("year", "Observation year");
    tradeStatsCols.put("value", "Trade value in billions of dollars");
    tradeStatsCols.put("units", "Unit of measurement (billions of dollars)");
    tradeStatsCols.put("frequency", "Data frequency (A=Annual)");
    tradeStatsCols.put("trade_type", "Type of trade flow (Exports, Imports, or Other)");
    tradeStatsCols.put("category", "Parsed trade category (Goods, Services, Food, Capital Goods, etc.)");
    tradeStatsCols.put("trade_balance", "Calculated trade balance (exports minus imports) for category");
    ECON_COLUMN_COMMENTS.put("trade_statistics", tradeStatsCols);

    // ita_data table
    ECON_TABLE_COMMENTS.put("ita_data",
        "International Transactions Accounts (ITA) from BEA providing comprehensive balance "
        + "of payments statistics. Includes trade balance, current account balance, capital "
        + "account flows, and primary/secondary income balances. Critical for understanding "
        + "international financial flows and the U.S. position in global markets.");

    Map<String, String> itaDataCols = new HashMap<>();
    itaDataCols.put("indicator", "ITA indicator code (e.g., BalGds, BalCurrAcct, BalCapAcct)");
    itaDataCols.put("indicator_description", "Human-readable description of the indicator");
    itaDataCols.put("area_or_country", "Geographic scope (typically 'AllCountries' for aggregates)");
    itaDataCols.put("frequency", "Data frequency (A=Annual, Q=Quarterly)");
    itaDataCols.put("year", "Observation year");
    itaDataCols.put("value", "Balance value in millions of USD (negative = deficit)");
    itaDataCols.put("units", "Unit of measurement (USD Millions)");
    itaDataCols.put("time_series_id", "BEA time series identifier");
    itaDataCols.put("time_series_description", "Detailed time series description");
    ECON_COLUMN_COMMENTS.put("ita_data", itaDataCols);

    // industry_gdp table
    ECON_TABLE_COMMENTS.put("industry_gdp",
        "GDP by Industry data from BEA showing value added by NAICS industry sectors. "
        + "Provides comprehensive breakdown of economic output by industry including "
        + "agriculture, mining, manufacturing, services, and government sectors. Available "
        + "at both annual and quarterly frequencies for detailed sectoral analysis.");

    Map<String, String> industryGdpCols = new HashMap<>();
    industryGdpCols.put("table_id", "BEA GDPbyIndustry table identifier");
    industryGdpCols.put("frequency", "Data frequency (A=Annual, Q=Quarterly)");
    industryGdpCols.put("year", "Observation year");
    industryGdpCols.put("quarter", "Quarter for quarterly data (Q1-Q4) or year for annual");
    industryGdpCols.put("industry_code", "NAICS industry classification code");
    industryGdpCols.put("industry_description", "Industry sector description");
    industryGdpCols.put("value", "Value added by industry in billions of dollars");
    industryGdpCols.put("units", "Unit of measurement (billions of dollars)");
    industryGdpCols.put("note_ref", "Reference to explanatory notes if applicable");
    ECON_COLUMN_COMMENTS.put("industry_gdp", industryGdpCols);

    // gdp_statistics table
    ECON_TABLE_COMMENTS.put("gdp_statistics",
        "Gross Domestic Product statistics from Bureau of Economic Analysis including nominal GDP, real GDP, "
        + "and major components (personal consumption, private investment, government spending, net exports). "
        + "Provides annual and quarterly data with both levels and percent changes. Essential for macroeconomic analysis "
        + "and understanding overall economic growth trends.");

    Map<String, String> gdpStatisticsCols = new HashMap<>();
    gdpStatisticsCols.put("year", "Year of the GDP observation");
    gdpStatisticsCols.put("quarter", "Quarter number (1-4) for quarterly data, null for annual data");
    gdpStatisticsCols.put("metric", "GDP metric name (e.g., 'Nominal GDP', 'Real GDP', 'Personal Consumption', 'Private Investment', 'Government Spending', 'Net Exports')");
    gdpStatisticsCols.put("value", "GDP value in millions of dollars");
    gdpStatisticsCols.put("percent_change", "Percent change from previous period (quarter-over-quarter for quarterly data, year-over-year for annual)");
    gdpStatisticsCols.put("seasonally_adjusted", "Whether data is seasonally adjusted ('Yes' or 'No')");
    ECON_COLUMN_COMMENTS.put("gdp_statistics", gdpStatisticsCols);

    // fred_data_series_catalog table
    ECON_TABLE_COMMENTS.put("fred_data_series_catalog",
        "Comprehensive catalog of all 800,000+ economic data series available from Federal Reserve Economic Data (FRED). "
        + "Contains metadata for each series including title, description, units, frequency, source agency, and category. "
        + "Enables discovery of economic indicators across national accounts, labor markets, prices, international data, "
        + "banking, and regional statistics. Essential for identifying relevant time series for research and analysis.");

    Map<String, String> fredDataSeriesCatalogCols = new HashMap<>();
    fredDataSeriesCatalogCols.put("series_id", "Unique FRED series identifier (e.g., 'UNRATE' for unemployment rate, 'GDP' for gross domestic product)");
    fredDataSeriesCatalogCols.put("title", "Full descriptive title of the economic data series");
    fredDataSeriesCatalogCols.put("observation_start", "Date of first available observation in ISO 8601 format (YYYY-MM-DD)");
    fredDataSeriesCatalogCols.put("observation_end", "Date of most recent observation in ISO 8601 format (YYYY-MM-DD)");
    fredDataSeriesCatalogCols.put("frequency", "Data frequency: 'Daily', 'Weekly', 'Biweekly', 'Monthly', 'Quarterly', 'Semiannual', 'Annual'");
    fredDataSeriesCatalogCols.put("frequency_short", "Abbreviated frequency code: 'D' (Daily), 'W' (Weekly), 'BW' (Biweekly), 'M' (Monthly), 'Q' (Quarterly), 'SA' (Semiannual), 'A' (Annual)");
    fredDataSeriesCatalogCols.put("units", "Units of measurement (e.g., 'Percent', 'Billions of Dollars', 'Index 1982-1984=100', 'Thousands of Persons')");
    fredDataSeriesCatalogCols.put("units_short", "Abbreviated units code");
    fredDataSeriesCatalogCols.put("seasonal_adjustment", "Seasonal adjustment status: 'Seasonally Adjusted', 'Not Seasonally Adjusted'");
    fredDataSeriesCatalogCols.put("seasonal_adjustment_short", "Abbreviated seasonal adjustment code: 'SA' (Seasonally Adjusted), 'NSA' (Not Seasonally Adjusted)");
    fredDataSeriesCatalogCols.put("last_updated", "Timestamp of last data update in ISO 8601 format");
    fredDataSeriesCatalogCols.put("popularity", "FRED popularity score indicating relative usage and interest level (higher = more popular)");
    fredDataSeriesCatalogCols.put("group_popularity", "Popularity score within the series' category group");
    fredDataSeriesCatalogCols.put("notes", "Detailed description including methodology, calculation details, source information, and important caveats");
    fredDataSeriesCatalogCols.put("category_id", "FRED category identifier for hierarchical classification (e.g., 32992 for 'Money, Banking, & Finance')");
    fredDataSeriesCatalogCols.put("category_name", "Human-readable category name (e.g., 'National Accounts', 'Labor Markets', 'Population, Employment, & Labor Markets')");
    fredDataSeriesCatalogCols.put("source_id", "Identifier for the originating data source agency or organization");
    fredDataSeriesCatalogCols.put("source_name", "Name of originating agency (e.g., 'U.S. Bureau of Labor Statistics', 'Board of Governors of the Federal Reserve System')");
    fredDataSeriesCatalogCols.put("series_status", "Series status: 'active' (currently updated with new observations) or 'discontinued' (no longer maintained)");
    ECON_COLUMN_COMMENTS.put("fred_data_series_catalog", fredDataSeriesCatalogCols);
  }

  // =========================== PUBLIC API METHODS ===========================

  /**
   * Gets the business definition comment for a SEC schema table.
   *
   * @param tableName name of the table (case-insensitive)
   * @return table comment or null if not found
   */
  public static @Nullable String getSecTableComment(String tableName) {
    return SEC_TABLE_COMMENTS.get(tableName.toLowerCase());
  }

  /**
   * Gets the business definition comment for a SEC schema column.
   *
   * @param tableName name of the table (case-insensitive)
   * @param columnName name of the column (case-insensitive)
   * @return column comment or null if not found
   */
  public static @Nullable String getSecColumnComment(String tableName, String columnName) {
    Map<String, String> tableColumns = SEC_COLUMN_COMMENTS.get(tableName.toLowerCase());
    return tableColumns != null ? tableColumns.get(columnName.toLowerCase()) : null;
  }

  /**
   * Gets all column comments for a SEC schema table.
   *
   * @param tableName name of the table (case-insensitive)
   * @return map of column names to comments, or empty map if table not found
   */
  public static Map<String, String> getSecColumnComments(String tableName) {
    Map<String, String> comments = SEC_COLUMN_COMMENTS.get(tableName.toLowerCase());
    return comments != null ? new HashMap<>(comments) : new HashMap<>();
  }

  /**
   * Gets the business definition comment for a GEO schema table.
   *
   * @param tableName name of the table (case-insensitive)
   * @return table comment or null if not found
   */
  public static @Nullable String getGeoTableComment(String tableName) {
    return GEO_TABLE_COMMENTS.get(tableName.toLowerCase());
  }

  /**
   * Gets the business definition comment for a GEO schema column.
   *
   * @param tableName name of the table (case-insensitive)
   * @param columnName name of the column (case-insensitive)
   * @return column comment or null if not found
   */
  public static @Nullable String getGeoColumnComment(String tableName, String columnName) {
    Map<String, String> tableColumns = GEO_COLUMN_COMMENTS.get(tableName.toLowerCase());
    return tableColumns != null ? tableColumns.get(columnName.toLowerCase()) : null;
  }

  /**
   * Gets all column comments for a GEO schema table.
   *
   * @param tableName name of the table (case-insensitive)
   * @return map of column names to comments, or empty map if table not found
   */
  public static Map<String, String> getGeoColumnComments(String tableName) {
    Map<String, String> comments = GEO_COLUMN_COMMENTS.get(tableName.toLowerCase());
    return comments != null ? new HashMap<>(comments) : new HashMap<>();
  }

  /**
   * Gets the business definition comment for an ECON schema table.
   *
   * @param tableName name of the table (case-insensitive)
   * @return table comment or null if not found
   */
  public static @Nullable String getEconTableComment(String tableName) {
    return ECON_TABLE_COMMENTS.get(tableName.toLowerCase());
  }

  /**
   * Gets the business definition comment for an ECON schema column.
   *
   * @param tableName name of the table (case-insensitive)
   * @param columnName name of the column (case-insensitive)
   * @return column comment or null if not found
   */
  public static @Nullable String getEconColumnComment(String tableName, String columnName) {
    Map<String, String> tableColumns = ECON_COLUMN_COMMENTS.get(tableName.toLowerCase());
    return tableColumns != null ? tableColumns.get(columnName.toLowerCase()) : null;
  }

  /**
   * Gets all column comments for an ECON schema table.
   *
   * @param tableName name of the table (case-insensitive)
   * @return map of column names to comments, or empty map if table not found
   */
  public static Map<String, String> getEconColumnComments(String tableName) {
    Map<String, String> comments = ECON_COLUMN_COMMENTS.get(tableName.toLowerCase());
    return comments != null ? new HashMap<>(comments) : new HashMap<>();
  }
}
