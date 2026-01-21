/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */

/**
 * XBRL adapter for Apache Calcite that processes SEC EDGAR filings.
 *
 * <p>This adapter extends the file adapter to provide specialized support for
 * XBRL (eXtensible Business Reporting Language) documents, particularly those
 * from the SEC EDGAR database. It converts XBRL instance documents and taxonomies
 * into partitioned Parquet files for efficient analytical querying.
 *
 * <h2>Key Features</h2>
 * <ul>
 *   <li>Automatic XBRL to Parquet conversion</li>
 *   <li>CIK/filing-type/date partitioning for efficient queries</li>
 *   <li>Financial statement line item extraction with hierarchy</li>
 *   <li>Footnote linking to financial statement items</li>
 *   <li>Company metadata extraction</li>
 *   <li>Support for all major SEC filing types (10-K, 10-Q, 8-K, etc.)</li>
 * </ul>
 *
 * <h2>Architecture</h2>
 * <p>The adapter consists of:
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.govdata.GovDataSchemaFactory} - Entry point that extends FileSchemaFactory</li>
 *   <li>{@link org.apache.calcite.adapter.govdata.sec.XbrlToParquetConverter} - Converts XBRL to Parquet</li>
 *   <li>{@link org.apache.calcite.adapter.sec.EdgarPartitionStrategy} - Implements CIK/type/date partitioning</li>
 * </ul>
 *
 * <h2>Configuration Example</h2>
 * <pre>{@code
 * {
 *   "version": "1.0",
 *   "defaultSchema": "EDGAR",
 *   "schemas": [{
 *     "name": "EDGAR",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *     "operand": {
 *       "directory": "/data/edgar",
 *       "executionEngine": "duckdb",
 *       "enableSecProcessing": true,
 *       "secSourceDirectory": "/data/edgar/sec",
 *       "processSecOnInit": true,
 *       "partitionedTables": [
 *         {
 *           "name": "financial_line_items",
 *           "partitionColumns": ["cik", "filing_type", "filing_date"]
 *         }
 *       ]
 *     }
 *   }]
 * }
 * }</pre>
 *
 * <h2>Query Examples</h2>
 * <pre>{@code
 * -- Query Apple's revenue over time
 * SELECT filing_date, value as revenue
 * FROM financial_line_items
 * WHERE cik = '0000320193'
 *   AND concept = 'Revenue'
 *   AND filing_type = '10-K'
 * ORDER BY filing_date;
 *
 * -- Find companies with highest net income
 * SELECT c.company_name, f.value as net_income
 * FROM financial_line_items f
 * JOIN company_info c ON f.cik = c.cik
 * WHERE f.concept = 'NetIncome'
 *   AND f.filing_type = '10-K'
 *   AND f.filing_date >= '2023-01-01'
 * ORDER BY f.value DESC
 * LIMIT 10;
 * }</pre>
 */
package org.apache.calcite.adapter.govdata.sec;
