#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env.prod"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found" >&2
  exit 1
fi

# Export credentials so subprocesses see them
while IFS='=' read -r key value; do
  [[ "$key" =~ ^[[:space:]]*# ]] && continue
  [[ -z "$key" ]] && continue
  key="${key// /}"
  export "$key=$value"
done < <(grep -E '^[A-Z_]+=.' "$ENV_FILE")

BASE="s3://govdata-parquet-v1/sec"

# Strip https:// — DuckDB CREATE SECRET ENDPOINT expects hostname only
ENDPOINT="${AWS_ENDPOINT_OVERRIDE#https://}"

# DuckDB S3 config via CREATE SECRET with REGION 'auto' (required for Cloudflare R2)
S3_SETUP="
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;
CREATE OR REPLACE SECRET r2 (
  TYPE S3,
  KEY_ID '${AWS_ACCESS_KEY_ID}',
  SECRET '${AWS_SECRET_ACCESS_KEY}',
  ENDPOINT '${ENDPOINT}',
  REGION 'auto',
  USE_SSL true
);
SET unsafe_enable_version_guessing = true;
"

run_duckdb() {
  duckdb -c "${S3_SETUP}
$1
" 2>&1
}

echo "============================================================"
echo "  SEC Iceberg Data Quality Spot Check"
echo "  Warehouse: ${BASE}"
echo "  Date: $(date)"
echo "============================================================"
echo ""

# ── filing_metadata ──────────────────────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "TABLE: filing_metadata"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
run_duckdb "
CREATE VIEW fm AS SELECT * FROM iceberg_scan('${BASE}/filing_metadata');

SELECT 'total rows'    AS metric, count(*)::VARCHAR           AS value FROM fm
UNION ALL
SELECT 'distinct years', count(DISTINCT year)::VARCHAR        FROM fm
UNION ALL
SELECT 'year range',     min(year)::VARCHAR||' - '||max(year)::VARCHAR FROM fm
UNION ALL
SELECT 'distinct types', count(DISTINCT filing_type)::VARCHAR FROM fm;

SELECT year, count(*) AS filings, count(DISTINCT filing_type) AS form_types
FROM fm GROUP BY year ORDER BY year;

SELECT filing_type, count(*) AS cnt FROM fm GROUP BY filing_type ORDER BY cnt DESC LIMIT 10;

SELECT col, null_pct FROM (
  SELECT 'cik'              AS col, round(100.0*sum(CASE WHEN cik IS NULL THEN 1 ELSE 0 END)/count(*),2) AS null_pct FROM fm
  UNION ALL SELECT 'accession_number', round(100.0*sum(CASE WHEN accession_number IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM fm
  UNION ALL SELECT 'filing_type',      round(100.0*sum(CASE WHEN filing_type IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM fm
  UNION ALL SELECT 'filing_date',      round(100.0*sum(CASE WHEN filing_date IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM fm
  UNION ALL SELECT 'year',             round(100.0*sum(CASE WHEN year IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM fm
  UNION ALL SELECT 'company_name',     round(100.0*sum(CASE WHEN company_name IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM fm
  UNION ALL SELECT 'period_of_report', round(100.0*sum(CASE WHEN period_of_report IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM fm
  UNION ALL SELECT 'ticker',           round(100.0*sum(CASE WHEN ticker IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM fm
  UNION ALL SELECT 'sic_code',         round(100.0*sum(CASE WHEN sic_code IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM fm
  UNION ALL SELECT 'primary_document', round(100.0*sum(CASE WHEN primary_document IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM fm
) ORDER BY null_pct DESC;

SELECT cik, accession_number, filing_type, filing_date, year, company_name, ticker
FROM fm ORDER BY random() LIMIT 5;
"
echo ""

# ── insider_transactions ─────────────────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "TABLE: insider_transactions"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
run_duckdb "
CREATE VIEW it AS SELECT * FROM iceberg_scan('${BASE}/insider_transactions');

SELECT 'total rows'    AS metric, count(*)::VARCHAR           AS value FROM it
UNION ALL
SELECT 'year range',     min(year)::VARCHAR||' - '||max(year)::VARCHAR FROM it
UNION ALL
SELECT 'distinct types', count(DISTINCT filing_type)::VARCHAR FROM it;

SELECT year, count(*) AS rows FROM it GROUP BY year ORDER BY year;

SELECT transaction_code, count(*) AS cnt FROM it GROUP BY transaction_code ORDER BY cnt DESC LIMIT 10;

SELECT col, null_pct FROM (
  SELECT 'reporting_person_name' AS col, round(100.0*sum(CASE WHEN reporting_person_name IS NULL THEN 1 ELSE 0 END)/count(*),2) AS null_pct FROM it
  UNION ALL SELECT 'transaction_date',  round(100.0*sum(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM it
  UNION ALL SELECT 'transaction_code',  round(100.0*sum(CASE WHEN transaction_code IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM it
  UNION ALL SELECT 'shares_transacted', round(100.0*sum(CASE WHEN shares_transacted IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM it
  UNION ALL SELECT 'price_per_share',   round(100.0*sum(CASE WHEN price_per_share IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM it
  UNION ALL SELECT 'shares_owned_after',round(100.0*sum(CASE WHEN shares_owned_after IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM it
  UNION ALL SELECT 'is_director',       round(100.0*sum(CASE WHEN is_director IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM it
  UNION ALL SELECT 'is_officer',        round(100.0*sum(CASE WHEN is_officer IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM it
  UNION ALL SELECT 'officer_title',     round(100.0*sum(CASE WHEN officer_title IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM it
  UNION ALL SELECT 'security_title',    round(100.0*sum(CASE WHEN security_title IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM it
) ORDER BY null_pct DESC;

SELECT cik, reporting_person_name, filing_type, transaction_code, transaction_date,
       shares_transacted, price_per_share
FROM it ORDER BY random() LIMIT 5;
"
echo ""

# ── vectorized_chunks ────────────────────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "TABLE: vectorized_chunks"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
run_duckdb "
CREATE VIEW vc AS SELECT * FROM iceberg_scan('${BASE}/vectorized_chunks');

SELECT 'total rows'     AS metric, count(*)::VARCHAR           AS value FROM vc
UNION ALL
SELECT 'year range',      min(year)::VARCHAR||' - '||max(year)::VARCHAR FROM vc
UNION ALL
SELECT 'with embedding',  sum(CASE WHEN embedding IS NOT NULL THEN 1 ELSE 0 END)::VARCHAR FROM vc
UNION ALL
SELECT 'null embedding',  sum(CASE WHEN embedding IS NULL THEN 1 ELSE 0 END)::VARCHAR FROM vc
UNION ALL
SELECT 'embed null %',    round(100.0*sum(CASE WHEN embedding IS NULL THEN 1 ELSE 0 END)/count(*),2)::VARCHAR||'%' FROM vc;

SELECT year, count(*) AS chunks,
       sum(CASE WHEN embedding IS NOT NULL THEN 1 ELSE 0 END) AS with_embed,
       round(100.0*sum(CASE WHEN embedding IS NOT NULL THEN 1 ELSE 0 END)/count(*),1) AS embed_pct
FROM vc GROUP BY year ORDER BY year;

SELECT source_type, count(*) AS chunks,
       round(100.0*sum(CASE WHEN embedding IS NOT NULL THEN 1 ELSE 0 END)/count(*),1) AS embed_pct
FROM vc GROUP BY source_type ORDER BY chunks DESC LIMIT 15;

SELECT array_length(embedding) AS dim, count(*) AS cnt
FROM vc WHERE embedding IS NOT NULL
GROUP BY dim ORDER BY cnt DESC LIMIT 5;

SELECT col, null_pct FROM (
  SELECT 'chunk_id'          AS col, round(100.0*sum(CASE WHEN chunk_id IS NULL THEN 1 ELSE 0 END)/count(*),2) AS null_pct FROM vc
  UNION ALL SELECT 'source_type',     round(100.0*sum(CASE WHEN source_type IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM vc
  UNION ALL SELECT 'section',         round(100.0*sum(CASE WHEN section IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM vc
  UNION ALL SELECT 'chunk_text',      round(100.0*sum(CASE WHEN chunk_text IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM vc
  UNION ALL SELECT 'enriched_text',   round(100.0*sum(CASE WHEN enriched_text IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM vc
  UNION ALL SELECT 'content_type',    round(100.0*sum(CASE WHEN content_type IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM vc
  UNION ALL SELECT 'financial_concepts', round(100.0*sum(CASE WHEN financial_concepts IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM vc
  UNION ALL SELECT 'speaker_name',    round(100.0*sum(CASE WHEN speaker_name IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM vc
  UNION ALL SELECT 'speaker_role',    round(100.0*sum(CASE WHEN speaker_role IS NULL THEN 1 ELSE 0 END)/count(*),2) FROM vc
) ORDER BY null_pct DESC;

SELECT cik, source_type, section,
       substring(chunk_text, 1, 100) AS text_preview,
       CASE WHEN embedding IS NULL THEN 'NULL'
            ELSE '[' || ROUND(embedding[1]::DECIMAL, 6)::VARCHAR || ', '
                     || ROUND(embedding[2]::DECIMAL, 6)::VARCHAR || ', ... ('
                     || array_length(embedding)::VARCHAR || ' dims)]'
       END AS embedding_preview
FROM vc WHERE embedding IS NOT NULL ORDER BY random() LIMIT 5;
"
echo ""

# ── Other tables: filing_contexts, financial_line_items, mda_sections, xbrl_relationships ────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "OTHER TABLES: filing_contexts / financial_line_items / mda_sections / xbrl_relationships / earnings_transcripts"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
run_duckdb "
SELECT 'filing_contexts'      AS table_name, count(*) AS rows, count(DISTINCT year) AS years
FROM iceberg_scan('${BASE}/filing_contexts')
UNION ALL
SELECT 'financial_line_items', count(*), count(DISTINCT year)
FROM iceberg_scan('${BASE}/financial_line_items')
UNION ALL
SELECT 'mda_sections',         count(*), count(DISTINCT year)
FROM iceberg_scan('${BASE}/mda_sections')
UNION ALL
SELECT 'xbrl_relationships',   count(*), count(DISTINCT year)
FROM iceberg_scan('${BASE}/xbrl_relationships')
UNION ALL
SELECT 'earnings_transcripts',  count(*), count(DISTINCT year)
FROM iceberg_scan('${BASE}/earnings_transcripts');
"
echo ""

echo "============================================================"
echo "  SUMMARY"
echo "  Warehouse: ${BASE}"
echo "  Tables checked: filing_metadata, insider_transactions, vectorized_chunks,"
echo "                  filing_contexts, financial_line_items, mda_sections,"
echo "                  xbrl_relationships, earnings_transcripts"
echo "============================================================"
