!connect jdbc:calcite:model=/Users/kennethstott/calcite/djia-production-model.json;lex=ORACLE;unquotedCasing=TO_LOWER admin admin

-- Test FINANCIAL_LINE_ITEMS
SELECT COUNT(*) as row_count FROM sec.financial_line_items;
SELECT * FROM sec.financial_line_items LIMIT 5;

-- Test FILING_METADATA
SELECT COUNT(*) as row_count FROM sec.filing_metadata;
SELECT * FROM sec.filing_metadata LIMIT 5;

-- Test FILING_CONTEXTS
SELECT COUNT(*) as row_count FROM sec.filing_contexts;
SELECT * FROM sec.filing_contexts LIMIT 5;

-- Test MDA_SECTIONS
SELECT COUNT(*) as row_count FROM sec.mda_sections;
SELECT * FROM sec.mda_sections LIMIT 5;

-- Test XBRL_RELATIONSHIPS
SELECT COUNT(*) as row_count FROM sec.xbrl_relationships;
SELECT * FROM sec.xbrl_relationships LIMIT 5;

-- Test INSIDER_TRANSACTIONS
SELECT COUNT(*) as row_count FROM sec.insider_transactions;
SELECT * FROM sec.insider_transactions LIMIT 5;

-- Test EARNINGS_TRANSCRIPTS
SELECT COUNT(*) as row_count FROM sec.earnings_transcripts;
SELECT * FROM sec.earnings_transcripts LIMIT 5;

-- Test STOCK_PRICES
SELECT COUNT(*) as row_count FROM sec.stock_prices;
SELECT * FROM sec.stock_prices LIMIT 5;

-- Test VECTORIZED_BLOBS
SELECT COUNT(*) as row_count FROM sec.vectorized_blobs;
SELECT * FROM sec.vectorized_blobs LIMIT 5;

!quit
