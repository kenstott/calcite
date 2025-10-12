!connect jdbc:calcite:model=/Users/kennethstott/calcite/djia-production-model.json;lex=ORACLE;unquotedCasing=TO_LOWER admin admin

-- Check if insider_transactions table exists and has data
SELECT COUNT(*) as row_count FROM sec.insider_transactions;

!quit
