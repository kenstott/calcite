!connect jdbc:calcite:model=/Users/kennethstott/calcite/test-apple-s3.json;lex=ORACLE;unquotedCasing=TO_LOWER "" ""
!tables
SELECT COUNT(*) FROM financial_line_items WHERE cik = '0000320193';
!quit
