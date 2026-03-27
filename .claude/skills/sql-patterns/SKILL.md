---
description: Standard SQL patterns supported by this project, known edge cases in validation, and dialect handling
model: haiku
effort: low
---

# SQL Patterns Guide

Reference when working on SQL-related code for $ARGUMENTS.

## SQL Conventions (This Project)

Default parser config:
- **Lex**: `ORACLE` — double-quote for identifiers, single-quote for strings
- **Unquoted casing**: `TO_LOWER` — unquoted identifiers become lowercase
- **Case sensitive**: `false`
- **Name generation**: `SMART_CASING`

```sql
-- These are equivalent (case insensitive, unquoted → lowercase)
SELECT Name FROM Employees
SELECT name FROM employees
SELECT "name" FROM "employees"

-- This is different (quoted preserves case)
SELECT "Name" FROM employees  -- Column "Name" (uppercase N)
```

## Supported SQL Patterns

### Standard queries
```sql
SELECT col1, COUNT(*) AS cnt FROM t GROUP BY col1 HAVING cnt > 5
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id
SELECT * FROM t WHERE x IN (SELECT y FROM t2)
SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY sal DESC) FROM emp
```

### Calcite-specific
```sql
-- Correlated subqueries (handled by SubQueryRemoveRule)
SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)

-- LATERAL joins
SELECT * FROM t, LATERAL TABLE(my_function(t.col))

-- UNNEST
SELECT * FROM t, UNNEST(t.array_col) AS u(element)
```

## Dialect Handling

```java
// Get dialect for SQL generation (adapter pushdown)
SqlDialect dialect = SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();

// Generate SQL string from RelNode (for JDBC adapters)
SqlNode sqlNode = new RelToSqlConverter(dialect).visitRoot(relNode);
String sql = sqlNode.toSqlString(dialect).getSql();
```

### Dialect registry
- `OracleSqlDialect`, `PostgresqlSqlDialect`, `MssqlSqlDialect`
- `PrestoSqlDialect`, `SnowflakeSqlDialect`, `SparkSqlDialect`
- Each defines quoting, type mappings, function translations

## Known Edge Cases

1. **TIMESTAMP literal format**: Calcite expects `TIMESTAMP '2024-01-01 00:00:00'` (with keyword)
2. **NULL in IN list**: `x IN (1, 2, NULL)` may not match NULL — use `IS NULL` separately
3. **Integer division**: `5/2 = 2` (integer division). Use `CAST(5 AS DOUBLE)/2` for 2.5
4. **LIKE escape**: Default escape is backslash. Use `LIKE pattern ESCAPE '\'` to be explicit
5. **Aggregate without GROUP BY**: `SELECT COUNT(*) FROM t` is valid; `SELECT x, COUNT(*) FROM t` is NOT (missing GROUP BY)
6. **Reserved words as identifiers**: Must be quoted: `SELECT "year", "type" FROM t`

## Common Validations Failures

```
"Column 'X' not found" → Check casing, may need quotes
"Table 'X' not found" → Check schema path, default schema
"Cannot apply '>' to arguments" → Type mismatch, check column types
"Aggregate expression not allowed" → Missing GROUP BY
```
