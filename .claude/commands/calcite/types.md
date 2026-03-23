---
description: RelDataType system, type inference, nullable handling, Java-to-SQL type mapping in this project
---

# Calcite Type System Guide

Reference when working on type-related code for $ARGUMENTS.

## Building Row Types

```java
// Standard pattern
RelDataType rowType = typeFactory.builder()
    .add("id", SqlTypeName.INTEGER)
    .add("name", SqlTypeName.VARCHAR, 255)        // With precision
    .add("amount", SqlTypeName.DECIMAL, 10, 2)    // precision, scale
    .add("created", SqlTypeName.TIMESTAMP)
    .add("data", SqlTypeName.ANY)                  // Dynamic type
    .build();

// With nullability
RelDataType nullable = typeFactory.createTypeWithNullability(
    typeFactory.createSqlType(SqlTypeName.INTEGER), true);

// Add nullable column to builder
builder.add("optional_col", nullable);
```

## Common SqlTypeName Mappings

| Java Type | SqlTypeName | Notes |
|-----------|-------------|-------|
| `int`/`Integer` | `INTEGER` | |
| `long`/`Long` | `BIGINT` | |
| `double`/`Double` | `DOUBLE` | |
| `float`/`Float` | `REAL` | |
| `BigDecimal` | `DECIMAL` | Requires precision/scale |
| `String` | `VARCHAR` | Optionally with max length |
| `boolean`/`Boolean` | `BOOLEAN` | |
| `java.sql.Date` | `DATE` | |
| `java.sql.Time` | `TIME` | Use java.sql.LocalTime (not deprecated Time) |
| `java.sql.Timestamp` | `TIMESTAMP` | |
| `byte[]` | `VARBINARY` | |

## Type Inference Pattern (CSV/JSON sources)

From `file/src/.../format/csv/CsvTypeInferrer.java`:

```java
// Sample rows to infer column types
// Priority order:
// 1. Try DateTimeFormatter patterns → TIMESTAMP/DATE/TIME
// 2. Try BigDecimal parsing → DECIMAL
// 3. Try Double parsing → DOUBLE
// 4. Fallback → VARCHAR

private SqlTypeName inferType(String value) {
  if (isTimestamp(value)) return SqlTypeName.TIMESTAMP;
  if (isDate(value)) return SqlTypeName.DATE;
  try { new BigDecimal(value); return SqlTypeName.DECIMAL; }
  catch (NumberFormatException e) { /* not numeric */ }
  try { Double.parseDouble(value); return SqlTypeName.DOUBLE; }
  catch (NumberFormatException e) { /* not numeric */ }
  return SqlTypeName.VARCHAR;
}
```

## Nullable Handling

```java
// RULE: All columns nullable by default unless explicitly non-null
// This matches SQL standard behavior

// Make a type nullable
RelDataType type = typeFactory.createSqlType(SqlTypeName.INTEGER);
RelDataType nullableType = typeFactory.createTypeWithNullability(type, true);

// Check nullability
boolean isNullable = type.isNullable();

// In type comparison, nullability matters:
// INTEGER NOT NULL ≠ INTEGER (nullable)
// Use SqlTypeUtil.equalSansNullability() to compare ignoring nullability
```

## Type Coercion

```java
// Calcite handles implicit coercion, but sometimes you need explicit:
import org.apache.calcite.sql.type.SqlTypeUtil;

// Check assignability
boolean ok = SqlTypeUtil.canAssignFrom(targetType, sourceType);

// Least restrictive type (for UNION, CASE)
RelDataType common = typeFactory.leastRestrictive(types);
```

## Common Mistakes

1. **Forgetting nullability**: Default `createSqlType()` creates non-nullable. Wrap with `createTypeWithNullability(type, true)` for nullable columns.
2. **VARCHAR without length**: Omitting length creates unlimited VARCHAR, which may cause issues with some adapters.
3. **TIMESTAMP vs DATE**: Don't use TIMESTAMP for date-only values — it affects comparison and partition pruning.
4. **Using deprecated java.sql.Time**: Use `java.sql.LocalTime` instead.
5. **Decimal precision**: Always specify precision and scale for DECIMAL types.
