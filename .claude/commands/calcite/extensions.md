---
description: How this project extends Calcite — custom operators, functions, aggregates, and where they are registered
---

# Calcite Extensions Guide

Reference when extending Calcite functionality for $ARGUMENTS.

## Extension Points This Project Uses

### 1. Custom Planner Rules
Location: Each adapter's `rules/` directory
Registration: Via `Convention.register(RelOptPlanner)` or explicit `planner.addRule()`

```java
// Example: file/src/.../rules/
SimpleFileFilterPushdownRule    — Statistics-based filter elimination
HLLCountDistinctRule            — COUNT(DISTINCT) → HLL approximation
CountStarStatisticsRule         — COUNT(*) from metadata (no scan)
SimpleFileJoinReorderRule       — Statistics-driven join reordering
SimpleFileColumnPruningRule     — Unused column elimination
PartitionDistinctRule           — DISTINCT on partitioned tables
```

### 2. Custom Conventions
Location: Adapter-specific convention classes
Pattern: Extend `Convention` or `JdbcConvention`

```java
// DuckDB convention (file/src/.../duckdb/DuckDBConvention.java)
// Extends JdbcConvention, registers DuckDB-specific rules
// Enables full SQL pushdown to embedded DuckDB
```

### 3. Custom Table Types
Location: Adapter `table/` directories
Pattern: Implement `TranslatableTable`, `ScannableTable`, `FilterableTable`

```java
// Key custom tables:
ParquetTranslatableTable    — Native Parquet with statistics
IcebergTable                — Time travel, partition pruning
GlobParquetTable            — Multi-file pattern matching
RefreshablePartitionedParquetTable — Auto-refreshing cache
```

### 4. Custom DDL Executor
Location: `sharepoint-list/src/.../SharePointDdlExecutor.java`
Pattern: Extend `DdlExecutorImpl`, handle `SqlCreateTable`/`SqlDropTable`

### 5. Custom Schema/Table Discovery
Location: Each adapter's `SchemaFactory`
Pattern: `SchemaFactory.create()` → `AbstractSchema.getTableMap()`

### 6. Custom Metadata Schemas
Location: Adapter `metadata/` directories
Pattern: PostgreSQL-compatible `pg_catalog` and `information_schema`

```java
// Added to schema during creation:
parentSchema.add("pg_catalog", new PostgreSqlCatalogSchema(parentSchema));
parentSchema.add("information_schema", new InformationSchema(parentSchema));
```

## Where to Register Extensions

| Extension Type | Where to Register | How |
|---------------|------------------|-----|
| Planner rule | Convention.register() | `planner.addRule(MyRule.INSTANCE)` |
| Core rule | CoreRules.java | `public static final MyRule MY_RULE = ...` |
| Table type | Schema.getTableMap() | Return in table map |
| Schema | Model JSON | `"factory": "com.example.MySchemaFactory"` |
| DDL handler | Server config | `DdlExecutor` in `CalcitePrepare.Context` |
| SQL function | SqlStdOperatorTable | Add static constant |

## Adding a Custom SQL Function

```java
// 1. Define the operator in SqlStdOperatorTable (or custom table)
public static final SqlFunction MY_FUNC =
    SqlBasicFunction.create("MY_FUNC",
        ReturnTypes.INTEGER,          // Return type
        OperandTypes.STRING,          // Operand types
        SqlFunctionCategory.USER_DEFINED_FUNCTION);

// 2. Implement in RexImpTable (for enumerable execution)
defineMethod(MY_FUNC, BuiltInMethod.MY_FUNC.method, NullPolicy.STRICT);

// 3. Implement the actual method in SqlFunctions
public static int myFunc(String input) { ... }
```

## Adding a Custom Aggregate

```java
// 1. Define aggregate function
public static final SqlAggFunction MY_AGG =
    SqlBasicAggFunction.create("MY_AGG",
        ReturnTypes.BIGINT,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);

// 2. Implement accumulator (for enumerable)
// See AggregateFunctionImpl pattern

// 3. For pushdown: write a rule that rewrites to adapter-native aggregate
// See DuckDBHLLCountDistinctRule for pattern
```

## Extension Patterns to Follow

1. **Statistics-aware rules**: Follow `SimpleFileFilterPushdownRule` — query table statistics before transforming
2. **Convention-based pushdown**: Follow `DuckDBConvention` — register rules in `Convention.register()`
3. **Metadata schemas**: Follow `PostgresMetadataSchema` — expose catalog metadata as queryable tables
4. **Refreshable tables**: Follow `RefreshablePartitionedParquetTable` — cache management with auto-refresh

## Common Mistakes

1. **Registering rule in wrong convention**: Rules must be registered by the convention that owns the target nodes
2. **Missing `@Value.Enclosing`**: Required on rule class for Immutables code generation
3. **Function not in RexImpTable**: Defining SqlFunction without implementation → runtime error
4. **Schema not in model JSON**: Custom schemas must be declared in the model file
