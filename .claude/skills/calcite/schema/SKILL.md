---
description: How to implement SchemaFactory, Schema, Table, and TableScan for this project's data sources
model: haiku
effort: low
---

# Calcite Schema Implementation Guide

Reference when implementing schema/table components for $ARGUMENTS.

## Schema Factory Chain

```
SchemaFactory.create()          → Entry point (model JSON operand map)
  └→ Schema (extends AbstractSchema)
       └→ getTableMap()         → Map<String, Table>
            └→ Table instances  → Row type + scan/translate
```

## SchemaFactory Pattern (This Project)

```java
public class MySchemaFactory implements SchemaFactory {
  @Override public Schema create(
      SchemaPlus parentSchema,
      String name,
      Map<String, Object> operand) {

    // Extract config from model JSON operand
    String dataSource = (String) operand.get("dataSource");
    String directory = (String) operand.get("directory");

    // Validate required params
    requireNonNull(dataSource, "dataSource is required in model operand");

    return new MySchema(parentSchema, name, dataSource, directory);
  }
}
```

Register in model JSON:
```json
{
  "schemas": [{
    "name": "myschema",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.mine.MySchemaFactory",
    "operand": {
      "dataSource": "source1",
      "directory": "/data/parquet"
    }
  }]
}
```

## Schema Pattern

```java
public class MySchema extends AbstractSchema {
  @Override protected Map<String, Table> getTableMap() {
    // Discover tables (lazy, called once and cached by AbstractSchema)
    Map<String, Table> tables = new LinkedHashMap<>();
    tables.put("table1", new MyTable(config1));
    tables.put("table2", new MyTable(config2));
    return tables;
  }

  // Optional: sub-schemas
  @Override protected Map<String, Schema> getSubSchemaMap() { ... }
}
```

## Table Implementations — Choose the Right Interface

| Interface | Push-down | When to Use |
|-----------|-----------|-------------|
| `ScannableTable` | None | Simple full-scan sources (JSON, CSV) |
| `FilterableTable` | Filters | Sources that can filter server-side |
| `ProjectableFilterableTable` | Filters + columns | Sources with column pruning |
| `TranslatableTable` | Full (custom rules) | Complex adapters (Parquet, DuckDB, Splunk) |

### ScannableTable (Simplest)

```java
public class MyTable extends AbstractTable implements ScannableTable {
  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("name", SqlTypeName.VARCHAR, 255)
        .add("created", SqlTypeName.TIMESTAMP)
        .nullable(true)  // Make last column nullable
        .build();
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new MyEnumerator(dataSource);
      }
    };
  }
}
```

### TranslatableTable (Full Pushdown)

```java
public class MyTranslatableTable extends AbstractQueryableTable
    implements TranslatableTable {

  @Override public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new MyTableScan(context.getCluster(),
        relOptTable, this);
  }

  @Override public <T> Queryable<T> asQueryable(
      QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();  // Use toRel path
  }
}
```

## Project-Specific Table Patterns

### Parquet table (file adapter):
`file/src/.../table/ParquetTranslatableTable.java`
- Implements `TranslatableTable` + `StatisticsProvider`
- Caches statistics and HLL sketches for optimization rules
- Native predicate pushdown via custom rules

### Iceberg table (file adapter):
`file/src/.../iceberg/IcebergTable.java`
- Implements `ScannableTable` + `CommentableTable`
- Time travel, partition pruning, schema evolution
- Metadata tables (snapshots, manifests, files)

### SharePoint table:
`sharepoint-list/src/.../SharePointListTable.java`
- Implements `ScannableTable` + `ModifiableTable`
- CRUD operations via SharePoint REST API
- ID column always present

## Metadata Schemas

This project adds PostgreSQL-compatible metadata schemas:
```java
// In your Schema constructor
parentSchema.add("pg_catalog", new PostgreSqlCatalogSchema(parentSchema));
parentSchema.add("information_schema", new InformationSchema(parentSchema));
```

See: `file/src/.../metadata/PostgresMetadataSchema.java`

## Testing a Schema

```java
@Tag("integration")
@Test void testSchemaDiscovery() {
  Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
  Statement stmt = conn.createStatement();
  ResultSet rs = stmt.executeQuery("SELECT * FROM myschema.table1 LIMIT 5");
  assertThat(rs.next(), is(true));
}
```
