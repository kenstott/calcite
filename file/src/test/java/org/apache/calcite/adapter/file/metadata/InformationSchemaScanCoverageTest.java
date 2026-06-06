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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.CommentableSchema;
import org.apache.calcite.schema.CommentableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for all inner table scan() methods in {@link InformationSchema}.
 *
 * <p>Covers the scan() bodies of:
 * <ul>
 *   <li>SchemataTable - iterates sub-schemas, checks CommentableSchema</li>
 *   <li>TablesTable - iterates tables in sub-schemas, checks CommentableTable</li>
 *   <li>ColumnsTable - iterates column metadata with type helper methods</li>
 *   <li>TableConstraintsTable - PK, UK, FK constraint rows</li>
 *   <li>ReferentialConstraintsTable - FK referential constraint rows</li>
 *   <li>CheckConstraintsTable - empty table scan</li>
 *   <li>ViewsTable - empty table scan</li>
 *   <li>RoutinesTable - empty table scan</li>
 *   <li>ParametersTable - empty table scan</li>
 * </ul>
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class InformationSchemaScanCoverageTest {

  private RelDataTypeFactory typeFactory;
  private DataContext dataContext;

  @BeforeEach
  void setUp() {
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    dataContext = new DataContext() {
      @Override public SchemaPlus getRootSchema() {
        return null;
      }

      @Override public JavaTypeFactoryImpl getTypeFactory() {
        return (JavaTypeFactoryImpl) typeFactory;
      }

      @Override public QueryProvider getQueryProvider() {
        return null;
      }

      @Override public Object get(String name) {
        return null;
      }
    };
  }

  // =========================================================================
  // Test helper classes
  // =========================================================================

  /**
   * Schema that implements CommentableSchema to test comment extraction.
   */
  static class CommentedSchema extends AbstractSchema implements CommentableSchema {
    private final Map<String, Table> tableMap;
    private final String comment;

    CommentedSchema(Map<String, Table> tableMap, String comment) {
      this.tableMap = tableMap;
      this.comment = comment;
    }

    @Override protected Map<String, Table> getTableMap() {
      return tableMap;
    }

    @Override public String getComment() {
      return comment;
    }
  }

  /**
   * Simple schema without comments.
   */
  static class PlainSchema extends AbstractSchema {
    private final Map<String, Table> tableMap;

    PlainSchema(Map<String, Table> tableMap) {
      this.tableMap = tableMap;
    }

    @Override protected Map<String, Table> getTableMap() {
      return tableMap;
    }
  }

  /**
   * A table with various column types to exercise the type helper methods
   * (mapSqlTypeToString, getCharMaxLength, getNumericPrecision, etc.).
   */
  static class MultiTypeTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("bool_col", SqlTypeName.BOOLEAN)
          .add("tinyint_col", SqlTypeName.TINYINT)
          .add("smallint_col", SqlTypeName.SMALLINT)
          .add("int_col", SqlTypeName.INTEGER)
          .add("bigint_col", SqlTypeName.BIGINT)
          .add("float_col", SqlTypeName.FLOAT)
          .add("real_col", SqlTypeName.REAL)
          .add("double_col", SqlTypeName.DOUBLE)
          .add("decimal_col", typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2))
          .add("char_col", typeFactory.createSqlType(SqlTypeName.CHAR, 5))
          .add("varchar_col", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
          .add("date_col", SqlTypeName.DATE)
          .add("time_col", typeFactory.createSqlType(SqlTypeName.TIME, 3))
          .add("ts_col", typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6))
          .add("tstz_col", typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3))
          .build();
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  /**
   * A table implementing CommentableTable to test comment extraction.
   */
  static class CommentedTable extends AbstractTable implements CommentableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("id", SqlTypeName.INTEGER)
          .add("name", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public String getTableComment() {
      return "This is a commented table";
    }

    @Override public String getColumnComment(String columnName) {
      if ("id".equalsIgnoreCase(columnName)) {
        return "Primary identifier";
      }
      if ("name".equalsIgnoreCase(columnName)) {
        return "Display name";
      }
      return null;
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  /**
   * A table with a primary key constraint.
   */
  static class PKTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("id", SqlTypeName.INTEGER)
          .add("name", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Statistic getStatistic() {
      return Statistics.of(100d, ImmutableList.of(ImmutableBitSet.of(0)));
    }
  }

  /**
   * A table with a PK and a unique key.
   */
  static class PKAndUKTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("id", SqlTypeName.INTEGER)
          .add("code", SqlTypeName.VARCHAR)
          .add("value", SqlTypeName.DOUBLE)
          .build();
    }

    @Override public Statistic getStatistic() {
      return Statistics.of(200d,
          ImmutableList.of(ImmutableBitSet.of(0), ImmutableBitSet.of(1)));
    }
  }

  /**
   * A table with a foreign key.
   */
  static class FKTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("id", SqlTypeName.INTEGER)
          .add("parent_id", SqlTypeName.INTEGER)
          .add("detail", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Statistic getStatistic() {
      List<ImmutableBitSet> keys = ImmutableList.of(ImmutableBitSet.of(0));
      List<RelReferentialConstraint> fks =
          ImmutableList.<RelReferentialConstraint>of(
              new SimpleFK(
              ImmutableList.of("test_schema", "fk_child"),
              ImmutableList.of("test_schema", "pk_parent"),
              ImmutableList.of(IntPair.of(1, 0))));
      return Statistics.of(50d, keys, fks, null);
    }
  }

  /**
   * Simple FK constraint for testing.
   */
  static class SimpleFK implements RelReferentialConstraint {
    private final List<String> sourceQualifiedName;
    private final List<String> targetQualifiedName;
    private final List<IntPair> columnPairs;

    SimpleFK(List<String> sourceQualifiedName,
        List<String> targetQualifiedName,
        List<IntPair> columnPairs) {
      this.sourceQualifiedName = sourceQualifiedName;
      this.targetQualifiedName = targetQualifiedName;
      this.columnPairs = columnPairs;
    }

    @Override public List<String> getSourceQualifiedName() {
      return sourceQualifiedName;
    }

    @Override public List<String> getTargetQualifiedName() {
      return targetQualifiedName;
    }

    @Override public List<IntPair> getColumnPairs() {
      return columnPairs;
    }

    @SuppressWarnings("deprecation")
    @Override public int getNumColumns() {
      return columnPairs.size();
    }
  }

  // =========================================================================
  // SchemataTable scan() tests
  // =========================================================================

  @Test void testSchemataTableScanEmpty() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "catalog1");

    ScannableTable schemataTable = (ScannableTable) infoSchema.getTableMap().get("SCHEMATA");
    assertNotNull(schemataTable);

    List<Object[]> rows = schemataTable.scan(dataContext).toList();

    // Even with no sub-schemas, it adds information_schema and pg_catalog
    assertEquals(2, rows.size());
    assertEquals("information_schema", rows.get(0)[1]);
    assertEquals("pg_catalog", rows.get(1)[1]);
  }

  @Test void testSchemataTableScanWithSubSchemas() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("t1", new PKTable());
    rootSchema.add("sales", new PlainSchema(tables));
    rootSchema.add("hr", new PlainSchema(new HashMap<String, Table>()));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "my_catalog");
    ScannableTable schemataTable = (ScannableTable) infoSchema.getTableMap().get("SCHEMATA");

    List<Object[]> rows = schemataTable.scan(dataContext).toList();

    // 2 user schemas + 2 metadata schemas = 4
    assertEquals(4, rows.size());

    // Check user schemas are present with correct catalog
    boolean foundSales = false;
    boolean foundHr = false;
    for (Object[] row : rows) {
      assertEquals("my_catalog", row[0]); // CATALOG_NAME
      if ("sales".equals(row[1])) {
        foundSales = true;
        assertEquals("CALCITE", row[2]); // SCHEMA_OWNER
        assertEquals("UTF8", row[5]); // DEFAULT_CHARACTER_SET_NAME
        assertNull(row[7]); // REMARKS (no comment since PlainSchema)
      }
      if ("hr".equals(row[1])) {
        foundHr = true;
      }
    }
    assertTrue(foundSales, "Should find sales schema");
    assertTrue(foundHr, "Should find hr schema");
  }

  @Test void testSchemataTableScanCommentableSchema() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("users", new PKTable());
    rootSchema.add("commented_schema",
        new CommentedSchema(tables, "This schema contains user data"));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable schemataTable = (ScannableTable) infoSchema.getTableMap().get("SCHEMATA");

    List<Object[]> rows = schemataTable.scan(dataContext).toList();
    // 1 user schema + 2 metadata schemas
    assertEquals(3, rows.size());

    // The first row should be our commented schema
    Object[] commentedRow = rows.get(0);
    assertEquals("commented_schema", commentedRow[1]);
    assertEquals("This schema contains user data", commentedRow[7]); // REMARKS
  }

  @Test void testSchemataTableGetStatistic() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    Table schemataTable = infoSchema.getTableMap().get("SCHEMATA");
    assertEquals(Statistics.UNKNOWN, schemataTable.getStatistic());
  }

  // =========================================================================
  // TablesTable scan() tests
  // =========================================================================

  @Test void testTablesTableScanEmpty() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "catalog1");

    ScannableTable tablesTable = (ScannableTable) infoSchema.getTableMap().get("TABLES");
    assertNotNull(tablesTable);

    List<Object[]> rows = tablesTable.scan(dataContext).toList();
    assertEquals(0, rows.size());
  }

  @Test void testTablesTableScanWithTables() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("employees", new PKTable());
    tables.put("departments", new MultiTypeTable());
    rootSchema.add("hr", new PlainSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "my_catalog");
    ScannableTable tablesTable = (ScannableTable) infoSchema.getTableMap().get("TABLES");

    List<Object[]> rows = tablesTable.scan(dataContext).toList();
    assertEquals(2, rows.size());

    for (Object[] row : rows) {
      assertEquals("my_catalog", row[0]); // TABLE_CATALOG
      assertEquals("hr", row[1]); // TABLE_SCHEMA
      assertEquals("BASE TABLE", row[3]); // TABLE_TYPE
      assertEquals("YES", row[10]); // IS_INSERTABLE_INTO
      assertEquals("NO", row[11]); // IS_TYPED
    }
  }

  @Test void testTablesTableScanWithCommentableTable() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("commented", new CommentedTable());
    tables.put("plain", new PKTable());
    rootSchema.add("s1", new PlainSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable tablesTable = (ScannableTable) infoSchema.getTableMap().get("TABLES");

    List<Object[]> rows = tablesTable.scan(dataContext).toList();
    assertEquals(2, rows.size());

    boolean foundCommented = false;
    for (Object[] row : rows) {
      if ("commented".equals(row[2])) {
        foundCommented = true;
        assertEquals("This is a commented table", row[4]); // REMARKS
      }
    }
    assertTrue(foundCommented, "Should find commented table");
  }

  @Test void testTablesTableScanMultipleSchemas() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables1 = new HashMap<String, Table>();
    tables1.put("t1", new PKTable());
    rootSchema.add("s1", new PlainSchema(tables1));

    Map<String, Table> tables2 = new HashMap<String, Table>();
    tables2.put("t2", new PKTable());
    tables2.put("t3", new MultiTypeTable());
    rootSchema.add("s2", new PlainSchema(tables2));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable tablesTable = (ScannableTable) infoSchema.getTableMap().get("TABLES");

    List<Object[]> rows = tablesTable.scan(dataContext).toList();
    assertEquals(3, rows.size());
  }

  @Test void testTablesTableGetStatistic() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    Table tablesTable = infoSchema.getTableMap().get("TABLES");
    assertEquals(Statistics.UNKNOWN, tablesTable.getStatistic());
  }

  // =========================================================================
  // ColumnsTable scan() tests
  // =========================================================================

  @Test void testColumnsTableScanEmpty() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");

    ScannableTable columnsTable = (ScannableTable) infoSchema.getTableMap().get("COLUMNS");
    assertNotNull(columnsTable);

    List<Object[]> rows = columnsTable.scan(dataContext).toList();
    assertEquals(0, rows.size());
  }

  @Test void testColumnsTableScanWithMultiTypeTable() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("multi_type", new MultiTypeTable());
    rootSchema.add("test_schema", new PlainSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable columnsTable = (ScannableTable) infoSchema.getTableMap().get("COLUMNS");

    List<Object[]> rows = columnsTable.scan(dataContext).toList();
    // MultiTypeTable has 15 columns
    assertEquals(15, rows.size());

    // Verify column metadata structure
    for (int i = 0; i < rows.size(); i++) {
      Object[] row = rows.get(i);
      assertEquals("cat", row[0]); // TABLE_CATALOG
      assertEquals("test_schema", row[1]); // TABLE_SCHEMA
      assertEquals("multi_type", row[2]); // TABLE_NAME
      assertNotNull(row[3]); // COLUMN_NAME
      assertEquals(i + 1, row[4]); // ORDINAL_POSITION (1-based)
      assertNotNull(row[7]); // DATA_TYPE string
    }

    // Check specific type mappings (mapSqlTypeToString coverage)
    assertEquals("BOOLEAN", rows.get(0)[7]); // bool_col
    assertEquals("TINYINT", rows.get(1)[7]); // tinyint_col
    assertEquals("SMALLINT", rows.get(2)[7]); // smallint_col
    assertEquals("INTEGER", rows.get(3)[7]); // int_col
    assertEquals("BIGINT", rows.get(4)[7]); // bigint_col
    assertEquals("REAL", rows.get(5)[7]); // float_col -> REAL
    assertEquals("REAL", rows.get(6)[7]); // real_col -> REAL
    assertEquals("DOUBLE", rows.get(7)[7]); // double_col
    assertEquals("NUMERIC", rows.get(8)[7]); // decimal_col -> NUMERIC
    assertEquals("CHAR", rows.get(9)[7]); // char_col
    assertEquals("VARCHAR", rows.get(10)[7]); // varchar_col
    assertEquals("DATE", rows.get(11)[7]); // date_col
    assertEquals("TIME", rows.get(12)[7]); // time_col
    assertEquals("TIMESTAMP", rows.get(13)[7]); // ts_col
    assertEquals("TIMESTAMP WITH TIME ZONE", rows.get(14)[7]); // tstz_col

    // Check char length for CHAR(5) column (index 9)
    assertEquals(5, rows.get(9)[8]); // CHARACTER_MAXIMUM_LENGTH
    assertEquals(20, rows.get(9)[9]); // CHARACTER_OCTET_LENGTH (5 * 4)
    assertEquals("UTF8", rows.get(9)[18]); // CHARACTER_SET_NAME for char types

    // Check varchar(255) column (index 10)
    assertEquals(255, rows.get(10)[8]); // CHARACTER_MAXIMUM_LENGTH
    assertEquals(1020, rows.get(10)[9]); // CHARACTER_OCTET_LENGTH (255 * 4)

    // Check numeric precision for various types
    assertEquals(3, rows.get(1)[10]); // NUMERIC_PRECISION for TINYINT
    assertEquals(5, rows.get(2)[10]); // NUMERIC_PRECISION for SMALLINT
    assertEquals(10, rows.get(3)[10]); // NUMERIC_PRECISION for INTEGER
    assertEquals(19, rows.get(4)[10]); // NUMERIC_PRECISION for BIGINT
    assertEquals(24, rows.get(5)[10]); // NUMERIC_PRECISION for FLOAT
    assertEquals(24, rows.get(6)[10]); // NUMERIC_PRECISION for REAL
    assertEquals(53, rows.get(7)[10]); // NUMERIC_PRECISION for DOUBLE
    assertEquals(10, rows.get(8)[10]); // NUMERIC_PRECISION for DECIMAL(10,2)

    // Check numeric precision radix for numeric types
    assertEquals(10, rows.get(3)[11]); // NUMERIC_PRECISION_RADIX for INTEGER
    assertEquals(10, rows.get(8)[11]); // NUMERIC_PRECISION_RADIX for DECIMAL
    assertNull(rows.get(10)[11]); // NUMERIC_PRECISION_RADIX null for VARCHAR
    assertNull(rows.get(11)[11]); // NUMERIC_PRECISION_RADIX null for DATE

    // Check numeric scale for DECIMAL
    assertEquals(2, rows.get(8)[12]); // NUMERIC_SCALE for DECIMAL(10,2)
    assertNull(rows.get(3)[12]); // NUMERIC_SCALE null for INTEGER

    // Check datetime precision
    assertNull(rows.get(3)[13]); // DATETIME_PRECISION null for INTEGER
    assertEquals(3, rows.get(12)[13]); // DATETIME_PRECISION for TIME(3)
    assertTrue(rows.get(13)[13] != null); // DATETIME_PRECISION for TIMESTAMP
    assertEquals(3, rows.get(14)[13]); // DATETIME_PRECISION for TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)

    // Non-char columns should not have CHARACTER_SET_NAME
    assertNull(rows.get(3)[18]); // CHARACTER_SET_NAME null for INTEGER

    // Fixed column values
    for (Object[] row : rows) {
      assertEquals("NO", row[33]); // IS_SELF_REFERENCING
      assertEquals("NO", row[34]); // IS_IDENTITY
      assertEquals("NEVER", row[41]); // IS_GENERATED
      assertEquals("YES", row[43]); // IS_UPDATABLE
    }
  }

  @Test void testColumnsTableScanWithCommentableTable() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("commented", new CommentedTable());
    rootSchema.add("s1", new PlainSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable columnsTable = (ScannableTable) infoSchema.getTableMap().get("COLUMNS");

    List<Object[]> rows = columnsTable.scan(dataContext).toList();
    assertEquals(2, rows.size());

    // Check REMARKS column (last column, index 44) has the comments
    boolean foundIdComment = false;
    boolean foundNameComment = false;
    for (Object[] row : rows) {
      if ("id".equals(row[3])) {
        assertEquals("Primary identifier", row[44]); // REMARKS
        foundIdComment = true;
      }
      if ("name".equals(row[3])) {
        assertEquals("Display name", row[44]); // REMARKS
        foundNameComment = true;
      }
    }
    assertTrue(foundIdComment, "Should find id column comment");
    assertTrue(foundNameComment, "Should find name column comment");
  }

  @Test void testColumnsTableScanNullableColumns() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("t", new PKTable());
    rootSchema.add("s", new PlainSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable columnsTable = (ScannableTable) infoSchema.getTableMap().get("COLUMNS");

    List<Object[]> rows = columnsTable.scan(dataContext).toList();
    assertTrue(rows.size() > 0);

    // Check IS_NULLABLE column
    for (Object[] row : rows) {
      String isNullable = (String) row[6];
      assertTrue("YES".equals(isNullable) || "NO".equals(isNullable),
          "IS_NULLABLE should be YES or NO");
    }
  }

  @Test void testColumnsTableGetStatistic() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    Table columnsTable = infoSchema.getTableMap().get("COLUMNS");
    assertEquals(Statistics.UNKNOWN, columnsTable.getStatistic());
  }

  // =========================================================================
  // TableConstraintsTable scan() tests
  // =========================================================================

  @Test void testTableConstraintsTableScanEmpty() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");

    ScannableTable constraintsTable =
        (ScannableTable) infoSchema.getTableMap().get("TABLE_CONSTRAINTS");
    assertNotNull(constraintsTable);

    List<Object[]> rows = constraintsTable.scan(dataContext).toList();
    assertEquals(0, rows.size());
  }

  @Test void testTableConstraintsTableScanWithPK() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("pk_table", new PKTable());
    rootSchema.add("my_schema", new PlainSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable constraintsTable =
        (ScannableTable) infoSchema.getTableMap().get("TABLE_CONSTRAINTS");

    List<Object[]> rows = constraintsTable.scan(dataContext).toList();
    assertEquals(1, rows.size());

    Object[] row = rows.get(0);
    assertEquals("cat", row[0]); // CONSTRAINT_CATALOG
    assertEquals("MY_SCHEMA", row[1]); // CONSTRAINT_SCHEMA (uppercased)
    assertTrue(((String) row[2]).startsWith("PK_")); // CONSTRAINT_NAME
    assertEquals("cat", row[3]); // TABLE_CATALOG
    assertEquals("MY_SCHEMA", row[4]); // TABLE_SCHEMA (uppercased)
    assertEquals("pk_table", row[5]); // TABLE_NAME
    assertEquals("PRIMARY KEY", row[6]); // CONSTRAINT_TYPE
    assertEquals("NO", row[7]); // IS_DEFERRABLE
    assertEquals("NO", row[8]); // INITIALLY_DEFERRED
  }

  @Test void testTableConstraintsTableScanWithPKAndUK() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("multi_key", new PKAndUKTable());
    rootSchema.add("s1", new PlainSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable constraintsTable =
        (ScannableTable) infoSchema.getTableMap().get("TABLE_CONSTRAINTS");

    List<Object[]> rows = constraintsTable.scan(dataContext).toList();
    assertEquals(2, rows.size());

    // First row: PRIMARY KEY
    assertEquals("PRIMARY KEY", rows.get(0)[6]);
    assertTrue(((String) rows.get(0)[2]).startsWith("PK_"));

    // Second row: UNIQUE
    assertEquals("UNIQUE", rows.get(1)[6]);
    assertTrue(((String) rows.get(1)[2]).startsWith("UK_"));
  }

  @Test void testTableConstraintsTableScanWithFK() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("fk_child", new FKTable());
    rootSchema.add("s1", new PlainSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable constraintsTable =
        (ScannableTable) infoSchema.getTableMap().get("TABLE_CONSTRAINTS");

    List<Object[]> rows = constraintsTable.scan(dataContext).toList();

    // Should have PK row + FK row
    boolean foundPK = false;
    boolean foundFK = false;
    for (Object[] row : rows) {
      if ("PRIMARY KEY".equals(row[6])) {
        foundPK = true;
      }
      if ("FOREIGN KEY".equals(row[6])) {
        foundFK = true;
        assertTrue(((String) row[2]).startsWith("FK_"));
      }
    }
    assertTrue(foundPK, "Should find PK constraint");
    assertTrue(foundFK, "Should find FK constraint");
  }

  @Test void testTableConstraintsTableScanNoConstraints() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("plain_table", new MultiTypeTable()); // UNKNOWN statistic
    rootSchema.add("s1", new PlainSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable constraintsTable =
        (ScannableTable) infoSchema.getTableMap().get("TABLE_CONSTRAINTS");

    List<Object[]> rows = constraintsTable.scan(dataContext).toList();
    // Statistics.UNKNOWN should have null keys, so no rows
    assertEquals(0, rows.size());
  }

  @Test void testTableConstraintsTableGetStatistic() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    Table constraintsTable = infoSchema.getTableMap().get("TABLE_CONSTRAINTS");
    assertEquals(Statistics.UNKNOWN, constraintsTable.getStatistic());
  }

  // =========================================================================
  // ReferentialConstraintsTable scan() tests
  // =========================================================================

  @Test void testReferentialConstraintsTableScanEmpty() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");

    ScannableTable refTable =
        (ScannableTable) infoSchema.getTableMap().get("REFERENTIAL_CONSTRAINTS");
    assertNotNull(refTable);

    List<Object[]> rows = refTable.scan(dataContext).toList();
    assertEquals(0, rows.size());
  }

  @Test void testReferentialConstraintsTableScanWithFK() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("fk_child", new FKTable());
    tables.put("pk_parent", new PKTable());
    rootSchema.add("test_schema", new PlainSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable refTable =
        (ScannableTable) infoSchema.getTableMap().get("REFERENTIAL_CONSTRAINTS");

    List<Object[]> rows = refTable.scan(dataContext).toList();

    // FKTable has one FK, PKTable has no FKs
    assertEquals(1, rows.size());

    Object[] row = rows.get(0);
    assertEquals("cat", row[0]); // CONSTRAINT_CATALOG
    assertEquals("TEST_SCHEMA", row[1]); // CONSTRAINT_SCHEMA (uppercased)
    assertTrue(((String) row[2]).startsWith("FK_")); // CONSTRAINT_NAME
    assertEquals("cat", row[3]); // UNIQUE_CONSTRAINT_CATALOG
    assertEquals("TEST_SCHEMA", row[4]); // UNIQUE_CONSTRAINT_SCHEMA
    assertTrue(((String) row[5]).startsWith("PK_")); // UNIQUE_CONSTRAINT_NAME
    assertEquals("FULL", row[6]); // MATCH_OPTION
    assertEquals("NO ACTION", row[7]); // UPDATE_RULE
    assertEquals("NO ACTION", row[8]); // DELETE_RULE
  }

  @Test void testReferentialConstraintsTableScanNoFKs() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("pk_only", new PKTable());
    rootSchema.add("s1", new PlainSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable refTable =
        (ScannableTable) infoSchema.getTableMap().get("REFERENTIAL_CONSTRAINTS");

    List<Object[]> rows = refTable.scan(dataContext).toList();
    // PK-only table has no foreign keys
    assertEquals(0, rows.size());
  }

  @Test void testReferentialConstraintsTableGetStatistic() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    Table refTable = infoSchema.getTableMap().get("REFERENTIAL_CONSTRAINTS");
    assertEquals(Statistics.UNKNOWN, refTable.getStatistic());
  }

  // =========================================================================
  // EmptyTable subclasses (CheckConstraints, Views, Routines, Parameters)
  // =========================================================================

  @Test void testCheckConstraintsTableScan() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");

    ScannableTable table = (ScannableTable) infoSchema.getTableMap().get("CHECK_CONSTRAINTS");
    assertNotNull(table);

    // Verify row type
    RelDataType rowType = table.getRowType(typeFactory);
    assertEquals(4, rowType.getFieldCount());
    assertEquals("CONSTRAINT_CATALOG", rowType.getFieldNames().get(0));
    assertEquals("CONSTRAINT_SCHEMA", rowType.getFieldNames().get(1));
    assertEquals("CONSTRAINT_NAME", rowType.getFieldNames().get(2));
    assertEquals("CHECK_CLAUSE", rowType.getFieldNames().get(3));

    // Scan returns empty
    List<Object[]> rows = table.scan(dataContext).toList();
    assertEquals(0, rows.size());

    // Statistic
    assertEquals(Statistics.UNKNOWN, table.getStatistic());
  }

  @Test void testViewsTableScan() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");

    ScannableTable table = (ScannableTable) infoSchema.getTableMap().get("VIEWS");
    assertNotNull(table);

    // Verify row type
    RelDataType rowType = table.getRowType(typeFactory);
    assertEquals(10, rowType.getFieldCount());
    assertEquals("TABLE_CATALOG", rowType.getFieldNames().get(0));
    assertEquals("TABLE_SCHEMA", rowType.getFieldNames().get(1));
    assertEquals("TABLE_NAME", rowType.getFieldNames().get(2));
    assertEquals("VIEW_DEFINITION", rowType.getFieldNames().get(3));

    // Scan returns empty
    List<Object[]> rows = table.scan(dataContext).toList();
    assertEquals(0, rows.size());

    // Statistic
    assertEquals(Statistics.UNKNOWN, table.getStatistic());
  }

  @Test void testRoutinesTableScan() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");

    ScannableTable table = (ScannableTable) infoSchema.getTableMap().get("ROUTINES");
    assertNotNull(table);

    // Verify row type has many columns
    RelDataType rowType = table.getRowType(typeFactory);
    assertTrue(rowType.getFieldCount() > 50, "ROUTINES should have many columns");
    assertEquals("SPECIFIC_CATALOG", rowType.getFieldNames().get(0));

    // Scan returns empty
    List<Object[]> rows = table.scan(dataContext).toList();
    assertEquals(0, rows.size());

    // Statistic
    assertEquals(Statistics.UNKNOWN, table.getStatistic());
  }

  @Test void testParametersTableScan() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");

    ScannableTable table = (ScannableTable) infoSchema.getTableMap().get("PARAMETERS");
    assertNotNull(table);

    // Verify row type
    RelDataType rowType = table.getRowType(typeFactory);
    assertEquals(31, rowType.getFieldCount());
    assertEquals("SPECIFIC_CATALOG", rowType.getFieldNames().get(0));
    assertEquals("PARAMETER_NAME", rowType.getFieldNames().get(7));

    // Scan returns empty
    List<Object[]> rows = table.scan(dataContext).toList();
    assertEquals(0, rows.size());

    // Statistic
    assertEquals(Statistics.UNKNOWN, table.getStatistic());
  }

  // =========================================================================
  // Row type verification tests (for coverage of getRowType in each table)
  // =========================================================================

  @Test void testSchemataTableRowType() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");

    Table table = infoSchema.getTableMap().get("SCHEMATA");
    RelDataType rowType = table.getRowType(typeFactory);
    assertEquals(8, rowType.getFieldCount());
    assertEquals("CATALOG_NAME", rowType.getFieldNames().get(0));
    assertEquals("SCHEMA_NAME", rowType.getFieldNames().get(1));
    assertEquals("REMARKS", rowType.getFieldNames().get(7));
  }

  @Test void testTablesTableRowType() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");

    Table table = infoSchema.getTableMap().get("TABLES");
    RelDataType rowType = table.getRowType(typeFactory);
    assertEquals(13, rowType.getFieldCount());
    assertEquals("TABLE_CATALOG", rowType.getFieldNames().get(0));
    assertEquals("TABLE_SCHEMA", rowType.getFieldNames().get(1));
    assertEquals("TABLE_NAME", rowType.getFieldNames().get(2));
    assertEquals("TABLE_TYPE", rowType.getFieldNames().get(3));
    assertEquals("REMARKS", rowType.getFieldNames().get(4));
  }

  @Test void testColumnsTableRowType() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");

    Table table = infoSchema.getTableMap().get("COLUMNS");
    RelDataType rowType = table.getRowType(typeFactory);
    assertEquals(45, rowType.getFieldCount());
    assertEquals("TABLE_CATALOG", rowType.getFieldNames().get(0));
    assertEquals("COLUMN_NAME", rowType.getFieldNames().get(3));
    assertEquals("ORDINAL_POSITION", rowType.getFieldNames().get(4));
    assertEquals("DATA_TYPE", rowType.getFieldNames().get(7));
    assertEquals("REMARKS", rowType.getFieldNames().get(44));
  }

  @Test void testTableConstraintsTableRowType() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");

    Table table = infoSchema.getTableMap().get("TABLE_CONSTRAINTS");
    RelDataType rowType = table.getRowType(typeFactory);
    assertEquals(9, rowType.getFieldCount());
    assertEquals("CONSTRAINT_CATALOG", rowType.getFieldNames().get(0));
    assertEquals("CONSTRAINT_TYPE", rowType.getFieldNames().get(6));
  }

  @Test void testReferentialConstraintsTableRowType() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");

    Table table = infoSchema.getTableMap().get("REFERENTIAL_CONSTRAINTS");
    RelDataType rowType = table.getRowType(typeFactory);
    assertEquals(9, rowType.getFieldCount());
    assertEquals("CONSTRAINT_CATALOG", rowType.getFieldNames().get(0));
    assertEquals("MATCH_OPTION", rowType.getFieldNames().get(6));
    assertEquals("UPDATE_RULE", rowType.getFieldNames().get(7));
    assertEquals("DELETE_RULE", rowType.getFieldNames().get(8));
  }

  // =========================================================================
  // Case-insensitive access and table map tests
  // =========================================================================

  @Test void testLowerCaseAccess() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    Map<String, Table> tableMap = infoSchema.getTableMap();

    assertNotNull(tableMap.get("schemata"));
    assertNotNull(tableMap.get("tables"));
    assertNotNull(tableMap.get("columns"));
    assertNotNull(tableMap.get("table_constraints"));
    assertNotNull(tableMap.get("referential_constraints"));
    assertNotNull(tableMap.get("check_constraints"));
    assertNotNull(tableMap.get("views"));
    assertNotNull(tableMap.get("routines"));
    assertNotNull(tableMap.get("parameters"));
  }

  @Test void testTitleCaseAccess() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    Map<String, Table> tableMap = infoSchema.getTableMap();

    // Title case: first char upper, rest lower
    assertNotNull(tableMap.get("Schemata"));
    assertNotNull(tableMap.get("Tables"));
    assertNotNull(tableMap.get("Columns"));
    assertNotNull(tableMap.get("Views"));
    assertNotNull(tableMap.get("Routines"));
    assertNotNull(tableMap.get("Parameters"));
  }

  // =========================================================================
  // Multi-schema integration tests for deeper coverage
  // =========================================================================

  @Test void testFullScanAllTablesWithRichSchema() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    // Schema 1 with commented schema and commented table
    Map<String, Table> schema1Tables = new HashMap<String, Table>();
    schema1Tables.put("commented_t", new CommentedTable());
    schema1Tables.put("multi_type_t", new MultiTypeTable());
    rootSchema.add("commented_schema",
        new CommentedSchema(schema1Tables, "Schema with comments"));

    // Schema 2 with FK tables
    Map<String, Table> schema2Tables = new HashMap<String, Table>();
    schema2Tables.put("parent", new PKTable());
    schema2Tables.put("child", new FKTable());
    schema2Tables.put("multi_key", new PKAndUKTable());
    rootSchema.add("constrained_schema", new PlainSchema(schema2Tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "full_cat");
    Map<String, Table> tableMap = infoSchema.getTableMap();

    // Scan SCHEMATA
    ScannableTable schemataTable = (ScannableTable) tableMap.get("SCHEMATA");
    List<Object[]> schemataRows = schemataTable.scan(dataContext).toList();
    // 2 user schemas + 2 metadata schemas
    assertEquals(4, schemataRows.size());

    // Scan TABLES
    ScannableTable tablesTable = (ScannableTable) tableMap.get("TABLES");
    List<Object[]> tableRows = tablesTable.scan(dataContext).toList();
    // 2 in commented_schema + 3 in constrained_schema = 5
    assertEquals(5, tableRows.size());

    // Scan COLUMNS - verify non-empty
    ScannableTable columnsTable = (ScannableTable) tableMap.get("COLUMNS");
    List<Object[]> columnRows = columnsTable.scan(dataContext).toList();
    assertTrue(columnRows.size() > 10, "Should have many column rows");

    // Scan TABLE_CONSTRAINTS - should have PK, UK, FK
    ScannableTable constraintsTable = (ScannableTable) tableMap.get("TABLE_CONSTRAINTS");
    List<Object[]> constraintRows = constraintsTable.scan(dataContext).toList();
    assertTrue(constraintRows.size() >= 3, "Should have PK, UK, and FK constraints");

    // Scan REFERENTIAL_CONSTRAINTS
    ScannableTable refTable = (ScannableTable) tableMap.get("REFERENTIAL_CONSTRAINTS");
    List<Object[]> refRows = refTable.scan(dataContext).toList();
    assertTrue(refRows.size() >= 1, "Should have at least one FK reference");

    // Scan empty tables
    assertEquals(0, ((ScannableTable) tableMap.get("CHECK_CONSTRAINTS")).scan(dataContext).toList().size());
    assertEquals(0, ((ScannableTable) tableMap.get("VIEWS")).scan(dataContext).toList().size());
    assertEquals(0, ((ScannableTable) tableMap.get("ROUTINES")).scan(dataContext).toList().size());
    assertEquals(0, ((ScannableTable) tableMap.get("PARAMETERS")).scan(dataContext).toList().size());
  }

  @Test void testColumnsTableScanMultipleSchemas() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables1 = new HashMap<String, Table>();
    tables1.put("t1", new PKTable());
    rootSchema.add("s1", new PlainSchema(tables1));

    Map<String, Table> tables2 = new HashMap<String, Table>();
    tables2.put("t2", new CommentedTable());
    rootSchema.add("s2", new PlainSchema(tables2));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable columnsTable = (ScannableTable) infoSchema.getTableMap().get("COLUMNS");

    List<Object[]> rows = columnsTable.scan(dataContext).toList();
    // PKTable has 2 columns, CommentedTable has 2 columns = 4 total
    assertEquals(4, rows.size());

    boolean foundS1 = false;
    boolean foundS2 = false;
    for (Object[] row : rows) {
      if ("s1".equals(row[1])) {
        foundS1 = true;
      }
      if ("s2".equals(row[1])) {
        foundS2 = true;
      }
    }
    assertTrue(foundS1, "Should have columns from s1");
    assertTrue(foundS2, "Should have columns from s2");
  }

  @Test void testTableConstraintsMultipleSchemas() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables1 = new HashMap<String, Table>();
    tables1.put("pk_t", new PKTable());
    rootSchema.add("schema_a", new PlainSchema(tables1));

    Map<String, Table> tables2 = new HashMap<String, Table>();
    tables2.put("pk_uk_t", new PKAndUKTable());
    rootSchema.add("schema_b", new PlainSchema(tables2));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable constraintsTable =
        (ScannableTable) infoSchema.getTableMap().get("TABLE_CONSTRAINTS");

    List<Object[]> rows = constraintsTable.scan(dataContext).toList();
    // schema_a: 1 PK, schema_b: 1 PK + 1 UK = 3
    assertEquals(3, rows.size());

    boolean foundSchemaA = false;
    boolean foundSchemaB = false;
    for (Object[] row : rows) {
      if ("SCHEMA_A".equals(row[1])) {
        foundSchemaA = true;
      }
      if ("SCHEMA_B".equals(row[1])) {
        foundSchemaB = true;
      }
    }
    assertTrue(foundSchemaA, "Should have constraints from schema_a");
    assertTrue(foundSchemaB, "Should have constraints from schema_b");
  }

  @Test void testReferentialConstraintsMultipleSchemas() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables1 = new HashMap<String, Table>();
    tables1.put("parent", new PKTable());
    rootSchema.add("schema_a", new PlainSchema(tables1));

    Map<String, Table> tables2 = new HashMap<String, Table>();
    tables2.put("child", new FKTable());
    rootSchema.add("schema_b", new PlainSchema(tables2));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "cat");
    ScannableTable refTable =
        (ScannableTable) infoSchema.getTableMap().get("REFERENTIAL_CONSTRAINTS");

    List<Object[]> rows = refTable.scan(dataContext).toList();
    // Only schema_b has FK
    assertEquals(1, rows.size());
    assertEquals("SCHEMA_B", rows.get(0)[1]);
  }
}
