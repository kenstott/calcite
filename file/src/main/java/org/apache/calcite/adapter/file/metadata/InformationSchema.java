/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.CommentableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * SQL standard INFORMATION_SCHEMA implementation for the file adapter.
 * Provides metadata views compatible with SQL standard and common BI tools.
 */
public class InformationSchema extends AbstractSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(InformationSchema.class);

  private final SchemaPlus rootSchema;
  private final String catalogName;

  public InformationSchema(SchemaPlus rootSchema, String catalogName) {
    this.rootSchema = rootSchema;
    this.catalogName = catalogName;

    LOGGER.info("InformationSchema constructor: Created with rootSchema tables: {}",
                rootSchema.tables().getNames(LikePattern.any()));
    LOGGER.info("InformationSchema constructor: Available sub-schemas: {}",
                rootSchema.subSchemas().getNames(LikePattern.any()));
  }

  private final Map<String, Table> tableMap = createCaseInsensitiveTableMap();

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }


  private Map<String, Table> createCaseInsensitiveTableMap() {
    Map<String, Table> tables = new HashMap<>();

    // Add tables with both upper and lower case keys for case-insensitive lookup
    Map<String, Table> originalTables = ImmutableMap.<String, Table>builder()
        .put("SCHEMATA", new SchemataTable())
        .put("TABLES", new TablesTable())
        .put("COLUMNS", new ColumnsTable())
        .put("TABLE_CONSTRAINTS", new TableConstraintsTable())
        .put("KEY_COLUMN_USAGE", new KeyColumnUsageTable())
        .put("REFERENTIAL_CONSTRAINTS", new ReferentialConstraintsTable())
        .put("CHECK_CONSTRAINTS", new CheckConstraintsTable())
        .put("VIEWS", new ViewsTable())
        .put("ROUTINES", new RoutinesTable())
        .put("PARAMETERS", new ParametersTable())
        .build();

    // Add each table with multiple case variations for case-insensitive access
    for (Map.Entry<String, Table> entry : originalTables.entrySet()) {
      String tableName = entry.getKey();
      Table table = entry.getValue();

      // Add upper, lower, and mixed case variations
      tables.put(tableName.toUpperCase(Locale.ROOT), table);  // SCHEMATA
      tables.put(tableName.toLowerCase(Locale.ROOT), table); // schemata
      // Add title case variation
      if (tableName.length() > 1) {
        String titleCase = tableName.charAt(0) + tableName.substring(1).toLowerCase(Locale.ROOT);
        tables.put(titleCase, table); // Schemata
      }
    }

    return tables;
  }

  /**
   * SCHEMATA - all schemas in the catalog
   */
  private class SchemataTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("CATALOG_NAME", SqlTypeName.VARCHAR)
          .add("SCHEMA_NAME", SqlTypeName.VARCHAR)
          .add("SCHEMA_OWNER", SqlTypeName.VARCHAR)
          .add("DEFAULT_CHARACTER_SET_CATALOG", SqlTypeName.VARCHAR)
          .add("DEFAULT_CHARACTER_SET_SCHEMA", SqlTypeName.VARCHAR)
          .add("DEFAULT_CHARACTER_SET_NAME", SqlTypeName.VARCHAR)
          .add("SQL_PATH", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        rows.add(new Object[]{
            catalogName,
            schemaName,
            "CALCITE",
            null,
            null,
            "UTF8",
            null
        });
      }

      // Also add metadata schemas
      rows.add(new Object[]{catalogName, "information_schema", "CALCITE", null, null, "UTF8", null});
      rows.add(new Object[]{catalogName, "pg_catalog", "CALCITE", null, null, "UTF8", null});

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  /**
   * TABLES - all tables in the catalog
   */
  private class TablesTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("TABLE_CATALOG", SqlTypeName.VARCHAR)
          .add("TABLE_SCHEMA", SqlTypeName.VARCHAR)
          .add("TABLE_NAME", SqlTypeName.VARCHAR)
          .add("TABLE_TYPE", SqlTypeName.VARCHAR)
          .add("REMARKS", SqlTypeName.VARCHAR)  // Standard JDBC column for table comments
          .add("SELF_REFERENCING_COLUMN_NAME", SqlTypeName.VARCHAR)
          .add("REFERENCE_GENERATION", SqlTypeName.VARCHAR)
          .add("USER_DEFINED_TYPE_CATALOG", SqlTypeName.VARCHAR)
          .add("USER_DEFINED_TYPE_SCHEMA", SqlTypeName.VARCHAR)
          .add("USER_DEFINED_TYPE_NAME", SqlTypeName.VARCHAR)
          .add("IS_INSERTABLE_INTO", SqlTypeName.VARCHAR)
          .add("IS_TYPED", SqlTypeName.VARCHAR)
          .add("COMMIT_ACTION", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus schema = rootSchema.subSchemas().get(schemaName);
        if (schema != null) {
          for (String tableName : schema.tables().getNames(LikePattern.any())) {
            Table table = schema.tables().get(tableName);
            String tableComment = null;

            // Get table comment if available
            if (table instanceof CommentableTable) {
              tableComment = ((CommentableTable) table).getTableComment();
            }

            rows.add(new Object[]{
                catalogName,
                schemaName,
                tableName,
                "BASE TABLE",
                tableComment,  // REMARKS
                null,
                null,
                null,
                null,
                null,
                "YES",
                "NO",
                null
            });
          }
        }
      }

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  /**
   * COLUMNS - all columns of all tables
   */
  private class ColumnsTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("TABLE_CATALOG", SqlTypeName.VARCHAR)
          .add("TABLE_SCHEMA", SqlTypeName.VARCHAR)
          .add("TABLE_NAME", SqlTypeName.VARCHAR)
          .add("COLUMN_NAME", SqlTypeName.VARCHAR)
          .add("ORDINAL_POSITION", SqlTypeName.INTEGER)
          .add("COLUMN_DEFAULT", SqlTypeName.VARCHAR)
          .add("IS_NULLABLE", SqlTypeName.VARCHAR)
          .add("DATA_TYPE", SqlTypeName.VARCHAR)
          .add("CHARACTER_MAXIMUM_LENGTH", SqlTypeName.INTEGER)
          .add("CHARACTER_OCTET_LENGTH", SqlTypeName.INTEGER)
          .add("NUMERIC_PRECISION", SqlTypeName.INTEGER)
          .add("NUMERIC_PRECISION_RADIX", SqlTypeName.INTEGER)
          .add("NUMERIC_SCALE", SqlTypeName.INTEGER)
          .add("DATETIME_PRECISION", SqlTypeName.INTEGER)
          .add("INTERVAL_TYPE", SqlTypeName.VARCHAR)
          .add("INTERVAL_PRECISION", SqlTypeName.INTEGER)
          .add("CHARACTER_SET_CATALOG", SqlTypeName.VARCHAR)
          .add("CHARACTER_SET_SCHEMA", SqlTypeName.VARCHAR)
          .add("CHARACTER_SET_NAME", SqlTypeName.VARCHAR)
          .add("COLLATION_CATALOG", SqlTypeName.VARCHAR)
          .add("COLLATION_SCHEMA", SqlTypeName.VARCHAR)
          .add("COLLATION_NAME", SqlTypeName.VARCHAR)
          .add("DOMAIN_CATALOG", SqlTypeName.VARCHAR)
          .add("DOMAIN_SCHEMA", SqlTypeName.VARCHAR)
          .add("DOMAIN_NAME", SqlTypeName.VARCHAR)
          .add("UDT_CATALOG", SqlTypeName.VARCHAR)
          .add("UDT_SCHEMA", SqlTypeName.VARCHAR)
          .add("UDT_NAME", SqlTypeName.VARCHAR)
          .add("SCOPE_CATALOG", SqlTypeName.VARCHAR)
          .add("SCOPE_SCHEMA", SqlTypeName.VARCHAR)
          .add("SCOPE_NAME", SqlTypeName.VARCHAR)
          .add("MAXIMUM_CARDINALITY", SqlTypeName.INTEGER)
          .add("DTD_IDENTIFIER", SqlTypeName.VARCHAR)
          .add("IS_SELF_REFERENCING", SqlTypeName.VARCHAR)
          .add("IS_IDENTITY", SqlTypeName.VARCHAR)
          .add("IDENTITY_GENERATION", SqlTypeName.VARCHAR)
          .add("IDENTITY_START", SqlTypeName.VARCHAR)
          .add("IDENTITY_INCREMENT", SqlTypeName.VARCHAR)
          .add("IDENTITY_MAXIMUM", SqlTypeName.VARCHAR)
          .add("IDENTITY_MINIMUM", SqlTypeName.VARCHAR)
          .add("IDENTITY_CYCLE", SqlTypeName.VARCHAR)
          .add("IS_GENERATED", SqlTypeName.VARCHAR)
          .add("GENERATION_EXPRESSION", SqlTypeName.VARCHAR)
          .add("IS_UPDATABLE", SqlTypeName.VARCHAR)
          .add("REMARKS", SqlTypeName.VARCHAR)  // Standard JDBC column for column comments
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      LOGGER.info("ColumnsTable.scan: Available sub-schemas: {}",
                  rootSchema.subSchemas().getNames(LikePattern.any()));

      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus schema = rootSchema.subSchemas().get(schemaName);
        LOGGER.info("ColumnsTable.scan: Processing schema '{}', schema != null: {}",
                    schemaName, schema != null);
        if (schema != null) {
          LOGGER.info("ColumnsTable.scan: Schema '{}' contains tables: {}",
                      schemaName, schema.tables().getNames(LikePattern.any()));
          for (String tableName : schema.tables().getNames(LikePattern.any())) {
            Table table = schema.tables().get(tableName);
            LOGGER.info("ColumnsTable.scan: Processing table '{}' in schema '{}'",
                        tableName, schemaName);
            if (table != null) {
              RelDataType rowType = table.getRowType(root.getTypeFactory());

              int position = 1;
              for (RelDataTypeField field : rowType.getFieldList()) {
                SqlTypeName sqlType = field.getType().getSqlTypeName();
                String columnComment = null;

                // Get column comment if available
                if (table instanceof CommentableTable) {
                  columnComment = ((CommentableTable) table).getColumnComment(field.getName());
                }

                rows.add(new Object[]{
                    catalogName,                        // TABLE_CATALOG
                    schemaName,                        // TABLE_SCHEMA
                    tableName,                         // TABLE_NAME
                    field.getName(),                   // COLUMN_NAME
                    position++,                        // ORDINAL_POSITION
                    null,                             // COLUMN_DEFAULT
                    field.getType().isNullable() ? "YES" : "NO", // IS_NULLABLE
                    mapSqlTypeToString(sqlType),      // DATA_TYPE
                    getCharMaxLength(field.getType()), // CHARACTER_MAXIMUM_LENGTH
                    getCharOctetLength(field.getType()), // CHARACTER_OCTET_LENGTH
                    getNumericPrecision(field.getType()), // NUMERIC_PRECISION
                    getNumericPrecisionRadix(field.getType()), // NUMERIC_PRECISION_RADIX
                    getNumericScale(field.getType()), // NUMERIC_SCALE
                    getDatetimePrecision(field.getType()), // DATETIME_PRECISION
                    null,                             // INTERVAL_TYPE
                    null,                             // INTERVAL_PRECISION
                    null,                             // CHARACTER_SET_CATALOG
                    null,                             // CHARACTER_SET_SCHEMA
                    isCharType(sqlType) ? "UTF8" : null, // CHARACTER_SET_NAME
                    null,                             // COLLATION_CATALOG
                    null,                             // COLLATION_SCHEMA
                    null,                             // COLLATION_NAME
                    null,                             // DOMAIN_CATALOG
                    null,                             // DOMAIN_SCHEMA
                    null,                             // DOMAIN_NAME
                    null,                             // UDT_CATALOG
                    null,                             // UDT_SCHEMA
                    null,                             // UDT_NAME
                    null,                             // SCOPE_CATALOG
                    null,                             // SCOPE_SCHEMA
                    null,                             // SCOPE_NAME
                    null,                             // MAXIMUM_CARDINALITY
                    null,                             // DTD_IDENTIFIER
                    "NO",                             // IS_SELF_REFERENCING
                    "NO",                             // IS_IDENTITY
                    null,                             // IDENTITY_GENERATION
                    null,                             // IDENTITY_START
                    null,                             // IDENTITY_INCREMENT
                    null,                             // IDENTITY_MAXIMUM
                    null,                             // IDENTITY_MINIMUM
                    null,                             // IDENTITY_CYCLE
                    "NEVER",                          // IS_GENERATED
                    null,                             // GENERATION_EXPRESSION
                    "YES",                            // IS_UPDATABLE
                    columnComment                     // REMARKS
                });
              }
            }
          }
        }
      }

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  // Table for constraint metadata
  private class TableConstraintsTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("CONSTRAINT_CATALOG", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_SCHEMA", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_NAME", SqlTypeName.VARCHAR)
          .add("TABLE_CATALOG", SqlTypeName.VARCHAR)
          .add("TABLE_SCHEMA", SqlTypeName.VARCHAR)
          .add("TABLE_NAME", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_TYPE", SqlTypeName.VARCHAR)
          .add("IS_DEFERRABLE", SqlTypeName.VARCHAR)
          .add("INITIALLY_DEFERRED", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      // Iterate through all schemas
      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus schema = rootSchema.subSchemas().get(schemaName);
        if (schema != null) {
          // Get the unwrapped schema to check its actual type
          Schema unwrappedSchema = schema.unwrap(Schema.class);

          // Iterate through all tables in the schema
          for (String tableName : schema.tables().getNames(LikePattern.any())) {
            Table table;

            // Check if this is a ConstraintAwareJdbcSchema - if so, use its getTable method
            // to get the ConstraintAwareJdbcTable wrapper with constraint metadata
            if (unwrappedSchema != null &&
                unwrappedSchema.getClass().getName().contains("ConstraintAwareJdbcSchema")) {
              // Use the schema's getTable method to get the wrapped table
              @SuppressWarnings("deprecation")
              Table wrappedTable = unwrappedSchema.getTable(tableName);
              table = wrappedTable;
            } else {
              // Normal path for other schema types
              table = schema.tables().get(tableName);
            }

            if (table != null) {
              LOGGER.info("InformationSchema: Checking constraints for table '{}' in schema '{}', type: {}",
                          tableName, schemaName, table.getClass().getSimpleName());
              // Get table statistics which contain constraint information
              Statistic statistic = table.getStatistic();
              LOGGER.info("  - statistic: {}, keys: {}, referentialConstraints: {}",
                          statistic != null ? statistic.getClass().getSimpleName() : "null",
                          statistic != null && statistic.getKeys() != null ? statistic.getKeys().size() : "null",
                          statistic != null && statistic.getReferentialConstraints() != null ?
                              statistic.getReferentialConstraints().size() : "null");
              if (statistic != null) {
                // Extract primary keys
                List<ImmutableBitSet> keys = statistic.getKeys();
                if (keys != null) {
                  int keyIndex = 0;
                  for (ImmutableBitSet key : keys) {
                    String constraintName = (keyIndex == 0)
                        ? "PK_" + tableName.toUpperCase()
                        : "UK_" + tableName.toUpperCase() + "_" + keyIndex;

                    rows.add(new Object[]{
                        catalogName,                    // CONSTRAINT_CATALOG
                        schemaName.toUpperCase(),       // CONSTRAINT_SCHEMA
                        constraintName,                  // CONSTRAINT_NAME
                        catalogName,                    // TABLE_CATALOG
                        schemaName.toUpperCase(),       // TABLE_SCHEMA
                        tableName,                       // TABLE_NAME
                        (keyIndex == 0) ? "PRIMARY KEY" : "UNIQUE", // CONSTRAINT_TYPE
                        "NO",                           // IS_DEFERRABLE
                        "NO"                            // INITIALLY_DEFERRED
                    });
                    keyIndex++;
                  }
                }

                // Extract foreign keys
                List<RelReferentialConstraint> foreignKeys = statistic.getReferentialConstraints();
                if (foreignKeys != null) {
                  int fkIndex = 0;
                  for (RelReferentialConstraint fk : foreignKeys) {
                    String constraintName = "FK_" + tableName.toUpperCase() + "_" + fkIndex;

                    rows.add(new Object[]{
                        catalogName,                    // CONSTRAINT_CATALOG
                        schemaName.toUpperCase(),       // CONSTRAINT_SCHEMA
                        constraintName,                  // CONSTRAINT_NAME
                        catalogName,                    // TABLE_CATALOG
                        schemaName.toUpperCase(),       // TABLE_SCHEMA
                        tableName,                       // TABLE_NAME
                        "FOREIGN KEY",                  // CONSTRAINT_TYPE
                        "NO",                           // IS_DEFERRABLE
                        "NO"                            // INITIALLY_DEFERRED
                    });
                    fkIndex++;
                  }
                }
              }
            }
          }
        }
      }

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  private class KeyColumnUsageTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("CONSTRAINT_CATALOG", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_SCHEMA", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_NAME", SqlTypeName.VARCHAR)
          .add("TABLE_CATALOG", SqlTypeName.VARCHAR)
          .add("TABLE_SCHEMA", SqlTypeName.VARCHAR)
          .add("TABLE_NAME", SqlTypeName.VARCHAR)
          .add("COLUMN_NAME", SqlTypeName.VARCHAR)
          .add("ORDINAL_POSITION", SqlTypeName.INTEGER)
          .add("POSITION_IN_UNIQUE_CONSTRAINT", SqlTypeName.INTEGER)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      // Iterate through all schemas
      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus schema = rootSchema.subSchemas().get(schemaName);
        if (schema != null) {
          // Iterate through all tables in the schema
          for (String tableName : schema.tables().getNames(LikePattern.any())) {
            Table table = schema.tables().get(tableName);
            if (table != null) {
              // Get column names for the table
              RelDataType rowType = table.getRowType(root.getTypeFactory());
              List<String> columnNames = new ArrayList<>();
              for (RelDataTypeField field : rowType.getFieldList()) {
                columnNames.add(field.getName());
              }

              // Get table statistics which contain constraint information
              Statistic statistic = table.getStatistic();
              if (statistic != null) {
                // Extract primary and unique keys
                List<ImmutableBitSet> keys = statistic.getKeys();
                if (keys != null) {
                  int keyIndex = 0;
                  for (ImmutableBitSet key : keys) {
                    String constraintName = (keyIndex == 0)
                        ? "PK_" + tableName.toUpperCase()
                        : "UK_" + tableName.toUpperCase() + "_" + keyIndex;

                    // Add a row for each column in the key
                    int ordinalPosition = 1;
                    for (int columnIndex : key) {
                      if (columnIndex < columnNames.size()) {
                        rows.add(new Object[]{
                            catalogName,                    // CONSTRAINT_CATALOG
                            schemaName.toUpperCase(),       // CONSTRAINT_SCHEMA
                            constraintName,                  // CONSTRAINT_NAME
                            catalogName,                    // TABLE_CATALOG
                            schemaName.toUpperCase(),       // TABLE_SCHEMA
                            tableName,                       // TABLE_NAME
                            columnNames.get(columnIndex),   // COLUMN_NAME
                            ordinalPosition,                 // ORDINAL_POSITION
                            null                            // POSITION_IN_UNIQUE_CONSTRAINT
                        });
                        ordinalPosition++;
                      }
                    }
                    keyIndex++;
                  }
                }

                // Extract foreign keys
                List<RelReferentialConstraint> foreignKeys = statistic.getReferentialConstraints();
                if (foreignKeys != null) {
                  int fkIndex = 0;
                  for (RelReferentialConstraint fk : foreignKeys) {
                    String constraintName = "FK_" + tableName.toUpperCase() + "_" + fkIndex;

                    // Add a row for each column in the foreign key
                    int ordinalPosition = 1;
                    for (IntPair pair : fk.getColumnPairs()) {
                      int columnIndex = pair.source;
                      if (columnIndex < columnNames.size()) {
                        Integer positionInUnique = pair.target + 1;

                        rows.add(new Object[]{
                            catalogName,                    // CONSTRAINT_CATALOG
                            schemaName.toUpperCase(),       // CONSTRAINT_SCHEMA
                            constraintName,                  // CONSTRAINT_NAME
                            catalogName,                    // TABLE_CATALOG
                            schemaName.toUpperCase(),       // TABLE_SCHEMA
                            tableName,                       // TABLE_NAME
                            columnNames.get(columnIndex),   // COLUMN_NAME
                            ordinalPosition,                 // ORDINAL_POSITION
                            positionInUnique                // POSITION_IN_UNIQUE_CONSTRAINT
                        });
                        ordinalPosition++;
                      }
                    }
                    fkIndex++;
                  }
                }
              }
            }
          }
        }
      }

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  private class ReferentialConstraintsTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("CONSTRAINT_CATALOG", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_SCHEMA", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_NAME", SqlTypeName.VARCHAR)
          .add("UNIQUE_CONSTRAINT_CATALOG", SqlTypeName.VARCHAR)
          .add("UNIQUE_CONSTRAINT_SCHEMA", SqlTypeName.VARCHAR)
          .add("UNIQUE_CONSTRAINT_NAME", SqlTypeName.VARCHAR)
          .add("MATCH_OPTION", SqlTypeName.VARCHAR)
          .add("UPDATE_RULE", SqlTypeName.VARCHAR)
          .add("DELETE_RULE", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      // Iterate through all schemas
      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus schema = rootSchema.subSchemas().get(schemaName);
        if (schema != null) {
          // Get the unwrapped schema to check its actual type
          Schema unwrappedSchema = schema.unwrap(Schema.class);

          // Iterate through all tables in the schema
          for (String tableName : schema.tables().getNames(LikePattern.any())) {
            Table table;

            // Check if this is a ConstraintAwareJdbcSchema - if so, use its getTable method
            // to get the ConstraintAwareJdbcTable wrapper with constraint metadata
            if (unwrappedSchema != null &&
                unwrappedSchema.getClass().getName().contains("ConstraintAwareJdbcSchema")) {
              // Use the schema's getTable method to get the wrapped table
              @SuppressWarnings("deprecation")
              Table wrappedTable = unwrappedSchema.getTable(tableName);
              table = wrappedTable;
            } else {
              // Normal path for other schema types
              table = schema.tables().get(tableName);
            }

            if (table != null) {
              LOGGER.info("InformationSchema: Checking constraints for table '{}' in schema '{}', type: {}",
                          tableName, schemaName, table.getClass().getSimpleName());
              // Get table statistics which contain constraint information
              Statistic statistic = table.getStatistic();
              if (statistic != null) {
                // Extract foreign keys
                List<RelReferentialConstraint> foreignKeys = statistic.getReferentialConstraints();
                if (foreignKeys != null) {
                  int fkIndex = 0;
                  for (RelReferentialConstraint fk : foreignKeys) {
                    String constraintName = "FK_" + tableName.toUpperCase() + "_" + fkIndex;

                    // Get referenced table info
                    String referencedSchema = schemaName.toUpperCase(); // Assume same schema
                    List<String> qualifiedName = fk.getTargetQualifiedName();
                    String referencedTable = qualifiedName.get(qualifiedName.size() - 1);

                    // Generate name for the referenced constraint (primary key)
                    String uniqueConstraintName = "PK_" + referencedTable.toUpperCase();

                    rows.add(new Object[]{
                        catalogName,                    // CONSTRAINT_CATALOG
                        schemaName.toUpperCase(),       // CONSTRAINT_SCHEMA
                        constraintName,                  // CONSTRAINT_NAME
                        catalogName,                    // UNIQUE_CONSTRAINT_CATALOG
                        referencedSchema,                // UNIQUE_CONSTRAINT_SCHEMA
                        uniqueConstraintName,            // UNIQUE_CONSTRAINT_NAME
                        "FULL",                          // MATCH_OPTION
                        "NO ACTION",                     // UPDATE_RULE
                        "NO ACTION"                      // DELETE_RULE
                    });
                    fkIndex++;
                  }
                }
              }
            }
          }
        }
      }

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  private class CheckConstraintsTable extends EmptyTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("CONSTRAINT_CATALOG", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_SCHEMA", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_NAME", SqlTypeName.VARCHAR)
          .add("CHECK_CLAUSE", SqlTypeName.VARCHAR)
          .build();
    }
  }

  private class ViewsTable extends EmptyTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("TABLE_CATALOG", SqlTypeName.VARCHAR)
          .add("TABLE_SCHEMA", SqlTypeName.VARCHAR)
          .add("TABLE_NAME", SqlTypeName.VARCHAR)
          .add("VIEW_DEFINITION", SqlTypeName.VARCHAR)
          .add("CHECK_OPTION", SqlTypeName.VARCHAR)
          .add("IS_UPDATABLE", SqlTypeName.VARCHAR)
          .add("INSERTABLE_INTO", SqlTypeName.VARCHAR)
          .add("IS_TRIGGER_UPDATABLE", SqlTypeName.VARCHAR)
          .add("IS_TRIGGER_DELETABLE", SqlTypeName.VARCHAR)
          .add("IS_TRIGGER_INSERTABLE_INTO", SqlTypeName.VARCHAR)
          .build();
    }
  }

  private class RoutinesTable extends EmptyTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("SPECIFIC_CATALOG", SqlTypeName.VARCHAR)
          .add("SPECIFIC_SCHEMA", SqlTypeName.VARCHAR)
          .add("SPECIFIC_NAME", SqlTypeName.VARCHAR)
          .add("ROUTINE_CATALOG", SqlTypeName.VARCHAR)
          .add("ROUTINE_SCHEMA", SqlTypeName.VARCHAR)
          .add("ROUTINE_NAME", SqlTypeName.VARCHAR)
          .add("ROUTINE_TYPE", SqlTypeName.VARCHAR)
          .add("MODULE_CATALOG", SqlTypeName.VARCHAR)
          .add("MODULE_SCHEMA", SqlTypeName.VARCHAR)
          .add("MODULE_NAME", SqlTypeName.VARCHAR)
          .add("UDT_CATALOG", SqlTypeName.VARCHAR)
          .add("UDT_SCHEMA", SqlTypeName.VARCHAR)
          .add("UDT_NAME", SqlTypeName.VARCHAR)
          .add("DATA_TYPE", SqlTypeName.VARCHAR)
          .add("CHARACTER_MAXIMUM_LENGTH", SqlTypeName.INTEGER)
          .add("CHARACTER_OCTET_LENGTH", SqlTypeName.INTEGER)
          .add("COLLATION_CATALOG", SqlTypeName.VARCHAR)
          .add("COLLATION_SCHEMA", SqlTypeName.VARCHAR)
          .add("COLLATION_NAME", SqlTypeName.VARCHAR)
          .add("CHARACTER_SET_CATALOG", SqlTypeName.VARCHAR)
          .add("CHARACTER_SET_SCHEMA", SqlTypeName.VARCHAR)
          .add("CHARACTER_SET_NAME", SqlTypeName.VARCHAR)
          .add("NUMERIC_PRECISION", SqlTypeName.INTEGER)
          .add("NUMERIC_PRECISION_RADIX", SqlTypeName.INTEGER)
          .add("NUMERIC_SCALE", SqlTypeName.INTEGER)
          .add("DATETIME_PRECISION", SqlTypeName.INTEGER)
          .add("INTERVAL_TYPE", SqlTypeName.VARCHAR)
          .add("INTERVAL_PRECISION", SqlTypeName.INTEGER)
          .add("TYPE_UDT_CATALOG", SqlTypeName.VARCHAR)
          .add("TYPE_UDT_SCHEMA", SqlTypeName.VARCHAR)
          .add("TYPE_UDT_NAME", SqlTypeName.VARCHAR)
          .add("SCOPE_CATALOG", SqlTypeName.VARCHAR)
          .add("SCOPE_SCHEMA", SqlTypeName.VARCHAR)
          .add("SCOPE_NAME", SqlTypeName.VARCHAR)
          .add("MAXIMUM_CARDINALITY", SqlTypeName.INTEGER)
          .add("DTD_IDENTIFIER", SqlTypeName.VARCHAR)
          .add("ROUTINE_BODY", SqlTypeName.VARCHAR)
          .add("ROUTINE_DEFINITION", SqlTypeName.VARCHAR)
          .add("EXTERNAL_NAME", SqlTypeName.VARCHAR)
          .add("EXTERNAL_LANGUAGE", SqlTypeName.VARCHAR)
          .add("PARAMETER_STYLE", SqlTypeName.VARCHAR)
          .add("IS_DETERMINISTIC", SqlTypeName.VARCHAR)
          .add("SQL_DATA_ACCESS", SqlTypeName.VARCHAR)
          .add("IS_NULL_CALL", SqlTypeName.VARCHAR)
          .add("SQL_PATH", SqlTypeName.VARCHAR)
          .add("SCHEMA_LEVEL_ROUTINE", SqlTypeName.VARCHAR)
          .add("MAX_DYNAMIC_RESULT_SETS", SqlTypeName.INTEGER)
          .add("IS_USER_DEFINED_CAST", SqlTypeName.VARCHAR)
          .add("IS_IMPLICITLY_INVOCABLE", SqlTypeName.VARCHAR)
          .add("SECURITY_TYPE", SqlTypeName.VARCHAR)
          .add("TO_SQL_SPECIFIC_CATALOG", SqlTypeName.VARCHAR)
          .add("TO_SQL_SPECIFIC_SCHEMA", SqlTypeName.VARCHAR)
          .add("TO_SQL_SPECIFIC_NAME", SqlTypeName.VARCHAR)
          .add("AS_LOCATOR", SqlTypeName.VARCHAR)
          .add("CREATED", SqlTypeName.TIMESTAMP)
          .add("LAST_ALTERED", SqlTypeName.TIMESTAMP)
          .add("NEW_SAVEPOINT_LEVEL", SqlTypeName.VARCHAR)
          .add("IS_UDT_DEPENDENT", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_FROM_DATA_TYPE", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_AS_LOCATOR", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_CHAR_MAX_LENGTH", SqlTypeName.INTEGER)
          .add("RESULT_CAST_CHAR_OCTET_LENGTH", SqlTypeName.INTEGER)
          .add("RESULT_CAST_CHAR_SET_CATALOG", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_CHAR_SET_SCHEMA", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_CHAR_SET_NAME", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_COLLATION_CATALOG", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_COLLATION_SCHEMA", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_COLLATION_NAME", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_NUMERIC_PRECISION", SqlTypeName.INTEGER)
          .add("RESULT_CAST_NUMERIC_PRECISION_RADIX", SqlTypeName.INTEGER)
          .add("RESULT_CAST_NUMERIC_SCALE", SqlTypeName.INTEGER)
          .add("RESULT_CAST_DATETIME_PRECISION", SqlTypeName.INTEGER)
          .add("RESULT_CAST_INTERVAL_TYPE", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_INTERVAL_PRECISION", SqlTypeName.INTEGER)
          .add("RESULT_CAST_TYPE_UDT_CATALOG", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_TYPE_UDT_SCHEMA", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_TYPE_UDT_NAME", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_SCOPE_CATALOG", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_SCOPE_SCHEMA", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_SCOPE_NAME", SqlTypeName.VARCHAR)
          .add("RESULT_CAST_MAXIMUM_CARDINALITY", SqlTypeName.INTEGER)
          .add("RESULT_CAST_DTD_IDENTIFIER", SqlTypeName.VARCHAR)
          .build();
    }
  }

  private class ParametersTable extends EmptyTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("SPECIFIC_CATALOG", SqlTypeName.VARCHAR)
          .add("SPECIFIC_SCHEMA", SqlTypeName.VARCHAR)
          .add("SPECIFIC_NAME", SqlTypeName.VARCHAR)
          .add("ORDINAL_POSITION", SqlTypeName.INTEGER)
          .add("PARAMETER_MODE", SqlTypeName.VARCHAR)
          .add("IS_RESULT", SqlTypeName.VARCHAR)
          .add("AS_LOCATOR", SqlTypeName.VARCHAR)
          .add("PARAMETER_NAME", SqlTypeName.VARCHAR)
          .add("DATA_TYPE", SqlTypeName.VARCHAR)
          .add("CHARACTER_MAXIMUM_LENGTH", SqlTypeName.INTEGER)
          .add("CHARACTER_OCTET_LENGTH", SqlTypeName.INTEGER)
          .add("COLLATION_CATALOG", SqlTypeName.VARCHAR)
          .add("COLLATION_SCHEMA", SqlTypeName.VARCHAR)
          .add("COLLATION_NAME", SqlTypeName.VARCHAR)
          .add("CHARACTER_SET_CATALOG", SqlTypeName.VARCHAR)
          .add("CHARACTER_SET_SCHEMA", SqlTypeName.VARCHAR)
          .add("CHARACTER_SET_NAME", SqlTypeName.VARCHAR)
          .add("NUMERIC_PRECISION", SqlTypeName.INTEGER)
          .add("NUMERIC_PRECISION_RADIX", SqlTypeName.INTEGER)
          .add("NUMERIC_SCALE", SqlTypeName.INTEGER)
          .add("DATETIME_PRECISION", SqlTypeName.INTEGER)
          .add("INTERVAL_TYPE", SqlTypeName.VARCHAR)
          .add("INTERVAL_PRECISION", SqlTypeName.INTEGER)
          .add("UDT_CATALOG", SqlTypeName.VARCHAR)
          .add("UDT_SCHEMA", SqlTypeName.VARCHAR)
          .add("UDT_NAME", SqlTypeName.VARCHAR)
          .add("SCOPE_CATALOG", SqlTypeName.VARCHAR)
          .add("SCOPE_SCHEMA", SqlTypeName.VARCHAR)
          .add("SCOPE_NAME", SqlTypeName.VARCHAR)
          .add("MAXIMUM_CARDINALITY", SqlTypeName.INTEGER)
          .add("DTD_IDENTIFIER", SqlTypeName.VARCHAR)
          .build();
    }
  }

  // Base class for empty tables
  private abstract class EmptyTable extends AbstractTable implements ScannableTable {
    @Override public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  // Helper methods
  private String mapSqlTypeToString(SqlTypeName typeName) {
    switch (typeName) {
      case BOOLEAN:
        return "BOOLEAN";
      case TINYINT:
        return "TINYINT";
      case SMALLINT:
        return "SMALLINT";
      case INTEGER:
        return "INTEGER";
      case BIGINT:
        return "BIGINT";
      case FLOAT:
      case REAL:
        return "REAL";
      case DOUBLE:
        return "DOUBLE";
      case DECIMAL:
        return "NUMERIC";
      case CHAR:
        return "CHAR";
      case VARCHAR:
        return "VARCHAR";
      case DATE:
        return "DATE";
      case TIME:
        return "TIME";
      case TIMESTAMP:
        return "TIMESTAMP";
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return "TIMESTAMP WITH TIME ZONE";
      default:
        return "VARCHAR";
    }
  }

  private Integer getCharMaxLength(RelDataType type) {
    if (type.getSqlTypeName() == SqlTypeName.CHAR ||
        type.getSqlTypeName() == SqlTypeName.VARCHAR) {
      return type.getPrecision() > 0 ? type.getPrecision() : null;
    }
    return null;
  }

  private Integer getCharOctetLength(RelDataType type) {
    Integer charMax = getCharMaxLength(type);
    return charMax != null ? charMax * 4 : null; // UTF-8 max 4 bytes per char
  }

  private Integer getNumericPrecision(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case TINYINT:
        return 3;
      case SMALLINT:
        return 5;
      case INTEGER:
        return 10;
      case BIGINT:
        return 19;
      case FLOAT:
      case REAL:
        return 24;
      case DOUBLE:
        return 53;
      case DECIMAL:
        return type.getPrecision() > 0 ? type.getPrecision() : 38;
      default:
        return null;
    }
  }

  private Integer getNumericPrecisionRadix(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case REAL:
      case DOUBLE:
      case DECIMAL:
        return 10;
      default:
        return null;
    }
  }

  private Integer getNumericScale(RelDataType type) {
    if (type.getSqlTypeName() == SqlTypeName.DECIMAL) {
      return type.getScale();
    }
    return null;
  }

  private Integer getDatetimePrecision(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return type.getPrecision() > 0 ? type.getPrecision() : 6;
      default:
        return null;
    }
  }

  private boolean isCharType(SqlTypeName typeName) {
    return typeName == SqlTypeName.CHAR ||
           typeName == SqlTypeName.VARCHAR;
  }

  // Removed duplicate helper class - use org.apache.calcite.rel.type.RelDataTypeField directly
}
