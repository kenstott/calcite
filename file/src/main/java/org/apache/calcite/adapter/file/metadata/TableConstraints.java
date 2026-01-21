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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelReferentialConstraintImpl;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

/**
 * Table constraint metadata for file-based tables.
 * Allows defining primary keys and foreign keys through configuration,
 * even though the underlying storage (e.g., Parquet) doesn't enforce them.
 *
 * <p>These constraints serve as metadata hints for query optimization and
 * documentation of the logical data model.
 */
public class TableConstraints {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableConstraints.class);

  /**
   * Creates a Statistic object from constraint configuration.
   *
   * @param tableConfig The table configuration map
   * @param columnNames The ordered list of column names in the table
   * @param rowCount Optional row count estimate
   * @return A Statistic with the configured constraints
   */
  @SuppressWarnings("unchecked")
  public static Statistic fromConfig(
      Map<String, Object> tableConfig,
      List<String> columnNames,
      @Nullable Double rowCount) {
    return fromConfig(tableConfig, columnNames, rowCount, null, null);
  }

  /**
   * Creates a Statistic object from constraint configuration with qualified name.
   *
   * @param tableConfig The table configuration map
   * @param columnNames The ordered list of column names in the table
   * @param rowCount Optional row count estimate
   * @param schemaName The schema name this table belongs to
   * @param tableName The table name
   * @return A Statistic with the configured constraints
   */
  @SuppressWarnings("unchecked")
  public static Statistic fromConfig(
      Map<String, Object> tableConfig,
      List<String> columnNames,
      @Nullable Double rowCount,
      @Nullable String schemaName,
      @Nullable String tableName) {

    // Extract constraint configuration
    Map<String, Object> constraints = (Map<String, Object>) tableConfig.get("constraints");
    if (constraints == null) {
      // No constraints defined
      return Statistics.UNKNOWN;
    }

    // If column names are not available yet (lazy evaluation), extract them from constraints
    List<String> effectiveColumnNames = columnNames;
    if (columnNames == null || columnNames.isEmpty()) {
      effectiveColumnNames = extractColumnNamesFromConstraints(constraints);
    }

    // Parse primary keys
    List<ImmutableBitSet> keys = parsePrimaryKeys(constraints, effectiveColumnNames);

    // Parse foreign keys
    List<RelReferentialConstraint> foreignKeys = parseForeignKeys(constraints, effectiveColumnNames, schemaName, tableName);

    // Parse collations (optional)
    List<RelCollation> collations = parseCollations(constraints, columnNames);

    // Create and return the statistic
    return Statistics.of(rowCount, keys, foreignKeys, collations);
  }

  /**
   * Extracts all column names referenced in constraints.
   * This is used as a fallback when column names are not yet available (lazy evaluation).
   *
   * @param constraints The constraints configuration map
   * @return List of unique column names from all constraints
   */
  @SuppressWarnings("unchecked")
  private static List<String> extractColumnNamesFromConstraints(Map<String, Object> constraints) {
    List<String> allColumns = new ArrayList<>();

    // Extract from primary key
    Object primaryKey = constraints.get("primaryKey");
    if (primaryKey instanceof List) {
      @SuppressWarnings("unchecked")
      List<String> pkColumns = (List<String>) primaryKey;
      allColumns.addAll(pkColumns);
    } else if (primaryKey instanceof String) {
      // Handle single column primary key
      allColumns.add((String) primaryKey);
    }

    // Extract from unique keys
    List<List<String>> uniqueKeys = (List<List<String>>) constraints.get("uniqueKeys");
    if (uniqueKeys != null) {
      for (List<String> keyColumns : uniqueKeys) {
        allColumns.addAll(keyColumns);
      }
    }

    // Extract from foreign keys
    Object foreignKeysObj = constraints.get("foreignKeys");
    if (foreignKeysObj instanceof List) {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> foreignKeys = (List<Map<String, Object>>) foreignKeysObj;
      for (Map<String, Object> fk : foreignKeys) {
        Object columnsObj = fk.get("columns");
        if (columnsObj instanceof List) {
          @SuppressWarnings("unchecked")
          List<String> columns = (List<String>) columnsObj;
          allColumns.addAll(columns);
        }
      }
    }

    // Return unique columns in order
    return new ArrayList<>(new java.util.LinkedHashSet<>(allColumns));
  }

  /**
   * Parses primary key definitions from configuration.
   *
   * @param constraints The constraints configuration map
   * @param columnNames The ordered list of column names
   * @return List of primary keys as bit sets
   */
  @SuppressWarnings("unchecked")
  private static List<ImmutableBitSet> parsePrimaryKeys(
      Map<String, Object> constraints,
      List<String> columnNames) {

    List<ImmutableBitSet> keys = new ArrayList<>();

    // Primary key can be a single column list or multiple keys
    Object primaryKey = constraints.get("primaryKey");
    if (primaryKey instanceof List) {
      List<String> keyColumns = (List<String>) primaryKey;
      ImmutableBitSet.Builder keyBuilder = ImmutableBitSet.builder();
      for (String col : keyColumns) {
        int index = columnNames.indexOf(col);
        if (index >= 0) {
          keyBuilder.set(index);
        }
      }
      ImmutableBitSet key = keyBuilder.build();
      if (!key.isEmpty()) {
        keys.add(key);
      }
    }

    // Also support multiple unique keys
    List<List<String>> uniqueKeys = (List<List<String>>) constraints.get("uniqueKeys");
    if (uniqueKeys != null) {
      for (List<String> keyColumns : uniqueKeys) {
        ImmutableBitSet.Builder keyBuilder = ImmutableBitSet.builder();
        for (String col : keyColumns) {
          int index = columnNames.indexOf(col);
          if (index >= 0) {
            keyBuilder.set(index);
          }
        }
        ImmutableBitSet key = keyBuilder.build();
        if (!key.isEmpty()) {
          keys.add(key);
        }
      }
    }

    return keys;
  }

  /**
   * Parses foreign key definitions from configuration.
   *
   * @param constraints The constraints configuration map
   * @param columnNames The ordered list of column names
   * @param schemaName The schema name this table belongs to
   * @param tableName The table name
   * @return List of foreign key constraints
   */
  @SuppressWarnings("unchecked")
  private static List<RelReferentialConstraint> parseForeignKeys(
      Map<String, Object> constraints,
      List<String> columnNames,
      @Nullable String schemaName,
      @Nullable String tableName) {

    List<RelReferentialConstraint> foreignKeys = new ArrayList<>();

    List<Map<String, Object>> fkList =
        (List<Map<String, Object>>) constraints.get("foreignKeys");
    if (fkList == null) {
      return foreignKeys;
    }

    for (Map<String, Object> fkDef : fkList) {
      // Extract foreign key definition
      Object columnsObj = fkDef.get("columns");
      Object targetTableObj = fkDef.get("targetTable");
      Object targetColumnsObj = fkDef.get("targetColumns");

      // Handle columns (should be List<String>)
      List<String> sourceColumns = null;
      if (columnsObj instanceof List) {
        sourceColumns = (List<String>) columnsObj;
      }

      // Handle targetTable (can be String or List<String>)
      List<String> targetTable = null;
      if (targetTableObj instanceof String) {
        // If targetSchema is specified separately, use it
        String targetSchema = (String) fkDef.get("targetSchema");
        if (targetSchema != null) {
          targetTable = List.of(targetSchema, (String) targetTableObj);
        } else if (schemaName != null) {
          // Default to same schema as source table
          targetTable = List.of(schemaName, (String) targetTableObj);
        } else {
          targetTable = List.of((String) targetTableObj);
        }
      } else if (targetTableObj instanceof List) {
        targetTable = (List<String>) targetTableObj;
      }

      // Handle targetColumns (should be List<String>)
      List<String> targetColumns = null;
      if (targetColumnsObj instanceof List) {
        targetColumns = (List<String>) targetColumnsObj;
      }

      if (sourceColumns == null || targetTable == null || targetColumns == null) {
        continue; // Skip invalid FK definition
      }

      // Build column pairs
      List<IntPair> columnPairs = new ArrayList<>();
      for (int i = 0; i < sourceColumns.size() && i < targetColumns.size(); i++) {
        int sourceIndex = columnNames.indexOf(sourceColumns.get(i));
        int targetIndex = i; // Target column ordinal in target table
        if (sourceIndex >= 0) {
          columnPairs.add(IntPair.of(sourceIndex, targetIndex));
        }
      }

      if (!columnPairs.isEmpty()) {
        // Source table name should be provided in the config
        List<String> sourceTable = (List<String>) fkDef.get("sourceTable");
        if (sourceTable == null) {
          // If not specified, use the provided schema and table names
          if (schemaName != null && tableName != null) {
            sourceTable = List.of(schemaName, tableName);
          } else {
            // Fallback to empty list if names not provided
            sourceTable = new ArrayList<>();
          }
        }

        // If target table doesn't have a schema, assume same schema as source
        if (targetTable.size() == 1 && schemaName != null) {
          // Only table name provided, prepend the schema
          targetTable = List.of(schemaName, targetTable.get(0));
        }

        RelReferentialConstraint fk =
            RelReferentialConstraintImpl.of(sourceTable, targetTable, columnPairs);
        foreignKeys.add(fk);
      }
    }

    return foreignKeys;
  }

  /**
   * Parses collation definitions from configuration.
   *
   * @param constraints The constraints configuration map
   * @param columnNames The ordered list of column names
   * @return List of collations (currently returns null as not implemented)
   */
  private static @Nullable List<RelCollation> parseCollations(
      Map<String, Object> constraints,
      List<String> columnNames) {
    // Collations could be added in the future if needed
    // For now, return null to indicate no specific collations
    return null;
  }

  /**
   * Creates a simple primary key constraint configuration.
   *
   * @param keyColumns The columns that form the primary key
   * @return A configuration map with the primary key
   */
  public static Map<String, Object> primaryKey(String... keyColumns) {
    return Map.of("primaryKey", List.of(keyColumns));
  }

  /**
   * Creates a foreign key constraint configuration.
   *
   * @param columns The source columns
   * @param targetSchema The target schema name
   * @param targetTable The target table name
   * @param targetColumns The target columns
   * @return A configuration map with the foreign key
   */
  public static Map<String, Object> foreignKey(
      List<String> columns,
      String targetSchema,
      String targetTable,
      List<String> targetColumns) {
    return Map.of(
        "columns", columns,
        "targetTable", List.of(targetSchema, targetTable),
        "targetColumns", targetColumns);
  }

  /**
   * Validates foreign key constraints and returns a new Statistic with only valid FKs.
   *
   * <p>This method filters out FKs that reference non-existent tables and logs
   * warnings for each invalid constraint found.
   *
   * @param statistic The original statistic containing FK constraints
   * @param sourceTableName Name of the source table (for logging)
   * @param tableExistsChecker Predicate that checks if a table exists given (schemaName, tableName)
   * @return A new Statistic with only valid FK constraints
   */
  public static Statistic validateForeignKeys(
      Statistic statistic,
      String sourceTableName,
      BiPredicate<String, String> tableExistsChecker) {

    if (statistic == null || statistic.getReferentialConstraints() == null
        || statistic.getReferentialConstraints().isEmpty()) {
      return statistic;
    }

    List<RelReferentialConstraint> validFks = new ArrayList<>();
    List<RelReferentialConstraint> invalidFks = new ArrayList<>();

    for (RelReferentialConstraint fk : statistic.getReferentialConstraints()) {
      List<String> targetQualifiedName = fk.getTargetQualifiedName();

      // Extract schema and table from qualified name
      String targetSchema = null;
      String targetTable = null;

      if (targetQualifiedName.size() >= 2) {
        targetSchema = targetQualifiedName.get(targetQualifiedName.size() - 2);
        targetTable = targetQualifiedName.get(targetQualifiedName.size() - 1);
      } else if (targetQualifiedName.size() == 1) {
        targetTable = targetQualifiedName.get(0);
      }

      if (targetTable != null && tableExistsChecker.test(targetSchema, targetTable)) {
        validFks.add(fk);
      } else {
        invalidFks.add(fk);
      }
    }

    // Log warnings for invalid FKs
    for (RelReferentialConstraint invalidFk : invalidFks) {
      LOGGER.warn("Removing invalid FK constraint from table '{}': target table '{}' not found",
          sourceTableName, invalidFk.getTargetQualifiedName());
    }

    if (invalidFks.isEmpty()) {
      return statistic; // No changes needed
    }

    // Return new statistic with filtered FKs
    return Statistics.of(
        statistic.getRowCount(),
        statistic.getKeys(),
        validFks,
        statistic.getCollations());
  }
}
