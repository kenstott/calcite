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
package org.apache.calcite.adapter.file.rules;

import org.apache.calcite.adapter.file.partition.AlternatePartitionRegistry;
import org.apache.calcite.adapter.file.partition.AlternatePartitionRegistry.AlternateInfo;
import org.apache.calcite.adapter.file.table.PartitionedParquetTable;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Planner rule that substitutes a table scan with an alternate partition table
 * to minimize the number of files scanned.
 *
 * <p>The goal is to find the partition layout that requires scanning the fewest files
 * while still supporting partition pruning for the query's WHERE clause predicates.
 *
 * <p>Selection criteria:
 * <ol>
 *   <li>The alternate must have all filter columns as partition keys (enables pruning)</li>
 *   <li>Among qualifying alternates, select the one with fewest total partition keys</li>
 * </ol>
 *
 * <p>Why fewer partition keys = fewer files:
 * <ul>
 *   <li>More partition keys = more directory nesting = more files</li>
 *   <li>An alternate with fewer keys has pre-consolidated data along other dimensions</li>
 *   <li>As long as filter columns are partition keys, pruning still works</li>
 * </ul>
 *
 * <p>Example:
 * <pre>
 * Source:    year=*\/state=*\/population.parquet  (50 years x 50 states = 2500 files)
 * Alternate: state=*\/population.parquet          (50 files, one per state)
 *
 * Query: SELECT * FROM population WHERE state = 'CA'
 *
 * Source scan:    50 files (all years for CA)
 * Alternate scan: 1 file   (just CA, all years pre-consolidated)
 * </pre>
 *
 * <p>The alternate wins because it scans fewer files while still pruning by state.
 */
@Value.Enclosing
public class AlternatePartitionSelectionRule extends RelRule<AlternatePartitionSelectionRule.Config> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlternatePartitionSelectionRule.class);

  public static final AlternatePartitionSelectionRule INSTANCE = Config.DEFAULT.toRule();

  private AlternatePartitionSelectionRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);
    final TableScan scan = call.rel(1);

    // Only process Parquet tables
    Table table = scan.getTable().unwrap(Table.class);
    if (!(table instanceof PartitionedParquetTable)) {
      return;
    }

    PartitionedParquetTable parquetTable = (PartitionedParquetTable) table;

    // Check if this table has alternate partitions registered
    List<AlternatePartitionInfo> alternates = getAlternatePartitions(scan.getTable());
    if (alternates.isEmpty()) {
      return;
    }

    // Extract filter columns from the query
    Set<String> filterColumns = extractFilterColumns(filter.getCondition(), scan);
    if (filterColumns.isEmpty()) {
      return;
    }

    LOGGER.debug("AlternatePartitionSelectionRule: table={}, filterColumns={}",
        scan.getTable().getQualifiedName(), filterColumns);

    // Find the best alternate partition based on matching partition keys
    AlternatePartitionInfo bestAlternate = selectBestAlternate(alternates, filterColumns);

    if (bestAlternate != null && bestAlternate.matchingKeyCount > 0) {
      // Transform to use the alternate partition table
      RelNode newScan = createAlternateScan(scan, bestAlternate, call);
      if (newScan != null) {
        RelNode newFilter = LogicalFilter.create(newScan, filter.getCondition());
        call.transformTo(newFilter);

        LOGGER.debug("AlternatePartitionSelectionRule: substituted '{}' with alternate '{}' "
            + "(matching keys: {})",
            scan.getTable().getQualifiedName(),
            bestAlternate.tableName,
            bestAlternate.matchingKeyCount);
      }
    }
  }

  /**
   * Extracts the column names used in filter conditions.
   */
  private Set<String> extractFilterColumns(RexNode condition, TableScan scan) {
    Set<String> columns = new HashSet<>();
    extractFilterColumnsRecursive(condition, scan, columns);
    return columns;
  }

  private void extractFilterColumnsRecursive(RexNode node, TableScan scan, Set<String> columns) {
    if (node instanceof RexInputRef) {
      RexInputRef ref = (RexInputRef) node;
      String columnName = scan.getRowType().getFieldNames().get(ref.getIndex());
      columns.add(columnName.toLowerCase());
    } else if (node instanceof RexCall) {
      RexCall call = (RexCall) node;
      // Only consider equality and comparison operations
      SqlKind kind = call.getKind();
      if (kind == SqlKind.EQUALS || kind == SqlKind.LESS_THAN || kind == SqlKind.GREATER_THAN
          || kind == SqlKind.LESS_THAN_OR_EQUAL || kind == SqlKind.GREATER_THAN_OR_EQUAL
          || kind == SqlKind.AND || kind == SqlKind.OR || kind == SqlKind.IN) {
        for (RexNode operand : call.getOperands()) {
          extractFilterColumnsRecursive(operand, scan, columns);
        }
      }
    }
  }

  /**
   * Gets alternate partition information for a table.
   * This looks up registered alternate partitions from the schema's registry.
   */
  private List<AlternatePartitionInfo> getAlternatePartitions(RelOptTable table) {
    List<AlternatePartitionInfo> alternates = new ArrayList<AlternatePartitionInfo>();

    // Get alternate partition metadata from the table
    Table unwrappedTable = table.unwrap(Table.class);
    if (!(unwrappedTable instanceof PartitionedParquetTable)) {
      return alternates;
    }

    // Get the source table name
    List<String> qualifiedName = table.getQualifiedName();
    if (qualifiedName.size() < 2) {
      return alternates;
    }
    String tableName = qualifiedName.get(qualifiedName.size() - 1);

    // Try to get the AlternatePartitionRegistry from the schema
    AlternatePartitionRegistry registry = getRegistryFromSchema(table);
    if (registry == null) {
      LOGGER.debug("No AlternatePartitionRegistry found for table: {}", qualifiedName);
      return alternates;
    }

    // Get all alternates for this source table
    List<AlternateInfo> registryAlternates = registry.getAlternates(tableName);
    for (AlternateInfo info : registryAlternates) {
      // Only include materialized alternates
      if (info.isMaterialized()) {
        alternates.add(
            new AlternatePartitionInfo(
            info.getAlternateName(),
            info.getPattern(),
            info.getPartitionKeys()));
      }
    }

    LOGGER.debug("Found {} alternate partitions for table {}: {}",
        alternates.size(), tableName, alternates);

    return alternates;
  }

  /**
   * Tries to get the AlternatePartitionRegistry from the schema.
   * Uses reflection to avoid circular dependencies.
   */
  private AlternatePartitionRegistry getRegistryFromSchema(RelOptTable table) {
    try {
      // Get the schema from the table's qualified name
      RelOptSchema relOptSchema = table.getRelOptSchema();
      if (relOptSchema == null) {
        return null;
      }

      // Try to get the underlying Schema
      List<String> qualifiedName = table.getQualifiedName();
      if (qualifiedName.size() < 2) {
        return null;
      }

      // Look up the schema by name
      List<String> schemaPath = qualifiedName.subList(0, qualifiedName.size() - 1);
      RelOptTable schemaTable = relOptSchema.getTableForMember(schemaPath);

      // Use reflection to call getAlternatePartitionRegistry() if available
      Schema schema = null;
      if (schemaTable != null) {
        schema = schemaTable.unwrap(Schema.class);
      }

      if (schema == null) {
        // Try getting schema from the table itself
        schema = table.unwrap(Schema.class);
      }

      if (schema != null) {
        try {
          Method method = schema.getClass().getMethod("getAlternatePartitionRegistry");
          Object result = method.invoke(schema);
          if (result instanceof AlternatePartitionRegistry) {
            return (AlternatePartitionRegistry) result;
          }
        } catch (NoSuchMethodException e) {
          LOGGER.debug("Schema does not have getAlternatePartitionRegistry method");
        } catch (Exception e) {
          LOGGER.debug("Error getting registry via reflection: {}", e.getMessage());
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Error getting AlternatePartitionRegistry: {}", e.getMessage());
    }
    return null;
  }

  /**
   * Selects the best alternate partition based on filter coverage and key count.
   * Implements the "fewest keys that cover the filter" heuristic.
   *
   * <p>Algorithm:
   * 1. Check if alternate's partition keys cover all filter columns
   * 2. Among alternates that cover all filters, select the one with fewest total keys
   * 3. Fewer keys = more consolidated data = fewer files to scan
   */
  private AlternatePartitionInfo selectBestAlternate(
      List<AlternatePartitionInfo> alternates, Set<String> filterColumns) {

    AlternatePartitionInfo best = null;
    int bestTotalKeys = Integer.MAX_VALUE;

    for (AlternatePartitionInfo alternate : alternates) {
      // Convert partition keys to lowercase set for comparison
      Set<String> partitionKeySet = new HashSet<>();
      for (String key : alternate.partitionKeys) {
        partitionKeySet.add(key.toLowerCase());
      }

      // Check if all filter columns are covered by partition keys
      boolean coversAllFilters = true;
      int matchCount = 0;
      for (String filterCol : filterColumns) {
        if (partitionKeySet.contains(filterCol)) {
          matchCount++;
        } else {
          coversAllFilters = false;
        }
      }

      // Only consider alternates that cover all filter columns
      if (coversAllFilters && matchCount > 0) {
        int totalKeys = alternate.partitionKeys.size();

        // Prefer alternate with fewer total partition keys (more consolidated)
        if (totalKeys < bestTotalKeys) {
          bestTotalKeys = totalKeys;
          alternate.matchingKeyCount = matchCount;
          best = alternate;
        }
      }
    }

    return best;
  }

  /**
   * Creates a new table scan for the alternate partition table.
   */
  private RelNode createAlternateScan(TableScan originalScan,
      AlternatePartitionInfo alternate, RelOptRuleCall call) {

    // Look up the alternate table in the schema
    // The alternate table should have been created by expandAlternatePartitions()
    RelOptTable alternateTable = findAlternateTable(originalScan, alternate.tableName, call);

    if (alternateTable != null) {
      return LogicalTableScan.create(
          originalScan.getCluster(),
          alternateTable,
          originalScan.getHints());
    }

    return null;
  }

  /**
   * Finds an alternate table by name in the same schema.
   */
  private RelOptTable findAlternateTable(TableScan originalScan, String alternateName,
      RelOptRuleCall call) {
    List<String> qualifiedName = originalScan.getTable().getQualifiedName();
    if (qualifiedName.size() < 2) {
      return null;
    }

    // Build qualified name for alternate table (same schema, different table name)
    List<String> alternatePath = new ArrayList<>(qualifiedName.subList(0, qualifiedName.size() - 1));
    alternatePath.add(alternateName);

    // Look up in the table's schema
    RelOptSchema schema = originalScan.getTable().getRelOptSchema();
    if (schema == null) {
      return null;
    }
    return schema.getTableForMember(alternatePath);
  }

  /**
   * Holds information about an alternate partition table.
   */
  private static class AlternatePartitionInfo {
    final String tableName;
    final String pattern;
    final List<String> partitionKeys;
    int matchingKeyCount;

    AlternatePartitionInfo(String tableName, String pattern, List<String> partitionKeys) {
      this.tableName = tableName;
      this.pattern = pattern;
      this.partitionKeys = partitionKeys;
      this.matchingKeyCount = 0;
    }
  }

  /** Configuration for AlternatePartitionSelectionRule. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAlternatePartitionSelectionRule.Config.builder()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalFilter.class)
                .oneInput(b1 ->
                    b1.operand(TableScan.class).noInputs()))
        .build();

    @Override default AlternatePartitionSelectionRule toRule() {
      return new AlternatePartitionSelectionRule(this);
    }
  }
}
