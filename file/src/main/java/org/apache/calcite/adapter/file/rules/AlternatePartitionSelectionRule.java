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
package org.apache.calcite.adapter.file.rules;

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
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * This looks up registered alternate partitions from the schema metadata.
   */
  private List<AlternatePartitionInfo> getAlternatePartitions(RelOptTable table) {
    List<AlternatePartitionInfo> alternates = new ArrayList<>();

    // Get alternate partition metadata from the table
    // This metadata is set during schema creation by expandAlternatePartitions()
    Table unwrappedTable = table.unwrap(Table.class);
    if (unwrappedTable instanceof PartitionedParquetTable) {
      PartitionedParquetTable ppt = (PartitionedParquetTable) unwrappedTable;

      // Access alternate partition metadata via the config
      // The alternate tables are stored in the schema, linked by _sourceTableName
      // For now, we rely on the schema to provide this information through
      // a registry pattern. This is a placeholder for the actual implementation.

      // TODO: Implement schema-level registry for alternate partitions
      // The registry should map source table names to their alternates
      LOGGER.debug("Looking up alternate partitions for table: {}",
          table.getQualifiedName());
    }

    return alternates;
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
