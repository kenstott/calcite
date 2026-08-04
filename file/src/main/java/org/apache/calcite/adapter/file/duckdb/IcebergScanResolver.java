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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Resolves the Iceberg table behind a planner scan.
 *
 * <p>Several rules answer a query from Iceberg metadata instead of letting it reach DuckDB —
 * {@code COUNT(*)} from manifests, distinct counts from published Puffin statistics. They all need
 * the same walk to get there: scan → {@link JdbcTableScan} → {@link DuckDBJdbcSchema} →
 * {@code FileSchema} → conversion record → table location → an {@code org.apache.iceberg.Table}
 * loaded through whichever storage the schema uses. Sharing that walk keeps the rules honest about
 * what "this scan is Iceberg-backed" means; each re-deriving it would let their notions drift.
 *
 * <p>Every step returns null rather than throwing when the shape does not match. A scan that is
 * not Iceberg-backed is the ordinary case, not an error — the rule simply declines and the query
 * executes normally.
 */
public final class IcebergScanResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergScanResolver.class);

  private IcebergScanResolver() {
  }

  /**
   * Finds the {@link TableScan} under a planner node, tolerating Volcano's {@code RelSubset}
   * indirection.
   *
   * @param stopAtFilter when true, recursion stops at a Filter. Whole-table answers (a cached row
   *     count, a column cardinality) describe the unfiltered table, so a predicate between the
   *     aggregate and the scan invalidates them; callers that only need to identify the table can
   *     pass false.
   */
  public static TableScan findTableScan(RelNode node, boolean stopAtFilter) {
    if (node == null) {
      return null;
    }
    if (stopAtFilter && node instanceof org.apache.calcite.rel.core.Filter) {
      return null;
    }
    // RelSubset is not on the public API surface; reach through it reflectively to whichever
    // concrete node it currently represents.
    if (node.getClass().getName().contains("RelSubset")) {
      try {
        java.lang.reflect.Method getBest = node.getClass().getMethod("getBest");
        RelNode best = (RelNode) getBest.invoke(node);
        if (best != null && best != node) {
          return findTableScan(best, stopAtFilter);
        }
        java.lang.reflect.Method getOriginal = node.getClass().getMethod("getOriginal");
        RelNode original = (RelNode) getOriginal.invoke(node);
        if (original != null && original != node) {
          return findTableScan(original, stopAtFilter);
        }
      } catch (Exception e) {
        // fall through to the ordinary traversal
      }
    }
    if (node instanceof TableScan) {
      return (TableScan) node;
    }
    for (RelNode input : node.getInputs()) {
      TableScan scan = findTableScan(input, stopAtFilter);
      if (scan != null) {
        return scan;
      }
    }
    return null;
  }

  /** The DuckDB-backed schema behind a scan, or null when the scan is not one of ours. */
  public static DuckDBJdbcSchema duckDbSchema(TableScan tableScan) {
    if (!(tableScan instanceof JdbcTableScan)) {
      return null;
    }
    JdbcTable jdbcTable = ((JdbcTableScan) tableScan).jdbcTable;
    JdbcSchema jdbcSchema = jdbcTable.jdbcSchema;
    return jdbcSchema instanceof DuckDBJdbcSchema ? (DuckDBJdbcSchema) jdbcSchema : null;
  }

  /** Unqualified table name from a scan's qualified name. */
  public static String tableName(TableScan tableScan) {
    List<String> qualified = tableScan.getTable().getQualifiedName();
    return qualified.isEmpty() ? "" : qualified.get(qualified.size() - 1);
  }

  /**
   * Loads the Iceberg table backing a scan, or null when the scan is not Iceberg-backed.
   *
   * <p>Loading goes through the schema's own {@code StorageProvider} rather than a path-scheme
   * check: an S3-backed schema loads via Iceberg's S3FileIO (AWS SDK v2), everything else via
   * HadoopTables. The provider type is the storage signal.
   */
  public static org.apache.iceberg.Table resolveIcebergTable(RelNode input) {
    TableScan scan = findTableScan(input, false);
    if (scan == null) {
      return null;
    }
    DuckDBJdbcSchema duckDbSchema = duckDbSchema(scan);
    if (duckDbSchema == null) {
      return null;
    }
    org.apache.calcite.adapter.file.FileSchema fileSchema = duckDbSchema.getFileSchema();
    if (fileSchema == null) {
      return null;
    }
    org.apache.calcite.adapter.file.metadata.ConversionMetadata conversionMetadata =
        fileSchema.getConversionMetadata();
    if (conversionMetadata == null) {
      return null;
    }
    String tableName = tableName(scan);
    org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord record =
        conversionMetadata.getAllConversions().get(tableName);
    if (record == null || !"ICEBERG_PARQUET".equals(record.getConversionType())) {
      return null;
    }
    String tableLocation = record.getSourceFile();
    if (tableLocation == null || tableLocation.isEmpty()) {
      return null;
    }
    try {
      org.apache.calcite.adapter.file.storage.StorageProvider storage =
          fileSchema.getStorageProvider();
      if (storage instanceof org.apache.calcite.adapter.file.storage.S3StorageProvider) {
        java.util.Map<String, String> s3Config =
            ((org.apache.calcite.adapter.file.storage.S3StorageProvider) storage).getS3Config();
        return org.apache.calcite.adapter.file.iceberg.S3FileIOTables.load(tableLocation, s3Config);
      }
      return new org.apache.iceberg.hadoop.HadoopTables(
          new org.apache.hadoop.conf.Configuration()).load(tableLocation);
    } catch (Exception e) {
      LOGGER.debug("Could not load Iceberg table at {}: {}", tableLocation, e.toString());
      return null;
    }
  }
}
