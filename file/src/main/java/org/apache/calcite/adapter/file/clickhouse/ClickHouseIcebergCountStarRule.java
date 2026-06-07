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
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Rule that replaces COUNT(*) operations on ClickHouse/JDBC views backed by Iceberg tables
 * with pre-computed row counts from Iceberg metadata.
 *
 * <p>This provides instant row counts for Iceberg tables by reading the record count
 * from Iceberg metadata (cached in ConversionMetadata), avoiding expensive full table
 * scans through ClickHouse.
 *
 * <p>Matches:
 * <ul>
 *   <li>Aggregate with no GROUP BY</li>
 *   <li>Single COUNT(*) call (no arguments, not distinct)</li>
 *   <li>Table is ICEBERG_PARQUET with cached row count in ConversionMetadata</li>
 * </ul>
 */
public class ClickHouseIcebergCountStarRule extends RelOptRule {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ClickHouseIcebergCountStarRule.class);

  public static final ClickHouseIcebergCountStarRule INSTANCE =
      new ClickHouseIcebergCountStarRule();

  @SuppressWarnings("deprecation")
  private ClickHouseIcebergCountStarRule() {
    super(
        operand(Aggregate.class, any()),
        "ClickHouseIcebergCountStarRule");
  }

  @Override public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);

    // Only handle simple aggregates without GROUP BY
    if (!aggregate.getGroupSet().isEmpty()) {
      return false;
    }

    // Must have exactly one aggregate call
    List<AggregateCall> aggCalls = aggregate.getAggCallList();
    if (aggCalls.size() != 1) {
      return false;
    }

    AggregateCall aggCall = aggCalls.get(0);

    // Must be COUNT function (not DISTINCT)
    if (aggCall.getAggregation().getKind() != SqlKind.COUNT) {
      return false;
    }
    if (aggCall.isDistinct()) {
      return false;
    }
    // Must be COUNT(*) with no arguments
    if (!aggCall.getArgList().isEmpty()) {
      return false;
    }

    LOGGER.debug("[CH-ICEBERG-COUNT*] matches() returning true - found COUNT(*)");
    return true;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = aggregate.getInput();

    // Find the table scan in the input tree
    TableScan tableScan = findTableScan(input);
    if (tableScan == null) {
      LOGGER.debug("[CH-ICEBERG-COUNT*] No table scan found in input tree");
      return;
    }

    // Get table name and schema from the scan
    RelOptTable relOptTable = tableScan.getTable();
    List<String> qualifiedName = relOptTable.getQualifiedName();
    String schemaName = qualifiedName.size() >= 2
        ? qualifiedName.get(qualifiedName.size() - 2) : "";
    String tableName = qualifiedName.isEmpty()
        ? "" : qualifiedName.get(qualifiedName.size() - 1);

    LOGGER.debug("[CH-ICEBERG-COUNT*] Looking for Iceberg table: {}.{}", schemaName, tableName);

    // Get the ClickHouseJdbcSchema from the JdbcTableScan's JdbcTable
    ClickHouseJdbcSchema clickHouseSchema = getClickHouseSchema(tableScan);
    if (clickHouseSchema == null) {
      LOGGER.debug("[CH-ICEBERG-COUNT*] Cannot find ClickHouseJdbcSchema from table scan");
      return;
    }

    // Get the FileSchema which holds the IcebergTable
    org.apache.calcite.adapter.file.FileSchema fileSchema = clickHouseSchema.getFileSchema();
    if (fileSchema == null) {
      LOGGER.debug("[CH-ICEBERG-COUNT*] FileSchema not available");
      return;
    }

    // Check if this is an Iceberg-backed table via ConversionMetadata
    org.apache.calcite.adapter.file.metadata.ConversionMetadata conversionMetadata =
        fileSchema.getConversionMetadata();
    if (conversionMetadata == null) {
      LOGGER.debug("[CH-ICEBERG-COUNT*] No ConversionMetadata available");
      return;
    }

    org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord record =
        conversionMetadata.getAllConversions().get(tableName);
    if (record == null) {
      LOGGER.debug("[CH-ICEBERG-COUNT*] No conversion record for table '{}'", tableName);
      return;
    }

    String conversionType = record.getConversionType();
    if (!"ICEBERG_PARQUET".equals(conversionType)) {
      LOGGER.debug("[CH-ICEBERG-COUNT*] Table '{}' is not ICEBERG_PARQUET (type: {})",
          tableName, conversionType);
      return;
    }

    // Get row count from ConversionRecord (stored during ETL materialization)
    Long rowCount = record.rowCount;
    if (rowCount == null || rowCount == 0L) {
      // Self-heal: read row count from Iceberg metadata and cache it
      LOGGER.info("[CH-ICEBERG-COUNT*] No cached row count for '{}' - reading from Iceberg metadata",
          tableName);
      String tableLocation = record.getSourceFile();
      if (tableLocation != null) {
        rowCount = readRowCountFromIcebergDirect(tableLocation, fileSchema.getStorageProvider());
        if (rowCount != null) {
          conversionMetadata.updateMaterializationInfo(tableName, tableLocation,
              "ICEBERG_PARQUET", rowCount);
          LOGGER.info("[CH-ICEBERG-COUNT*] Cached row count {} for table '{}'",
              rowCount, tableName);
        }
      }
      if (rowCount == null) {
        LOGGER.debug("[CH-ICEBERG-COUNT*] Could not read row count from Iceberg metadata");
        return;
      }
    }

    LOGGER.info("[CH-ICEBERG-COUNT*] Replacing COUNT(*) on '{}' with cached row count: {}",
        tableName, rowCount);

    // Create VALUES node with the row count
    RelNode valuesNode = createCountStarValues(aggregate, rowCount);
    if (valuesNode != null) {
      call.transformTo(valuesNode);
    }
  }

  /**
   * Gets ClickHouseJdbcSchema from a TableScan by following the JdbcTable chain.
   */
  private ClickHouseJdbcSchema getClickHouseSchema(TableScan tableScan) {
    if (!(tableScan instanceof JdbcTableScan)) {
      return null;
    }

    JdbcTableScan jdbcScan = (JdbcTableScan) tableScan;
    JdbcTable jdbcTable = jdbcScan.jdbcTable;
    JdbcSchema jdbcSchema = jdbcTable.jdbcSchema;

    if (jdbcSchema instanceof ClickHouseJdbcSchema) {
      return (ClickHouseJdbcSchema) jdbcSchema;
    }

    return null;
  }

  /**
   * Finds TableScan in the input tree, handling RelSubset nodes.
   */
  private TableScan findTableScan(RelNode node) {
    if (node == null) {
      return null;
    }

    // Handle RelSubset nodes from Volcano planner
    if (node.getClass().getName().contains("RelSubset")) {
      try {
        java.lang.reflect.Method getBest = node.getClass().getMethod("getBest");
        RelNode best = (RelNode) getBest.invoke(node);
        if (best != null && best != node) {
          return findTableScan(best);
        }

        java.lang.reflect.Method getOriginal = node.getClass().getMethod("getOriginal");
        RelNode original = (RelNode) getOriginal.invoke(node);
        if (original != null && original != node) {
          return findTableScan(original);
        }
      } catch (Exception e) {
        // Silently continue
      }
    }

    if (node instanceof TableScan) {
      return (TableScan) node;
    }

    for (RelNode input : node.getInputs()) {
      TableScan scan = findTableScan(input);
      if (scan != null) {
        return scan;
      }
    }

    return null;
  }

  /**
   * Reads row count from Iceberg metadata directly using the table location.
   * Uses StorageProvider for S3/local credentials.
   */
  private Long readRowCountFromIcebergDirect(String tableLocation,
      org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    try {
      LOGGER.info("[CH-ICEBERG-COUNT*] Loading Iceberg table at: {}", tableLocation);

      org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
      if (storageProvider instanceof org.apache.calcite.adapter.file.storage.S3StorageProvider) {
        org.apache.calcite.adapter.file.storage.S3StorageProvider s3Provider =
            (org.apache.calcite.adapter.file.storage.S3StorageProvider) storageProvider;
        java.util.Map<String, String> s3Config = s3Provider.getS3Config();
        if (s3Config != null) {
          hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
          hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
          if (s3Config.get("accessKeyId") != null) {
            hadoopConf.set("fs.s3a.access.key", s3Config.get("accessKeyId"));
          }
          if (s3Config.get("secretAccessKey") != null) {
            hadoopConf.set("fs.s3a.secret.key", s3Config.get("secretAccessKey"));
          }
          if (s3Config.get("endpoint") != null) {
            hadoopConf.set("fs.s3a.endpoint", s3Config.get("endpoint"));
            hadoopConf.set("fs.s3a.path.style.access", "true");
          }
        }
      }

      org.apache.iceberg.hadoop.HadoopTables tables =
          new org.apache.iceberg.hadoop.HadoopTables(hadoopConf);
      org.apache.iceberg.Table icebergTable = tables.load(tableLocation);

      org.apache.iceberg.Snapshot snapshot = icebergTable.currentSnapshot();
      if (snapshot == null) {
        return 0L;
      }

      long totalRecords = 0;
      try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> fileScanTasks =
          icebergTable.newScan().planFiles()) {
        for (org.apache.iceberg.FileScanTask task : fileScanTasks) {
          totalRecords += task.file().recordCount();
        }
      }

      LOGGER.info("[CH-ICEBERG-COUNT*] Read row count {} from Iceberg metadata at {}",
          totalRecords, tableLocation);
      return totalRecords;

    } catch (Exception e) {
      LOGGER.debug("[CH-ICEBERG-COUNT*] Failed to read row count from Iceberg: {}",
          e.getMessage());
      return null;
    }
  }

  /**
   * Creates a VALUES node containing the row count.
   */
  private RelNode createCountStarValues(Aggregate aggregate, long rowCount) {
    try {
      RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
      RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();

      AggregateCall aggCall = aggregate.getAggCallList().get(0);
      String fieldName = aggCall.getName() != null ? aggCall.getName() : "EXPR$0";

      RelDataType bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT);
      RelDataType rowType = typeFactory.builder()
          .add(fieldName, bigIntType)
          .build();

      RexLiteral literal = (RexLiteral) rexBuilder.makeLiteral(rowCount, bigIntType, true);

      return EnumerableValues.create(
          aggregate.getCluster(),
          rowType,
          ImmutableList.of(ImmutableList.of(literal)));
    } catch (Exception e) {
      LOGGER.error("[CH-ICEBERG-COUNT*] Failed to create COUNT(*) VALUES node", e);
      return null;
    }
  }
}
