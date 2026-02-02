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
package org.apache.calcite.adapter.file.duckdb;

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
 * Rule that replaces COUNT(*) operations on DuckDB/JDBC views backed by Iceberg tables
 * with pre-computed row counts from Iceberg metadata.
 *
 * <p>This rule provides instant row counts for Iceberg tables by reading the record count
 * from Iceberg metadata, avoiding expensive full table scans via DuckDB.
 */
public class DuckDBIcebergCountStarRule extends RelOptRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBIcebergCountStarRule.class);

  public static final DuckDBIcebergCountStarRule INSTANCE = new DuckDBIcebergCountStarRule();

  @SuppressWarnings("deprecation")
  private DuckDBIcebergCountStarRule() {
    super(
        operand(Aggregate.class, any()),
        "DuckDBIcebergCountStarRule");
  }

  @Override public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    LOGGER.info("[ICEBERG COUNT*] matches() called on aggregate: {}", aggregate.getClass().getSimpleName());

    // Only handle simple aggregates without GROUP BY
    if (!aggregate.getGroupSet().isEmpty()) {
      LOGGER.debug("[ICEBERG COUNT*] Skipping - has GROUP BY");
      return false;
    }

    // Must have exactly one aggregate call
    List<AggregateCall> aggCalls = aggregate.getAggCallList();
    if (aggCalls.size() != 1) {
      LOGGER.debug("[ICEBERG COUNT*] Skipping - {} agg calls", aggCalls.size());
      return false;
    }

    AggregateCall aggCall = aggCalls.get(0);

    // Must be COUNT function (not DISTINCT)
    if (aggCall.getAggregation().getKind() != SqlKind.COUNT) {
      LOGGER.debug("[ICEBERG COUNT*] Skipping - not COUNT");
      return false;
    }
    if (aggCall.isDistinct()) {
      LOGGER.debug("[ICEBERG COUNT*] Skipping - is DISTINCT");
      return false;
    }
    // Must be COUNT(*) with no arguments
    if (!aggCall.getArgList().isEmpty()) {
      LOGGER.debug("[ICEBERG COUNT*] Skipping - has args");
      return false;
    }

    LOGGER.info("[ICEBERG COUNT*] matches() returning true - found COUNT(*)");
    return true;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = aggregate.getInput();
    LOGGER.info("[ICEBERG COUNT*] onMatch() called for aggregate: {}, input: {}",
        aggregate.getClass().getSimpleName(), input.getClass().getSimpleName());

    // Find the table scan in the input tree
    TableScan tableScan = findTableScan(input);
    if (tableScan == null) {
      LOGGER.info("[ICEBERG COUNT*]: No table scan found in input tree (input class: {})",
          input.getClass().getName());
      return;
    }
    LOGGER.info("[ICEBERG COUNT*] Found table scan: {}", tableScan.getClass().getSimpleName());

    // Get table name and schema from the scan
    RelOptTable relOptTable = tableScan.getTable();
    List<String> qualifiedName = relOptTable.getQualifiedName();
    String schemaName = qualifiedName.size() >= 2 ? qualifiedName.get(qualifiedName.size() - 2) : "";
    String tableName = qualifiedName.isEmpty() ? "" : qualifiedName.get(qualifiedName.size() - 1);

    LOGGER.info("[ICEBERG COUNT*] Looking for Iceberg table: {}.{}", schemaName, tableName);

    // Get the DuckDBJdbcSchema from the JdbcTableScan's JdbcTable
    DuckDBJdbcSchema duckDBSchema = getDuckDBSchema(tableScan);
    if (duckDBSchema == null) {
      LOGGER.info("[ICEBERG COUNT*]: Cannot find DuckDBJdbcSchema from table scan");
      return;
    }
    LOGGER.info("[ICEBERG COUNT*] Found DuckDBJdbcSchema via JdbcTable chain");

    // Get the FileSchema which holds the IcebergTable
    org.apache.calcite.adapter.file.FileSchema fileSchema = duckDBSchema.getFileSchema();
    if (fileSchema == null) {
      LOGGER.info("[ICEBERG COUNT*]: FileSchema not available");
      return;
    }
    LOGGER.info("[ICEBERG COUNT*] Found FileSchema with {} tables",
        fileSchema.tables().getNames(org.apache.calcite.schema.lookup.LikePattern.any()).size());

    // Check if this is an Iceberg-backed table via ConversionMetadata
    org.apache.calcite.adapter.file.metadata.ConversionMetadata conversionMetadata =
        fileSchema.getConversionMetadata();
    if (conversionMetadata == null) {
      LOGGER.info("[ICEBERG COUNT*]: No ConversionMetadata available");
      return;
    }

    org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord record =
        conversionMetadata.getAllConversions().get(tableName);
    if (record == null) {
      LOGGER.info("[ICEBERG COUNT*]: No conversion record for table '{}'", tableName);
      return;
    }

    String conversionType = record.getConversionType();
    LOGGER.info("[ICEBERG COUNT*] Table '{}' conversionType: {}", tableName, conversionType);

    if (!"ICEBERG_PARQUET".equals(conversionType)) {
      LOGGER.info("[ICEBERG COUNT*]: Table '{}' is not ICEBERG_PARQUET (type: {})",
          tableName, conversionType);
      return;
    }

    // Get row count from ConversionRecord (stored during ETL materialization)
    Long rowCount = record.rowCount;
    if (rowCount == null || rowCount == 0L) {
      // Self-heal: read row count from Iceberg metadata and cache it
      // Also heal when rowCount is 0 - this can happen when table was registered before data was committed
      LOGGER.info("[ICEBERG COUNT*]: No valid cached row count for '{}' (current: {}) - attempting to read from Iceberg metadata", tableName, rowCount);
      String tableLocation = record.getSourceFile();
      if (tableLocation != null) {
        rowCount = readRowCountFromIcebergDirect(tableLocation, fileSchema.getStorageProvider());
        if (rowCount != null) {
          // Cache the row count for future queries
          conversionMetadata.updateMaterializationInfo(tableName, tableLocation, "ICEBERG_PARQUET", rowCount);
          LOGGER.info("[ICEBERG COUNT*]: Self-healed - cached row count {} for table '{}'", rowCount, tableName);
        }
      }
      if (rowCount == null) {
        LOGGER.info("[ICEBERG COUNT*]: Could not read row count from Iceberg metadata for '{}'", tableName);
        return;
      }
    }

    LOGGER.info("[ICEBERG COUNT*]: Replacing COUNT(*) on '{}' with cached row count: {}",
        tableName, rowCount);

    // Create VALUES node with the row count
    RelNode valuesNode = createCountStarValues(aggregate, rowCount);
    if (valuesNode != null) {
      call.transformTo(valuesNode);
    }
  }

  /**
   * Get DuckDBJdbcSchema from a TableScan by following the JdbcTable chain.
   * JdbcTableScan -> JdbcTable -> JdbcSchema (which may be DuckDBJdbcSchema)
   */
  private DuckDBJdbcSchema getDuckDBSchema(TableScan tableScan) {
    if (!(tableScan instanceof JdbcTableScan)) {
      LOGGER.debug("[ICEBERG COUNT*] Table scan is not JdbcTableScan: {}",
          tableScan.getClass().getSimpleName());
      return null;
    }

    JdbcTableScan jdbcScan = (JdbcTableScan) tableScan;
    JdbcTable jdbcTable = jdbcScan.jdbcTable;
    JdbcSchema jdbcSchema = jdbcTable.jdbcSchema;

    LOGGER.debug("[ICEBERG COUNT*] JdbcTable.jdbcSchema type: {}",
        jdbcSchema.getClass().getSimpleName());

    if (jdbcSchema instanceof DuckDBJdbcSchema) {
      return (DuckDBJdbcSchema) jdbcSchema;
    }

    LOGGER.debug("[ICEBERG COUNT*] JdbcSchema is not DuckDBJdbcSchema");
    return null;
  }

  /**
   * Find TableScan in the input tree, handling RelSubset nodes.
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

    // Recursively search through all inputs
    for (RelNode input : node.getInputs()) {
      TableScan scan = findTableScan(input);
      if (scan != null) {
        return scan;
      }
    }

    return null;
  }

  /**
   * Read row count from Iceberg metadata directly using the table location.
   * This is storage-agnostic - uses StorageProvider for S3/local credentials.
   */
  private Long readRowCountFromIcebergDirect(String tableLocation,
      org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    try {
      LOGGER.info("[ICEBERG COUNT*] Loading Iceberg table at: {}", tableLocation);

      // Configure Hadoop for S3 access if using S3 storage
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
          LOGGER.debug("[ICEBERG COUNT*] Configured S3A filesystem for Iceberg access");
        }
      }

      // Load Iceberg table using HadoopTables with configured credentials
      org.apache.iceberg.hadoop.HadoopTables tables = new org.apache.iceberg.hadoop.HadoopTables(hadoopConf);
      org.apache.iceberg.Table icebergTable = tables.load(tableLocation);

      org.apache.iceberg.Snapshot snapshot = icebergTable.currentSnapshot();
      if (snapshot == null) {
        LOGGER.info("[ICEBERG COUNT*] Iceberg table has no snapshot, returning 0");
        return 0L;
      }

      // Sum record counts from all data files in current snapshot
      long totalRecords = 0;
      try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> fileScanTasks =
          icebergTable.newScan().planFiles()) {
        for (org.apache.iceberg.FileScanTask task : fileScanTasks) {
          totalRecords += task.file().recordCount();
        }
      }

      LOGGER.info("[ICEBERG COUNT*] Read row count {} from Iceberg metadata at {}",
          totalRecords, tableLocation);
      return totalRecords;

    } catch (Exception e) {
      LOGGER.warn("[ICEBERG COUNT*] Failed to read row count from Iceberg table at '{}': {}",
          tableLocation, e.getMessage(), e);
      return null;
    }
  }

  /**
   * Create a VALUES node containing the row count.
   */
  private RelNode createCountStarValues(Aggregate aggregate, long rowCount) {
    try {
      RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
      RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();

      // Build row type matching the aggregate output
      AggregateCall aggCall = aggregate.getAggCallList().get(0);
      String fieldName = aggCall.getName() != null ? aggCall.getName() : "EXPR$0";

      RelDataType bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT);
      RelDataType rowType = typeFactory.builder()
          .add(fieldName, bigIntType)
          .build();

      // Create literal with row count
      RexLiteral literal = (RexLiteral) rexBuilder.makeLiteral(rowCount, bigIntType, true);

      // Create VALUES node
      return EnumerableValues.create(
          aggregate.getCluster(),
          rowType,
          ImmutableList.of(ImmutableList.of(literal)));
    } catch (Exception e) {
      LOGGER.error("Failed to create COUNT(*) VALUES node", e);
      return null;
    }
  }
}
