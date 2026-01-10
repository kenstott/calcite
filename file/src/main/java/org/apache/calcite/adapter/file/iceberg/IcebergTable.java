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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table implementation for Apache Iceberg tables.
 *
 * <p>Supports reading Iceberg tables with features like:
 * <ul>
 *   <li>Schema evolution</li>
 *   <li>Partition pruning</li>
 *   <li>Time travel queries</li>
 *   <li>Hidden partitioning</li>
 * </ul>
 */
public class IcebergTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTable.class);

  private final Table icebergTable;
  private final @Nullable Long snapshotId;
  private final @Nullable String asOfTimestamp;
  private final Source source;
  private final Map<String, Object> config;
  private @Nullable RelDataType rowType;
  private @Nullable Double cachedRowCount;

  /**
   * Creates an IcebergTable from a path.
   *
   * @param source The source (path) to the Iceberg table
   * @param config Configuration including snapshot, timestamp, etc.
   */
  public IcebergTable(Source source, Map<String, Object> config) {
    this.source = source;
    this.config = config;

    // Extract time travel parameters
    this.snapshotId = config.containsKey("snapshotId")
        ? ((Number) config.get("snapshotId")).longValue()
        : null;
    this.asOfTimestamp = (String) config.get("asOfTimestamp");

    // Initialize the Iceberg table
    String tablePath = source.path();
    // Direct path access using HadoopTables
    HadoopTables tables = new HadoopTables();
    this.icebergTable = tables.load(tablePath);
  }

  /**
   * Creates an IcebergTable from an existing Iceberg Table object.
   *
   * @param icebergTable The Iceberg table
   * @param source The source for reference
   */
  public IcebergTable(Table icebergTable, Source source) {
    this.icebergTable = icebergTable;
    this.source = source;
    this.config = new java.util.HashMap<>();
    this.snapshotId = null;
    this.asOfTimestamp = null;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      rowType = deduceRowType(typeFactory);
    }
    return rowType;
  }

  private RelDataType deduceRowType(RelDataTypeFactory typeFactory) {
    final List<String> fieldNames = new ArrayList<>();
    final List<RelDataType> fieldTypes = new ArrayList<>();

    Schema icebergSchema = icebergTable.schema();
    for (Types.NestedField field : icebergSchema.columns()) {
      fieldNames.add(field.name());
      fieldTypes.add(icebergTypeToSqlType(field.type(), typeFactory));
    }

    return typeFactory.createStructType(fieldTypes, fieldNames);
  }

  private RelDataType icebergTypeToSqlType(org.apache.iceberg.types.Type type,
                                           RelDataTypeFactory typeFactory) {
    switch (type.typeId()) {
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case INTEGER:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case LONG:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case FLOAT:
        return typeFactory.createSqlType(SqlTypeName.REAL);
      case DOUBLE:
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
      case STRING:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case DATE:
        return typeFactory.createSqlType(SqlTypeName.DATE);
      case TIMESTAMP:
        Types.TimestampType tsType = (Types.TimestampType) type;
        if (tsType.shouldAdjustToUTC()) {
          return typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        } else {
          return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        }
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) type;
        return typeFactory.createSqlType(SqlTypeName.DECIMAL,
            decimalType.precision(), decimalType.scale());
      case BINARY:
      case FIXED:
        return typeFactory.createSqlType(SqlTypeName.VARBINARY);
      case UUID:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR, 36);
      case STRUCT:
        Types.StructType structType = (Types.StructType) type;
        List<String> fieldNames = new ArrayList<>();
        List<RelDataType> fieldTypes = new ArrayList<>();
        for (Types.NestedField field : structType.fields()) {
          fieldNames.add(field.name());
          fieldTypes.add(icebergTypeToSqlType(field.type(), typeFactory));
        }
        return typeFactory.createStructType(fieldTypes, fieldNames);
      case LIST:
        Types.ListType listType = (Types.ListType) type;
        RelDataType elementType = icebergTypeToSqlType(listType.elementType(), typeFactory);
        return typeFactory.createArrayType(elementType, -1);
      case MAP:
        Types.MapType mapType = (Types.MapType) type;
        RelDataType keyType = icebergTypeToSqlType(mapType.keyType(), typeFactory);
        RelDataType valueType = icebergTypeToSqlType(mapType.valueType(), typeFactory);
        return typeFactory.createMapType(keyType, valueType);
      default:
        // Default to VARCHAR for unknown types
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        try {
          // Use simplified IcebergEnumerator for MVP
          return new IcebergEnumerator(
              icebergTable,
              snapshotId,
              asOfTimestamp,
              cancelFlag);
        } catch (Exception e) {
          throw new RuntimeException("Failed to create Iceberg enumerator", e);
        }
      }
    };
  }

  /**
   * Returns statistics for this Iceberg table, including row count.
   *
   * <p>Row count is extracted from Iceberg metadata by summing record counts
   * from all data files. This enables query optimization rules like
   * {@code CountStarStatisticsRule} to replace COUNT(*) with a constant value.
   *
   * @return Statistics with row count from Iceberg metadata
   */
  @Override public Statistic getStatistic() {
    if (cachedRowCount != null) {
      return Statistics.of(cachedRowCount, ImmutableList.of());
    }

    try {
      Snapshot snapshot = icebergTable.currentSnapshot();
      if (snapshot == null) {
        // Empty table - no snapshots yet
        LOGGER.debug("Iceberg table has no snapshot, returning 0 row count");
        cachedRowCount = 0.0;
        return Statistics.of(0.0, ImmutableList.of());
      }

      // Sum record counts from all data files
      long totalRecords = 0;
      try (CloseableIterable<FileScanTask> fileScanTasks =
               icebergTable.newScan().planFiles()) {
        for (FileScanTask task : fileScanTasks) {
          totalRecords += task.file().recordCount();
        }
      }

      cachedRowCount = (double) totalRecords;
      LOGGER.debug("Iceberg table row count from metadata: {}", totalRecords);
      return Statistics.of(cachedRowCount, ImmutableList.of());

    } catch (Exception e) {
      LOGGER.warn("Failed to get Iceberg statistics: {}", e.getMessage());
      return Statistics.UNKNOWN;
    }
  }

  /**
   * Gets the underlying Iceberg table.
   *
   * @return The Iceberg table
   */
  public Table getIcebergTable() {
    return icebergTable;
  }

  /**
   * Creates a new IcebergTable with a specific snapshot.
   *
   * @param snapshotId The snapshot ID
   * @return A new IcebergTable instance with the specified snapshot
   */
  public IcebergTable withSnapshot(long snapshotId) {
    Map<String, Object> newConfig = new java.util.HashMap<>(this.config);
    newConfig.put("snapshotId", snapshotId);
    return new IcebergTable(source, newConfig);
  }

  /**
   * Creates a new IcebergTable as of a specific timestamp.
   *
   * @param timestamp The timestamp
   * @return A new IcebergTable instance as of the specified timestamp
   */
  public IcebergTable asOf(String timestamp) {
    Map<String, Object> newConfig = new java.util.HashMap<>(this.config);
    newConfig.put("asOfTimestamp", timestamp);
    return new IcebergTable(source, newConfig);
  }
}
