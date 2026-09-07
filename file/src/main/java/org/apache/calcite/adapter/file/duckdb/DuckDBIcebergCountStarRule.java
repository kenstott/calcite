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

import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.adapter.file.iceberg.IcebergPartitionRowCount;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.Converter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Answers {@code COUNT(*)} on an Iceberg-backed DuckDB table from Iceberg metadata instead of
 * scanning it.
 *
 * <p>Three query shapes reach the same manifest read:
 *
 * <ul>
 *   <li>{@code COUNT(*)} over the whole table — the sum of every data file's row count, cached
 *       against the snapshot id so an unchanged table does not re-read manifests.</li>
 *   <li>{@code COUNT(*) ... WHERE <partition equalities>} — the sum over the data files whose
 *       partition tuple satisfies the predicate.</li>
 *   <li>{@code COUNT(*)} over a YAML view that is itself a partition filter, such as
 *       {@code SELECT ... FROM cftc_trades WHERE asset_class = 'COMMODITIES'}. Under the DuckDB
 *       engine such a view is a native DuckDB view, so no {@code Filter} node ever reaches the
 *       planner — the scan is of the view itself. The view's recorded defining SQL supplies the
 *       predicate that the plan tree does not carry.</li>
 * </ul>
 *
 * <p>What makes the filtered cases safe is not this class's own reasoning about partition specs:
 * it is Iceberg's residual. See {@link IcebergPartitionRowCount}. A predicate that is not settled
 * by the partition tuple — a non-partition column, a bucketed column, one term of a mixed
 * predicate — leaves a residual, the count is declined, and the query runs normally.
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

    // The answer describes ONE table, so the input must reduce to exactly one scan, optionally
    // behind one Filter. Without this the rule fired on an Aggregate over a Join and answered from
    // whichever scan it found first: COUNT(*) over a self cross join of a 33,791-row table returned
    // 33,791 instead of ~1.1e9. A join predicate becomes the join condition rather than a Filter,
    // so only a whitelisted walk of row-count-preserving operators catches it.
    if (resolveScanShape(aggregate.getInput()) == null) {
      LOGGER.debug("[ICEBERG COUNT*] Skipping - input is not a single scan with at most one filter");
      return false;
    }

    return true;
  }

  /** A single table scan and the one Filter directly above it, if any. */
  private static final class ScanShape {
    final TableScan scan;
    /** Null when nothing filters the scan; its input refs index {@code scan}'s row type. */
    final Filter filter;

    ScanShape(TableScan scan, Filter filter) {
      this.scan = scan;
      this.filter = filter;
    }
  }

  /**
   * Walks from an Aggregate's input down to its table scan, or null when the tree is anything other
   * than {@code {Project|Calc|Converter}* [Filter] TableScan}.
   *
   * <p>Deliberately a whitelist. A blacklist is the wrong shape for a rewrite that substitutes a
   * stored number for a real scan: every relational operator not yet thought of would default to
   * "safe" and silently return a wrong count. Only nodes that cannot change cardinality are walked
   * through — notably absent are Join and Union (multiply or add rows), Sort (a fetch/offset
   * truncates), and a nested Aggregate (collapses them).
   *
   * <p>A Filter is required to sit directly on the scan. Its condition's input references are then
   * indices into the scan's own row type, which is what lets them be read as column names; a
   * Project in between would renumber them.
   */
  private ScanShape resolveScanShape(RelNode node) {
    RelNode current = unwrap(node);
    Filter filter = null;
    while (current != null) {
      if (current instanceof TableScan) {
        return new ScanShape((TableScan) current, filter);
      }
      if (current instanceof Filter) {
        if (filter != null) {
          // Stacked filters would have to be ANDed across a possible renumbering; decline.
          return null;
        }
        filter = (Filter) current;
        RelNode below = unwrap(((Filter) current).getInput());
        // Nothing may sit between the Filter and the scan, or its input refs stop naming
        // the scan's columns.
        return below instanceof TableScan ? new ScanShape((TableScan) below, filter) : null;
      }
      if (current instanceof Calc && ((Calc) current).getProgram().getCondition() != null) {
        // A Calc carries its filter inside the program rather than as a Filter node.
        return null;
      }
      if (current instanceof Project || current instanceof Calc || current instanceof Converter) {
        if (current.getInputs().size() != 1) {
          return null;
        }
        current = unwrap(current.getInput(0));
        continue;
      }
      return null;
    }
    return null;
  }

  /**
   * Resolves Volcano's {@code RelSubset} to the concrete node it stands for.
   *
   * <p>{@code RelSubset} is not on the public API surface, hence the reflection. When it cannot be
   * resolved the caller gets null and declines — an uninspectable node cannot be proven to be a
   * plain scan.
   */
  private RelNode unwrap(RelNode node) {
    if (node == null || !node.getClass().getName().contains("RelSubset")) {
      return node;
    }
    try {
      java.lang.reflect.Method getBest = node.getClass().getMethod("getBest");
      RelNode best = (RelNode) getBest.invoke(node);
      if (best != null && best != node) {
        return unwrap(best);
      }
      java.lang.reflect.Method getOriginal = node.getClass().getMethod("getOriginal");
      RelNode original = (RelNode) getOriginal.invoke(node);
      if (original != null && original != node) {
        return unwrap(original);
      }
    } catch (ReflectiveOperationException e) {
      LOGGER.debug("[ICEBERG COUNT*] Cannot inspect {}: {}", node.getClass().getName(),
          e.toString());
    }
    return null;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    ScanShape shape = resolveScanShape(aggregate.getInput());
    if (shape == null) {
      return;
    }
    TableScan tableScan = shape.scan;

    RelOptTable relOptTable = tableScan.getTable();
    List<String> qualifiedName = relOptTable.getQualifiedName();
    String schemaName = qualifiedName.size() >= 2 ? qualifiedName.get(qualifiedName.size() - 2) : "";
    String scannedName = qualifiedName.isEmpty() ? "" : qualifiedName.get(qualifiedName.size() - 1);

    DuckDBJdbcSchema duckDBSchema = IcebergScanResolver.duckDbSchema(tableScan);
    if (duckDBSchema == null) {
      LOGGER.debug("[ICEBERG COUNT*] Not a DuckDB-backed scan: {}.{}", schemaName, scannedName);
      return;
    }
    org.apache.calcite.adapter.file.FileSchema fileSchema = duckDBSchema.getFileSchema();
    if (fileSchema == null) {
      LOGGER.debug("[ICEBERG COUNT*] FileSchema not available for {}", schemaName);
      return;
    }
    org.apache.calcite.adapter.file.metadata.ConversionMetadata conversionMetadata =
        fileSchema.getConversionMetadata();
    if (conversionMetadata == null) {
      LOGGER.debug("[ICEBERG COUNT*] No ConversionMetadata for {}", schemaName);
      return;
    }

    // Values the answer must be restricted to, keyed by column name. Empty means the whole table.
    Map<String, List<Object>> acceptedValues = new LinkedHashMap<>();

    // The scan may be of the Iceberg table itself, or of a YAML view that filters one.
    String icebergTableName = scannedName;
    org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord record =
        icebergRecord(conversionMetadata, scannedName);
    if (record == null) {
      PartitionEqualityFilter.ViewDefinition view =
          viewDefinition(duckDBSchema, schemaName, scannedName);
      if (view == null) {
        LOGGER.debug("[ICEBERG COUNT*] '{}' is neither an Iceberg table nor a filtering view",
            scannedName);
        return;
      }
      record = icebergRecord(conversionMetadata, view.baseTableName);
      if (record == null) {
        LOGGER.debug("[ICEBERG COUNT*] View '{}' reads '{}', which is not Iceberg-backed",
            scannedName, view.baseTableName);
        return;
      }
      icebergTableName = view.baseTableName;
      acceptedValues.putAll(view.acceptedValues);
    }

    if (shape.filter != null) {
      Map<String, List<Object>> fromWhere =
          PartitionEqualityFilter.fromRex(shape.filter.getCondition(),
              tableScan.getRowType().getFieldNames(),
              aggregate.getCluster().getRexBuilder());
      if (fromWhere == null) {
        LOGGER.debug("[ICEBERG COUNT*] WHERE on '{}' is not a pure equality/IN predicate",
            scannedName);
        return;
      }
      for (Map.Entry<String, List<Object>> entry : fromWhere.entrySet()) {
        String column = entry.getKey().toLowerCase(Locale.ROOT);
        if (acceptedValues.containsKey(column)) {
          // The view already pins this column; intersecting the two sets is not attempted.
          LOGGER.debug("[ICEBERG COUNT*] Column '{}' constrained by both the view and the query",
              column);
          return;
        }
        acceptedValues.put(column, entry.getValue());
      }
    }

    String tableLocation = record.getSourceFile();
    if (tableLocation == null || tableLocation.isEmpty()) {
      LOGGER.debug("[ICEBERG COUNT*] No table location recorded for '{}'", icebergTableName);
      return;
    }

    Long rowCount = acceptedValues.isEmpty()
        ? wholeTableCount(icebergTableName, tableLocation, fileSchema, conversionMetadata, record)
        : partitionCount(icebergTableName, tableLocation, fileSchema, acceptedValues);
    if (rowCount == null) {
      // Not provable from metadata: let COUNT(*) run normally rather than serve a guess.
      return;
    }

    LOGGER.info("[ICEBERG COUNT*] Answering COUNT(*) on '{}'{} from Iceberg metadata: {}",
        scannedName, acceptedValues.isEmpty() ? "" : " " + acceptedValues, rowCount);

    RelNode valuesNode = createCountStarValues(aggregate, rowCount.longValue());
    if (valuesNode != null) {
      call.transformTo(valuesNode);
    }
  }

  /** The ICEBERG_PARQUET conversion record for a table name, or null if there is none. */
  private org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord icebergRecord(
      org.apache.calcite.adapter.file.metadata.ConversionMetadata conversionMetadata, String name) {
    Map<String, org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord>
        conversions = conversionMetadata.getAllConversions();
    org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord record =
        conversions.get(name);
    if (record == null) {
      record = conversions.get(name.toLowerCase(Locale.ROOT));
    }
    return record != null && "ICEBERG_PARQUET".equals(record.getConversionType()) ? record : null;
  }

  /**
   * The recorded YAML view behind a scanned name, reduced to base table plus pinned constants, or
   * null when the name is not such a view.
   */
  private PartitionEqualityFilter.ViewDefinition viewDefinition(DuckDBJdbcSchema duckDBSchema,
      String schemaName, String viewName) {
    String catalogPath = duckDBSchema.getCatalogPath();
    if (catalogPath == null) {
      return null;
    }
    String viewSql = DuckDBPendingViews.sqlViewDefinition(catalogPath, schemaName, viewName);
    if (viewSql == null) {
      return null;
    }
    return PartitionEqualityFilter.fromViewSql(viewSql);
  }

  /**
   * Row count for the whole table, validating the cached count against the current snapshot.
   *
   * <p>A cached rowCount must NOT be trusted blindly: when a table is emptied/purged its current
   * snapshot becomes null (count 0) but the old nonzero rowCount lingers, so COUNT(*) would report
   * phantom rows. The current snapshot is always resolved (a cheap metadata load) and manifests are
   * re-read only when it moved since the cached count was taken.
   */
  private Long wholeTableCount(String tableName, String tableLocation,
      org.apache.calcite.adapter.file.FileSchema fileSchema,
      org.apache.calcite.adapter.file.metadata.ConversionMetadata conversionMetadata,
      org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord record) {
    org.apache.iceberg.Table icebergTable = loadTable(tableLocation, fileSchema);
    if (icebergTable == null) {
      return null;
    }
    org.apache.iceberg.Snapshot snapshot = icebergTable.currentSnapshot();
    Long snapshotId = snapshot == null ? null : Long.valueOf(snapshot.snapshotId());

    long rowCount;
    if (snapshot == null) {
      rowCount = 0L;
    } else if (record.snapshotId != null && record.snapshotId.equals(snapshotId)
        && record.rowCount != null) {
      rowCount = record.rowCount.longValue();
    } else {
      long total = 0L;
      try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
          icebergTable.newScan().planFiles()) {
        for (org.apache.iceberg.FileScanTask task : tasks) {
          total += task.file().recordCount();
        }
      } catch (java.io.IOException e) {
        throw new java.io.UncheckedIOException(
            "Failed to close the manifest scan for Iceberg table " + tableLocation, e);
      }
      rowCount = total;
    }

    // Refresh the persisted cache whenever the count or snapshot changed (self-heals stale counts).
    if (record.rowCount == null || record.rowCount.longValue() != rowCount
        || !java.util.Objects.equals(record.snapshotId, snapshotId)) {
      record.snapshotId = snapshotId;
      conversionMetadata.updateMaterializationInfo(tableName, tableLocation, "ICEBERG_PARQUET",
          rowCount);
      LOGGER.info("[ICEBERG COUNT*] Refreshed count for '{}' -> {} (snapshot {})",
          tableName, rowCount, snapshotId);
    }
    return Long.valueOf(rowCount);
  }

  /**
   * Row count for the data files whose partition tuple satisfies {@code acceptedValues}, or null
   * when Iceberg cannot prove the predicate is settled by partitioning.
   */
  private Long partitionCount(String tableName, String tableLocation,
      org.apache.calcite.adapter.file.FileSchema fileSchema,
      Map<String, List<Object>> acceptedValues) {
    org.apache.iceberg.expressions.Expression predicate =
        IcebergPartitionRowCount.toPredicate(acceptedValues);
    if (predicate == null) {
      return null;
    }
    org.apache.iceberg.Table icebergTable = loadTable(tableLocation, fileSchema);
    if (icebergTable == null) {
      return null;
    }
    Long count = IcebergPartitionRowCount.countMatching(icebergTable, predicate);
    if (count == null) {
      LOGGER.debug("[ICEBERG COUNT*] '{}' cannot answer {} from partitions", tableName,
          acceptedValues);
    }
    return count;
  }

  /**
   * Loads the Iceberg table at a location, or null when its metadata is not readable.
   *
   * <p>Storage-agnostic: an S3-backed schema loads through Iceberg's S3FileIO (AWS SDK v2),
   * everything else through HadoopTables. The {@code StorageProvider} type is the storage signal,
   * not a path-scheme literal.
   */
  private org.apache.iceberg.Table loadTable(String tableLocation,
      org.apache.calcite.adapter.file.FileSchema fileSchema) {
    try {
      org.apache.calcite.adapter.file.storage.StorageProvider storageProvider =
          fileSchema.getStorageProvider();
      if (storageProvider instanceof org.apache.calcite.adapter.file.storage.S3StorageProvider) {
        Map<String, String> s3Config =
            ((org.apache.calcite.adapter.file.storage.S3StorageProvider) storageProvider)
                .getS3Config();
        return org.apache.calcite.adapter.file.iceberg.S3FileIOTables.load(tableLocation, s3Config);
      }
      return new org.apache.iceberg.hadoop.HadoopTables(
          new org.apache.hadoop.conf.Configuration()).load(tableLocation);
    // fallback-guard: allow Consistent with the sibling Iceberg count-star rules: an unloadable table means the COUNT(*) rewrite doesn't fire, falling back to a real scan.
    } catch (Exception e) {
      LOGGER.warn("[ICEBERG COUNT*] Failed to load Iceberg table at '{}': {}", tableLocation,
          e.getMessage(), e);
      return null;
    }
  }

  /**
   * Create a VALUES node containing the row count.
   */
  private RelNode createCountStarValues(Aggregate aggregate, long rowCount) {
    RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();

    // Build row type matching the aggregate output
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
  }
}
