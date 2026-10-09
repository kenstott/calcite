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
package org.apache.calcite.adapter.ops;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsSortHandler;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Base class for Cloud Ops tables.
 * Handles common functionality like provider filtering and parallel execution.
 * Filters choose the clouds and accounts to call; ORDER BY, LIMIT and OFFSET are handed to
 * {@link #scan(DataContext, List, int[], RelCollation, RexNode, RexNode)} by
 * {@link CloudOpsSortScanRule} and applied once over the rows of all clouds.
 */
public abstract class AbstractCloudOpsTable extends AbstractTable
    implements ProjectableFilterableTable, TranslatableTable {
  private static final Logger logger = LoggerFactory.getLogger(AbstractCloudOpsTable.class);

  /**
   * Threads the cloud providers are queried on. Not the common pool: the Azure and Google
   * credential libraries fetch tokens on the common pool, so provider calls that occupy all
   * of its threads would wait for tokens that have no thread left to be fetched on.
   */
  private static final ExecutorService PROVIDER_POOL =
      Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "cloudops-provider");
        thread.setDaemon(true);
        return thread;
      });

  /** Schema name used when the model does not choose one. */
  public static final String DEFAULT_SCHEMA_NAME = "cloud";

  protected final CloudOpsConfig config;
  /** Name the schema holding this table is registered under; foreign keys refer to it. */
  protected final String schemaName;

  protected AbstractCloudOpsTable(CloudOpsConfig config) {
    this(config, DEFAULT_SCHEMA_NAME);
  }

  protected AbstractCloudOpsTable(CloudOpsConfig config, String schemaName) {
    this.config = config;
    this.schemaName = schemaName;
  }

  /**
   * Lightweight type factory used only to resolve column ordinals by name for the logical
   * key/foreign-key metadata. Column names are independent of the type system, so the default
   * system is sufficient.
   */
  protected static final RelDataTypeFactory METADATA_TYPE_FACTORY =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  /**
   * Declares logical key and foreign-key metadata for the table. These are unenforced hints for the
   * planner (and catalog introspection) describing the cloud resource data model; the adapter does
   * not validate them. Every table's logical primary key is {@code resource_id} (the globally-unique
   * ARN / Resource ID). Subclasses contribute additional unique keys via {@link #additionalKeys} and
   * referential constraints via {@link #referentialConstraints}.
   */
  @Override public Statistic getStatistic() {
    final List<String> columnNames = getRowType(METADATA_TYPE_FACTORY).getFieldNames();
    final List<ImmutableBitSet> keys = new ArrayList<>();
    final int pkIndex = columnNames.indexOf("resource_id");
    if (pkIndex >= 0) {
      keys.add(ImmutableBitSet.of(pkIndex));
    }
    keys.addAll(additionalKeys(columnNames));
    return Statistics.of(null, keys, referentialConstraints(columnNames), null);
  }

  /**
   * Additional logical unique keys beyond the {@code resource_id} primary key. Default: none.
   *
   * @param columnNames ordered column names of this table
   */
  protected List<ImmutableBitSet> additionalKeys(List<String> columnNames) {
    return Collections.emptyList();
  }

  /**
   * Logical foreign keys from this table to other tables in the {@code cloud} schema. Default: none.
   *
   * @param columnNames ordered column names of this table
   */
  protected List<RelReferentialConstraint> referentialConstraints(List<String> columnNames) {
    return Collections.emptyList();
  }

  @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters,
      int @Nullable [] projects) {
    // Log the received optimization hints
    logOptimizationHints(filters, projects, null, null, null);

    // Call the enhanced scan method with null for sort/pagination
    return scan(root, filters, projects, null, null, null);
  }

  /**
   * Registers the rule that hands ORDER BY / OFFSET / FETCH to this table. The scan itself
   * is the planner's standard one, so filter and projection handling are unchanged.
   */
  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    context.getCluster().getPlanner().addRule(CloudOpsSortScanRule.INSTANCE);
    return LogicalTableScan.create(context.getCluster(), relOptTable, context.getTableHints());
  }

  /**
   * Scan with a sort order and a row window, as planned by {@link CloudOpsSortScanRule}.
   *
   * <p>The rows of all providers are combined, sorted by {@code collation} (table ordinals),
   * cut to {@code offset}/{@code fetch} and only then projected. Providers are never given
   * the sort or the offset: a provider's own ordering of strings and nulls is not SQL's, so
   * a provider that sorted and truncated could keep the wrong rows. A provider is told to
   * return at most offset + fetch rows only when any rows will do, that is, when there is
   * neither a sort nor a filter.
   *
   * @param collation sort order in table ordinals, or null
   * @param offset literal number of leading rows to skip, or null
   * @param fetch literal maximum number of rows to return, or null
   */
  public Enumerable<Object[]> scan(DataContext root,
                                   List<RexNode> filters,
                                   int @Nullable [] projects,
                                   @Nullable RelCollation collation,
                                   @Nullable RexNode offset,
                                   @Nullable RexNode fetch) {
    // Log all received optimization hints
    logOptimizationHints(filters, projects, collation, offset, fetch);

    final RelDataType rowType = getRowType(root.getTypeFactory());

    // Create projection handler for optimization
    CloudOpsProjectionHandler projectionHandler =
        new CloudOpsProjectionHandler(rowType, projects);

    final CloudOpsSortHandler sortHandler = new CloudOpsSortHandler(rowType, collation);
    final long offsetRows = offset == null ? 0L : rowCount(offset, "OFFSET");
    final long fetchRows = fetch == null ? -1L : rowCount(fetch, "FETCH");

    // What the providers see: no sort, and a row cap only when any rows satisfy the query
    final boolean anyRowsWillDo =
        fetch != null && !sortHandler.hasSort() && (filters == null || filters.isEmpty());
    final CloudOpsSortHandler providerSort = new CloudOpsSortHandler(rowType, null);
    final CloudOpsPaginationHandler providerPagination = anyRowsWillDo
        ? CloudOpsPaginationHandler.firstRows(offsetRows + fetchRows)
        : CloudOpsPaginationHandler.none();

    // Create filter handler for optimization
    CloudOpsFilterHandler filterHandler =
        new CloudOpsFilterHandler(rowType, filters);

    // Extract filters
    Set<String> providers = extractProviders(filterHandler);
    List<String> accounts = extractAccounts(filterHandler);

    // If no providers specified, use all configured providers
    if (providers.isEmpty()) {
      providers = new HashSet<>(config.providers);
    }

    // Providers hand back whatever their API returned (Azure Resource Graph, for one, sends
    // booleans as 0/1); every row is brought to the declared column types before it leaves
    final List<RelDataTypeField> fields = rowType.getFieldList();
    final SqlTypeName[] columnTypes = new SqlTypeName[fields.size()];
    for (int i = 0; i < columnTypes.length; i++) {
      columnTypes[i] = fields.get(i).getType().getSqlTypeName();
    }

    // Query providers in parallel; rows stay full-width until sorted
    List<CompletableFuture<List<Object[]>>> futures = new ArrayList<>();

    if (providers.contains("azure") && config.azure != null) {
      futures.add(
          CompletableFuture.supplyAsync(() ->
              toColumnTypes(
                  queryAzure(accounts.isEmpty() ? config.azure.subscriptionIds : accounts,
                      projectionHandler, providerSort, providerPagination, filterHandler),
                  columnTypes), PROVIDER_POOL));
    }

    if (providers.contains("gcp") && config.gcp != null) {
      futures.add(
          CompletableFuture.supplyAsync(() ->
              toColumnTypes(
                  queryGCP(accounts.isEmpty() ? config.gcp.projectIds : accounts,
                      projectionHandler, providerSort, providerPagination, filterHandler),
                  columnTypes), PROVIDER_POOL));
    }

    if (providers.contains("aws") && config.aws != null) {
      futures.add(
          CompletableFuture.supplyAsync(() ->
              toColumnTypes(
                  queryAWS(accounts.isEmpty() ? config.aws.accountIds : accounts,
                      projectionHandler, providerSort, providerPagination, filterHandler),
                  columnTypes), PROVIDER_POOL));
    }

    // Combine results
    List<Object[]> allResults = futures.stream()
        .map(CompletableFuture::join)
        .flatMap(List::stream)
        .collect(Collectors.toList());

    if (logger.isDebugEnabled()) {
      logger.debug("Combined {} rows; sort {}, offset {}, fetch {}, provider row cap {}",
          allResults.size(), sortHandler.getSortFieldNames(), offsetRows,
          fetch == null ? "none" : String.valueOf(fetchRows),
          anyRowsWillDo ? String.valueOf(offsetRows + fetchRows) : "none");
    }

    // One sort and one window over the rows of all providers
    allResults = sortHandler.sortRows(allResults);
    if (offset != null || fetch != null) {
      final int from = (int) Math.min(offsetRows, allResults.size());
      final int to = fetch == null
          ? allResults.size()
          : (int) Math.min(offsetRows + fetchRows, allResults.size());
      allResults = allResults.subList(from, to);
    }

    // Apply remaining filters in memory
    return applyFilters(Linq4j.asEnumerable(projectionHandler.projectRows(allResults)), filters);
  }

  private static long rowCount(RexNode node, String clause) {
    if (!(node instanceof RexLiteral)) {
      throw new IllegalArgumentException(clause + " must be a literal, but is " + node);
    }
    final long rows = RexLiteral.intValue(node);
    if (rows < 0) {
      throw new IllegalArgumentException(clause + " must not be negative, but is " + rows);
    }
    return rows;
  }

  private static List<Object[]> toColumnTypes(List<Object[]> rows, SqlTypeName[] columnTypes) {
    final List<Object[]> converted = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      if (row.length != columnTypes.length) {
        throw new IllegalStateException("Row has " + row.length + " values for "
            + columnTypes.length + " columns");
      }
      converted.add(CloudOpsDataConverter.convertRow(row, columnTypes));
    }
    return converted;
  }

  /**
   * Extract cloud provider filter from predicates.
   */
  protected Set<String> extractProviders(CloudOpsFilterHandler filterHandler) {
    if (filterHandler == null || !filterHandler.hasFilters()) {
      return new HashSet<>(); // Query all providers
    }

    Set<String> providers = filterHandler.extractProviderConstraints();
    if (logger.isDebugEnabled() && !providers.isEmpty()) {
      logger.debug("Provider constraints extracted from filters: {}", providers);
    }

    return providers;
  }

  /**
   * Extract account/subscription/project IDs from predicates.
   */
  protected List<String> extractAccounts(CloudOpsFilterHandler filterHandler) {
    if (filterHandler == null || !filterHandler.hasFilters()) {
      return new ArrayList<>(); // Use configured defaults
    }

    List<String> accounts = filterHandler.extractAccountConstraints();
    if (logger.isDebugEnabled() && !accounts.isEmpty()) {
      logger.debug("Account constraints extracted from filters: {}", accounts);
    }

    return accounts;
  }

  /**
   * Apply remaining filters that weren't pushed down.
   */
  protected Enumerable<Object[]> applyFilters(Enumerable<Object[]> rows, List<RexNode> filters) {
    if (filters == null || filters.isEmpty()) {
      return rows;
    }

    // For client-side filtering, we would use Calcite's built-in filter evaluation
    // For now, just return all rows (server-side filtering is the main optimization)
    if (logger.isDebugEnabled()) {
      logger.debug("Client-side filter application: {} remaining filter(s) to evaluate", filters.size());
    }

    return rows;
  }

  /**
   * Log optimization hints received from query planner.
   */
  protected void logOptimizationHints(List<RexNode> filters,
                                     int @Nullable [] projects,
                                     @Nullable RelCollation collation,
                                     @Nullable RexNode offset,
                                     @Nullable RexNode fetch) {
    if (logger.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder("Query optimization hints received for ")
          .append(this.getClass().getSimpleName()).append(":\n");

      // Log filters
      sb.append("  Filters: ");
      if (filters != null && !filters.isEmpty()) {
        sb.append(filters.size()).append(" filter(s)\n");
        for (int i = 0; i < filters.size(); i++) {
          sb.append("    [").append(i).append("] ").append(filters.get(i)).append("\n");
        }
      } else {
        sb.append("none\n");
      }

      // Log projections
      sb.append("  Projections: ");
      if (projects != null) {
        sb.append("columns ").append(Arrays.toString(projects)).append("\n");
      } else {
        sb.append("all columns (no projection pushdown)\n");
      }

      // Log sort collation
      sb.append("  Sort: ");
      if (collation != null && !collation.getFieldCollations().isEmpty()) {
        sb.append(collation.getFieldCollations().size()).append(" sort field(s)\n");
        collation.getFieldCollations().forEach(fc ->
          sb.append("    Field ").append(fc.getFieldIndex())
            .append(" ").append(fc.getDirection())
            .append(" ").append(fc.nullDirection).append("\n"));
      } else {
        sb.append("none (no sort pushdown)\n");
      }

      // Log pagination
      sb.append("  Pagination:\n");
      sb.append("    Offset: ").append(offset != null ? offset.toString() : "none").append("\n");
      sb.append("    Fetch/Limit: ").append(fetch != null ? fetch.toString() : "none").append("\n");

      logger.debug(sb.toString());
    }
  }

  /**
   * Query Azure resources with projection, sort, pagination, and filter support.
   */
  protected abstract List<Object[]> queryAzure(List<String> subscriptionIds,
                                               CloudOpsProjectionHandler projectionHandler,
                                               CloudOpsSortHandler sortHandler,
                                               CloudOpsPaginationHandler paginationHandler,
                                               CloudOpsFilterHandler filterHandler);

  /**
   * Query GCP resources with projection, sort, pagination, and filter support.
   */
  protected abstract List<Object[]> queryGCP(List<String> projectIds,
                                             CloudOpsProjectionHandler projectionHandler,
                                             CloudOpsSortHandler sortHandler,
                                             CloudOpsPaginationHandler paginationHandler,
                                             CloudOpsFilterHandler filterHandler);

  /**
   * Query AWS resources with projection, sort, pagination, and filter support.
   */
  protected abstract List<Object[]> queryAWS(List<String> accountIds,
                                             CloudOpsProjectionHandler projectionHandler,
                                             CloudOpsSortHandler sortHandler,
                                             CloudOpsPaginationHandler paginationHandler,
                                             CloudOpsFilterHandler filterHandler);
}
