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
package org.apache.calcite.adapter.servicenow;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema holding the instance's listing tables and the ServiceNow tables registered for use.
 *
 * <p>{@link ListingTables#TABLES} and {@link ListingTables#COLUMNS} are always present: they list
 * every table the instance has and the columns of any one of them. Only the tables named in the
 * {@code tables} operand (the registered ones) are SQL tables; with no operand there are none. An
 * instance has thousands of tables, and a host such as the pgwire server reads the columns of
 * every SQL table when it starts, so exposing them all would read the column metadata of every
 * one (SNOW-010 as amended 2026-10-12).
 */
public class ServiceNowSchema extends AbstractSchema {

  private final ServiceNowConnection connection;
  private final int pageSize;
  private final int metadataPageSize;
  private final CatalogCache catalogCache;
  private final List<String> includeTables;
  private final Map<String, String> importSets;
  private final Set<String> excludedColumnTypes;
  private final PushdownCapabilities capabilities;
  private final PushdownObserver observer;
  private ServiceNowCatalog catalog;
  private Map<String, Table> tableMap;

  /**
   * Creates a schema.
   *
   * @param pageSize            rows per request when reading a table
   * @param metadataPageSize    rows per request when reading the metadata tables
   * @param catalogCacheDirectory where the catalog snapshot is kept between runs
   * @param catalogCacheTimeToLive how long a snapshot on disk is used for; zero keeps nothing
   * @param includeTables       names of the registered tables, the only ones exposed
   * @param importSets          registered table to the staging table its INSERTs go through
   * @param excludedColumnTypes field types whose columns are left out
   * @param capabilities        the pushdown entries that may be used
   * @param observer            told what each scan pushed, or null
   */
  ServiceNowSchema(ServiceNowConnection connection, int pageSize, int metadataPageSize,
      Path catalogCacheDirectory, Duration catalogCacheTimeToLive, List<String> includeTables,
      Map<String, String> importSets, Set<String> excludedColumnTypes, PushdownCapabilities capabilities,
      PushdownObserver observer) {
    this.connection = connection;
    this.pageSize = pageSize;
    this.metadataPageSize = metadataPageSize;
    this.catalogCache =
        new CatalogCache(catalogCacheDirectory, connection.scope(), catalogCacheTimeToLive);
    this.includeTables = includeTables;
    this.importSets = importSets;
    this.excludedColumnTypes = excludedColumnTypes;
    this.capabilities = capabilities;
    this.observer = observer;
  }

  ServiceNowConnection connection() {
    return connection;
  }

  int pageSize() {
    return pageSize;
  }

  int metadataPageSize() {
    return metadataPageSize;
  }

  /** The catalog, its table list loaded from the cache or the instance on first use. */
  synchronized ServiceNowCatalog catalog() {
    if (catalog == null) {
      catalog = new ServiceNowCatalog(
          catalogCache.get(() -> ServiceNowCatalog.load(connection, metadataPageSize)),
          excludedColumnTypes, ServiceNowCatalog.metadata(connection, metadataPageSize));
    }
    return catalog;
  }

  @Override protected synchronized Map<String, Table> getTableMap() {
    if (tableMap == null) {
      final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
      builder.put(ListingTables.TABLES, new ListingTables.Tables(this));
      builder.put(ListingTables.COLUMNS, new ListingTables.Columns(this));
      builder.put(ListingTables.TRANSFORM_MAPS, new ListingTables.TransformMaps(this));
      for (String name : includeTables) {
        if (name.equals(ListingTables.TABLES) || name.equals(ListingTables.COLUMNS)
            || name.equals(ListingTables.TRANSFORM_MAPS)) {
          throw new ServiceNowException("The 'tables' operand names '" + name + "', which is "
              + "the adapter's own listing table");
        }
        if (!catalog().hasTable(name)) {
          throw new ServiceNowException("The 'tables' operand names '" + name + "', which is not "
              + "in sys_db_object (or not readable by the integration user)");
        }
        builder.put(name, new ServiceNowTable(this, name,
            new EncodedQueryTranslator(capabilities), observer, importSets.get(name)));
      }
      tableMap = builder.build();
    }
    return tableMap;
  }
}
