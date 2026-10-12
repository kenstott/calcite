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
 * Schema holding one table per ServiceNow table.
 *
 * <p>The table set comes from the instance's {@code sys_db_object}; an optional include list
 * narrows it. All tables are in this one SQL schema. Putting each application scope in its own
 * SQL schema would be a change here and in the catalog (a scope column on the table rows), not in
 * how tables are read.
 */
public class ServiceNowSchema extends AbstractSchema {

  private final ServiceNowConnection connection;
  private final int pageSize;
  private final int metadataPageSize;
  private final CatalogCache catalogCache;
  private final List<String> includeTables;
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
   * @param includeTables       names of the only tables to expose, or empty for all of them
   * @param excludedColumnTypes field types whose columns are left out
   * @param capabilities        the pushdown entries that may be used
   * @param observer            told what each scan pushed, or null
   */
  ServiceNowSchema(ServiceNowConnection connection, int pageSize, int metadataPageSize,
      Path catalogCacheDirectory, Duration catalogCacheTimeToLive, List<String> includeTables,
      Set<String> excludedColumnTypes, PushdownCapabilities capabilities,
      PushdownObserver observer) {
    this.connection = connection;
    this.pageSize = pageSize;
    this.metadataPageSize = metadataPageSize;
    this.catalogCache =
        new CatalogCache(catalogCacheDirectory, connection.scope(), catalogCacheTimeToLive);
    this.includeTables = includeTables;
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

  /** The catalog, loaded from the cache or the instance on first use. */
  synchronized ServiceNowCatalog catalog() {
    if (catalog == null) {
      catalog = new ServiceNowCatalog(
          catalogCache.get(() -> ServiceNowCatalog.load(connection, metadataPageSize)),
          excludedColumnTypes);
    }
    return catalog;
  }

  @Override protected synchronized Map<String, Table> getTableMap() {
    if (tableMap == null) {
      final ServiceNowCatalog catalog = catalog();
      final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
      final List<String> names = includeTables.isEmpty() ? catalog.tableNames() : includeTables;
      for (String name : names) {
        if (!catalog.hasTable(name)) {
          throw new ServiceNowException("The 'tables' operand names '" + name + "', which is not "
              + "in sys_db_object (or not readable by the integration user)");
        }
        builder.put(name, new ServiceNowTable(this, name,
            new EncodedQueryTranslator(capabilities), observer));
      }
      tableMap = builder.build();
    }
    return tableMap;
  }
}
