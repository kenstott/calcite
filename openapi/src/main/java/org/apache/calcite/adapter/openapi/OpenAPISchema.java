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
package org.apache.calcite.adapter.openapi;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Schema that contains OpenAPI endpoint tables.
 * Each table represents a configured OpenAPI endpoint with its variants.
 */
public class OpenAPISchema extends AbstractSchema {

  private final OpenAPITransport transport;
  private final OpenAPIConfig config;
  private final Map<String, Table> tableMap;

  /**
   * Creates an OpenAPISchema.
   *
   * @param transport Transport layer for API calls
   * @param config Configuration defining tables and their endpoints
   */
  public OpenAPISchema(OpenAPITransport transport, OpenAPIConfig config) {
    super();
    this.transport = requireNonNull(transport, "transport");
    this.config = requireNonNull(config, "config");
    this.tableMap = createTables();
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  private Map<String, Table> createTables() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    // For now, create a single table per configuration
    // In a more sophisticated implementation, this would parse the config
    // to create multiple tables based on different endpoint groups
    String tableName = "api_table"; // Could be derived from config

    builder.put(tableName, new OpenAPITable(tableName, transport, config));

    return builder.build();
  }
}
