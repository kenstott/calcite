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
package org.apache.calcite.adapter.splunk;

import org.apache.calcite.adapter.splunk.search.SplunkConnection;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Schema for Splunk.
 * Supports both predefined CIM tables and custom table definitions.
 */
public class SplunkSchema extends AbstractSchema {
  public final SplunkConnection splunkConnection;
  private final Map<String, Table> predefinedTables;

  /**
   * Constructor for CIM model mode with predefined tables.
   *
   * @param connection Splunk connection
   * @param tables Map of table name to Table instances
   */
  public SplunkSchema(SplunkConnection connection, Map<String, Table> tables) {
    this.splunkConnection = connection;
    this.predefinedTables = tables;
  }

  /**
   * Constructor for custom table mode.
   * Tables will be defined via JSON table definitions.
   *
   * @param connection Splunk connection
   */
  public SplunkSchema(SplunkConnection connection) {
    this.splunkConnection = connection;
    this.predefinedTables = new HashMap<>();
  }

  @Override protected Map<String, Table> getTableMap() {
    // CIM model mode - return predefined tables
    return predefinedTables;
  }
}
