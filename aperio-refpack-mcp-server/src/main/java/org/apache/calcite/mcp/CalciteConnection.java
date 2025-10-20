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
package org.apache.calcite.mcp;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Wrapper for Calcite JDBC connection.
 *
 * <p>Manages connection lifecycle and provides access to database metadata.
 */
public class CalciteConnection {
  private final Connection connection;
  private final DatabaseMetaData metadata;

  /**
   * Create a new Calcite connection.
   *
   * @param jdbcUrl JDBC connection URL (e.g., "jdbc:calcite:model=model.json")
   * @throws SQLException if connection fails
   */
  public CalciteConnection(String jdbcUrl) throws SQLException {
    this.connection = DriverManager.getConnection(jdbcUrl);
    this.metadata = connection.getMetaData();
  }

  /**
   * Get the underlying JDBC connection.
   *
   * @return JDBC connection
   */
  public Connection getConnection() {
    return connection;
  }

  /**
   * Get database metadata.
   *
   * @return DatabaseMetaData instance
   */
  public DatabaseMetaData getMetadata() {
    return metadata;
  }

  /**
   * Close the connection.
   *
   * @throws SQLException if close fails
   */
  public void close() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }
}
