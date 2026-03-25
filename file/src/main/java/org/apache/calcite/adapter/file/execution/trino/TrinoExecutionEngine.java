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
package org.apache.calcite.adapter.file.execution.trino;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Trino execution engine utility for catalog-based Parquet and Iceberg access.
 *
 * <p>Trino is always server-mode — no embedded option. It connects via its own
 * JDBC driver ({@code io.trino.jdbc.TrinoDriver}) on port 8080 by default.
 *
 * <p>Unlike DuckDB (embedded) or Spark (backtick Parquet paths), Trino requires
 * tables to be registered in a catalog (Hive connector with
 * {@code external_location}) before querying. Iceberg is supported natively
 * via the Iceberg connector's {@code register_table} system procedure.
 *
 * <p>Usage:
 * <pre>{@code
 * {
 *   "schemas": [{
 *     "name": "analytics",
 *     "operand": {
 *       "directory": "/data",
 *       "executionEngine": "TRINO",
 *       "trinoConfig": {
 *         "host": "localhost",
 *         "port": "8080",
 *         "catalog": "hive",
 *         "schema": "default"
 *       }
 *     }
 *   }]
 * }
 * }</pre>
 */
public final class TrinoExecutionEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(TrinoExecutionEngine.class);

  /** Default timeout in milliseconds for server reachability check. */
  private static final int CONNECT_TIMEOUT_MS = 3000;

  /**
   * Check if the Trino JDBC driver is available on the classpath.
   *
   * @return true if the Trino JDBC driver is on the classpath
   */
  public static boolean isAvailable() {
    try {
      Class.forName("io.trino.jdbc.TrinoDriver");
      return true;
    } catch (ClassNotFoundException e) {
      LOGGER.debug("Trino JDBC driver not found on classpath: {}", e.getMessage());
      return false;
    } catch (Exception e) {
      LOGGER.debug("Error checking Trino JDBC availability: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Check if the Trino server is reachable via TCP socket probe.
   *
   * @param host the Trino server hostname
   * @param port the Trino server port
   * @return true if the server is reachable
   */
  public static boolean isServerReachable(String host, int port) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
      LOGGER.debug("Trino server reachable at {}:{}", host, port);
      return true;
    } catch (IOException e) {
      LOGGER.debug("Trino server not reachable at {}:{}: {}", host, port, e.getMessage());
      return false;
    }
  }

  /**
   * Get the engine type identifier.
   *
   * @return "TRINO"
   */
  public static String getEngineType() {
    return "TRINO";
  }

  private TrinoExecutionEngine() {
    // Utility class - all methods are static
  }
}
