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
package org.apache.calcite.adapter.file.execution.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Spark SQL execution engine via Thrift Server (HiveServer2 protocol).
 *
 * <p>Unlike DuckDB (embedded) or ClickHouse (has clickhouse-local), Spark has no
 * embedded mode and requires an external Thrift Server to be running.
 *
 * <p>Usage:
 * <pre>{@code
 * {
 *   "schemas": [{
 *     "name": "analytics",
 *     "operand": {
 *       "directory": "/data",
 *       "executionEngine": "SPARK",
 *       "sparkConfig": {
 *         "host": "localhost",
 *         "port": "10000",
 *         "database": "default"
 *       }
 *     }
 *   }]
 * }
 * }</pre>
 */
public final class SparkExecutionEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkExecutionEngine.class);

  /** Default timeout in milliseconds for server reachability check. */
  private static final int CONNECT_TIMEOUT_MS = 3000;

  /**
   * Check if the Hive JDBC driver (used for Spark Thrift Server) is available.
   *
   * @return true if the Hive JDBC driver is on the classpath
   */
  public static boolean isAvailable() {
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      return true;
    } catch (ClassNotFoundException e) {
      LOGGER.debug("Hive JDBC driver not found on classpath: {}", e.getMessage());
      return false;
    } catch (Exception e) {
      LOGGER.debug("Error checking Spark/Hive JDBC availability: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Check if the Spark Thrift Server is reachable via TCP socket probe.
   *
   * @param host the Thrift Server hostname
   * @param port the Thrift Server port
   * @return true if the server is reachable
   */
  public static boolean isServerReachable(String host, int port) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
      LOGGER.debug("Spark Thrift Server reachable at {}:{}", host, port);
      return true;
    } catch (IOException e) {
      LOGGER.debug("Spark Thrift Server not reachable at {}:{}: {}", host, port, e.getMessage());
      return false;
    }
  }

  /**
   * Get the engine type identifier.
   *
   * @return "SPARK"
   */
  public static String getEngineType() {
    return "SPARK";
  }

  private SparkExecutionEngine() {
    // Utility class - all methods are static
  }
}
