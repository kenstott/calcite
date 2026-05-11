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
package org.apache.calcite.adapter.file.execution.clickhouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * ClickHouse-based execution engine for OLAP and time-series analytics.
 *
 * <p>Supports two deployment modes:
 * <ul>
 *   <li><b>Server mode</b>: Connect to an existing ClickHouse server via JDBC</li>
 *   <li><b>Local/embedded mode</b>: Auto-start {@code clickhouse-local} as a subprocess
 *       with HTTP interface, connect via the same JDBC driver</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>{@code
 * {
 *   "schemas": [{
 *     "name": "analytics",
 *     "operand": {
 *       "directory": "/data",
 *       "executionEngine": "CLICKHOUSE",
 *       "clickhouseConfig": {
 *         "mode": "local",
 *         "maxMemory": "4GB",
 *         "maxThreads": 4
 *       }
 *     }
 *   }]
 * }
 * }</pre>
 */
public final class ClickHouseExecutionEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseExecutionEngine.class);

  /**
   * Check if ClickHouse JDBC driver is available on the classpath.
   *
   * @return true if ClickHouse JDBC driver is available, false otherwise
   */
  public static boolean isAvailable() {
    try {
      Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
      return true;
    } catch (ClassNotFoundException e) {
      LOGGER.debug("ClickHouse JDBC driver not found on classpath: {}", e.getMessage());
      return false;
    } catch (Exception e) {
      LOGGER.debug("Error checking ClickHouse availability: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Check if {@code clickhouse-local} binary is available for embedded mode.
   *
   * <p>Checks the {@code CLICKHOUSE_LOCAL_PATH} environment variable first,
   * then searches the system PATH.
   *
   * @return true if clickhouse-local binary is found, false otherwise
   */
  public static boolean isLocalAvailable() {
    // Check environment variable first
    String envPath = System.getenv("CLICKHOUSE_LOCAL_PATH");
    if (envPath != null && !envPath.isEmpty()) {
      File binary = new File(envPath);
      if (binary.exists() && binary.canExecute()) {
        LOGGER.debug("Found clickhouse-local via CLICKHOUSE_LOCAL_PATH: {}", envPath);
        return true;
      }
    }

    // Search PATH
    String path = System.getenv("PATH");
    if (path != null) {
      for (String dir : path.split(File.pathSeparator)) {
        File binary = new File(dir, "clickhouse-local");
        if (binary.exists() && binary.canExecute()) {
          LOGGER.debug("Found clickhouse-local on PATH: {}", binary.getAbsolutePath());
          return true;
        }
      }
    }

    LOGGER.debug("clickhouse-local binary not found");
    return false;
  }

  /**
   * Finds the path to the {@code clickhouse-local} binary.
   *
   * @param configuredPath optional configured path (from ClickHouseConfig)
   * @return the path to the binary, or null if not found
   */
  public static String findLocalBinaryPath(String configuredPath) {
    // Check configured path first
    if (configuredPath != null && !configuredPath.isEmpty()) {
      File binary = new File(configuredPath);
      if (binary.exists() && binary.canExecute()) {
        return binary.getAbsolutePath();
      }
    }

    // Check environment variable
    String envPath = System.getenv("CLICKHOUSE_LOCAL_PATH");
    if (envPath != null && !envPath.isEmpty()) {
      File binary = new File(envPath);
      if (binary.exists() && binary.canExecute()) {
        return binary.getAbsolutePath();
      }
    }

    // Search PATH
    String path = System.getenv("PATH");
    if (path != null) {
      for (String dir : path.split(File.pathSeparator)) {
        File binary = new File(dir, "clickhouse-local");
        if (binary.exists() && binary.canExecute()) {
          return binary.getAbsolutePath();
        }
      }
    }

    return null;
  }

  /**
   * Get the engine type identifier.
   *
   * @return "CLICKHOUSE"
   */
  public static String getEngineType() {
    return "CLICKHOUSE";
  }

  private ClickHouseExecutionEngine() {
    // Utility class - all methods are static
  }
}
