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

import java.util.Map;
import java.util.Properties;

/**
 * Configuration options specific to ClickHouse execution engine.
 *
 * <p>Supports two deployment modes:
 * <ul>
 *   <li><b>server</b> (default): Connect to an existing ClickHouse server</li>
 *   <li><b>local</b>: Start {@code clickhouse-local} as an embedded subprocess</li>
 * </ul>
 *
 * <p>Example configuration:
 * <pre>{@code
 * {
 *   "executionEngine": "CLICKHOUSE",
 *   "clickhouseConfig": {
 *     "mode": "local",
 *     "host": "localhost",
 *     "port": "8123",
 *     "database": "default",
 *     "maxMemory": "4GB",
 *     "maxThreads": 4
 *   }
 * }
 * }</pre>
 */
public class ClickHouseConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseConfig.class);

  public static final String DEFAULT_MODE = "server";
  public static final String DEFAULT_HOST = "localhost";
  public static final String DEFAULT_PORT = "8123";
  public static final String DEFAULT_DATABASE = "default";
  public static final String DEFAULT_MAX_MEMORY = "4GB";
  public static final int DEFAULT_MAX_THREADS = Runtime.getRuntime().availableProcessors();

  public static final String MODE_SERVER = "server";
  public static final String MODE_LOCAL = "local";

  private final String mode;
  private final String host;
  private final String port;
  private final String database;
  private final String localBinaryPath;
  private final String dataDir;
  private final String maxMemory;
  private final int maxThreads;
  private final Properties additionalSettings;

  /**
   * Creates ClickHouse configuration with default values (server mode).
   */
  public ClickHouseConfig() {
    this(DEFAULT_MODE, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DATABASE,
        null, null, DEFAULT_MAX_MEMORY, DEFAULT_MAX_THREADS, null);
  }

  /**
   * Creates ClickHouse configuration from a map (typically from JSON/YAML config).
   *
   * @param configMap configuration map with ClickHouse settings
   */
  public ClickHouseConfig(Map<String, Object> configMap) {
    this.mode = (String) configMap.getOrDefault("mode", DEFAULT_MODE);
    this.host = (String) configMap.getOrDefault("host", DEFAULT_HOST);

    Object portObj = configMap.get("port");
    if (portObj instanceof Number) {
      this.port = String.valueOf(((Number) portObj).intValue());
    } else if (portObj instanceof String) {
      this.port = (String) portObj;
    } else {
      this.port = DEFAULT_PORT;
    }

    this.database = (String) configMap.getOrDefault("database", DEFAULT_DATABASE);
    this.localBinaryPath = (String) configMap.get("localBinaryPath");
    this.dataDir = (String) configMap.get("dataDir");
    this.maxMemory = (String) configMap.getOrDefault("maxMemory", DEFAULT_MAX_MEMORY);

    Object threadsObj = configMap.get("maxThreads");
    this.maxThreads = threadsObj instanceof Number
        ? ((Number) threadsObj).intValue()
        : DEFAULT_MAX_THREADS;

    this.additionalSettings = new Properties();
    for (Map.Entry<String, Object> entry : configMap.entrySet()) {
      String key = entry.getKey();
      if (!isKnownSetting(key) && entry.getValue() != null) {
        additionalSettings.setProperty(key, entry.getValue().toString());
      }
    }
  }

  /**
   * Creates ClickHouse configuration with all parameters.
   */
  public ClickHouseConfig(String mode, String host, String port, String database,
      String localBinaryPath, String dataDir, String maxMemory, int maxThreads,
      Properties additionalSettings) {
    this.mode = mode != null ? mode : DEFAULT_MODE;
    this.host = host != null ? host : DEFAULT_HOST;
    this.port = port != null ? port : DEFAULT_PORT;
    this.database = database != null ? database : DEFAULT_DATABASE;
    this.localBinaryPath = localBinaryPath;
    this.dataDir = dataDir;
    this.maxMemory = maxMemory != null ? maxMemory : DEFAULT_MAX_MEMORY;
    this.maxThreads = maxThreads > 0 ? maxThreads : DEFAULT_MAX_THREADS;
    this.additionalSettings = additionalSettings != null ? additionalSettings : new Properties();
  }

  /**
   * Returns whether this configuration uses embedded (local) mode.
   */
  public boolean isLocalMode() {
    return MODE_LOCAL.equalsIgnoreCase(mode);
  }

  /**
   * Converts this configuration to ClickHouse SET statements.
   *
   * @return array of SQL SET statements to configure ClickHouse
   */
  public String[] toClickHouseSettings() {
    java.util.List<String> settings = new java.util.ArrayList<>();

    settings.add("SET max_memory_usage = " + parseMemoryToBytes(maxMemory));
    settings.add("SET max_threads = " + maxThreads);

    for (Map.Entry<Object, Object> entry : additionalSettings.entrySet()) {
      String key = entry.getKey().toString();
      String value = entry.getValue().toString();
      if (isNumericValue(value) || isBooleanValue(value)) {
        settings.add("SET " + key + " = " + value);
      } else {
        settings.add("SET " + key + " = '" + value + "'");
      }
    }

    return settings.toArray(new String[0]);
  }

  private static long parseMemoryToBytes(String memory) {
    if (memory == null) {
      return 4L * 1024 * 1024 * 1024; // 4GB default
    }
    String upper = memory.toUpperCase(java.util.Locale.ROOT).trim();
    long multiplier = 1;
    String numberPart = upper;
    if (upper.endsWith("GB")) {
      multiplier = 1024L * 1024 * 1024;
      numberPart = upper.substring(0, upper.length() - 2).trim();
    } else if (upper.endsWith("MB")) {
      multiplier = 1024L * 1024;
      numberPart = upper.substring(0, upper.length() - 2).trim();
    } else if (upper.endsWith("KB")) {
      multiplier = 1024L;
      numberPart = upper.substring(0, upper.length() - 2).trim();
    }
    try {
      return Long.parseLong(numberPart) * multiplier;
    } catch (NumberFormatException e) {
      return 4L * 1024 * 1024 * 1024; // 4GB default
    }
  }

  private boolean isKnownSetting(String key) {
    return "mode".equals(key) || "host".equals(key) || "port".equals(key)
        || "database".equals(key) || "localBinaryPath".equals(key)
        || "dataDir".equals(key) || "maxMemory".equals(key)
        || "maxThreads".equals(key);
  }

  private boolean isNumericValue(String value) {
    try {
      Double.parseDouble(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  private boolean isBooleanValue(String value) {
    return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)
        || "1".equals(value) || "0".equals(value);
  }

  // Getters
  public String getMode() {
    return mode;
  }

  public String getHost() {
    return host;
  }

  public String getPort() {
    return port;
  }

  public String getDatabase() {
    return database;
  }

  public String getLocalBinaryPath() {
    return localBinaryPath;
  }

  public String getDataDir() {
    return dataDir;
  }

  public String getMaxMemory() {
    return maxMemory;
  }

  public int getMaxThreads() {
    return maxThreads;
  }

  public Properties getAdditionalSettings() {
    return additionalSettings;
  }

  @Override public String toString() {
    return "ClickHouseConfig{"
        + "mode='" + mode + '\''
        + ", host='" + host + '\''
        + ", port='" + port + '\''
        + ", database='" + database + '\''
        + ", localBinaryPath='" + localBinaryPath + '\''
        + ", dataDir='" + dataDir + '\''
        + ", maxMemory='" + maxMemory + '\''
        + ", maxThreads=" + maxThreads
        + ", additionalSettings=" + additionalSettings.size() + " items"
        + '}';
  }
}
