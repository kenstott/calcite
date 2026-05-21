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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration options specific to Spark SQL execution engine via Thrift Server.
 *
 * <p>Spark SQL connections are always server-mode (no embedded option).
 * The Thrift Server must be running for connections to succeed.
 *
 * <p>Example configuration:
 * <pre>{@code
 * {
 *   "executionEngine": "SPARK",
 *   "sparkConfig": {
 *     "host": "localhost",
 *     "port": "10000",
 *     "database": "default",
 *     "icebergCatalogType": "hadoop",
 *     "icebergWarehouse": "/data/warehouse",
 *     "maxMemory": "4g",
 *     "maxThreads": 4
 *   }
 * }
 * }</pre>
 */
public class SparkConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkConfig.class);

  public static final String DEFAULT_HOST = "localhost";
  public static final String DEFAULT_PORT = "10000";
  public static final String DEFAULT_DATABASE = "default";
  public static final String DEFAULT_ICEBERG_CATALOG_TYPE = "hadoop";

  /** Name used for the Iceberg catalog in Spark SQL SET commands. */
  public static final String ICEBERG_CATALOG_NAME = "aperio_iceberg";

  private final String host;
  private final String port;
  private final String database;
  private final String user;
  private final String password;
  private final String icebergCatalogType;
  private final String icebergWarehouse;
  private final String maxMemory;
  private final int maxThreads;
  private final Properties additionalSettings;

  /**
   * Creates Spark configuration with default values.
   */
  public SparkConfig() {
    this(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DATABASE, null, null,
        DEFAULT_ICEBERG_CATALOG_TYPE, null, null, 0, null);
  }

  /**
   * Creates Spark configuration from a map (typically from JSON/YAML config).
   *
   * @param configMap configuration map with Spark settings
   */
  public SparkConfig(Map<String, Object> configMap) {
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
    this.user = (String) configMap.get("user");
    this.password = (String) configMap.get("password");
    this.icebergCatalogType =
        (String) configMap.getOrDefault("icebergCatalogType", DEFAULT_ICEBERG_CATALOG_TYPE);
    this.icebergWarehouse = (String) configMap.get("icebergWarehouse");

    this.maxMemory = (String) configMap.get("maxMemory");

    Object threadsObj = configMap.get("maxThreads");
    this.maxThreads = threadsObj instanceof Number
        ? ((Number) threadsObj).intValue() : 0;

    this.additionalSettings = new Properties();
    for (Map.Entry<String, Object> entry : configMap.entrySet()) {
      String key = entry.getKey();
      if (!isKnownSetting(key) && entry.getValue() != null) {
        additionalSettings.setProperty(key, entry.getValue().toString());
      }
    }
  }

  /**
   * Creates Spark configuration with all parameters.
   */
  public SparkConfig(String host, String port, String database,
      String user, String password,
      String icebergCatalogType, String icebergWarehouse,
      String maxMemory, int maxThreads,
      Properties additionalSettings) {
    this.host = host != null ? host : DEFAULT_HOST;
    this.port = port != null ? port : DEFAULT_PORT;
    this.database = database != null ? database : DEFAULT_DATABASE;
    this.user = user;
    this.password = password;
    this.icebergCatalogType = icebergCatalogType != null
        ? icebergCatalogType : DEFAULT_ICEBERG_CATALOG_TYPE;
    this.icebergWarehouse = icebergWarehouse;
    this.maxMemory = maxMemory;
    this.maxThreads = maxThreads;
    this.additionalSettings = additionalSettings != null ? additionalSettings : new Properties();
  }

  /**
   * Converts this configuration to Spark SQL SET statements for session configuration.
   *
   * @return array of SQL SET statements to configure the Spark session
   */
  public String[] toSparkSettings() {
    List<String> settings = new ArrayList<>();

    if (maxMemory != null && !maxMemory.isEmpty()) {
      settings.add("SET spark.executor.memory = " + maxMemory);
    }
    if (maxThreads > 0) {
      settings.add("SET spark.executor.cores = " + maxThreads);
    }

    for (Map.Entry<Object, Object> entry : additionalSettings.entrySet()) {
      String key = entry.getKey().toString();
      String value = entry.getValue().toString();
      settings.add("SET " + key + " = " + value);
    }

    return settings.toArray(new String[0]);
  }

  /**
   * Generates the SET statements for configuring the Iceberg catalog in Spark.
   *
   * @param warehousePath the warehouse path (derived from directoryPath if not set)
   * @return array of SQL SET statements for Iceberg catalog setup
   */
  public String[] toIcebergCatalogSettings(String warehousePath) {
    String warehouse = icebergWarehouse != null ? icebergWarehouse : warehousePath;
    List<String> settings = new ArrayList<>();
    settings.add("SET spark.sql.catalog." + ICEBERG_CATALOG_NAME
        + " = org.apache.iceberg.spark.SparkCatalog");
    settings.add("SET spark.sql.catalog." + ICEBERG_CATALOG_NAME
        + ".type = " + icebergCatalogType);
    if (warehouse != null) {
      settings.add("SET spark.sql.catalog." + ICEBERG_CATALOG_NAME
          + ".warehouse = " + warehouse);
    }
    return settings.toArray(new String[0]);
  }

  private boolean isKnownSetting(String key) {
    return "host".equals(key) || "port".equals(key) || "database".equals(key)
        || "user".equals(key) || "password".equals(key)
        || "icebergCatalogType".equals(key) || "icebergWarehouse".equals(key)
        || "maxMemory".equals(key) || "maxThreads".equals(key);
  }

  // Getters
  public String getHost() {
    return host;
  }

  public String getPort() {
    return port;
  }

  public String getDatabase() {
    return database;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public String getIcebergCatalogType() {
    return icebergCatalogType;
  }

  public String getIcebergWarehouse() {
    return icebergWarehouse;
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
    return "SparkConfig{"
        + "host='" + host + '\''
        + ", port='" + port + '\''
        + ", database='" + database + '\''
        + ", user='" + (user != null ? "***" : "null") + '\''
        + ", icebergCatalogType='" + icebergCatalogType + '\''
        + ", icebergWarehouse='" + icebergWarehouse + '\''
        + ", maxMemory='" + maxMemory + '\''
        + ", maxThreads=" + maxThreads
        + ", additionalSettings=" + additionalSettings.size() + " items"
        + '}';
  }
}
