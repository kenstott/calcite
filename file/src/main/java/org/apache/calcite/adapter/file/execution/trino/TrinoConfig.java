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
package org.apache.calcite.adapter.file.execution.trino;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration options specific to Trino execution engine.
 *
 * <p>Trino connections are always server-mode (no embedded option).
 * A Trino server must be running for connections to succeed.
 *
 * <p>Unlike Spark, Trino uses two distinct catalogs configured server-side:
 * <ul>
 *   <li><b>Hive catalog</b> — for Parquet external tables via
 *       {@code CREATE TABLE ... WITH (external_location)}</li>
 *   <li><b>Iceberg catalog</b> — for Iceberg tables via
 *       {@code CALL iceberg.system.register_table()}</li>
 * </ul>
 *
 * <p>Example configuration:
 * <pre>{@code
 * {
 *   "executionEngine": "TRINO",
 *   "trinoConfig": {
 *     "host": "localhost",
 *     "port": "8080",
 *     "catalog": "hive",
 *     "schema": "default",
 *     "icebergCatalog": "iceberg",
 *     "warehouseDir": "/data/warehouse"
 *   }
 * }
 * }</pre>
 */
public class TrinoConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(TrinoConfig.class);

  public static final String DEFAULT_HOST = "localhost";
  public static final String DEFAULT_PORT = "8080";
  public static final String DEFAULT_CATALOG = "hive";
  public static final String DEFAULT_SCHEMA = "default";
  public static final String DEFAULT_ICEBERG_CATALOG = "iceberg";
  public static final String DEFAULT_WAREHOUSE_DIR = "/data/warehouse";

  private final String host;
  private final String port;
  private final String catalog;
  private final String schema;
  private final String user;
  private final String password;
  private final String icebergCatalog;
  private final String warehouseDir;
  private final String s3AccessKey;
  private final String s3SecretKey;
  private final String s3Endpoint;
  private final Properties additionalSettings;

  /**
   * Creates Trino configuration with default values.
   */
  public TrinoConfig() {
    this(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_CATALOG, DEFAULT_SCHEMA,
        null, null, DEFAULT_ICEBERG_CATALOG, DEFAULT_WAREHOUSE_DIR,
        null, null, null, null);
  }

  /**
   * Creates Trino configuration from a map (typically from JSON/YAML config).
   *
   * @param configMap configuration map with Trino settings
   */
  public TrinoConfig(Map<String, Object> configMap) {
    this.host = (String) configMap.getOrDefault("host", DEFAULT_HOST);

    Object portObj = configMap.get("port");
    if (portObj instanceof Number) {
      this.port = String.valueOf(((Number) portObj).intValue());
    } else if (portObj instanceof String) {
      this.port = (String) portObj;
    } else {
      this.port = DEFAULT_PORT;
    }

    this.catalog = (String) configMap.getOrDefault("catalog", DEFAULT_CATALOG);
    this.schema = (String) configMap.getOrDefault("schema", DEFAULT_SCHEMA);
    this.user = (String) configMap.get("user");
    this.password = (String) configMap.get("password");
    this.icebergCatalog = (String) configMap.getOrDefault("icebergCatalog", DEFAULT_ICEBERG_CATALOG);
    this.warehouseDir = (String) configMap.getOrDefault("warehouseDir", DEFAULT_WAREHOUSE_DIR);
    this.s3AccessKey = (String) configMap.get("s3AccessKey");
    this.s3SecretKey = (String) configMap.get("s3SecretKey");
    this.s3Endpoint = (String) configMap.get("s3Endpoint");

    this.additionalSettings = new Properties();
    for (Map.Entry<String, Object> entry : configMap.entrySet()) {
      String key = entry.getKey();
      if (!isKnownSetting(key) && entry.getValue() != null) {
        additionalSettings.setProperty(key, entry.getValue().toString());
      }
    }
  }

  /**
   * Creates Trino configuration with all parameters.
   */
  public TrinoConfig(String host, String port, String catalog, String schema,
      String user, String password, String icebergCatalog, String warehouseDir,
      String s3AccessKey, String s3SecretKey, String s3Endpoint,
      Properties additionalSettings) {
    this.host = host != null ? host : DEFAULT_HOST;
    this.port = port != null ? port : DEFAULT_PORT;
    this.catalog = catalog != null ? catalog : DEFAULT_CATALOG;
    this.schema = schema != null ? schema : DEFAULT_SCHEMA;
    this.user = user;
    this.password = password;
    this.icebergCatalog = icebergCatalog != null ? icebergCatalog : DEFAULT_ICEBERG_CATALOG;
    this.warehouseDir = warehouseDir != null ? warehouseDir : DEFAULT_WAREHOUSE_DIR;
    this.s3AccessKey = s3AccessKey;
    this.s3SecretKey = s3SecretKey;
    this.s3Endpoint = s3Endpoint;
    this.additionalSettings = additionalSettings != null ? additionalSettings : new Properties();
  }

  /**
   * Generates SET SESSION statements for additional session configuration.
   *
   * @return array of SQL SET SESSION statements
   */
  public String[] toSessionSettings() {
    List<String> settings = new ArrayList<>();
    for (Map.Entry<Object, Object> entry : additionalSettings.entrySet()) {
      String key = entry.getKey().toString();
      String value = entry.getValue().toString();
      settings.add("SET SESSION " + key + " = '" + value + "'");
    }
    return settings.toArray(new String[0]);
  }

  /**
   * Returns the Hive catalog properties for Parquet external table access.
   *
   * @return Properties object for hive.properties catalog file
   */
  public Properties getHiveCatalogProperties() {
    Properties props = new Properties();
    props.setProperty("connector.name", "hive");
    props.setProperty("hive.metastore", "file");
    props.setProperty("hive.metastore.catalog.dir", warehouseDir);
    props.setProperty("hive.security", "allow-all");
    props.setProperty("hive.non-managed-table-writes-enabled", "true");
    if (s3AccessKey != null && s3SecretKey != null) {
      props.setProperty("hive.s3.aws-access-key", s3AccessKey);
      props.setProperty("hive.s3.aws-secret-key", s3SecretKey);
      if (s3Endpoint != null) {
        props.setProperty("hive.s3.endpoint", s3Endpoint);
        props.setProperty("hive.s3.path-style-access", "true");
      }
    }
    return props;
  }

  /**
   * Returns the Iceberg catalog properties for Iceberg table access.
   *
   * @return Properties object for iceberg.properties catalog file
   */
  public Properties getIcebergCatalogProperties() {
    Properties props = new Properties();
    props.setProperty("connector.name", "iceberg");
    props.setProperty("iceberg.catalog.type", "TESTING_FILE_METASTORE");
    props.setProperty("hive.metastore.catalog.dir", warehouseDir);
    if (s3AccessKey != null && s3SecretKey != null) {
      props.setProperty("hive.s3.aws-access-key", s3AccessKey);
      props.setProperty("hive.s3.aws-secret-key", s3SecretKey);
      if (s3Endpoint != null) {
        props.setProperty("hive.s3.endpoint", s3Endpoint);
        props.setProperty("hive.s3.path-style-access", "true");
      }
    }
    return props;
  }

  /**
   * Generates Trino catalog property files to a directory.
   *
   * <p>Writes {@code hive.properties} and {@code iceberg.properties} files
   * that the user can place in their Trino server's {@code etc/catalog/} directory.
   *
   * @param outputDir the directory to write catalog files to
   * @throws IOException if file writing fails
   */
  public void generateCatalogFiles(String outputDir) throws IOException {
    File dir = new File(outputDir);
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Failed to create catalog output directory: " + outputDir);
    }

    // Write hive.properties
    File hiveFile = new File(dir, catalog + ".properties");
    Properties hiveProps = getHiveCatalogProperties();
    try (OutputStream out = new FileOutputStream(hiveFile)) {
      hiveProps.store(out, "Trino Hive catalog for Parquet external tables (generated by Aperio)");
    }
    LOGGER.info("Generated Trino Hive catalog file: {}", hiveFile.getAbsolutePath());

    // Write iceberg.properties
    File icebergFile = new File(dir, icebergCatalog + ".properties");
    Properties icebergProps = getIcebergCatalogProperties();
    try (OutputStream out = new FileOutputStream(icebergFile)) {
      icebergProps.store(out, "Trino Iceberg catalog for Iceberg tables (generated by Aperio)");
    }
    LOGGER.info("Generated Trino Iceberg catalog file: {}", icebergFile.getAbsolutePath());
  }

  private boolean isKnownSetting(String key) {
    return "host".equals(key) || "port".equals(key) || "catalog".equals(key)
        || "schema".equals(key) || "user".equals(key) || "password".equals(key)
        || "icebergCatalog".equals(key) || "warehouseDir".equals(key)
        || "s3AccessKey".equals(key) || "s3SecretKey".equals(key) || "s3Endpoint".equals(key);
  }

  // Getters
  public String getHost() {
    return host;
  }

  public String getPort() {
    return port;
  }

  public String getCatalog() {
    return catalog;
  }

  public String getSchema() {
    return schema;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public String getIcebergCatalog() {
    return icebergCatalog;
  }

  public String getWarehouseDir() {
    return warehouseDir;
  }

  public String getS3AccessKey() {
    return s3AccessKey;
  }

  public String getS3SecretKey() {
    return s3SecretKey;
  }

  public String getS3Endpoint() {
    return s3Endpoint;
  }

  public Properties getAdditionalSettings() {
    return additionalSettings;
  }

  @Override public String toString() {
    return "TrinoConfig{"
        + "host='" + host + '\''
        + ", port='" + port + '\''
        + ", catalog='" + catalog + '\''
        + ", schema='" + schema + '\''
        + ", user='" + (user != null ? "***" : "null") + '\''
        + ", icebergCatalog='" + icebergCatalog + '\''
        + ", warehouseDir='" + warehouseDir + '\''
        + ", additionalSettings=" + additionalSettings.size() + " items"
        + '}';
  }
}
