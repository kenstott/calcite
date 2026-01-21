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
package org.apache.calcite.adapter.file.etl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for a complete schema with tables and lifecycle hooks.
 *
 * <p>SchemaConfig is the top-level configuration that defines:
 * <ul>
 *   <li>Schema name and metadata</li>
 *   <li>Schema-level lifecycle hooks</li>
 *   <li>List of table configurations (EtlPipelineConfig)</li>
 * </ul>
 *
 * <h3>YAML Configuration Example</h3>
 * <pre>{@code
 * schema:
 *   name: econ
 *   sourceDirectory: "s3://bucket/raw/"         # default for file-based sources
 *   materializeDirectory: "s3://bucket/parquet/" # default output directory
 *   hooks:
 *     schemaLifecycleListener: "org.example.EconSchemaListener"
 *     tableLifecycleListener: "org.example.EconTableListener"
 *
 *   tables:
 *     - name: gdp
 *       source: { type: http, url: "..." }
 *       hooks:
 *         dimensionResolver: "org.example.BeaDimensionResolver"
 *         responseTransformer: "org.example.BeaResponseTransformer"
 *       materialize: { ... }
 *
 *     - name: employment
 *       source: { type: file, location: "employment/" }  # relative to sourceDirectory
 *       materialize: { ... }
 * }</pre>
 *
 * @see SchemaLifecycleProcessor
 * @see EtlPipelineConfig
 */
public class SchemaConfig {

  private final String name;
  private final String sourceDirectory;
  private final String materializeDirectory;
  private final Map<String, BulkDownloadConfig> bulkDownloads;
  private final List<EtlPipelineConfig> tables;
  private final SchemaHooksConfig hooks;
  private final Map<String, Object> metadata;

  private SchemaConfig(Builder builder) {
    this.name = builder.name;
    this.sourceDirectory = builder.sourceDirectory;
    this.materializeDirectory = builder.materializeDirectory;
    this.bulkDownloads = builder.bulkDownloads != null
        ? Collections.unmodifiableMap(new LinkedHashMap<>(builder.bulkDownloads))
        : Collections.<String, BulkDownloadConfig>emptyMap();
    this.tables = builder.tables != null
        ? Collections.unmodifiableList(new ArrayList<EtlPipelineConfig>(builder.tables))
        : Collections.<EtlPipelineConfig>emptyList();
    this.hooks = builder.hooks != null ? builder.hooks : SchemaHooksConfig.empty();
    this.metadata = builder.metadata != null
        ? Collections.unmodifiableMap(builder.metadata)
        : Collections.<String, Object>emptyMap();
  }

  /**
   * Returns the schema name.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the source directory for file-based sources.
   *
   * <p>This is the default directory for tables with file-based sources.
   * Can be a local path, S3 URI, HDFS path, etc.
   * Tables can override this with their own source location.
   *
   * @return Source directory string, or null if not configured
   */
  public String getSourceDirectory() {
    return sourceDirectory;
  }

  /**
   * Returns the materialize directory for output files.
   *
   * <p>This is the default directory for materialized data (Parquet, Iceberg, etc.).
   * Can be a local path, S3 URI, HDFS path, etc.
   * Tables can override this with their own output location.
   *
   * @return Materialize directory string, or null if not configured
   */
  public String getMaterializeDirectory() {
    return materializeDirectory;
  }

  /**
   * Returns the bulk download configurations.
   *
   * <p>Bulk downloads are large files downloaded once and shared by multiple tables.
   * They are processed in a dedicated phase before table processing begins.
   *
   * @return Map of bulk download name to configuration
   */
  public Map<String, BulkDownloadConfig> getBulkDownloads() {
    return bulkDownloads;
  }

  /**
   * Gets a bulk download configuration by name.
   *
   * @param name Bulk download name
   * @return BulkDownloadConfig, or null if not found
   */
  public BulkDownloadConfig getBulkDownload(String name) {
    return bulkDownloads.get(name);
  }

  /**
   * Returns the list of table configurations.
   */
  public List<EtlPipelineConfig> getTables() {
    return tables;
  }

  /**
   * Returns the schema-level hooks configuration.
   */
  public SchemaHooksConfig getHooks() {
    return hooks;
  }

  /**
   * Returns additional metadata from the schema definition.
   */
  public Map<String, Object> getMetadata() {
    return metadata;
  }

  /**
   * Gets a table configuration by name.
   *
   * @param tableName Table name to find
   * @return Table configuration, or null if not found
   */
  public EtlPipelineConfig getTable(String tableName) {
    for (EtlPipelineConfig table : tables) {
      if (table.getName().equals(tableName)) {
        return table;
      }
    }
    return null;
  }

  /**
   * Returns the number of tables in this schema.
   */
  public int getTableCount() {
    return tables.size();
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a SchemaConfig from a YAML/JSON map.
   *
   * @param map Configuration map with 'name', 'tables', and optional 'hooks'
   * @return SchemaConfig instance
   */
  @SuppressWarnings("unchecked")
  public static SchemaConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    Builder builder = builder();

    // Check for both "name" (standard) and "schemaName" (file adapter format)
    Object nameObj = map.get("name");
    if (nameObj == null) {
      nameObj = map.get("schemaName");
    }
    if (nameObj instanceof String) {
      builder.name(VariableResolver.resolveEnvVars((String) nameObj));
    }

    Object sourceDirObj = map.get("sourceDirectory");
    if (sourceDirObj instanceof String) {
      builder.sourceDirectory(VariableResolver.resolveEnvVars((String) sourceDirObj));
    }

    Object materializeDirObj = map.get("materializeDirectory");
    if (materializeDirObj instanceof String) {
      builder.materializeDirectory(VariableResolver.resolveEnvVars((String) materializeDirObj));
    }

    // Parse bulk downloads
    Object bulkDownloadsObj = map.get("bulkDownloads");
    if (bulkDownloadsObj instanceof Map) {
      Map<String, Object> bulkDownloadsMap = (Map<String, Object>) bulkDownloadsObj;
      for (Map.Entry<String, Object> entry : bulkDownloadsMap.entrySet()) {
        if (entry.getValue() instanceof Map) {
          BulkDownloadConfig config = BulkDownloadConfig.fromMap(
              entry.getKey(), (Map<String, Object>) entry.getValue());
          builder.bulkDownload(entry.getKey(), config);
        }
      }
    }

    // Check for partitionedTables first (file adapter format), then tables (standard format)
    Object tablesObj = map.get("partitionedTables");
    if (tablesObj == null) {
      tablesObj = map.get("tables");
    }
    if (tablesObj instanceof List) {
      List<EtlPipelineConfig> tables = new ArrayList<EtlPipelineConfig>();
      for (Object tableObj : (List<?>) tablesObj) {
        if (tableObj instanceof Map) {
          Map<String, Object> tableMap = (Map<String, Object>) tableObj;
          // Skip view definitions - they don't have source/materialize and aren't ETL pipelines
          Object typeObj = tableMap.get("type");
          if (typeObj instanceof String && "view".equals(typeObj)) {
            continue;
          }
          // Skip tables without source configuration (metadata-only tables)
          if (tableMap.get("source") == null) {
            continue;
          }
          EtlPipelineConfig tableConfig = EtlPipelineConfig.fromMap(tableMap);
          if (tableConfig != null) {
            tables.add(tableConfig);
          }
        }
      }
      builder.tables(tables);
    }

    Object hooksObj = map.get("hooks");
    if (hooksObj instanceof Map) {
      builder.hooks(SchemaHooksConfig.fromMap((Map<String, Object>) hooksObj));
    }

    // Store remaining keys as metadata
    builder.metadata(map);

    return builder.build();
  }

  /**
   * Builder for SchemaConfig.
   */
  public static class Builder {
    private String name;
    private String sourceDirectory;
    private String materializeDirectory;
    private Map<String, BulkDownloadConfig> bulkDownloads;
    private List<EtlPipelineConfig> tables;
    private SchemaHooksConfig hooks;
    private Map<String, Object> metadata;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder sourceDirectory(String sourceDirectory) {
      this.sourceDirectory = sourceDirectory;
      return this;
    }

    public Builder materializeDirectory(String materializeDirectory) {
      this.materializeDirectory = materializeDirectory;
      return this;
    }

    public Builder bulkDownloads(Map<String, BulkDownloadConfig> bulkDownloads) {
      this.bulkDownloads = bulkDownloads;
      return this;
    }

    public Builder bulkDownload(String name, BulkDownloadConfig config) {
      if (this.bulkDownloads == null) {
        this.bulkDownloads = new LinkedHashMap<>();
      }
      this.bulkDownloads.put(name, config);
      return this;
    }

    public Builder tables(List<EtlPipelineConfig> tables) {
      this.tables = tables;
      return this;
    }

    public Builder addTable(EtlPipelineConfig table) {
      if (this.tables == null) {
        this.tables = new ArrayList<EtlPipelineConfig>();
      }
      this.tables.add(table);
      return this;
    }

    public Builder hooks(SchemaHooksConfig hooks) {
      this.hooks = hooks;
      return this;
    }

    public Builder metadata(Map<String, Object> metadata) {
      this.metadata = metadata;
      return this;
    }

    public SchemaConfig build() {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Schema name is required");
      }
      return new SchemaConfig(this);
    }
  }

  /**
   * Schema-level hooks configuration.
   */
  public static class SchemaHooksConfig {
    private final String schemaLifecycleListenerClass;
    private final String tableLifecycleListenerClass;

    private SchemaHooksConfig(String schemaLifecycleListenerClass,
        String tableLifecycleListenerClass) {
      this.schemaLifecycleListenerClass = schemaLifecycleListenerClass;
      this.tableLifecycleListenerClass = tableLifecycleListenerClass;
    }

    public String getSchemaLifecycleListenerClass() {
      return schemaLifecycleListenerClass;
    }

    public String getTableLifecycleListenerClass() {
      return tableLifecycleListenerClass;
    }

    public static SchemaHooksConfig empty() {
      return new SchemaHooksConfig(null, null);
    }

    @SuppressWarnings("unchecked")
    public static SchemaHooksConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return empty();
      }

      String schemaListener = null;
      String tableListener = null;

      Object schemaObj = map.get("schemaLifecycleListener");
      if (schemaObj instanceof String) {
        schemaListener = (String) schemaObj;
      }

      Object tableObj = map.get("tableLifecycleListener");
      if (tableObj instanceof String) {
        tableListener = (String) tableObj;
      }

      return new SchemaHooksConfig(schemaListener, tableListener);
    }
  }
}
