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

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import java.util.HashMap;
import java.util.Map;

/**
 * Context for table-level lifecycle processing.
 *
 * <p>Provides access to table configuration, schema context, and
 * a mutable attributes map for passing state between lifecycle phases.
 *
 * @see TableLifecycleListener
 * @see SchemaLifecycleProcessor
 */
public class TableContext {

  private final EtlPipelineConfig tableConfig;
  private final SchemaContext schemaContext;
  private final int tableIndex;
  private final int totalTables;
  private final Map<String, Object> attributes;

  private TableContext(Builder builder) {
    this.tableConfig = builder.tableConfig;
    this.schemaContext = builder.schemaContext;
    this.tableIndex = builder.tableIndex;
    this.totalTables = builder.totalTables;
    this.attributes = new HashMap<String, Object>();
  }

  /**
   * Returns the table configuration.
   */
  public EtlPipelineConfig getTableConfig() {
    return tableConfig;
  }

  /**
   * Returns the table name.
   */
  public String getTableName() {
    return tableConfig.getName();
  }

  /**
   * Returns the source configuration for this table.
   */
  public HttpSourceConfig getSourceConfig() {
    return tableConfig.getSource();
  }

  /**
   * Returns the source URL pattern from the table config.
   */
  public String getSourceUrl() {
    HttpSourceConfig source = tableConfig.getSource();
    return source != null ? source.getUrl() : null;
  }

  /**
   * Returns the hooks configuration for this table.
   */
  public HooksConfig getHooksConfig() {
    return tableConfig.getHooks();
  }

  /**
   * Returns the materialize configuration for this table.
   */
  public MaterializeConfig getMaterializeConfig() {
    return tableConfig.getMaterialize();
  }

  /**
   * Returns the dimension configurations from the table config.
   */
  public Map<String, DimensionConfig> getDimensions() {
    return tableConfig.getDimensions();
  }

  /**
   * Detects the data source based on URL patterns or table name.
   *
   * @return Source identifier (e.g., "bls", "fred", "bea", "treasury", "worldbank")
   */
  public String detectSource() {
    String url = getSourceUrl();
    String tableName = getTableName();

    // Detect from URL
    if (url != null) {
      if (url.contains("bls.gov")) {
        return "bls";
      }
      if (url.contains("stlouisfed.org") || url.contains("fred")) {
        return "fred";
      }
      if (url.contains("bea.gov")) {
        return "bea";
      }
      if (url.contains("treasury.gov")) {
        return "treasury";
      }
      if (url.contains("worldbank.org")) {
        return "worldbank";
      }
    }

    // Detect from table name
    if (tableName != null) {
      if (tableName.startsWith("bls_") || tableName.contains("employment") ||
          tableName.contains("inflation") || tableName.contains("wage")) {
        return "bls";
      }
      if (tableName.startsWith("fred_") || tableName.equals("economic_indicators")) {
        return "fred";
      }
      if (tableName.startsWith("bea_") || tableName.contains("gdp") ||
          tableName.contains("regional_income")) {
        return "bea";
      }
      if (tableName.startsWith("treasury_")) {
        return "treasury";
      }
      if (tableName.startsWith("world_")) {
        return "worldbank";
      }
    }

    return "unknown";
  }

  /**
   * Returns the required API key name for this table's source.
   *
   * @return API key environment variable name, or null if no key required
   */
  public String getRequiredApiKeyName() {
    String source = detectSource();
    switch (source) {
      case "bls":
        return "BLS_API_KEY";
      case "fred":
        return "FRED_API_KEY";
      case "bea":
        return "BEA_API_KEY";
      default:
        return null; // No API key required
    }
  }

  /**
   * Returns the parent schema context.
   */
  public SchemaContext getSchemaContext() {
    return schemaContext;
  }

  // --- Bulk Download Support ---

  /**
   * Returns true if this table's source references a bulk download.
   */
  public boolean isBulkDownloadSource() {
    HttpSourceConfig source = tableConfig.getSource();
    return source != null && source.isBulkDownloadSource();
  }

  /**
   * Returns the bulk download name from this table's source config.
   *
   * @return Bulk download name, or null if not using bulk download
   */
  public String getBulkDownloadName() {
    HttpSourceConfig source = tableConfig.getSource();
    return source != null ? source.getBulkDownload() : null;
  }

  /**
   * Gets the cached file path for this table's bulk download source.
   *
   * <p>Use this in fetchData hooks to read from the cached bulk file
   * instead of making HTTP requests.
   *
   * @param variables Dimension variable values
   * @return Cached file path, or null if not found or not using bulk download
   */
  public String getBulkDownloadPath(Map<String, String> variables) {
    String bulkDownloadName = getBulkDownloadName();
    if (bulkDownloadName == null) {
      return null;
    }

    // Create variable key (same format used in SchemaLifecycleProcessor)
    String variableKey = createVariableKey(variables);
    return schemaContext.getBulkDownloadPath(bulkDownloadName, variableKey);
  }

  /**
   * Creates a variable key string from dimension values.
   */
  private String createVariableKey(Map<String, String> variables) {
    if (variables == null || variables.isEmpty()) {
      return "default";
    }
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(entry.getKey()).append("=").append(entry.getValue());
    }
    return sb.toString();
  }

  /**
   * Returns the schema name.
   */
  public String getSchemaName() {
    return schemaContext.getSchemaName();
  }

  /**
   * Returns the storage provider.
   */
  public StorageProvider getStorageProvider() {
    return schemaContext.getStorageProvider();
  }

  /**
   * Returns the incremental tracker.
   */
  public IncrementalTracker getIncrementalTracker() {
    return schemaContext.getIncrementalTracker();
  }

  /**
   * Returns the source directory for file-based sources.
   */
  public String getSourceDirectory() {
    return schemaContext.getSourceDirectory();
  }

  /**
   * Returns the materialize directory for output files.
   */
  public String getMaterializeDirectory() {
    return schemaContext.getMaterializeDirectory();
  }

  /**
   * Returns the base directory for output.
   * @deprecated Use {@link #getMaterializeDirectory()} instead
   */
  @Deprecated
  public String getBaseDirectory() {
    return schemaContext.getBaseDirectory();
  }

  /**
   * Returns the 0-based index of this table in the schema.
   */
  public int getTableIndex() {
    return tableIndex;
  }

  /**
   * Returns the total number of tables in the schema.
   */
  public int getTotalTables() {
    return totalTables;
  }

  /**
   * Returns mutable attributes map for sharing state between phases.
   *
   * <p>Table attributes are separate from schema attributes.
   * Use schemaContext.getAttributes() for schema-level state.
   */
  public Map<String, Object> getAttributes() {
    return attributes;
  }

  /**
   * Gets an attribute value from table attributes.
   */
  @SuppressWarnings("unchecked")
  public <T> T getAttribute(String key) {
    return (T) attributes.get(key);
  }

  /**
   * Sets an attribute value in table attributes.
   */
  public void setAttribute(String key, Object value) {
    attributes.put(key, value);
  }

  /**
   * Gets an attribute from schema context.
   */
  @SuppressWarnings("unchecked")
  public <T> T getSchemaAttribute(String key) {
    return schemaContext.getAttribute(key);
  }

  /**
   * Creates a new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for TableContext.
   */
  public static class Builder {
    private EtlPipelineConfig tableConfig;
    private SchemaContext schemaContext;
    private int tableIndex;
    private int totalTables;

    public Builder tableConfig(EtlPipelineConfig tableConfig) {
      this.tableConfig = tableConfig;
      return this;
    }

    public Builder schemaContext(SchemaContext schemaContext) {
      this.schemaContext = schemaContext;
      return this;
    }

    public Builder tableIndex(int tableIndex) {
      this.tableIndex = tableIndex;
      return this;
    }

    public Builder totalTables(int totalTables) {
      this.totalTables = totalTables;
      return this;
    }

    public TableContext build() {
      if (tableConfig == null) {
        throw new IllegalArgumentException("Table config is required");
      }
      if (schemaContext == null) {
        throw new IllegalArgumentException("Schema context is required");
      }
      return new TableContext(this);
    }
  }
}
