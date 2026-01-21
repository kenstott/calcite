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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for data materialization to Iceberg or hive-partitioned Parquet files.
 *
 * <p>This is the main configuration class for materialization. It supports two formats:
 * <ul>
 *   <li>{@code iceberg} (default) - Uses {@code IcebergMaterializer} for Iceberg tables</li>
 *   <li>{@code parquet} - Uses {@code HiveParquetWriter} for hive-partitioned Parquet</li>
 * </ul>
 *
 * <h3>YAML Configuration Example</h3>
 * <pre>
 * materialize:
 *   enabled: true
 *   format: iceberg          # or parquet (default: iceberg)
 *   targetTableId: my_table  # Iceberg table identifier (required for iceberg format)
 *   trigger: auto            # or manual, onFirstQuery
 *   output:
 *     location: "s3://bucket/data/"
 *     pattern: "type=sales/year=STAR/region=STAR/"
 *     compression: snappy
 *   partition:
 *     columns: [year, region]
 *     batchBy: [year]
 *   columns:
 *     - name: region_code, type: VARCHAR, source: regionCode
 *     - name: year, type: INTEGER, source: fiscalYear
 *     - name: quarter, type: VARCHAR, expression: "SUBSTR(period, 1, 2)"
 *   options:
 *     threads: 4
 *     rowGroupSize: 100000
 * </pre>
 *
 * <h3>Trigger Types</h3>
 * <ul>
 *   <li>{@code auto} - Materialize when schema loads</li>
 *   <li>{@code manual} - Only via explicit API call</li>
 *   <li>{@code onFirstQuery} - Materialize on first table access</li>
 * </ul>
 *
 * @see MaterializeOutputConfig
 * @see MaterializePartitionConfig
 * @see MaterializeOptionsConfig
 * @see ColumnConfig
 */
public class MaterializeConfig {

  /**
   * Output format for materialization.
   *
   * <p>Currently implemented:
   * <ul>
   *   <li>{@link #ICEBERG} - Iceberg tables with atomic commits</li>
   *   <li>{@link #PARQUET} - Hive-partitioned Parquet files</li>
   * </ul>
   *
   * <p>Planned for future implementation:
   * <ul>
   *   <li>{@link #DELTA} - Delta Lake tables</li>
   *   <li>{@link #SNOWFLAKE} - Snowflake tables via staging + COPY INTO</li>
   *   <li>{@link #BIGQUERY} - BigQuery tables via GCS staging + load</li>
   *   <li>{@link #DATABRICKS} - Databricks Unity Catalog tables</li>
   * </ul>
   */
  public enum Format {
    /** Iceberg table format (default). Uses DuckDB for transform + Iceberg API for atomic commits. */
    ICEBERG,
    /** Hive-partitioned Parquet format. Uses DuckDB COPY with PARTITION_BY. */
    PARQUET,
    /** Delta Lake format. Stage to Parquet then commit via delta-standalone. (Not yet implemented) */
    DELTA,
    /** Snowflake format. Stage to Parquet, upload to stage, COPY INTO. (Not yet implemented) */
    SNOWFLAKE,
    /** BigQuery format. Stage to Parquet, upload to GCS, bq load. (Not yet implemented) */
    BIGQUERY,
    /** Databricks Unity Catalog format. Stage to cloud storage then register. (Not yet implemented) */
    DATABRICKS
  }

  /**
   * Trigger types for materialization.
   */
  public enum Trigger {
    /** Materialize when schema loads. */
    AUTO,
    /** Only materialize via explicit API call. */
    MANUAL,
    /** Materialize on first table access. */
    ON_FIRST_QUERY
  }

  private final boolean enabled;
  private final Format format;
  private final String targetTableId;
  private final Trigger trigger;
  private final MaterializeOutputConfig output;
  private final MaterializePartitionConfig partition;
  private final List<ColumnConfig> columns;
  private final MaterializeOptionsConfig options;
  private final String name;
  private final IcebergConfig iceberg;
  private final String tableComment;
  private final Map<String, String> columnComments;

  private MaterializeConfig(Builder builder) {
    this.enabled = builder.enabled != null ? builder.enabled : true;
    this.format = builder.format != null ? builder.format : Format.ICEBERG;
    this.targetTableId = builder.targetTableId;
    this.trigger = builder.trigger != null ? builder.trigger : Trigger.AUTO;
    this.output = builder.output;
    this.partition = builder.partition;
    this.columns = builder.columns != null
        ? Collections.unmodifiableList(new ArrayList<ColumnConfig>(builder.columns))
        : Collections.<ColumnConfig>emptyList();
    this.options = builder.options != null ? builder.options : MaterializeOptionsConfig.defaults();
    this.name = builder.name;
    this.iceberg = builder.iceberg;
    this.tableComment = builder.tableComment;
    this.columnComments = builder.columnComments != null
        ? Collections.unmodifiableMap(new HashMap<String, String>(builder.columnComments))
        : Collections.<String, String>emptyMap();
  }

  /**
   * Returns whether materialization is enabled.
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Returns the output format for materialization.
   * Defaults to ICEBERG if not specified.
   */
  public Format getFormat() {
    return format;
  }

  /**
   * Returns the Iceberg table identifier.
   * Required when format is ICEBERG.
   */
  public String getTargetTableId() {
    return targetTableId;
  }

  /**
   * Returns the trigger type for materialization.
   */
  public Trigger getTrigger() {
    return trigger;
  }

  /**
   * Returns the output configuration.
   */
  public MaterializeOutputConfig getOutput() {
    return output;
  }

  /**
   * Returns the partition configuration.
   */
  public MaterializePartitionConfig getPartition() {
    return partition;
  }

  /**
   * Returns the column configurations.
   */
  public List<ColumnConfig> getColumns() {
    return columns;
  }

  /**
   * Returns the processing options.
   */
  public MaterializeOptionsConfig getOptions() {
    return options;
  }

  /**
   * Returns the name identifier for this materialization config.
   * Used for logging and tracking.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the Iceberg-specific configuration.
   * Only applicable when format is ICEBERG.
   */
  public IcebergConfig getIceberg() {
    return iceberg;
  }

  /**
   * Returns the table-level comment/description.
   * Used for JDBC metadata REMARKS column in getTables().
   */
  public String getTableComment() {
    return tableComment;
  }

  /**
   * Returns the column comments/descriptions.
   * Used for JDBC metadata REMARKS column in getColumns().
   *
   * @return Map of column name to comment, never null
   */
  public Map<String, String> getColumnComments() {
    return columnComments;
  }

  /**
   * Creates a new builder for MaterializeConfig.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a MaterializeConfig from a YAML/JSON map.
   *
   * @param map Configuration map
   * @return MaterializeConfig instance, or null if not enabled
   */
  @SuppressWarnings("unchecked")
  public static MaterializeConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    Builder builder = builder();

    // Parse enabled flag
    Object enabledObj = map.get("enabled");
    if (enabledObj instanceof Boolean) {
      builder.enabled((Boolean) enabledObj);
    }

    // Parse format (default: iceberg)
    Object formatObj = map.get("format");
    if (formatObj instanceof String) {
      builder.format(parseFormat((String) formatObj));
    }

    // Parse targetTableId (required for iceberg format)
    Object targetTableIdObj = map.get("targetTableId");
    if (targetTableIdObj instanceof String) {
      builder.targetTableId((String) targetTableIdObj);
    }

    // Parse trigger
    Object triggerObj = map.get("trigger");
    if (triggerObj instanceof String) {
      builder.trigger(parseTrigger((String) triggerObj));
    }

    // Parse name
    Object nameObj = map.get("name");
    if (nameObj instanceof String) {
      builder.name((String) nameObj);
    }

    // Parse nested configs
    Object outputObj = map.get("output");
    if (outputObj instanceof Map) {
      builder.output(MaterializeOutputConfig.fromMap((Map<String, Object>) outputObj));
    }

    Object partitionObj = map.get("partition");
    if (partitionObj instanceof Map) {
      builder.partition(MaterializePartitionConfig.fromMap((Map<String, Object>) partitionObj));
    }

    Object columnsObj = map.get("columns");
    if (columnsObj instanceof List) {
      builder.columns(ColumnConfig.fromList((List<?>) columnsObj));
    }

    Object optionsObj = map.get("options");
    if (optionsObj instanceof Map) {
      builder.options(MaterializeOptionsConfig.fromMap((Map<String, Object>) optionsObj));
    }

    Object icebergObj = map.get("iceberg");
    if (icebergObj instanceof Map) {
      builder.iceberg(IcebergConfig.fromMap((Map<String, Object>) icebergObj));
    }

    // Parse table comment
    Object tableCommentObj = map.get("tableComment");
    if (tableCommentObj instanceof String) {
      builder.tableComment((String) tableCommentObj);
    }

    // Parse column comments
    Object columnCommentsObj = map.get("columnComments");
    if (columnCommentsObj instanceof Map) {
      Map<String, String> comments = new HashMap<String, String>();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) columnCommentsObj).entrySet()) {
        if (entry.getKey() instanceof String && entry.getValue() instanceof String) {
          comments.put((String) entry.getKey(), (String) entry.getValue());
        }
      }
      builder.columnComments(comments);
    }

    return builder.build();
  }

  private static Format parseFormat(String value) {
    if (value == null) {
      return Format.ICEBERG;
    }
    switch (value.toLowerCase()) {
      case "iceberg":
        return Format.ICEBERG;
      case "parquet":
        return Format.PARQUET;
      case "delta":
        return Format.DELTA;
      case "snowflake":
        return Format.SNOWFLAKE;
      case "bigquery":
        return Format.BIGQUERY;
      case "databricks":
        return Format.DATABRICKS;
      default:
        return Format.ICEBERG;
    }
  }

  private static Trigger parseTrigger(String value) {
    if (value == null) {
      return Trigger.AUTO;
    }
    switch (value.toLowerCase()) {
      case "auto":
        return Trigger.AUTO;
      case "manual":
        return Trigger.MANUAL;
      case "onfirstquery":
      case "on_first_query":
        return Trigger.ON_FIRST_QUERY;
      default:
        return Trigger.AUTO;
    }
  }

  /**
   * Builder for MaterializeConfig.
   */
  public static class Builder {
    private Boolean enabled;
    private Format format;
    private String targetTableId;
    private Trigger trigger;
    private MaterializeOutputConfig output;
    private MaterializePartitionConfig partition;
    private List<ColumnConfig> columns;
    private MaterializeOptionsConfig options;
    private String name;
    private IcebergConfig iceberg;
    private String tableComment;
    private Map<String, String> columnComments;

    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder format(Format format) {
      this.format = format;
      return this;
    }

    public Builder targetTableId(String targetTableId) {
      this.targetTableId = targetTableId;
      return this;
    }

    public Builder trigger(Trigger trigger) {
      this.trigger = trigger;
      return this;
    }

    public Builder output(MaterializeOutputConfig output) {
      this.output = output;
      return this;
    }

    public Builder partition(MaterializePartitionConfig partition) {
      this.partition = partition;
      return this;
    }

    public Builder columns(List<ColumnConfig> columns) {
      this.columns = columns;
      return this;
    }

    public Builder options(MaterializeOptionsConfig options) {
      this.options = options;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder iceberg(IcebergConfig iceberg) {
      this.iceberg = iceberg;
      return this;
    }

    public Builder tableComment(String tableComment) {
      this.tableComment = tableComment;
      return this;
    }

    public Builder columnComments(Map<String, String> columnComments) {
      this.columnComments = columnComments;
      return this;
    }

    public MaterializeConfig build() {
      if (output == null) {
        throw new IllegalArgumentException("Output configuration is required");
      }
      return new MaterializeConfig(this);
    }
  }

  /**
   * Configuration specific to Iceberg format materialization.
   *
   * <h3>YAML Configuration Example</h3>
   * <pre>
   * materialize:
   *   format: iceberg
   *   iceberg:
   *     catalogType: hadoop        # hadoop, rest, or hive
   *     warehousePath: "/data/warehouse"
   *     namespace: "default"
   *     restUri: "http://localhost:8181"  # for REST catalog
   *     batchPartitionColumns: [year]     # columns for OOM prevention batching
   *     incrementalKeys: [year]           # columns for incremental processing
   *     maxRetries: 3
   *     retryDelayMs: 1000
   * </pre>
   */
  public static class IcebergConfig {

    /**
     * Iceberg catalog types.
     */
    public enum CatalogType {
      /** Hadoop filesystem-based catalog. */
      HADOOP,
      /** REST catalog (e.g., Nessie, Tabular). */
      REST,
      /** Hive metastore catalog. */
      HIVE
    }

    private final CatalogType catalogType;
    private final String warehousePath;
    private final String namespace;
    private final String restUri;
    private final List<String> batchPartitionColumns;
    private final List<String> incrementalKeys;
    private final int maxRetries;
    private final long retryDelayMs;
    private final boolean runMaintenance;
    private final int snapshotRetentionDays;
    private final boolean runCompaction;
    private final long compactionTargetFileSizeBytes;
    private final int compactionMinFiles;
    private final long compactionSmallFileSizeBytes;

    private IcebergConfig(IcebergConfigBuilder builder) {
      this.catalogType = builder.catalogType != null ? builder.catalogType : CatalogType.HADOOP;
      this.warehousePath = builder.warehousePath;
      this.namespace = builder.namespace != null ? builder.namespace : "default";
      this.restUri = builder.restUri;
      this.batchPartitionColumns = builder.batchPartitionColumns != null
          ? Collections.unmodifiableList(new ArrayList<String>(builder.batchPartitionColumns))
          : Collections.<String>emptyList();
      this.incrementalKeys = builder.incrementalKeys != null
          ? Collections.unmodifiableList(new ArrayList<String>(builder.incrementalKeys))
          : Collections.<String>emptyList();
      this.maxRetries = builder.maxRetries > 0 ? builder.maxRetries : 3;
      this.retryDelayMs = builder.retryDelayMs > 0 ? builder.retryDelayMs : 1000;
      this.runMaintenance = builder.runMaintenance;
      this.snapshotRetentionDays = builder.snapshotRetentionDays > 0
          ? builder.snapshotRetentionDays : 7;
      this.runCompaction = builder.runCompaction;
      this.compactionTargetFileSizeBytes = builder.compactionTargetFileSizeBytes > 0
          ? builder.compactionTargetFileSizeBytes : 128L * 1024 * 1024; // 128MB default
      this.compactionMinFiles = builder.compactionMinFiles > 0
          ? builder.compactionMinFiles : 10;
      this.compactionSmallFileSizeBytes = builder.compactionSmallFileSizeBytes > 0
          ? builder.compactionSmallFileSizeBytes : 10L * 1024 * 1024; // 10MB default
    }

    public CatalogType getCatalogType() {
      return catalogType;
    }

    public String getWarehousePath() {
      return warehousePath;
    }

    public String getNamespace() {
      return namespace;
    }

    public String getRestUri() {
      return restUri;
    }

    public List<String> getBatchPartitionColumns() {
      return batchPartitionColumns;
    }

    public List<String> getIncrementalKeys() {
      return incrementalKeys;
    }

    public int getMaxRetries() {
      return maxRetries;
    }

    public long getRetryDelayMs() {
      return retryDelayMs;
    }

    public boolean isRunMaintenance() {
      return runMaintenance;
    }

    public int getSnapshotRetentionDays() {
      return snapshotRetentionDays;
    }

    public boolean isRunCompaction() {
      return runCompaction;
    }

    public long getCompactionTargetFileSizeBytes() {
      return compactionTargetFileSizeBytes;
    }

    public int getCompactionMinFiles() {
      return compactionMinFiles;
    }

    public long getCompactionSmallFileSizeBytes() {
      return compactionSmallFileSizeBytes;
    }

    public static IcebergConfigBuilder builder() {
      return new IcebergConfigBuilder();
    }

    @SuppressWarnings("unchecked")
    public static IcebergConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return null;
      }

      IcebergConfigBuilder builder = builder();

      Object catalogTypeObj = map.get("catalogType");
      if (catalogTypeObj instanceof String) {
        builder.catalogType(parseCatalogType((String) catalogTypeObj));
      }

      Object warehouseObj = map.get("warehousePath");
      if (warehouseObj instanceof String) {
        builder.warehousePath((String) warehouseObj);
      }

      Object namespaceObj = map.get("namespace");
      if (namespaceObj instanceof String) {
        builder.namespace((String) namespaceObj);
      }

      Object restUriObj = map.get("restUri");
      if (restUriObj instanceof String) {
        builder.restUri((String) restUriObj);
      }

      Object batchColsObj = map.get("batchPartitionColumns");
      if (batchColsObj instanceof List) {
        List<String> cols = new ArrayList<String>();
        for (Object item : (List<?>) batchColsObj) {
          if (item instanceof String) {
            cols.add((String) item);
          }
        }
        builder.batchPartitionColumns(cols);
      }

      Object incrementalObj = map.get("incrementalKeys");
      if (incrementalObj instanceof List) {
        List<String> keys = new ArrayList<String>();
        for (Object item : (List<?>) incrementalObj) {
          if (item instanceof String) {
            keys.add((String) item);
          }
        }
        builder.incrementalKeys(keys);
      }

      Object retriesObj = map.get("maxRetries");
      if (retriesObj instanceof Number) {
        builder.maxRetries(((Number) retriesObj).intValue());
      }

      Object delayObj = map.get("retryDelayMs");
      if (delayObj instanceof Number) {
        builder.retryDelayMs(((Number) delayObj).longValue());
      }

      Object maintenanceObj = map.get("runMaintenance");
      if (maintenanceObj instanceof Boolean) {
        builder.runMaintenance((Boolean) maintenanceObj);
      }

      Object retentionObj = map.get("snapshotRetentionDays");
      if (retentionObj instanceof Number) {
        builder.snapshotRetentionDays(((Number) retentionObj).intValue());
      }

      Object compactionObj = map.get("runCompaction");
      if (compactionObj instanceof Boolean) {
        builder.runCompaction((Boolean) compactionObj);
      }

      Object targetSizeObj = map.get("compactionTargetFileSizeBytes");
      if (targetSizeObj instanceof Number) {
        builder.compactionTargetFileSizeBytes(((Number) targetSizeObj).longValue());
      }

      Object minFilesObj = map.get("compactionMinFiles");
      if (minFilesObj instanceof Number) {
        builder.compactionMinFiles(((Number) minFilesObj).intValue());
      }

      Object smallSizeObj = map.get("compactionSmallFileSizeBytes");
      if (smallSizeObj instanceof Number) {
        builder.compactionSmallFileSizeBytes(((Number) smallSizeObj).longValue());
      }

      return builder.build();
    }

    private static CatalogType parseCatalogType(String value) {
      if (value == null) {
        return CatalogType.HADOOP;
      }
      switch (value.toLowerCase()) {
        case "rest":
          return CatalogType.REST;
        case "hive":
          return CatalogType.HIVE;
        case "hadoop":
        default:
          return CatalogType.HADOOP;
      }
    }

    /**
     * Builder for IcebergConfig.
     */
    public static class IcebergConfigBuilder {
      private CatalogType catalogType;
      private String warehousePath;
      private String namespace;
      private String restUri;
      private List<String> batchPartitionColumns;
      private List<String> incrementalKeys;
      private int maxRetries;
      private long retryDelayMs;
      private boolean runMaintenance;
      private int snapshotRetentionDays;
      private boolean runCompaction;
      private long compactionTargetFileSizeBytes;
      private int compactionMinFiles;
      private long compactionSmallFileSizeBytes;

      public IcebergConfigBuilder catalogType(CatalogType catalogType) {
        this.catalogType = catalogType;
        return this;
      }

      public IcebergConfigBuilder warehousePath(String warehousePath) {
        this.warehousePath = warehousePath;
        return this;
      }

      public IcebergConfigBuilder namespace(String namespace) {
        this.namespace = namespace;
        return this;
      }

      public IcebergConfigBuilder restUri(String restUri) {
        this.restUri = restUri;
        return this;
      }

      public IcebergConfigBuilder batchPartitionColumns(List<String> batchPartitionColumns) {
        this.batchPartitionColumns = batchPartitionColumns;
        return this;
      }

      public IcebergConfigBuilder incrementalKeys(List<String> incrementalKeys) {
        this.incrementalKeys = incrementalKeys;
        return this;
      }

      public IcebergConfigBuilder maxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
      }

      public IcebergConfigBuilder retryDelayMs(long retryDelayMs) {
        this.retryDelayMs = retryDelayMs;
        return this;
      }

      public IcebergConfigBuilder runMaintenance(boolean runMaintenance) {
        this.runMaintenance = runMaintenance;
        return this;
      }

      public IcebergConfigBuilder snapshotRetentionDays(int snapshotRetentionDays) {
        this.snapshotRetentionDays = snapshotRetentionDays;
        return this;
      }

      public IcebergConfigBuilder runCompaction(boolean runCompaction) {
        this.runCompaction = runCompaction;
        return this;
      }

      public IcebergConfigBuilder compactionTargetFileSizeBytes(long compactionTargetFileSizeBytes) {
        this.compactionTargetFileSizeBytes = compactionTargetFileSizeBytes;
        return this;
      }

      public IcebergConfigBuilder compactionMinFiles(int compactionMinFiles) {
        this.compactionMinFiles = compactionMinFiles;
        return this;
      }

      public IcebergConfigBuilder compactionSmallFileSizeBytes(long compactionSmallFileSizeBytes) {
        this.compactionSmallFileSizeBytes = compactionSmallFileSizeBytes;
        return this;
      }

      public IcebergConfig build() {
        return new IcebergConfig(this);
      }
    }
  }
}
