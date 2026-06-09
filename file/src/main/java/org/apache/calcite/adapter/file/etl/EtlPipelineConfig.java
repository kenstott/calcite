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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for a complete ETL pipeline.
 *
 * <p>EtlPipelineConfig combines source, dimension, column, and materialization
 * configurations into a single declarative definition for transforming data
 * from HTTP APIs to queryable hive-partitioned Parquet tables.
 *
 * <h3>YAML Configuration Example</h3>
 * <pre>{@code
 * tables:
 *   - name: sales_by_region
 *
 *     source:
 *       type: http
 *       url: "https://api.example.com/v1/sales"
 *       parameters:
 *         year: "{year}"
 *         region: "{region}"
 *
 *     dimensions:
 *       year:
 *         type: range
 *         start: 2020
 *         end: 2024
 *       region:
 *         type: list
 *         values: [NORTH, SOUTH, EAST, WEST]
 *
 *     columns:
 *       - name: region_code, type: VARCHAR, source: regionCode
 *       - name: year, type: INTEGER, source: fiscalYear
 *       - name: quarter, type: VARCHAR, expression: "SUBSTR(period, 1, 2)"
 *
 *     materialize:
 *       enabled: true
 *       output:
 *         pattern: "type=sales/year=STAR/region=STAR/"
 *       partition:
 *         columns: [year, region]
 *         batchBy: [year, region]
 * }</pre>
 *
 * @see EtlPipeline
 * @see HttpSourceConfig
 * @see DimensionConfig
 * @see MaterializeConfig
 */
public class EtlPipelineConfig {

  /** Source type identifier for HTTP/REST API sources. */
  public static final String SOURCE_TYPE_HTTP = "http";

  /** Source type identifier for file-based sources (xlsx, csv, json, parquet). */
  public static final String SOURCE_TYPE_FILE = "file";

  /** Source type identifier for YAML constants sources. */
  public static final String SOURCE_TYPE_CONSTANTS = "constants";

  /** Source type identifier for document-based sources (XBRL, HTML filings, etc.). */
  public static final String SOURCE_TYPE_DOCUMENT = "document";

  private final String name;
  private final boolean enabled;
  private final String sourceType;
  private final HttpSourceConfig source;
  private final Map<String, Object> rawSourceConfig;
  private final Map<String, DimensionConfig> dimensions;
  private final List<ColumnConfig> columns;
  private final MaterializeConfig materialize;
  private final ErrorHandlingConfig errorHandling;
  private final HooksConfig hooks;
  private final FreshnessConfig freshness;
  private final String datasetType;
  private final String backfillPeriod;
  private final int dqRowLimit;

  private EtlPipelineConfig(Builder builder) {
    this.name = builder.name;
    this.enabled = builder.enabled;
    this.sourceType = builder.sourceType != null ? builder.sourceType : SOURCE_TYPE_HTTP;
    this.source = builder.source;
    this.rawSourceConfig = builder.rawSourceConfig;
    this.dimensions = builder.dimensions != null
        ? Collections.unmodifiableMap(new LinkedHashMap<String, DimensionConfig>(builder.dimensions))
        : Collections.<String, DimensionConfig>emptyMap();
    this.columns = builder.columns != null
        ? Collections.unmodifiableList(builder.columns)
        : Collections.<ColumnConfig>emptyList();
    this.materialize = builder.materialize;
    this.errorHandling = builder.errorHandling != null
        ? builder.errorHandling
        : ErrorHandlingConfig.defaults();
    this.hooks = builder.hooks != null
        ? builder.hooks
        : HooksConfig.empty();
    this.freshness = builder.freshness;
    this.datasetType = builder.datasetType != null ? builder.datasetType : "delta";
    this.backfillPeriod = builder.backfillPeriod;
    this.dqRowLimit = builder.dqRowLimit;
  }

  /**
   * Returns the pipeline/table name.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns whether this table is enabled for ETL processing.
   *
   * <p>When false, the table is skipped during ETL pipeline execution.
   *
   * @return true if enabled (default), false to skip
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Returns the source type identifier ("http" or "constants").
   */
  public String getSourceType() {
    return sourceType;
  }

  /**
   * Returns the HTTP data source configuration.
   * Only valid when sourceType is "http".
   */
  public HttpSourceConfig getSource() {
    return source;
  }

  /**
   * Returns the raw source configuration map.
   * Used for non-HTTP source types like "constants".
   */
  public Map<String, Object> getRawSourceConfig() {
    return rawSourceConfig;
  }

  /**
   * Returns the dimension configurations.
   */
  public Map<String, DimensionConfig> getDimensions() {
    return dimensions;
  }

  /**
   * Returns the column configurations.
   */
  public List<ColumnConfig> getColumns() {
    return columns;
  }

  /**
   * Returns the materialization configuration.
   */
  public MaterializeConfig getMaterialize() {
    return materialize;
  }

  /**
   * Returns the error handling configuration.
   */
  public ErrorHandlingConfig getErrorHandling() {
    return errorHandling;
  }

  /**
   * Returns the hooks configuration for extensibility.
   */
  public HooksConfig getHooks() {
    return hooks;
  }

  /** Optional freshness check (snapshot/computed_delta); null = off. */
  public FreshnessConfig getFreshness() {
    return freshness;
  }

  /** Dataset type: {@code snapshot | delta | computed_delta}. Defaults to {@code delta}. */
  public String getDatasetType() {
    return datasetType;
  }

  /** Delta backfill window granularity ({@code annual|quarterly|weekly|daily}), or null. */
  public String getBackfillPeriod() {
    return backfillPeriod;
  }

  /**
   * DQ sample cap: maximum rows to materialise <b>per fetch-unit (per period)</b>, applied only
   * when DQ sample mode is active ({@code GOVDATA_DQ=true}); {@code <= 0} means uncapped.
   *
   * <p>The cap wraps the per-combination source iterator, so each period (e.g. each program
   * year) yields at most this many rows — a fast, bounded sample that still spans every period
   * (so year-over-year/format changes are exercised). It never truncates the raw cache: caching
   * sources write their full body during fetch before the iterator runs, and {@code CSV_STREAM}
   * writes no cache (there the cap also stops the download, which is the main speed win).
   */
  public int getDqRowLimit() {
    return dqRowLimit;
  }

  /**
   * Returns the name of the per-row modification field used by {@code computed_delta}.
   *
   * <p>Reads from {@code source.incremental.dateField} if the source is HTTP and has
   * an incremental config, since that field already names the modification-date column.
   * Returns null when the field is not configured (computed_delta then falls back to
   * full-upsert without modification filtering, writing every fetched row).
   */
  public String getModifiedField() {
    if (source != null && source.getIncremental() != null) {
      return source.getIncremental().getDateField();
    }
    return null;
  }

  /**
   * Creates a new builder for EtlPipelineConfig.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates an EtlPipelineConfig from a YAML/JSON map.
   *
   * <p>Handles both Map and JsonNode values in the metadata. This allows schema
   * parsers to pass metadata directly without manual conversion.
   */
  @SuppressWarnings("unchecked")
  public static EtlPipelineConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    Builder builder = builder();
    builder.name((String) map.get("name"));

    // Parse table-level enabled flag with environment variable interpolation
    Object enabledObj = map.get("enabled");
    if (enabledObj != null) {
      builder.enabled(parseEnabledValue(enabledObj));
    }

    Map<String, Object> sourceMap = toMap(map.get("source"));
    if (sourceMap != null) {
      // Detect source type - default to "http" if not specified
      String sourceType = (String) sourceMap.get("type");
      if (sourceType == null || sourceType.isEmpty()) {
        sourceType = SOURCE_TYPE_HTTP;
      }
      builder.sourceType(sourceType);
      builder.rawSourceConfig(sourceMap);

      // Only parse as HttpSourceConfig for HTTP sources
      if (SOURCE_TYPE_HTTP.equals(sourceType)) {
        builder.source(HttpSourceConfig.fromMap(sourceMap));
      }
    }

    Map<String, Object> dimensionsMap = toMap(map.get("dimensions"));
    if (dimensionsMap != null) {
      builder.dimensions(DimensionConfig.fromDimensionsMap(dimensionsMap));
    }

    Object columnsObj = map.get("columns");
    if (columnsObj instanceof List) {
      builder.columns(ColumnConfig.fromList((List<?>) columnsObj));
    }

    Map<String, Object> materializeMap = toMap(map.get("materialize"));
    if (materializeMap != null) {
      // Propagate table comment from table level to materialize config
      Object tableComment = map.get("comment");
      if (tableComment instanceof String && !materializeMap.containsKey("tableComment")) {
        materializeMap = new LinkedHashMap<>(materializeMap);
        materializeMap.put("tableComment", tableComment);
      }

      // Extract column comments from columns array and propagate to materialize config
      if (!materializeMap.containsKey("columnComments") && columnsObj instanceof List) {
        Map<String, String> columnComments = extractColumnComments((List<?>) columnsObj);
        if (!columnComments.isEmpty()) {
          materializeMap = materializeMap instanceof LinkedHashMap
              ? materializeMap : new LinkedHashMap<>(materializeMap);
          materializeMap.put("columnComments", columnComments);
        }
      }

      builder.materialize(MaterializeConfig.fromMap(materializeMap));
    }

    Map<String, Object> errorHandlingMap = toMap(map.get("errorHandling"));
    if (errorHandlingMap != null) {
      builder.errorHandling(ErrorHandlingConfig.fromMap(errorHandlingMap));
    }

    Map<String, Object> hooksMap = toMap(map.get("hooks"));
    if (hooksMap != null) {
      builder.hooks(HooksConfig.fromMap(hooksMap));
    }

    Map<String, Object> freshnessMap = toMap(map.get("freshness"));
    if (freshnessMap != null) {
      builder.freshness(FreshnessConfig.fromMap(freshnessMap));
    }

    Object datasetTypeObj = map.get("dataset_type");
    if (datasetTypeObj instanceof String) {
      builder.datasetType((String) datasetTypeObj);
    }

    Object backfillObj = map.get("backfill_period");
    if (backfillObj instanceof String) {
      builder.backfillPeriod((String) backfillObj);
    }

    Object dqRowLimitObj = map.get("dqRowLimit");
    if (dqRowLimitObj instanceof Number) {
      builder.dqRowLimit(((Number) dqRowLimitObj).intValue());
    } else if (dqRowLimitObj instanceof String) {
      try {
        builder.dqRowLimit(Integer.parseInt(((String) dqRowLimitObj).trim()));
      } catch (NumberFormatException e) {
        // ignore invalid value — leaves the default (uncapped)
      }
    }

    return builder.build();
  }

  /**
   * Extracts column comments from a list of column definitions.
   *
   * @param columns List of column config maps
   * @return Map of column name to comment
   */
  @SuppressWarnings("unchecked")
  private static Map<String, String> extractColumnComments(List<?> columns) {
    Map<String, String> comments = new LinkedHashMap<>();
    for (Object item : columns) {
      if (item instanceof Map) {
        Map<String, Object> colMap = (Map<String, Object>) item;
        String name = (String) colMap.get("name");
        Object comment = colMap.get("comment");
        if (name != null && comment instanceof String) {
          comments.put(name, (String) comment);
        }
      }
    }
    return comments;
  }

  /**
   * Parses an enabled value with environment variable interpolation.
   *
   * <p>Supports:
   * <ul>
   *   <li>Boolean values (true/false)</li>
   *   <li>String "true"/"false"</li>
   *   <li>Environment variable syntax: "${VAR_NAME:default}"</li>
   * </ul>
   *
   * @param value The value from YAML (Boolean or String)
   * @return Resolved boolean value
   */
  private static boolean parseEnabledValue(Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof String) {
      String strValue = (String) value;

      // Handle environment variable syntax: ${VAR_NAME:default}
      // After variable interpolation, the value may already be resolved
      // (e.g., "${OPENFIGI_API_KEY:}" becomes "49cdb6ff-..." or "")
      // So we check: empty/false = disabled, anything else = enabled
      if (strValue.startsWith("${") && strValue.endsWith("}")) {
        String varSpec = strValue.substring(2, strValue.length() - 1);
        int colonIdx = varSpec.indexOf(':');
        String envVar;
        String defaultValue;
        if (colonIdx >= 0) {
          envVar = varSpec.substring(0, colonIdx);
          defaultValue = varSpec.substring(colonIdx + 1);
        } else {
          envVar = varSpec;
          defaultValue = "true";
        }

        String envValue = System.getenv(envVar);
        if (envValue != null && !envValue.isEmpty()) {
          return isTruthyString(envValue);
        }
        return isTruthyString(defaultValue);
      }

      return isTruthyString(strValue);
    }
    return true; // Default to enabled
  }

  /**
   * Determines if a string value is "truthy" for the enabled field.
   * Empty string and "false" are falsy; any other non-empty string is truthy.
   * This allows patterns like {@code enabled: "${API_KEY:}"} where a non-empty
   * API key enables the table and an empty default disables it.
   */
  private static boolean isTruthyString(String value) {
    return value != null && !value.isEmpty() && !"false".equalsIgnoreCase(value);
  }

  /**
   * Converts an object to a Map, handling both Map and JsonNode types.
   *
   * @param obj Object that may be a Map or JsonNode
   * @return Map representation, or null if not convertible
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> toMap(Object obj) {
    if (obj == null) {
      return null;
    }
    if (obj instanceof Map) {
      return (Map<String, Object>) obj;
    }
    // Handle JsonNode without direct dependency - use reflection or ObjectMapper
    if (obj.getClass().getName().contains("JsonNode")) {
      try {
        // Use Jackson ObjectMapper to convert JsonNode to Map
        Class<?> objectMapperClass = Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
        Object mapper = objectMapperClass.getDeclaredConstructor().newInstance();
        java.lang.reflect.Method convertMethod =
            objectMapperClass.getMethod("convertValue", Object.class, Class.class);
        return (Map<String, Object>) convertMethod.invoke(mapper, obj, Map.class);
      } catch (Exception e) {
        // If reflection fails, return null
        return null;
      }
    }
    return null;
  }

  /**
   * Error handling configuration.
   */
  public static class ErrorHandlingConfig {
    private final ErrorAction transientErrorAction;
    private final ErrorAction notFoundAction;
    private final ErrorAction apiErrorAction;
    private final ErrorAction authErrorAction;
    private final int transientRetries;
    private final long transientBackoffMs;
    private final int notFoundRetryDays;
    private final int apiErrorRetryDays;

    private ErrorHandlingConfig(Builder builder) {
      this.transientErrorAction = builder.transientErrorAction;
      this.notFoundAction = builder.notFoundAction;
      this.apiErrorAction = builder.apiErrorAction;
      this.authErrorAction = builder.authErrorAction;
      this.transientRetries = builder.transientRetries;
      this.transientBackoffMs = builder.transientBackoffMs;
      this.notFoundRetryDays = builder.notFoundRetryDays;
      this.apiErrorRetryDays = builder.apiErrorRetryDays;
    }

    public enum ErrorAction {
      FAIL, SKIP, WARN, RETRY
    }

    public static ErrorHandlingConfig defaults() {
      return new Builder()
          .transientErrorAction(ErrorAction.RETRY)
          .notFoundAction(ErrorAction.SKIP)
          .apiErrorAction(ErrorAction.SKIP)
          .authErrorAction(ErrorAction.FAIL)
          .transientRetries(3)
          .transientBackoffMs(1000)
          .notFoundRetryDays(7)
          .apiErrorRetryDays(7)
          .build();
    }

    public ErrorAction getTransientErrorAction() {
      return transientErrorAction;
    }

    public ErrorAction getNotFoundAction() {
      return notFoundAction;
    }

    public ErrorAction getApiErrorAction() {
      return apiErrorAction;
    }

    public ErrorAction getAuthErrorAction() {
      return authErrorAction;
    }

    public int getTransientRetries() {
      return transientRetries;
    }

    public long getTransientBackoffMs() {
      return transientBackoffMs;
    }

    public int getNotFoundRetryDays() {
      return notFoundRetryDays;
    }

    public int getApiErrorRetryDays() {
      return apiErrorRetryDays;
    }

    public static ErrorHandlingConfig fromMap(Map<String, Object> map) {
      if (map == null) {
        return defaults();
      }

      Builder builder = new Builder();

      Object transientObj = map.get("transientErrorAction");
      if (transientObj instanceof String) {
        builder.transientErrorAction(ErrorAction.valueOf(((String) transientObj).toUpperCase()));
      }

      Object notFoundObj = map.get("notFoundAction");
      if (notFoundObj instanceof String) {
        builder.notFoundAction(ErrorAction.valueOf(((String) notFoundObj).toUpperCase()));
      }

      Object apiObj = map.get("apiErrorAction");
      if (apiObj instanceof String) {
        builder.apiErrorAction(ErrorAction.valueOf(((String) apiObj).toUpperCase()));
      }

      Object authObj = map.get("authErrorAction");
      if (authObj instanceof String) {
        builder.authErrorAction(ErrorAction.valueOf(((String) authObj).toUpperCase()));
      }

      Object retriesObj = map.get("transientRetries");
      if (retriesObj instanceof Number) {
        builder.transientRetries(((Number) retriesObj).intValue());
      }

      Object backoffObj = map.get("transientBackoffMs");
      if (backoffObj instanceof Number) {
        builder.transientBackoffMs(((Number) backoffObj).longValue());
      }

      Object notFoundRetryObj = map.get("notFoundRetryDays");
      if (notFoundRetryObj instanceof Number) {
        builder.notFoundRetryDays(((Number) notFoundRetryObj).intValue());
      }

      Object apiRetryObj = map.get("apiErrorRetryDays");
      if (apiRetryObj instanceof Number) {
        builder.apiErrorRetryDays(((Number) apiRetryObj).intValue());
      }

      return builder.build();
    }

    public static class Builder {
      private ErrorAction transientErrorAction = ErrorAction.RETRY;
      private ErrorAction notFoundAction = ErrorAction.SKIP;
      private ErrorAction apiErrorAction = ErrorAction.SKIP;
      private ErrorAction authErrorAction = ErrorAction.FAIL;
      private int transientRetries = 3;
      private long transientBackoffMs = 1000;
      private int notFoundRetryDays = 7;
      private int apiErrorRetryDays = 7;

      public Builder transientErrorAction(ErrorAction action) {
        this.transientErrorAction = action;
        return this;
      }

      public Builder notFoundAction(ErrorAction action) {
        this.notFoundAction = action;
        return this;
      }

      public Builder apiErrorAction(ErrorAction action) {
        this.apiErrorAction = action;
        return this;
      }

      public Builder authErrorAction(ErrorAction action) {
        this.authErrorAction = action;
        return this;
      }

      public Builder transientRetries(int retries) {
        this.transientRetries = retries;
        return this;
      }

      public Builder transientBackoffMs(long backoffMs) {
        this.transientBackoffMs = backoffMs;
        return this;
      }

      public Builder notFoundRetryDays(int days) {
        this.notFoundRetryDays = days;
        return this;
      }

      public Builder apiErrorRetryDays(int days) {
        this.apiErrorRetryDays = days;
        return this;
      }

      public ErrorHandlingConfig build() {
        return new ErrorHandlingConfig(this);
      }
    }
  }

  /**
   * Builder for EtlPipelineConfig.
   */
  public static class Builder {
    private String name;
    private boolean enabled = true;  // Enabled by default
    private String sourceType;
    private HttpSourceConfig source;
    private Map<String, Object> rawSourceConfig;
    private Map<String, DimensionConfig> dimensions;
    private List<ColumnConfig> columns;
    private MaterializeConfig materialize;
    private ErrorHandlingConfig errorHandling;
    private HooksConfig hooks;
    private FreshnessConfig freshness;
    private String datasetType;
    private String backfillPeriod;
    private int dqRowLimit;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder sourceType(String sourceType) {
      this.sourceType = sourceType;
      return this;
    }

    public Builder source(HttpSourceConfig source) {
      this.source = source;
      return this;
    }

    public Builder rawSourceConfig(Map<String, Object> rawSourceConfig) {
      this.rawSourceConfig = rawSourceConfig;
      return this;
    }

    public Builder dimensions(Map<String, DimensionConfig> dimensions) {
      this.dimensions = dimensions;
      return this;
    }

    public Builder columns(List<ColumnConfig> columns) {
      this.columns = columns;
      return this;
    }

    public Builder materialize(MaterializeConfig materialize) {
      this.materialize = materialize;
      return this;
    }

    public Builder errorHandling(ErrorHandlingConfig errorHandling) {
      this.errorHandling = errorHandling;
      return this;
    }

    public Builder hooks(HooksConfig hooks) {
      this.hooks = hooks;
      return this;
    }

    public Builder freshness(FreshnessConfig freshness) {
      this.freshness = freshness;
      return this;
    }

    public Builder datasetType(String datasetType) {
      this.datasetType = datasetType;
      return this;
    }

    public Builder backfillPeriod(String backfillPeriod) {
      this.backfillPeriod = backfillPeriod;
      return this;
    }

    public Builder dqRowLimit(int dqRowLimit) {
      this.dqRowLimit = dqRowLimit;
      return this;
    }

    public EtlPipelineConfig build() {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Pipeline name is required");
      }
      // Skip source/materialize validation for disabled pipelines
      if (enabled) {
        // Source is required unless hooks are enabled (hooks-only tables skip ETL pipeline)
        boolean hooksEnabled = hooks != null && hooks.isEnabled();
        if (source == null && rawSourceConfig == null && !hooksEnabled) {
          throw new IllegalArgumentException("Source configuration is required");
        }
        // Materialize is required unless this is a hooks-only table
        if (materialize == null && !hooksEnabled) {
          throw new IllegalArgumentException("Materialize configuration is required");
        }
      }
      return new EtlPipelineConfig(this);
    }
  }
}
