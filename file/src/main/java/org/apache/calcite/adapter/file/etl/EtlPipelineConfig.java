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
  private final String sourceType;
  private final HttpSourceConfig source;
  private final Map<String, Object> rawSourceConfig;
  private final Map<String, DimensionConfig> dimensions;
  private final List<ColumnConfig> columns;
  private final MaterializeConfig materialize;
  private final ErrorHandlingConfig errorHandling;
  private final HooksConfig hooks;

  private EtlPipelineConfig(Builder builder) {
    this.name = builder.name;
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
  }

  /**
   * Returns the pipeline/table name.
   */
  public String getName() {
    return name;
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
        java.lang.reflect.Method convertMethod = objectMapperClass.getMethod(
            "convertValue", Object.class, Class.class);
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
    private String sourceType;
    private HttpSourceConfig source;
    private Map<String, Object> rawSourceConfig;
    private Map<String, DimensionConfig> dimensions;
    private List<ColumnConfig> columns;
    private MaterializeConfig materialize;
    private ErrorHandlingConfig errorHandling;
    private HooksConfig hooks;

    public Builder name(String name) {
      this.name = name;
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

    public EtlPipelineConfig build() {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Pipeline name is required");
      }
      // For HTTP sources, require HttpSourceConfig; for other types, require rawSourceConfig
      if (source == null && rawSourceConfig == null) {
        throw new IllegalArgumentException("Source configuration is required");
      }
      if (materialize == null) {
        throw new IllegalArgumentException("Materialize configuration is required");
      }
      return new EtlPipelineConfig(this);
    }
  }
}
