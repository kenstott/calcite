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
 *         location: "{baseDirectory}"
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

  private final String name;
  private final HttpSourceConfig source;
  private final Map<String, DimensionConfig> dimensions;
  private final List<ColumnConfig> columns;
  private final MaterializeConfig materialize;
  private final ErrorHandlingConfig errorHandling;

  private EtlPipelineConfig(Builder builder) {
    this.name = builder.name;
    this.source = builder.source;
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
  }

  /**
   * Returns the pipeline/table name.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the data source configuration.
   */
  public HttpSourceConfig getSource() {
    return source;
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
   * Creates a new builder for EtlPipelineConfig.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates an EtlPipelineConfig from a YAML/JSON map.
   */
  @SuppressWarnings("unchecked")
  public static EtlPipelineConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    Builder builder = builder();
    builder.name((String) map.get("name"));

    Object sourceObj = map.get("source");
    if (sourceObj instanceof Map) {
      builder.source(HttpSourceConfig.fromMap((Map<String, Object>) sourceObj));
    }

    Object dimensionsObj = map.get("dimensions");
    if (dimensionsObj instanceof Map) {
      builder.dimensions(DimensionConfig.fromDimensionsMap((Map<String, Object>) dimensionsObj));
    }

    Object columnsObj = map.get("columns");
    if (columnsObj instanceof List) {
      builder.columns(ColumnConfig.fromList((List<?>) columnsObj));
    }

    Object materializeObj = map.get("materialize");
    if (materializeObj instanceof Map) {
      builder.materialize(MaterializeConfig.fromMap((Map<String, Object>) materializeObj));
    }

    Object errorHandlingObj = map.get("errorHandling");
    if (errorHandlingObj instanceof Map) {
      builder.errorHandling(ErrorHandlingConfig.fromMap((Map<String, Object>) errorHandlingObj));
    }

    return builder.build();
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
    private HttpSourceConfig source;
    private Map<String, DimensionConfig> dimensions;
    private List<ColumnConfig> columns;
    private MaterializeConfig materialize;
    private ErrorHandlingConfig errorHandling;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder source(HttpSourceConfig source) {
      this.source = source;
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

    public EtlPipelineConfig build() {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Pipeline name is required");
      }
      if (source == null) {
        throw new IllegalArgumentException("Source configuration is required");
      }
      if (materialize == null) {
        throw new IllegalArgumentException("Materialize configuration is required");
      }
      return new EtlPipelineConfig(this);
    }
  }
}
