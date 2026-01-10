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
import java.util.List;
import java.util.Map;

/**
 * Configuration for bulk downloads in the ETL lifecycle.
 *
 * <p>Bulk downloads are large source files that feed multiple tables.
 * For example, a QCEW annual ZIP file (~80MB) contains data for
 * state_wages, county_wages, county_qcew, and metro_wages tables.
 *
 * <p>This enables "download once, use many" optimization by downloading
 * bulk files in a dedicated phase before table processing begins.
 *
 * <p>Example YAML configuration:
 * <pre>{@code
 * bulkDownloads:
 *   qcew_annual_bulk:
 *     cachePattern: "bulk/qcew/year={year}/{frequency}_singlefile.zip"
 *     url: "https://data.bls.gov/cew/data/files/{year}/csv/{year}_{frequency}_singlefile.zip"
 *     dimensions:
 *       year: { type: range, start: 2020, end: 2023 }
 *       frequency: { type: list, values: [annual] }
 * }</pre>
 *
 * <p>Tables reference bulk downloads via the source section:
 * <pre>{@code
 * partitionedTables:
 *   - name: state_wages
 *     source:
 *       bulkDownload: qcew_annual_bulk
 *       extractPattern: "*.csv"
 * }</pre>
 */
public final class BulkDownloadConfig {
  private final String name;
  private final String cachePattern;
  private final String url;
  private final Map<String, DimensionConfig> dimensions;
  private final String comment;

  private BulkDownloadConfig(Builder builder) {
    this.name = builder.name;
    this.cachePattern = builder.cachePattern;
    this.url = builder.url;
    this.dimensions = builder.dimensions != null
        ? Collections.unmodifiableMap(builder.dimensions)
        : Collections.emptyMap();
    this.comment = builder.comment;
  }

  /** Returns the unique name of this bulk download. */
  public String getName() {
    return name;
  }

  /** Returns the cache pattern with variables (e.g., "bulk/qcew/year={year}/data.zip"). */
  public String getCachePattern() {
    return cachePattern;
  }

  /** Returns the URL template for downloading. */
  public String getUrl() {
    return url;
  }

  /** Returns the dimension configurations for this bulk download. */
  public Map<String, DimensionConfig> getDimensions() {
    return dimensions;
  }

  /** Returns the human-readable comment/description. */
  public String getComment() {
    return comment;
  }

  /**
   * Resolves the cache pattern by substituting variables.
   *
   * @param variables Map of variable names to values
   * @return The resolved cache path
   */
  public String resolveCachePath(Map<String, String> variables) {
    String resolved = cachePattern;
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      resolved = resolved.replace("{" + entry.getKey() + "}", entry.getValue());
    }
    return resolved;
  }

  /**
   * Resolves the URL by substituting variables.
   *
   * @param variables Map of variable names to values
   * @return The resolved download URL
   */
  public String resolveUrl(Map<String, String> variables) {
    String resolved = url;
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      resolved = resolved.replace("{" + entry.getKey() + "}", entry.getValue());
    }
    return resolved;
  }

  /**
   * Creates a BulkDownloadConfig from a map (parsed from YAML/JSON).
   *
   * @param name The bulk download name
   * @param map The configuration map
   * @return A new BulkDownloadConfig
   */
  @SuppressWarnings("unchecked")
  public static BulkDownloadConfig fromMap(String name, Map<String, Object> map) {
    Builder builder = builder()
        .name(name)
        .cachePattern((String) map.get("cachePattern"))
        .url((String) map.get("url"))
        .comment((String) map.get("comment"));

    // Parse dimensions using the shared parsing logic that handles all formats
    Object dimsObj = map.get("dimensions");
    if (dimsObj instanceof Map) {
      Map<String, DimensionConfig> parsedDims =
          DimensionConfig.fromDimensionsMap((Map<String, Object>) dimsObj);
      builder.dimensions(parsedDims);
    }

    // Legacy support: convert 'variables' list to dimensions
    Object varsObj = map.get("variables");
    if (varsObj instanceof List && builder.dimensions.isEmpty()) {
      List<String> vars = (List<String>) varsObj;
      for (String var : vars) {
        // Create placeholder dimension - actual values come from table dimensions
        builder.dimension(var, DimensionConfig.builder()
            .name(var)
            .type(DimensionType.LIST)
            .build());
      }
    }

    return builder.build();
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override public String toString() {
    return "BulkDownloadConfig{name='" + name + "', cachePattern='" + cachePattern
        + "', url='" + url + "', dimensions=" + dimensions.keySet() + "}";
  }

  /** Builder for BulkDownloadConfig. */
  public static final class Builder {
    private String name;
    private String cachePattern;
    private String url;
    private Map<String, DimensionConfig> dimensions = new java.util.LinkedHashMap<>();
    private String comment;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder cachePattern(String cachePattern) {
      this.cachePattern = cachePattern;
      return this;
    }

    public Builder url(String url) {
      this.url = url;
      return this;
    }

    public Builder dimension(String name, DimensionConfig config) {
      this.dimensions.put(name, config);
      return this;
    }

    public Builder dimensions(Map<String, DimensionConfig> dimensions) {
      this.dimensions = new java.util.LinkedHashMap<>(dimensions);
      return this;
    }

    public Builder comment(String comment) {
      this.comment = comment;
      return this;
    }

    public BulkDownloadConfig build() {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("name is required");
      }
      if (cachePattern == null || cachePattern.isEmpty()) {
        throw new IllegalArgumentException("cachePattern is required");
      }
      if (url == null || url.isEmpty()) {
        throw new IllegalArgumentException("url is required");
      }
      return new BulkDownloadConfig(this);
    }
  }
}
