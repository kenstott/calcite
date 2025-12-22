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

import java.util.Map;

/**
 * Configuration for materialization output settings.
 *
 * <p>Specifies where and how to write materialized data:
 * <ul>
 *   <li>location - Base output directory (supports local, s3://, etc.)</li>
 *   <li>pattern - Glob pattern describing output structure</li>
 *   <li>format - Output format (currently only "parquet" is supported)</li>
 *   <li>compression - Compression codec (snappy, zstd, lz4, gzip, none)</li>
 * </ul>
 *
 * <h3>YAML Configuration</h3>
 * <pre>
 * output:
 *   pattern: "type=sales/year=STAR/region=STAR/"
 *   compression: snappy
 * </pre>
 *
 * <p>Note: location is optional. If not specified, the materializedDirectory
 * configured at the schema level is used directly.
 */
public class MaterializeOutputConfig {
  private static final String DEFAULT_FORMAT = "parquet";
  private static final String DEFAULT_COMPRESSION = "snappy";

  private final String location;
  private final String pattern;
  private final String format;
  private final String compression;

  private MaterializeOutputConfig(Builder builder) {
    this.location = builder.location;
    this.pattern = builder.pattern;
    this.format = builder.format != null ? builder.format : DEFAULT_FORMAT;
    this.compression = builder.compression != null ? builder.compression : DEFAULT_COMPRESSION;
  }

  /**
   * Returns the base output location.
   * Supports local paths, s3://, and other storage provider URLs.
   */
  public String getLocation() {
    return location;
  }

  /**
   * Returns the glob pattern describing output structure.
   */
  public String getPattern() {
    return pattern;
  }

  /**
   * Returns the output format (default: "parquet").
   */
  public String getFormat() {
    return format;
  }

  /**
   * Returns the compression codec (default: "snappy").
   * Supported values: snappy, zstd, lz4, gzip, none.
   */
  public String getCompression() {
    return compression;
  }

  /**
   * Creates a new builder for MaterializeOutputConfig.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a MaterializeOutputConfig from a YAML/JSON map.
   *
   * @param map Configuration map with keys: location, pattern, format, compression
   * @return MaterializeOutputConfig instance
   */
  public static MaterializeOutputConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    return builder()
        .location((String) map.get("location"))
        .pattern((String) map.get("pattern"))
        .format((String) map.get("format"))
        .compression((String) map.get("compression"))
        .build();
  }

  /**
   * Builder for MaterializeOutputConfig.
   */
  public static class Builder {
    private String location;
    private String pattern;
    private String format;
    private String compression;

    public Builder location(String location) {
      this.location = location;
      return this;
    }

    public Builder pattern(String pattern) {
      this.pattern = pattern;
      return this;
    }

    public Builder format(String format) {
      this.format = format;
      return this;
    }

    public Builder compression(String compression) {
      this.compression = compression;
      return this;
    }

    public MaterializeOutputConfig build() {
      // Location is now optional - if not specified, baseDirectory is used directly
      return new MaterializeOutputConfig(this);
    }
  }
}
