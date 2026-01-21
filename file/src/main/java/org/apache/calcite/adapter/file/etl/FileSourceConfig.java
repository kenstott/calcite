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

import java.util.Map;

/**
 * Configuration for file-based data sources.
 *
 * <h3>YAML Configuration</h3>
 * <pre>{@code
 * source:
 *   type: file
 *   path: "s3://bucket/data/${year}/report.xlsx"
 *   format: xlsx      # optional, auto-detected from extension
 *   sheet: Sheet1     # optional, for Excel files
 * }</pre>
 */
public class FileSourceConfig {

  private final String path;
  private final String format;
  private final String sheet;

  private FileSourceConfig(Builder builder) {
    this.path = builder.path;
    this.format = builder.format;
    this.sheet = builder.sheet;
  }

  public String getPath() {
    return path;
  }

  public String getFormat() {
    return format;
  }

  public String getSheet() {
    return sheet;
  }

  public static FileSourceConfig fromMap(Map<String, Object> map) {
    Builder builder = new Builder();
    if (map.containsKey("path")) {
      builder.path((String) map.get("path"));
    }
    if (map.containsKey("location")) {
      // Alias for path
      builder.path((String) map.get("location"));
    }
    if (map.containsKey("format")) {
      builder.format((String) map.get("format"));
    }
    if (map.containsKey("sheet")) {
      builder.sheet((String) map.get("sheet"));
    }
    return builder.build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String path;
    private String format;
    private String sheet;

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    public Builder format(String format) {
      this.format = format;
      return this;
    }

    public Builder sheet(String sheet) {
      this.sheet = sheet;
      return this;
    }

    public FileSourceConfig build() {
      if (path == null || path.isEmpty()) {
        throw new IllegalArgumentException("FileSourceConfig requires 'path'");
      }
      return new FileSourceConfig(this);
    }
  }
}
