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
package org.apache.calcite.adapter.govdata;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Configuration for bulk downloads across all govdata schemas.
 *
 * <p>Bulk downloads are large source files that feed multiple tables.
 * For example, the QCEW annual ZIP file (~80MB) contains data for
 * state_wages, county_wages, county_qcew, and metro_wages tables.
 *
 * <p>This configuration enables "download once, convert many" optimization
 * by explicitly defining the shared download dependency.
 *
 * <p>Example from schema JSON:
 * <pre>
 * "bulkDownloads": {
 *   "qcew_annual_bulk": {
 *     "cachePattern": "type=qcew_bulk/year={year}/frequency={frequency}/qcew.zip",
 *     "url": "https://data.bls.gov/cew/data/files/{year}/csv/{year}_{frequency}_singlefile.zip",
 *     "variables": ["year", "frequency"],
 *     "comment": "QCEW bulk CSV download shared by multiple tables"
 *   }
 * }
 * </pre>
 */
public final class BulkDownloadConfig {
  private final String name;
  private final String cachePattern;
  private final String url;
  private final List<String> variables;
  private final String comment;

  /**
   * Creates a new bulk download configuration.
   *
   * @param name Unique identifier for this bulk download (e.g., "qcew_annual_bulk")
   * @param cachePattern Hive-style partition pattern for cache location
   * @param url URL template for downloading the bulk file
   * @param variables List of variable names that appear in patterns
   * @param comment Human-readable description
   */
  public BulkDownloadConfig(String name, String cachePattern, String url,
      List<String> variables, String comment) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("name cannot be null or empty");
    }
    if (cachePattern == null || cachePattern.isEmpty()) {
      throw new IllegalArgumentException("cachePattern cannot be null or empty");
    }
    if (url == null || url.isEmpty()) {
      throw new IllegalArgumentException("url cannot be null or empty");
    }

    this.name = name;
    this.cachePattern = cachePattern;
    this.url = url;
    this.variables = variables != null
        ? Collections.unmodifiableList(variables)
        : Collections.emptyList();
    this.comment = comment != null ? comment : "";
  }

  /** Returns the unique name of this bulk download. */
  public String getName() {
    return name;
  }

  /** Returns the cache pattern with variables (e.g., "type=qcew_bulk/year={year}/qcew.zip"). */
  public String getCachePattern() {
    return cachePattern;
  }

  /** Returns the URL template for downloading. */
  public String getUrl() {
    return url;
  }

  /** Returns the list of variable names used in patterns. */
  public List<String> getVariables() {
    return variables;
  }

  /** Returns the human-readable comment/description. */
  public String getComment() {
    return comment;
  }

  /**
   * Resolves the cache pattern by substituting variables.
   *
   * @param variableValues Map of variable names to values
   * @return The resolved cache path
   * @throws IllegalArgumentException if required variables are missing
   */
  public String resolveCachePath(Map<String, String> variableValues) {
    String resolved = cachePattern;
    for (String variable : variables) {
      String value = variableValues.get(variable);
      if (value == null) {
        throw new IllegalArgumentException(
            "Missing required variable '" + variable + "' for bulk download '" + name + "'");
      }
      resolved = resolved.replace("{" + variable + "}", value);
    }
    return resolved;
  }

  /**
   * Resolves the URL by substituting variables.
   *
   * @param variableValues Map of variable names to values
   * @return The resolved download URL
   * @throws IllegalArgumentException if required variables are missing
   */
  public String resolveUrl(Map<String, String> variableValues) {
    String resolved = url;
    for (String variable : variables) {
      String value = variableValues.get(variable);
      if (value == null) {
        throw new IllegalArgumentException(
            "Missing required variable '" + variable + "' for bulk download '" + name + "'");
      }
      resolved = resolved.replace("{" + variable + "}", value);
    }
    return resolved;
  }

  @Override public String toString() {
    return "BulkDownloadConfig{name='" + name + "', cachePattern='" + cachePattern
        + "', url='" + url + "', variables=" + variables + "}";
  }
}
