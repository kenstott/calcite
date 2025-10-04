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
package org.apache.calcite.adapter.govdata.econ;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Configuration for a group of FRED series with common partitioning strategy.
 *
 * <p>Allows grouping related FRED series (e.g., Treasury rates, employment indicators)
 * and applying optimized partitioning strategies based on their characteristics.
 *
 * <p>Example configuration:
 * <pre>{@code
 * {
 *   "groupName": "treasuries",
 *   "series": ["DGS10", "DGS30", "DGS*"],
 *   "partitionStrategy": "MANUAL",
 *   "partitionFields": ["year", "maturity"]
 * }
 * }</pre>
 */
public class FredSeriesGroup {

  private final String groupName;
  private final List<String> series;
  private final PartitionStrategy partitionStrategy;
  private final List<String> partitionFields;
  private final List<Pattern> compiledPatterns;

  public FredSeriesGroup(String groupName, List<String> series,
                         PartitionStrategy partitionStrategy,
                         @Nullable List<String> partitionFields) {
    this.groupName = groupName;
    this.series = new ArrayList<>(series);
    this.partitionStrategy = partitionStrategy;
    this.partitionFields = partitionFields != null ? new ArrayList<>(partitionFields) : new ArrayList<>();

    // Pre-compile regex patterns for efficient matching
    this.compiledPatterns = new ArrayList<>();
    for (String seriesPattern : this.series) {
      if (seriesPattern.contains("*") || seriesPattern.contains("?")) {
        // Convert glob pattern to regex
        String regex = seriesPattern
            .replace("*", ".*")
            .replace("?", ".");
        this.compiledPatterns.add(Pattern.compile(regex, Pattern.CASE_INSENSITIVE));
      }
    }
  }

  /**
   * Partition strategies for FRED series groups.
   */
  public enum PartitionStrategy {
    /** No partitioning - single file per series group */
    NONE,
    /** Automatic partitioning based on series characteristics */
    AUTO,
    /** Manual partitioning using specified fields */
    MANUAL
  }

  public String getGroupName() {
    return groupName;
  }

  public List<String> getSeries() {
    return new ArrayList<>(series);
  }

  public PartitionStrategy getPartitionStrategy() {
    return partitionStrategy;
  }

  public List<String> getPartitionFields() {
    return new ArrayList<>(partitionFields);
  }

  /**
   * Check if a FRED series ID matches this group's series patterns.
   *
   * @param seriesId FRED series identifier (e.g., "DGS10", "UNRATE")
   * @return true if series matches any pattern in this group
   */
  public boolean matchesSeries(String seriesId) {
    // Check exact matches first
    for (String seriesPattern : series) {
      if (!seriesPattern.contains("*") && !seriesPattern.contains("?")) {
        if (seriesPattern.equalsIgnoreCase(seriesId)) {
          return true;
        }
      }
    }

    // Check regex patterns
    for (Pattern pattern : compiledPatterns) {
      if (pattern.matcher(seriesId).matches()) {
        return true;
      }
    }

    return false;
  }

  /**
   * Generate table name for this series group.
   * Converts group name to valid SQL identifier.
   */
  public String getTableName() {
    return "fred_" + groupName.toLowerCase()
        .replaceAll("[^a-z0-9_]", "_")
        .replaceAll("_+", "_")
        .replaceAll("^_|_$", "");
  }

  @Override public String toString() {
    return String.format("FredSeriesGroup{name=%s, series=%s, strategy=%s, fields=%s}",
                        groupName, series, partitionStrategy, partitionFields);
  }
}
