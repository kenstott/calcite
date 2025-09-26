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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Analyzes FRED series characteristics to determine optimal partitioning strategies.
 *
 * <p>This class implements smart partitioning logic that considers:
 * <ul>
 *   <li>Data frequency patterns (daily, monthly, quarterly, annual)</li>
 *   <li>Series volume and historical depth</li>
 *   <li>Common query patterns</li>
 *   <li>Series type categorization</li>
 * </ul>
 */
public class FredSeriesPartitionAnalyzer {
  private static final Logger LOGGER = LoggerFactory.getLogger(FredSeriesPartitionAnalyzer.class);

  // Thresholds for auto-partitioning decisions
  private static final int LARGE_SERIES_COLLECTION_THRESHOLD = 50;
  private static final int MIXED_FREQUENCY_THRESHOLD = 2;

  // Common FRED series patterns and their characteristics
  private static final Map<String, SeriesCharacteristics> SERIES_PATTERNS = new HashMap<>();

  static {
    // Treasury rates - typically daily, benefit from maturity partitioning
    SERIES_PATTERNS.put("DGS.*", new SeriesCharacteristics("DAILY", "TREASURY", Arrays.asList("year", "maturity")));
    SERIES_PATTERNS.put(".*RATE", new SeriesCharacteristics("MONTHLY", "INTEREST_RATE", Arrays.asList("year", "frequency")));

    // Employment indicators - typically monthly
    SERIES_PATTERNS.put("UNRATE", new SeriesCharacteristics("MONTHLY", "EMPLOYMENT", Arrays.asList("year")));
    SERIES_PATTERNS.put("PAYEMS", new SeriesCharacteristics("MONTHLY", "EMPLOYMENT", Arrays.asList("year")));
    SERIES_PATTERNS.put(".*EMP.*", new SeriesCharacteristics("MONTHLY", "EMPLOYMENT", Arrays.asList("year")));

    // GDP and economic activity - typically quarterly
    SERIES_PATTERNS.put("GDP.*", new SeriesCharacteristics("QUARTERLY", "ECONOMIC_ACTIVITY", Arrays.asList("year")));

    // Financial conditions - typically weekly or monthly
    SERIES_PATTERNS.put(".*FCI", new SeriesCharacteristics("WEEKLY", "FINANCIAL_CONDITIONS", Arrays.asList("year", "frequency")));

    // Exchange rates - typically daily
    SERIES_PATTERNS.put("DEX.*", new SeriesCharacteristics("DAILY", "EXCHANGE_RATE", Arrays.asList("year")));

    // Commodity prices - typically daily
    SERIES_PATTERNS.put(".*OIL.*", new SeriesCharacteristics("DAILY", "COMMODITY", Arrays.asList("year")));
  }

  /**
   * Series characteristics for partitioning decisions.
   */
  private static class SeriesCharacteristics {
    final String frequency;
    final String category;
    final List<String> suggestedPartitions;

    SeriesCharacteristics(String frequency, String category, List<String> suggestedPartitions) {
      this.frequency = frequency;
      this.category = category;
      this.suggestedPartitions = suggestedPartitions;
    }
  }

  /**
   * Analyze a series group and determine optimal partitioning strategy.
   *
   * @param group The FRED series group to analyze
   * @param allCustomSeries All custom series being configured (for volume analysis)
   * @return Analyzed partitioning recommendation
   */
  public PartitionAnalysis analyzeGroup(FredSeriesGroup group, List<String> allCustomSeries) {
    LOGGER.debug("Analyzing partition strategy for group: {}", group.getGroupName());

    if (group.getPartitionStrategy() == FredSeriesGroup.PartitionStrategy.NONE) {
      return new PartitionAnalysis(group.getPartitionStrategy(), new ArrayList<>(),
                                  "Partitioning disabled by configuration");
    }

    if (group.getPartitionStrategy() == FredSeriesGroup.PartitionStrategy.MANUAL) {
      return new PartitionAnalysis(group.getPartitionStrategy(), group.getPartitionFields(),
                                  "Manual partitioning fields specified");
    }

    // AUTO strategy - analyze series characteristics
    return analyzeAutoPartitioning(group, allCustomSeries);
  }

  private PartitionAnalysis analyzeAutoPartitioning(FredSeriesGroup group, List<String> allCustomSeries) {
    Set<String> frequencies = new HashSet<>();
    Set<String> categories = new HashSet<>();
    List<String> recommendedFields = new ArrayList<>();

    // Analyze each series in the group
    for (String seriesId : group.getSeries()) {
      SeriesCharacteristics characteristics = getSeriesCharacteristics(seriesId);
      if (characteristics != null) {
        frequencies.add(characteristics.frequency);
        categories.add(characteristics.category);
      }
    }

    // Always partition by year for time series data
    recommendedFields.add("year");

    // Add frequency partitioning if mixed frequencies
    if (frequencies.size() >= MIXED_FREQUENCY_THRESHOLD) {
      recommendedFields.add("frequency");
      LOGGER.debug("Mixed frequencies detected for group {}: {}. Adding frequency partitioning.",
                  group.getGroupName(), frequencies);
    }

    // Add series_id partitioning for large collections
    if (allCustomSeries.size() >= LARGE_SERIES_COLLECTION_THRESHOLD) {
      recommendedFields.add("series_id");
      LOGGER.debug("Large series collection detected ({} series). Adding series_id partitioning.",
                  allCustomSeries.size());
    }

    // Category-specific optimizations
    if (categories.contains("TREASURY")) {
      // Treasury series benefit from maturity-based partitioning
      if (!recommendedFields.contains("maturity")) {
        recommendedFields.add("maturity");
      }
      LOGGER.debug("Treasury series detected in group {}. Adding maturity partitioning.",
                  group.getGroupName());
    }

    String reasoning = String.format("Auto-analysis: frequencies=%s, categories=%s, totalSeries=%d",
                                   frequencies, categories, allCustomSeries.size());

    return new PartitionAnalysis(FredSeriesGroup.PartitionStrategy.AUTO, recommendedFields, reasoning);
  }

  private SeriesCharacteristics getSeriesCharacteristics(String seriesId) {
    // Check against known patterns
    for (Map.Entry<String, SeriesCharacteristics> entry : SERIES_PATTERNS.entrySet()) {
      String pattern = entry.getKey();
      if (seriesId.matches(pattern)) {
        LOGGER.debug("Series {} matches pattern {} with characteristics: {}",
                    seriesId, pattern, entry.getValue().category);
        return entry.getValue();
      }
    }

    // Default characteristics for unknown series
    LOGGER.debug("No specific pattern found for series {}. Using default characteristics.", seriesId);
    return new SeriesCharacteristics("MONTHLY", "UNKNOWN", Arrays.asList("year"));
  }

  /**
   * Result of partition analysis for a series group.
   */
  public static class PartitionAnalysis {
    private final FredSeriesGroup.PartitionStrategy strategy;
    private final List<String> partitionFields;
    private final String reasoning;

    public PartitionAnalysis(FredSeriesGroup.PartitionStrategy strategy,
                           List<String> partitionFields, String reasoning) {
      this.strategy = strategy;
      this.partitionFields = new ArrayList<>(partitionFields);
      this.reasoning = reasoning;
    }

    public FredSeriesGroup.PartitionStrategy getStrategy() {
      return strategy;
    }

    public List<String> getPartitionFields() {
      return new ArrayList<>(partitionFields);
    }

    public String getReasoning() {
      return reasoning;
    }

    @Override
    public String toString() {
      return String.format("PartitionAnalysis{strategy=%s, fields=%s, reason='%s'}",
                          strategy, partitionFields, reasoning);
    }
  }
}