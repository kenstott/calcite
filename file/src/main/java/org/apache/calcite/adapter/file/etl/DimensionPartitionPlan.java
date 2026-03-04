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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lightweight plan for partitioned dimension expansion.
 *
 * <p>Holds the metadata needed to lazily expand one partition at a time,
 * without materializing all partitions' combinations simultaneously.
 * The prefix combinations (non-CUSTOM dimensions) are kept in memory (~4MB for 8K combos),
 * and each partition's full combinations are expanded on demand via
 * {@link DimensionIterator#expandPartition(DimensionPartitionPlan, String)}.
 *
 * @see DimensionIterator#planPartitions(Map)
 * @see DimensionIterator#expandPartition(DimensionPartitionPlan, String)
 */
public class DimensionPartitionPlan {

  private final String contextKey;
  private final List<String> contextValues;
  private final String customDimName;
  private final DimensionConfig customDimConfig;
  private final Map<String, List<Map<String, String>>> prefixByContext;

  DimensionPartitionPlan(String contextKey, List<String> contextValues,
      String customDimName, DimensionConfig customDimConfig,
      Map<String, List<Map<String, String>>> prefixByContext) {
    this.contextKey = contextKey;
    this.contextValues = Collections.unmodifiableList(new ArrayList<String>(contextValues));
    this.customDimName = customDimName;
    this.customDimConfig = customDimConfig;
    this.prefixByContext = prefixByContext;
  }

  /** The dimension key used to partition (e.g., "state_abbr"). */
  public String getContextKey() {
    return contextKey;
  }

  /** Ordered list of distinct context values (e.g., ["AL", "AK", ...]). */
  public List<String> getContextValues() {
    return contextValues;
  }

  /** Number of partitions. */
  public int getPartitionCount() {
    return contextValues.size();
  }

  /** Name of the CUSTOM dimension (e.g., "ori"). */
  public String getCustomDimName() {
    return customDimName;
  }

  /** Config of the CUSTOM dimension. */
  public DimensionConfig getCustomDimConfig() {
    return customDimConfig;
  }

  /** Prefix combinations for a specific context value. */
  public List<Map<String, String>> getPrefixCombinations(String contextValue) {
    List<Map<String, String>> combos = prefixByContext.get(contextValue);
    return combos != null ? combos : Collections.<Map<String, String>>emptyList();
  }

  /** Total prefix combinations across all context values. */
  public int getTotalPrefixCount() {
    int total = 0;
    for (List<Map<String, String>> combos : prefixByContext.values()) {
      total += combos.size();
    }
    return total;
  }

  @Override public String toString() {
    return "DimensionPartitionPlan{contextKey=" + contextKey
        + ", partitions=" + contextValues.size()
        + ", customDim=" + customDimName
        + ", totalPrefix=" + getTotalPrefixCount() + "}";
  }
}
