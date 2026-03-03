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
 * A partition of dimension combinations grouped by the context key of a CUSTOM dimension.
 *
 * <p>When CUSTOM dimensions depend on a context key (e.g., {@code ori} depends on
 * {@code state_abbr}), the full Cartesian product can be enormous (millions of combinations).
 * Instead of materializing all combinations at once, {@link DimensionIterator#expandByPartition}
 * produces one {@code DimensionPartition} per distinct context value, keeping only one
 * partition's combinations in memory at a time.
 *
 * @see DimensionIterator#expandByPartition(Map)
 */
public class DimensionPartition {

  private final Map<String, String> partitionContext;
  private final List<Map<String, String>> combinations;

  /**
   * Creates a dimension partition.
   *
   * @param partitionContext The context values that define this partition
   *                         (e.g., {state_abbr: "CA"})
   * @param combinations The expanded combinations for this partition
   */
  public DimensionPartition(Map<String, String> partitionContext,
      List<Map<String, String>> combinations) {
    this.partitionContext = Collections.unmodifiableMap(partitionContext);
    this.combinations = combinations;
  }

  /**
   * Returns the context values that define this partition.
   */
  public Map<String, String> getPartitionContext() {
    return partitionContext;
  }

  /**
   * Returns the expanded combinations for this partition.
   */
  public List<Map<String, String>> getCombinations() {
    return combinations;
  }

  @Override public String toString() {
    return "DimensionPartition{context=" + partitionContext
        + ", combinations=" + combinations.size() + "}";
  }
}
