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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import java.util.List;
import java.util.Map;

/**
 * Custom dimension value resolution with context support.
 *
 * <p>DimensionResolver allows adapters to implement custom logic for resolving
 * dimension values beyond the built-in types (range, list, query, yearRange).
 * This is useful for:
 * <ul>
 *   <li>Fetching dimension values from external APIs</li>
 *   <li>Computing dimension values based on runtime state</li>
 *   <li>Filtering dimension values based on availability</li>
 *   <li>Generating dimension values from complex business rules</li>
 *   <li><b>Dependent dimensions</b> - values that depend on other dimension values</li>
 * </ul>
 *
 * <h3>Context-Aware Resolution</h3>
 * <p>The resolver receives a context map containing the current values of
 * previously-resolved dimensions. This enables dependent dimension patterns:
 *
 * <pre>{@code
 * public class BeaDimensionResolver implements DimensionResolver {
 *     public List<String> resolve(String dimensionName, DimensionConfig config,
 *             Map<String, String> context) {
 *         if ("line_code".equals(dimensionName)) {
 *             // Get valid line codes for the current tablename
 *             String tablename = context.get("tablename");
 *             return fetchValidLineCodes(tablename);
 *         }
 *         return Collections.emptyList();
 *     }
 * }
 * }</pre>
 *
 * <h3>Schema Configuration</h3>
 * <pre>{@code
 * hooks:
 *   dimensionResolver: "org.apache.calcite.adapter.govdata.econ.BeaDimensionResolver"
 *
 * dimensions:
 *   tablename:
 *     - SAINC1
 *     - SAINC30
 *   line_code:
 *     type: custom  # resolved per tablename using context
 * }</pre>
 *
 * @see DimensionConfig
 * @see DimensionIterator
 * @see HooksConfig
 */
public interface DimensionResolver {

  /**
   * Resolves dimension values with context from other dimensions.
   *
   * <p>This method is called by {@link DimensionIterator} when expanding
   * dimensions. The implementation should return a list of string values
   * that will be used to generate batch combinations.
   *
   * <p>The context map contains the current values of dimensions that were
   * resolved before this one. This enables dependent dimension patterns where
   * valid values depend on previously selected values.
   *
   * @param dimensionName Name of the dimension being resolved
   * @param config Dimension configuration from schema (may contain custom properties)
   * @param context Map of already-resolved dimension names to their current values
   * @param storageProvider Storage provider for accessing files (local or S3)
   * @return List of dimension values to iterate, never null (may be empty)
   */
  List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider);
}
