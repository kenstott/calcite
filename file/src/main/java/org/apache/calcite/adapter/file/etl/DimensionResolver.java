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

import java.util.List;

/**
 * Custom dimension value resolution.
 *
 * <p>DimensionResolver allows adapters to implement custom logic for resolving
 * dimension values beyond the built-in types (range, list, query, yearRange).
 * This is useful for:
 * <ul>
 *   <li>Fetching dimension values from external APIs</li>
 *   <li>Computing dimension values based on runtime state</li>
 *   <li>Filtering dimension values based on availability</li>
 *   <li>Generating dimension values from complex business rules</li>
 * </ul>
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * public class ActiveRegionResolver implements DimensionResolver {
 *     public List<String> resolve(String dimensionName, DimensionConfig config) {
 *         // Fetch active regions from an API or database
 *         return fetchActiveRegions();
 *     }
 * }
 * }</pre>
 *
 * <h3>Schema Configuration</h3>
 * <pre>{@code
 * hooks:
 *   dimensionResolver: "org.apache.calcite.adapter.govdata.ActiveRegionResolver"
 *
 * dimensions:
 *   region:
 *     type: custom  # triggers custom resolver
 *     resolverConfig:
 *       apiEndpoint: "https://api.example.com/regions"
 * }</pre>
 *
 * @see DimensionConfig
 * @see DimensionIterator
 * @see HooksConfig
 */
public interface DimensionResolver {

  /**
   * Resolves dimension values.
   *
   * <p>This method is called by {@link DimensionIterator} when expanding
   * dimensions. The implementation should return a list of string values
   * that will be used to generate batch combinations.
   *
   * @param dimensionName Name of the dimension being resolved
   * @param config Dimension configuration from schema (may contain custom properties)
   * @return List of dimension values to iterate, never null (may be empty)
   */
  List<String> resolve(String dimensionName, DimensionConfig config);
}
