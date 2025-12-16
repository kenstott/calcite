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

/**
 * Transforms raw API response before parsing.
 *
 * <p>ResponseTransformer is called after an HTTP fetch completes but before
 * the response is parsed. This allows adapters to normalize API-specific
 * response formats, handle errors embedded in successful responses, or
 * extract data from nested structures.
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * public class BeaResponseTransformer implements ResponseTransformer {
 *     public String transform(String response, RequestContext context) {
 *         // Handle BEA-specific response quirks
 *         JsonNode root = parse(response);
 *         if (root.has("BEAAPI") && root.path("BEAAPI").has("Error")) {
 *             throw new ApiException(root.path("BEAAPI").path("Error").asText());
 *         }
 *         return root.path("BEAAPI").path("Results").path("Data").toString();
 *     }
 * }
 * }</pre>
 *
 * <h3>Schema Configuration</h3>
 * <pre>{@code
 * hooks:
 *   responseTransformer: "org.apache.calcite.adapter.govdata.BeaResponseTransformer"
 * }</pre>
 *
 * <h3>Discovery</h3>
 * <p>ResponseTransformer implementations can be discovered via:
 * <ul>
 *   <li>Schema reference: Fully qualified class name in YAML</li>
 *   <li>Java SPI: META-INF/services/org.apache.calcite.adapter.file.etl.ResponseTransformer</li>
 * </ul>
 *
 * @see RequestContext
 * @see HooksConfig
 */
public interface ResponseTransformer {

  /**
   * Transforms the raw API response.
   *
   * @param response Raw API response (typically JSON string)
   * @param context Request context including URL, parameters, headers, and dimension values
   * @return Transformed response ready for parsing
   * @throws RuntimeException if the response indicates an error condition
   */
  String transform(String response, RequestContext context);
}
