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
