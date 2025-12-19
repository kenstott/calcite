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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Context information passed to {@link ResponseTransformer} during response transformation.
 *
 * <p>RequestContext provides access to the original request details including the URL,
 * query parameters, headers, and dimension values that were used to make the HTTP request.
 * This allows transformers to make context-aware decisions during transformation.
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * public class MyResponseTransformer implements ResponseTransformer {
 *     public String transform(String response, RequestContext context) {
 *         String year = context.getDimensionValues().get("year");
 *         String url = context.getUrl();
 *         // Transform based on context
 *         return transformedResponse;
 *     }
 * }
 * }</pre>
 *
 * @see ResponseTransformer
 */
public class RequestContext {

  private final String url;
  private final Map<String, String> parameters;
  private final Map<String, String> headers;
  private final Map<String, String> dimensionValues;

  private RequestContext(Builder builder) {
    this.url = builder.url;
    this.parameters = builder.parameters != null
        ? Collections.unmodifiableMap(new LinkedHashMap<String, String>(builder.parameters))
        : Collections.<String, String>emptyMap();
    this.headers = builder.headers != null
        ? Collections.unmodifiableMap(new LinkedHashMap<String, String>(builder.headers))
        : Collections.<String, String>emptyMap();
    this.dimensionValues = builder.dimensionValues != null
        ? Collections.unmodifiableMap(new LinkedHashMap<String, String>(builder.dimensionValues))
        : Collections.<String, String>emptyMap();
  }

  /**
   * Returns the URL that was requested.
   *
   * @return The request URL
   */
  public String getUrl() {
    return url;
  }

  /**
   * Returns the query parameters used in the request.
   *
   * @return Unmodifiable map of parameter name to value
   */
  public Map<String, String> getParameters() {
    return parameters;
  }

  /**
   * Returns the headers used in the request.
   *
   * @return Unmodifiable map of header name to value
   */
  public Map<String, String> getHeaders() {
    return headers;
  }

  /**
   * Returns the dimension values that were substituted into the request.
   *
   * @return Unmodifiable map of dimension name to value
   */
  public Map<String, String> getDimensionValues() {
    return dimensionValues;
  }

  /**
   * Creates a new builder for RequestContext.
   *
   * @return A new Builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("RequestContext{url='").append(url).append("'");
    if (!parameters.isEmpty()) {
      sb.append(", parameters=").append(parameters);
    }
    if (!headers.isEmpty()) {
      sb.append(", headers=").append(headers);
    }
    if (!dimensionValues.isEmpty()) {
      sb.append(", dimensionValues=").append(dimensionValues);
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Builder for RequestContext.
   */
  public static class Builder {
    private String url;
    private Map<String, String> parameters;
    private Map<String, String> headers;
    private Map<String, String> dimensionValues;

    /**
     * Sets the request URL.
     *
     * @param url The URL
     * @return This builder
     */
    public Builder url(String url) {
      this.url = url;
      return this;
    }

    /**
     * Sets the request parameters.
     *
     * @param parameters Map of parameter name to value
     * @return This builder
     */
    public Builder parameters(Map<String, String> parameters) {
      this.parameters = parameters;
      return this;
    }

    /**
     * Sets the request headers.
     *
     * @param headers Map of header name to value
     * @return This builder
     */
    public Builder headers(Map<String, String> headers) {
      this.headers = headers;
      return this;
    }

    /**
     * Sets the dimension values used in the request.
     *
     * @param dimensionValues Map of dimension name to value
     * @return This builder
     */
    public Builder dimensionValues(Map<String, String> dimensionValues) {
      this.dimensionValues = dimensionValues;
      return this;
    }

    /**
     * Builds the RequestContext.
     *
     * @return A new RequestContext instance
     */
    public RequestContext build() {
      return new RequestContext(this);
    }
  }
}
