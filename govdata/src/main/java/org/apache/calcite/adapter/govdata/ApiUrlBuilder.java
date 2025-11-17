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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Builder for constructing API URLs with query parameters.
 * Handles URL encoding and parameter management for government data APIs.
 */
public class ApiUrlBuilder {
    private final String baseUrl;
    private final Map<String, String> params = new LinkedHashMap<>();

    public ApiUrlBuilder(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    /**
     * Add a parameter to the URL.
     * Null or empty values are ignored.
     *
     * @param key Parameter name
     * @param value Parameter value
     * @return This builder for chaining
     */
    public ApiUrlBuilder param(String key, String value) {
        if (value != null && !value.isEmpty()) {
            params.put(key, value);
        }
        return this;
    }

    /**
     * Add an API key parameter.
     *
     * @param apiKey The API key value
     * @return This builder for chaining
     */
    public ApiUrlBuilder apiKey(String apiKey) {
        return param("api_key", apiKey);
    }

    /**
     * Add all parameters from a map.
     *
     * @param parameters Map of parameter names to values
     * @return This builder for chaining
     */
    public ApiUrlBuilder params(Map<String, String> parameters) {
        if (parameters != null) {
            parameters.forEach(this::param);
        }
        return this;
    }

    /**
     * Build the final URL with encoded parameters.
     *
     * @return Complete URL with query string
     */
    public String build() {
        if (params.isEmpty()) {
            return baseUrl;
        }

        String queryString = params.entrySet().stream()
            .map(e -> encode(e.getKey()) + "=" + encode(e.getValue()))
            .collect(Collectors.joining("&"));

        return baseUrl + "?" + queryString;
    }

    private String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
