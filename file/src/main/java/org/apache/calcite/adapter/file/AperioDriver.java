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
package org.apache.calcite.adapter.file;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver for the Aperio file adapter.
 *
 * <p>Accepts {@code jdbc:aperio:} URLs, parses path and query parameters,
 * builds an inline Calcite model targeting {@link FileSchemaFactory},
 * and delegates to the Calcite JDBC driver.
 *
 * <p>URL formats:
 * <ul>
 *   <li>{@code jdbc:aperio:/path/to/data} — local directory</li>
 *   <li>{@code jdbc:aperio:/path?executionEngine=duckdb&recursive=true} — with params</li>
 *   <li>{@code jdbc:aperio:model=/path/to/model.json} — pass-through to Calcite model file</li>
 * </ul>
 */
public class AperioDriver extends org.apache.calcite.jdbc.Driver {

    private static final Logger LOGGER = Logger.getLogger(AperioDriver.class.getName());

    static {
        new AperioDriver().register();
    }

    @Override protected String getConnectStringPrefix() {
        return "jdbc:aperio:";
    }

    @Override public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        String remainder = url.substring(getConnectStringPrefix().length());

        if (remainder.startsWith("model=")) {
            return super.connect("jdbc:calcite:model=" + remainder.substring(6), info);
        }

        String path;
        Properties params = new Properties(info);

        int queryIndex = remainder.indexOf('?');
        if (queryIndex != -1) {
            path = remainder.substring(0, queryIndex);
            parseQueryParams(remainder.substring(queryIndex + 1), params);
        } else {
            path = remainder;
        }

        String model = buildModel(path, params);
        return super.connect("jdbc:calcite:model=inline:" + model, params);
    }

    private void parseQueryParams(String queryString, Properties props) {
        if (queryString == null || queryString.isEmpty()) {
            return;
        }
        for (String pair : queryString.split("&")) {
            int idx = pair.indexOf('=');
            if (idx > 0) {
                try {
                    String key = URLDecoder.decode(pair.substring(0, idx), "UTF-8");
                    String value = idx < pair.length() - 1
                        ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8")
                        : "";
                    props.setProperty(key, value);
                } catch (UnsupportedEncodingException e) {
                    LOGGER.warning("Failed to decode query parameter: " + pair);
                }
            }
        }
    }

    private String buildModel(String path, Properties info) throws SQLException {
        try {
            Map<String, Object> operand = buildOperand(path, info);

            Map<String, Object> schema = new HashMap<String, Object>();
            schema.put("name", info.getProperty("schema", "files"));
            schema.put("type", "custom");
            schema.put("factory", "org.apache.calcite.adapter.file.FileSchemaFactory");
            schema.put("operand", operand);

            List<Map<String, Object>> schemas = new ArrayList<Map<String, Object>>();
            schemas.add(schema);

            Map<String, Object> model = new HashMap<String, Object>();
            model.put("version", "1.0");
            model.put("defaultSchema", info.getProperty("schema", "files"));
            model.put("schemas", schemas);

            return new ObjectMapper().writeValueAsString(model);
        } catch (Exception e) {
            throw new SQLException("Failed to create Calcite model: " + e.getMessage(), e);
        }
    }

    private Map<String, Object> buildOperand(String path, Properties info) {
        Map<String, Object> operand = new HashMap<String, Object>();

        // Directory path
        if (path.startsWith("//")) {
            String[] parts = path.substring(2).split("/", 2);
            if (parts.length > 1) {
                operand.put("directory", "/" + parts[1]);
            } else {
                operand.put("directory", System.getProperty("user.dir"));
            }
        } else if (!path.isEmpty()) {
            operand.put("directory", path);
        } else {
            operand.put("directory", System.getProperty("user.dir"));
        }

        // Execution engine
        String engine =
            info.getProperty("executionEngine", System.getenv("APERIO_EXECUTION_ENGINE") != null
                ? System.getenv("APERIO_EXECUTION_ENGINE")
                : "duckdb");
        operand.put("executionEngine", engine);

        // DuckDB config
        if ("duckdb".equalsIgnoreCase(engine)) {
            Map<String, Object> duckdbConfig = new HashMap<String, Object>();
            addStringFromEnv(info, "duckdbMemoryLimit", "DUCKDB_MEMORY_LIMIT", duckdbConfig, "memory_limit");
            addIntFromEnv(info, "duckdbThreads", "DUCKDB_THREADS", duckdbConfig, "threads");
            addStringFromEnv(info, "duckdbTempDirectory", "DUCKDB_TEMP_DIRECTORY", duckdbConfig, "temp_directory");
            if (!duckdbConfig.isEmpty()) {
                operand.put("duckdbConfig", duckdbConfig);
            }
        }

        // Core properties
        addIntFromEnv(info, "batchSize", "APERIO_BATCH_SIZE", operand, "batchSize");
        addLongFromEnv(info, "memoryThreshold", "APERIO_MEMORY_THRESHOLD", operand, "memoryThreshold");
        addStringFromEnv(info, "spillDirectory", "APERIO_SPILL_DIRECTORY", operand, "spillDirectory");
        addBoolIfPresent(info, "recursive", operand);
        addBoolIfPresent(info, "multiTable", operand);
        addStringIfPresent(info, "directoryPattern", operand);

        // SQL config
        operand.put("lex", info.getProperty("lex", "ORACLE"));
        operand.put("unquotedCasing", info.getProperty("unquotedCasing", "TO_LOWER"));
        operand.put("quotedCasing", info.getProperty("quotedCasing", "UNCHANGED"));
        addBoolIfPresent(info, "caseSensitive", operand);
        addStringIfPresent(info, "conformance", operand);
        operand.put("tableNameCasing", info.getProperty("tableNameCasing", "SMART_CASING"));
        operand.put("columnNameCasing", info.getProperty("columnNameCasing", "SMART_CASING"));
        addStringIfPresent(info, "refreshInterval", operand);
        addStringIfPresent(info, "encoding", operand);

        // CSV type inference
        String csvInferTypes = info.getProperty("csvInferTypes", System.getenv("APERIO_CSV_INFER_TYPES"));
        if ("true".equalsIgnoreCase(csvInferTypes)) {
            Map<String, Object> csvConfig = new HashMap<String, Object>();
            csvConfig.put("enabled", true);
            addDoubleFromEnv(info, "csvSamplingRate", "CSV_SAMPLING_RATE", csvConfig, "samplingRate");
            addIntFromEnv(info, "csvMaxSampleRows", "CSV_MAX_SAMPLE_ROWS", csvConfig, "maxSampleRows");
            String inferDates = info.getProperty("csvInferDates", System.getenv("CSV_INFER_DATES"));
            if (inferDates != null) {
                csvConfig.put("inferDates", "true".equalsIgnoreCase(inferDates));
            }
            operand.put("csvTypeInference", csvConfig);
        }

        // Statistics and caching
        addBoolIfPresent(info, "enableStatistics", operand);
        addStringIfPresent(info, "statisticsCache", operand);
        addBoolIfPresent(info, "cacheEnabled", operand);
        addIntIfPresent(info, "maxConcurrency", operand);

        // Storage config
        String storageType = info.getProperty("storageType", System.getenv("APERIO_STORAGE_TYPE"));
        if (storageType != null) {
            operand.put("storageType", storageType);
            if ("s3".equalsIgnoreCase(storageType)) {
                Map<String, Object> s3 = new HashMap<String, Object>();
                addStringFromEnv(info, "s3Bucket", "S3_BUCKET", s3, "bucket");
                String region =
                    info.getProperty("s3Region", System.getenv("AWS_REGION") != null ? System.getenv("AWS_REGION") : System.getenv("AWS_DEFAULT_REGION"));
                if (region != null) {
                    s3.put("region", region);
                }
                addStringFromEnv(info, "s3AccessKey", "AWS_ACCESS_KEY_ID", s3, "accessKey");
                addStringFromEnv(info, "s3SecretKey", "AWS_SECRET_ACCESS_KEY", s3, "secretKey");
                if (!s3.isEmpty()) {
                    operand.put("storageConfig", s3);
                }
            } else if ("http".equalsIgnoreCase(storageType) || "https".equalsIgnoreCase(storageType)) {
                Map<String, Object> http = new HashMap<String, Object>();
                addStringFromEnv(info, "httpBaseUrl", "APERIO_HTTP_BASE_URL", http, "baseUrl");
                addStringFromEnv(info, "httpAuthType", "APERIO_HTTP_AUTH_TYPE", http, "authType");
                addStringFromEnv(info, "httpAuthToken", "APERIO_HTTP_AUTH_TOKEN", http, "authToken");
                if (!http.isEmpty()) {
                    operand.put("storageConfig", http);
                }
            }
        }

        // Iceberg
        String icebergEnabled = info.getProperty("icebergEnabled", System.getenv("APERIO_ICEBERG_ENABLED"));
        if ("true".equalsIgnoreCase(icebergEnabled)) {
            Map<String, Object> iceberg = new HashMap<String, Object>();
            iceberg.put("enabled", true);
            addStringFromEnv(info, "icebergCatalogType", "ICEBERG_CATALOG_TYPE", iceberg, "catalogType");
            addStringFromEnv(info, "icebergWarehouse", "ICEBERG_WAREHOUSE", iceberg, "warehouse");
            operand.put("icebergConfig", iceberg);
        }

        // File filtering
        addStringFromEnv(info, "filePattern", "APERIO_FILE_PATTERN", operand, "filePattern");
        addStringFromEnv(info, "excludePattern", "APERIO_EXCLUDE_PATTERN", operand, "excludePattern");

        // Parallelism
        addIntFromEnv(info, "parallelism", "APERIO_PARALLELISM", operand, "parallelism");
        addIntFromEnv(info, "fetchSize", "APERIO_FETCH_SIZE", operand, "fetchSize");

        return expandEnvVars(operand);
    }

    // ── helper methods ────────────────────────────────────────────────────────

    private void addStringIfPresent(Properties info, String key, Map<String, Object> target) {
        if (info.containsKey(key)) {
            target.put(key, info.getProperty(key));
        }
    }

    private void addBoolIfPresent(Properties info, String key, Map<String, Object> target) {
        if (info.containsKey(key)) {
            target.put(key, Boolean.parseBoolean(info.getProperty(key)));
        }
    }

    private void addIntIfPresent(Properties info, String key, Map<String, Object> target) {
        if (info.containsKey(key)) {
            try {
                target.put(key, Integer.parseInt(info.getProperty(key)));
            } catch (NumberFormatException e) {
                LOGGER.warning("Invalid integer value for " + key + ": " + info.getProperty(key));
            }
        }
    }

    private void addStringFromEnv(Properties info, String prop, String envVar,
                                   Map<String, Object> target, String targetKey) {
        String value = info.getProperty(prop, System.getenv(envVar));
        if (value != null) {
            target.put(targetKey, value);
        }
    }

    private void addIntFromEnv(Properties info, String prop, String envVar,
                                Map<String, Object> target, String targetKey) {
        String value = info.getProperty(prop, System.getenv(envVar));
        if (value != null) {
            try {
                target.put(targetKey, Integer.parseInt(value));
            } catch (NumberFormatException e) {
                LOGGER.warning("Invalid integer value for " + prop + ": " + value);
            }
        }
    }

    private void addLongFromEnv(Properties info, String prop, String envVar,
                                 Map<String, Object> target, String targetKey) {
        String value = info.getProperty(prop, System.getenv(envVar));
        if (value != null) {
            try {
                target.put(targetKey, Long.parseLong(value));
            } catch (NumberFormatException e) {
                LOGGER.warning("Invalid long value for " + prop + ": " + value);
            }
        }
    }

    private void addDoubleFromEnv(Properties info, String prop, String envVar,
                                   Map<String, Object> target, String targetKey) {
        String value = info.getProperty(prop, System.getenv(envVar));
        if (value != null) {
            try {
                target.put(targetKey, Double.parseDouble(value));
            } catch (NumberFormatException e) {
                LOGGER.warning("Invalid double value for " + prop + ": " + value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> expandEnvVars(Map<String, Object> map) {
        Map<String, Object> expanded = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                expanded.put(entry.getKey(), expandVarString((String) value));
            } else if (value instanceof Map) {
                expanded.put(entry.getKey(), expandEnvVars((Map<String, Object>) value));
            } else {
                expanded.put(entry.getKey(), value);
            }
        }
        return expanded;
    }

    private String expandVarString(String value) {
        if (value == null) {
            return null;
        }
        String result = value;
        int start = 0;
        while ((start = result.indexOf("${", start)) != -1) {
            int end = result.indexOf("}", start);
            if (end == -1) {
                break;
            }
            String varName = result.substring(start + 2, end);
            String envValue = System.getenv(varName);
            if (envValue == null) {
                envValue = System.getProperty(varName, "${" + varName + "}");
            }
            result = result.substring(0, start) + envValue + result.substring(end + 1);
            start += envValue.length();
        }
        return result;
    }
}
