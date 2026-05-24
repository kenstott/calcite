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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.adapter.file.execution.linq4j.JsonEnumerator;
import org.apache.calcite.adapter.file.execution.linq4j.JsonEnumerator.JsonDataConverter;
import org.apache.calcite.adapter.file.format.json.JsonSearchConfig;
import org.apache.calcite.adapter.file.format.json.SharedJsonData;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;

import com.fasterxml.jackson.databind.JsonNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Table based on a JSON file.
 */
public class JsonTable extends AbstractTable {
  private final Source source;
  private final SharedJsonData sharedData;
  private final String jsonPath;
  @SuppressWarnings("UnusedVariable")
  private final JsonSearchConfig config;
  private @Nullable RelDataType rowType;
  protected @Nullable List<Object> dataList;
  protected final Map<String, Object> options;
  protected final String columnNameCasing;
  private final @Nullable String forcedDataType;
  private @Nullable JsonDataConverter cachedConverter;
  private long lastModifiedTime = -1;

  public JsonTable(Source source) {
    this(source, null, "UNCHANGED");
  }

  public JsonTable(Source source, Map<String, Object> options) {
    this(source, options, "UNCHANGED");
  }

  public JsonTable(Source source, Map<String, Object> options, String columnNameCasing) {
    this(source, options, columnNameCasing, null);
  }

  public JsonTable(Source source, Map<String, Object> options, String columnNameCasing,
      @Nullable String forcedDataType) {
    this.source = source;
    this.sharedData = null;
    this.jsonPath = null;
    this.config = null;
    this.options = options;
    this.columnNameCasing = columnNameCasing;
    this.forcedDataType = forcedDataType;
  }

  /**
   * Constructor for path-specific table using shared JSON data.
   *
   * @param sharedData The shared parsed JSON data
   * @param jsonPath The JSONPath to extract data from
   * @param config The JSON search configuration
   */
  public JsonTable(SharedJsonData sharedData, String jsonPath, JsonSearchConfig config) {
    this(sharedData, jsonPath, config, "UNCHANGED");
  }

  /**
   * Constructor for path-specific table using shared JSON data with column casing.
   *
   * @param sharedData The shared parsed JSON data
   * @param jsonPath The JSONPath to extract data from
   * @param config The JSON search configuration
   * @param columnNameCasing The column name casing strategy
   */
  public JsonTable(SharedJsonData sharedData, String jsonPath, JsonSearchConfig config, String columnNameCasing) {
    this.source = null;
    this.sharedData = sharedData;
    this.jsonPath = jsonPath;
    this.config = config;
    this.options = config.getOptions();
    this.columnNameCasing = columnNameCasing;
    this.forcedDataType = null;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      if (source == null && sharedData != null) {
        rowType = deduceRowTypeFromSharedData(typeFactory);
      } else {
        if (cachedConverter == null) {
          cachedConverter = deduceRowTypeFromSource(typeFactory);
        }
        rowType = cachedConverter.getRelDataType();
      }
    }
    return rowType;
  }

  /** Returns the data list of the table. */
  public List<Object> getDataList(RelDataTypeFactory typeFactory) {
    if (source == null && sharedData != null) {
      if (dataList == null) {
        dataList = buildDataListFromSharedData(typeFactory);
      }
      return dataList;
    }

    // Check if we need to invalidate cache due to file changes
    boolean needsRefresh = false;
    if (source != null) {
      File file = source.file();
      if (file != null && file.exists()) {
        long currentModified = file.lastModified();
        if (lastModifiedTime == -1) {
          lastModifiedTime = currentModified;
        } else if (currentModified > lastModifiedTime) {
          needsRefresh = true;
          lastModifiedTime = currentModified;
        }
      }
    }

    if (dataList == null || needsRefresh) {
      if (needsRefresh) {
        // Clear cached data
        dataList = null;
        cachedConverter = null;
        rowType = null;
      }

      if (cachedConverter == null) {
        cachedConverter = deduceRowTypeFromSource(typeFactory);
      }
      dataList = cachedConverter.getDataList();
    }
    return dataList;
  }


  /**
   * Deduce row type from source, using forced data type if available.
   */
  private JsonDataConverter deduceRowTypeFromSource(RelDataTypeFactory typeFactory) {
    if (forcedDataType != null) {
      return JsonEnumerator.deduceRowType(typeFactory, source, forcedDataType, options, columnNameCasing);
    }
    return JsonEnumerator.deduceRowType(typeFactory, source, options, columnNameCasing);
  }

  /**
   * Deduce the row type from SharedJsonData at the configured jsonPath.
   * Inspects the first element of the array at the path to determine field names and types.
   */
  private RelDataType deduceRowTypeFromSharedData(RelDataTypeFactory typeFactory) {
    JsonNode pathData = sharedData.getDataAtPath(jsonPath);
    if (pathData == null || !pathData.isArray() || pathData.size() == 0) {
      // Return empty struct type
      return typeFactory.createStructType(
          new ArrayList<Map.Entry<String, RelDataType>>());
    }

    // Collect field names from the first element, scan multiple rows for types
    Set<String> allFieldNames = new LinkedHashSet<>();
    Map<String, Class<?>> columnTypes = new LinkedHashMap<>();
    int rowsToScan = Math.min(10, pathData.size());

    for (int i = 0; i < rowsToScan; i++) {
      JsonNode element = pathData.get(i);
      if (!element.isObject()) {
        continue;
      }
      Iterator<String> fieldNames = element.fieldNames();
      while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        allFieldNames.add(fieldName);
        if (!columnTypes.containsKey(fieldName)) {
          JsonNode fieldNode = element.get(fieldName);
          if (fieldNode != null && !fieldNode.isNull()) {
            columnTypes.put(fieldName, inferJavaType(fieldNode));
          }
        }
      }
    }

    List<String> names = new ArrayList<>();
    List<RelDataType> types = new ArrayList<>();
    for (String fieldName : allFieldNames) {
      Class<?> clazz = columnTypes.containsKey(fieldName) ? columnTypes.get(fieldName) : String.class;
      names.add(fieldName);
      types.add(typeFactory.createJavaType(clazz));
    }

    return typeFactory.createStructType(Pair.zip(names, types));
  }

  /**
   * Build a data list from SharedJsonData at the configured jsonPath.
   * Converts each JSON object to a LinkedHashMap matching the deduced field order.
   */
  @SuppressWarnings("UnusedVariable")
  private List<Object> buildDataListFromSharedData(RelDataTypeFactory typeFactory) {
    JsonNode pathData = sharedData.getDataAtPath(jsonPath);
    if (pathData == null || !pathData.isArray()) {
      return new ArrayList<>();
    }

    List<Object> result = new ArrayList<>();
    for (JsonNode element : pathData) {
      if (element.isObject()) {
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = element.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> field = fields.next();
          row.put(field.getKey(), convertJsonValue(field.getValue()));
        }
        result.add(row);
      }
    }
    return result;
  }

  private static Class<?> inferJavaType(JsonNode node) {
    if (node.isBoolean()) {
      return Boolean.class;
    } else if (node.isIntegralNumber()) {
      return Integer.class;
    } else if (node.isNumber()) {
      return Double.class;
    } else {
      return String.class;
    }
  }

  private static Object convertJsonValue(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    } else if (node.isBoolean()) {
      return node.asBoolean();
    } else if (node.isIntegralNumber()) {
      return node.asInt();
    } else if (node.isNumber()) {
      return node.asDouble();
    } else if (node.isTextual()) {
      return node.asText();
    } else {
      return node.toString();
    }
  }

  @Override public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }
}
