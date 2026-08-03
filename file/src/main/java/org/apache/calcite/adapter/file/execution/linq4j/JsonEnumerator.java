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
package org.apache.calcite.adapter.file.execution.linq4j;

import org.apache.calcite.adapter.file.cache.SourceFileLockManager;
import org.apache.calcite.adapter.file.format.json.JsonFlattener;
import org.apache.calcite.adapter.file.util.ComparableArrayList;
import org.apache.calcite.adapter.file.util.ComparableLinkedHashMap;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.trace.CalciteLogger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Enumerator that reads from a Object List.
 */
public class JsonEnumerator implements Enumerator<@Nullable Object[]> {
  private static final CalciteLogger LOGGER =
      new CalciteLogger(LoggerFactory.getLogger(JsonEnumerator.class));

  private final Enumerator<@Nullable Object[]> enumerator;

  public JsonEnumerator(List<? extends @Nullable Object> list) {
    List<@Nullable Object[]> objs = new ArrayList<>();
    for (Object obj : list) {
      if (obj instanceof Collection) {
        //noinspection unchecked
        List<Object> tmp = (List<Object>) obj;
        objs.add(tmp.toArray());
      } else if (obj instanceof Map) {
        Map<String, Object> map = (Map<String, Object>) obj;
        // For Map objects, preserve the natural order of LinkedHashMap
        // which should match the column order determined during type deduction
        Object[] values = map.values().toArray();
        objs.add(values);
      } else {
        objs.add(new Object[]{obj});
      }
    }
    enumerator = Linq4j.enumerator(objs);
  }

  public static void replaceArrayLists(Map<String, Object> map) throws IllegalAccessException {
    Map<String, Object> replacements = new LinkedHashMap<String, Object>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      if (value instanceof ArrayList) {
        ArrayList listValue = (ArrayList) value;
        ComparableArrayList comparableList = new ComparableArrayList();
        comparableList.addAll(listValue);
        replacements.put(key, comparableList);
      } else if (value instanceof LinkedHashMap) {
        LinkedHashMap listValue = (LinkedHashMap) value;
        ComparableLinkedHashMap comparableList = new ComparableLinkedHashMap();
        comparableList.putAll(listValue);
        replacements.put(key, comparableList);
      } else if (value instanceof Map) {
        replaceArrayLists((Map<String, Object>) value);
      }
    }
    map.putAll(replacements);
  }

  public static JsonDataConverter deduceRowType(RelDataTypeFactory typeFactory, Source source) {
    return deduceRowType(typeFactory, source, (Map<String, Object>) null);
  }


  public static JsonDataConverter deduceRowType(RelDataTypeFactory typeFactory, Source source,
      Map<String, Object> options) {
    return deduceRowType(typeFactory, source, options, "UNCHANGED");
  }

  public static JsonDataConverter deduceRowType(RelDataTypeFactory typeFactory, Source source,
      Map<String, Object> options, String columnNameCasing) {
    Source sourceSansGz = source.trim(".gz");
    Source sourceSansJson = sourceSansGz.trimOrNull(".json");
    Source sourceSansYaml = sourceSansGz.trimOrNull(".yaml");
    if (sourceSansYaml == null) {
      sourceSansYaml = sourceSansGz.trimOrNull(".yml");
    }
    if (sourceSansYaml == null) {
      sourceSansYaml = sourceSansGz.trimOrNull(".hml");
    }
    if (sourceSansJson != null) {
      return deduceRowType(typeFactory, source, "json", options, columnNameCasing);
    } else if (sourceSansYaml != null) {
      return deduceRowType(typeFactory, source, "yaml", options, columnNameCasing);
    } else if (source instanceof org.apache.calcite.adapter.file.storage.StorageProviderSource) {
      // StorageProviderSource (HTTP) without a recognizable extension: default to JSON
      return deduceRowType(typeFactory, source, "json", options, columnNameCasing);
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + source);
    }
  }

  public static JsonDataConverter deduceRowType(RelDataTypeFactory typeFactory, Source source,
          String dataType) {
    return deduceRowType(typeFactory, source, dataType, (Map<String, Object>) null);
  }

  public static JsonDataConverter deduceRowType(RelDataTypeFactory typeFactory, Source source,
          String dataType, String columnNameCasing) {
    return deduceRowType(typeFactory, source, dataType, (Map<String, Object>) null, columnNameCasing);
  }

  /**
   * Deduces the names and types of a table's columns by reading the first line
   * of a JSON file.
   */
  public static JsonDataConverter deduceRowType(RelDataTypeFactory typeFactory, Source source,
          String dataType, Map<String, Object> options) {
    return deduceRowType(typeFactory, source, dataType, options, "UNCHANGED");
  }

  public static JsonDataConverter deduceRowType(RelDataTypeFactory typeFactory, Source source,
          String dataType, Map<String, Object> options, String columnNameCasing) {
    ObjectMapper jsonMapper = new ObjectMapper();
    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
//    yamlMapper.findAndRegisterModules();
    List<Object> list;
    LinkedHashMap<String, Object> jsonFieldMap = new LinkedHashMap<>(1);
    Object jsonObj = null;
    try {
      jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
          .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
          .configure(JsonParser.Feature.ALLOW_COMMENTS, true);

      ObjectMapper selectedMapper;
      if ("json".equalsIgnoreCase(dataType)) {
        selectedMapper = jsonMapper;
      } else if ("yaml".equalsIgnoreCase(dataType)) {
        selectedMapper = yamlMapper;
      } else {
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
      }

      if (source instanceof org.apache.calcite.adapter.file.storage.StorageProviderSource) {
        // Handle StorageProviderSource by using its reader method
        //noinspection unchecked
        jsonObj = selectedMapper.readValue(source.reader(), Object.class);
      } else if ("file".equals(source.protocol()) && source.file().exists()) {
        // Acquire read lock on source file
        SourceFileLockManager.LockHandle lockHandle = null;
        try {
          lockHandle = SourceFileLockManager.acquireReadLock(source.file());
          LOGGER.debug("Acquired read lock on JSON file: " + source.path());
          //noinspection unchecked
          jsonObj = selectedMapper.readValue(source.reader(), Object.class);
        } catch (IOException lockException) {
          LOGGER.warn("Could not acquire lock on file: "
              + source.path()
              + " - proceeding without lock");
          // Proceed without lock
          //noinspection unchecked
          jsonObj = selectedMapper.readValue(source.reader(), Object.class);
        } finally {
          if (lockHandle != null) {
            lockHandle.close();
            LOGGER.debug("Released read lock on JSON file");
          }
        }
      } else if (Arrays.asList("http", "https", "ftp").contains(source.protocol())) {
        //noinspection unchecked
        jsonObj = selectedMapper.readValue(source.url(), Object.class);
      } else {
        jsonObj = selectedMapper.readValue(source.reader(), Object.class);
      }

      if (jsonObj instanceof ArrayList) {
        ArrayList<Map<String, Object>> l = (ArrayList<Map<String, Object>>) jsonObj;
        for (Map<String, Object> item : l) {
          replaceArrayLists(item);
        }
      }

    } catch (MismatchedInputException e) {
      if (!e.getMessage().contains("No content")) {
        throw new RuntimeException("Couldn't read " + source, e);
      }
    } catch (Exception e) {
      throw new RuntimeException("Couldn't read " + source, e);
    }

    if (jsonObj == null) {
      list = new ArrayList<>();
      jsonFieldMap.put("EmptyFileHasNoColumns", Boolean.TRUE);
    } else if (jsonObj instanceof Collection) {
      //noinspection unchecked
      list = (List<Object>) jsonObj;

      if (list.isEmpty()) {
        jsonFieldMap.put("EmptyCollectionHasNoColumns", Boolean.TRUE);
      } else {
        //noinspection unchecked
        jsonFieldMap = (LinkedHashMap) list.get(0);
      }

      // Apply flattening if requested
      if (options != null && Boolean.TRUE.equals(options.get("flatten"))) {
        String flattenSeparator = options.containsKey("flattenSeparator")
            ? (String) options.get("flattenSeparator") : "__";
        JsonFlattener flattener = new JsonFlattener(",", 3, "", flattenSeparator);
        jsonFieldMap = new LinkedHashMap<>(flattener.flatten(jsonFieldMap));
        // Flatten all rows in the list
        for (int i = 0; i < list.size(); i++) {
          if (list.get(i) instanceof Map) {
            list.set(i, flattener.flatten((Map<String, Object>) list.get(i)));
          }
        }
      }
    } else if (jsonObj instanceof Map) {
      //noinspection unchecked
      jsonFieldMap = (LinkedHashMap) jsonObj;
      // Apply flattening if requested
      if (options != null && Boolean.TRUE.equals(options.get("flatten"))) {
        String flattenSeparator = options.containsKey("flattenSeparator")
            ? (String) options.get("flattenSeparator") : "__";
        JsonFlattener flattener = new JsonFlattener(",", 3, "", flattenSeparator);
        jsonFieldMap = new LinkedHashMap<>(flattener.flatten(jsonFieldMap));
      }
      //noinspection unchecked
//      list = new ArrayList(((LinkedHashMap) jsonObj).values());
      list = new ArrayList();
      ((List) list).add(jsonFieldMap);
    } else {
      jsonFieldMap.put("line", jsonObj);
      list = new ArrayList<>();
      list.add(0, jsonObj);
    }

    // Scan up to 10 rows to determine column types (to handle nulls in first row)
    Map<String, Class<?>> columnTypes = new HashMap<>();
    // Candidate DATE/TIME/TIMESTAMP columns: every sampled String value for the column
    // parses as the same ISO 8601 kind. Disqualified (and removed) on the first sampled
    // value that isn't a String or doesn't match, or that matches a different ISO kind.
    Map<String, SqlTypeName> dateColumnKinds = new HashMap<>();
    Set<String> dateColumnDisqualified = new HashSet<>();
    int rowsToScan = Math.min(10, list.size());

    // First, collect all column names from the first row (or the jsonFieldMap for single objects)
    Set<String> allColumns = new LinkedHashSet<>(jsonFieldMap.keySet());

    // Scan rows to find non-null values for type inference
    for (int i = 0; i < rowsToScan; i++) {
      Object rowObj = list.get(i);
      Map<String, Object> row;

      if (rowObj instanceof Map) {
        //noinspection unchecked
        row = (Map<String, Object>) rowObj;
      } else {
        // Skip non-map rows
        continue;
      }

      for (String key : row.keySet()) {
        Object value = row.get(key);
        if (value == null) {
          continue;
        }
        if (!columnTypes.containsKey(key)) {
          columnTypes.put(key, value.getClass());
        }
        if (!dateColumnDisqualified.contains(key)) {
          if (value instanceof String) {
            SqlTypeName kind = detectIsoDateTimeKind((String) value);
            if (kind == null) {
              dateColumnDisqualified.add(key);
              dateColumnKinds.remove(key);
            } else {
              SqlTypeName existingKind = dateColumnKinds.get(key);
              if (existingKind == null) {
                dateColumnKinds.put(key, kind);
              } else if (existingKind != kind) {
                dateColumnDisqualified.add(key);
                dateColumnKinds.remove(key);
              }
            }
          } else {
            dateColumnDisqualified.add(key);
            dateColumnKinds.remove(key);
          }
        }
      }
    }

    // Promote every value in a confirmed date/time/timestamp column from its raw ISO string
    // to the internal representation Calcite expects for that SQL type (epoch day / millis of
    // day / epoch millis), across the whole list, not just the sampled rows.
    for (Map.Entry<String, SqlTypeName> entry : dateColumnKinds.entrySet()) {
      String key = entry.getKey();
      SqlTypeName kind = entry.getValue();
      for (Object rowObj : list) {
        if (!(rowObj instanceof Map)) {
          continue;
        }
        //noinspection unchecked
        Map<String, Object> row = (Map<String, Object>) rowObj;
        Object value = row.get(key);
        if (value instanceof String) {
          try {
            row.put(key, convertIsoValue((String) value, kind));
          } catch (DateTimeParseException e) {
            LOGGER.warn("Column '" + key + "' was inferred as " + kind
                + " but value '" + value + "' doesn't match; storing null");
            row.put(key, null);
          }
        }
      }
    }

    final List<RelDataType> types = new ArrayList<RelDataType>(jsonFieldMap.size());
    final List<String> names = new ArrayList<String>(jsonFieldMap.size());

    for (Object key : jsonFieldMap.keySet()) {
      String keyStr = key.toString();
      SqlTypeName dateKind = dateColumnKinds.get(keyStr);
      final RelDataType type;
      if (dateKind != null) {
        type = typeFactory.createTypeWithNullability(typeFactory.createSqlType(dateKind), true);
      } else {
        // Use the discovered type, or default to String for columns that are all null
        Class<?> clazz = columnTypes.getOrDefault(keyStr, String.class);
        type = typeFactory.createJavaType(clazz);
      }
      String columnName = org.apache.calcite.adapter.file.util.SmartCasing.applyCasing(keyStr, columnNameCasing);
      names.add(columnName);
      types.add(type);
    }

    RelDataType relDataType = typeFactory.createStructType(Pair.zip(names, types));
    return new JsonDataConverter(relDataType, list);
  }

  /**
   * Detects whether a string is one of the ISO 8601 forms produced by
   * {@code ConverterUtils.setJsonValueWithTypeInference} (date, local datetime, or
   * offset datetime) or a plain ISO local time. Order matters: offset datetime is checked
   * before local datetime so a trailing zone offset isn't mistaken for a parse failure.
   *
   * @return the matching {@link SqlTypeName}, or null if the value doesn't match any of them
   */
  private static @Nullable SqlTypeName detectIsoDateTimeKind(String value) {
    try {
      OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
      return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    } catch (DateTimeParseException e) {
      // Not an offset datetime; try the next kind.
    }
    try {
      LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
      return SqlTypeName.TIMESTAMP;
    } catch (DateTimeParseException e) {
      // Not a local datetime; try the next kind.
    }
    try {
      LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE);
      return SqlTypeName.DATE;
    } catch (DateTimeParseException e) {
      // Not a date; try the next kind.
    }
    try {
      LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME);
      return SqlTypeName.TIME;
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  /**
   * Converts an ISO 8601 string already confirmed (via {@link #detectIsoDateTimeKind}) to
   * match {@code kind} into the internal representation Calcite uses for that SQL type:
   * epoch day (DATE), millis since midnight (TIME), or epoch millis (TIMESTAMP /
   * TIMESTAMP_WITH_LOCAL_TIME_ZONE). TIMESTAMP values carry no timezone info, so the wall
   * clock time is stored as if it were UTC, matching CsvTypeConverter's convention.
   */
  private static Object convertIsoValue(String value, SqlTypeName kind) {
    switch (kind) {
    case DATE:
      return (int) LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay();
    case TIME:
      return (int) (LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME).toNanoOfDay()
          / 1_000_000L);
    case TIMESTAMP:
      return LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          .toInstant(ZoneOffset.UTC).toEpochMilli();
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          .toInstant().toEpochMilli();
    default:
      throw new IllegalStateException("Unexpected ISO date/time kind: " + kind);
    }
  }

  @Override public Object[] current() {
    return enumerator.current();
  }

  @Override public boolean moveNext() {
    return enumerator.moveNext();
  }

  @Override public void reset() {
    enumerator.reset();
  }

  @Override public void close() {
    enumerator.close();
  }

  /**
   * Json data and relDataType Converter.
   */
  public static class JsonDataConverter {
    private final RelDataType relDataType;
    private final List<Object> dataList;

    private JsonDataConverter(RelDataType relDataType, List<Object> dataList) {
      this.relDataType = relDataType;
      this.dataList = dataList;
    }

    public RelDataType getRelDataType() {
      return relDataType;
    }

    public List<Object> getDataList() {
      return dataList;
    }
  }
}
