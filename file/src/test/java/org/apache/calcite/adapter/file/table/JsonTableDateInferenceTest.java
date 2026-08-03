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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * G2 (data-normalization-plan.md): a JSON string column whose sampled values are all one of
 * the ISO 8601 forms {@code ConverterUtils.setJsonValueWithTypeInference} produces (used by
 * the HTML/XML/DOCX/PPTX/Markdown converters, and equally applicable to hand-authored JSON)
 * must be promoted to a real DATE/TIME/TIMESTAMP SQL type instead of staying VARCHAR, with
 * row values converted to Calcite's internal representation for that type.
 */
@Tag("unit")
public class JsonTableDateInferenceTest {

  @TempDir
  java.nio.file.Path tempDir;

  private static final JavaTypeFactory TYPE_FACTORY =
      new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  private File createJsonFile(String name, String content) throws Exception {
    File file = new File(tempDir.toFile(), name);
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
    return file;
  }

  @Test public void testIsoDateColumnPromotedToDate() throws Exception {
    File jsonFile = createJsonFile("dates.json",
        "[{\"name\": \"alice\", \"born\": \"2024-01-15\"},"
        + " {\"name\": \"bob\", \"born\": \"1999-12-31\"}]");
    JsonTable table = new JsonTable(Sources.of(jsonFile));

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    RelDataType bornType = rowType.getField("born", false, false).getType();
    assertEquals(SqlTypeName.DATE, bornType.getSqlTypeName(),
        "ISO date string column must be inferred as DATE, not VARCHAR");

    List<Object> data = table.getDataList(TYPE_FACTORY);
    assertEquals(2, data.size());
    //noinspection unchecked
    Map<String, Object> row0 = (Map<String, Object>) data.get(0);
    assertEquals((int) LocalDate.parse("2024-01-15").toEpochDay(), row0.get("born"),
        "DATE value must be stored as epoch day, not the raw ISO string");
  }

  @Test public void testIsoTimestampColumnPromotedToTimestamp() throws Exception {
    File jsonFile = createJsonFile("timestamps.json",
        "[{\"event\": \"start\", \"at\": \"2024-01-15T10:30:00\"},"
        + " {\"event\": \"end\", \"at\": \"2024-01-15T11:45:00\"}]");
    JsonTable table = new JsonTable(Sources.of(jsonFile));

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    RelDataType atType = rowType.getField("at", false, false).getType();
    assertEquals(SqlTypeName.TIMESTAMP, atType.getSqlTypeName(),
        "ISO local datetime string column must be inferred as TIMESTAMP");

    List<Object> data = table.getDataList(TYPE_FACTORY);
    //noinspection unchecked
    Map<String, Object> row0 = (Map<String, Object>) data.get(0);
    long expectedMillis =
        LocalDateTime.parse("2024-01-15T10:30:00").toInstant(ZoneOffset.UTC).toEpochMilli();
    assertEquals(expectedMillis, row0.get("at"),
        "TIMESTAMP value must be stored as UTC wall-clock epoch millis");
  }

  @Test public void testIsoOffsetTimestampColumnPromotedToTimestampWithLocalTimeZone()
      throws Exception {
    File jsonFile = createJsonFile("tz_timestamps.json",
        "[{\"event\": \"start\", \"at\": \"2024-01-15T10:30:00+05:30\"},"
        + " {\"event\": \"end\", \"at\": \"2024-01-15T11:45:00+05:30\"}]");
    JsonTable table = new JsonTable(Sources.of(jsonFile));

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    RelDataType atType = rowType.getField("at", false, false).getType();
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, atType.getSqlTypeName(),
        "ISO offset datetime string column must be inferred as TIMESTAMP_WITH_LOCAL_TIME_ZONE");

    List<Object> data = table.getDataList(TYPE_FACTORY);
    //noinspection unchecked
    Map<String, Object> row0 = (Map<String, Object>) data.get(0);
    long expectedMillis =
        OffsetDateTime.parse("2024-01-15T10:30:00+05:30").toInstant().toEpochMilli();
    assertEquals(expectedMillis, row0.get("at"),
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE value must be stored as UTC epoch millis");
  }

  @Test public void testIsoTimeColumnPromotedToTime() throws Exception {
    File jsonFile = createJsonFile("times.json",
        "[{\"label\": \"open\", \"clock\": \"09:00:00\"},"
        + " {\"label\": \"close\", \"clock\": \"17:30:00\"}]");
    JsonTable table = new JsonTable(Sources.of(jsonFile));

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    RelDataType clockType = rowType.getField("clock", false, false).getType();
    assertEquals(SqlTypeName.TIME, clockType.getSqlTypeName(),
        "ISO local time string column must be inferred as TIME");

    List<Object> data = table.getDataList(TYPE_FACTORY);
    //noinspection unchecked
    Map<String, Object> row0 = (Map<String, Object>) data.get(0);
    int expectedMillis = (int) (LocalTime.parse("09:00:00").toNanoOfDay() / 1_000_000L);
    assertEquals(expectedMillis, row0.get("clock"),
        "TIME value must be stored as millis since midnight");
  }

  @Test public void testMixedColumnStaysVarchar() throws Exception {
    // One value doesn't match the ISO date pattern -> column must stay a plain string.
    File jsonFile = createJsonFile("mixed.json",
        "[{\"note\": \"2024-01-15\"}, {\"note\": \"not a date\"}]");
    JsonTable table = new JsonTable(Sources.of(jsonFile));

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    RelDataType noteType = rowType.getField("note", false, false).getType();
    assertNotNull(noteType);
    assertEquals(SqlTypeName.VARCHAR, noteType.getSqlTypeName(),
        "a column with even one non-ISO-date value must not be promoted");

    List<Object> data = table.getDataList(TYPE_FACTORY);
    //noinspection unchecked
    Map<String, Object> row0 = (Map<String, Object>) data.get(0);
    assertEquals("2024-01-15", row0.get("note"),
        "unpromoted column must keep the raw string value");
  }

  @Test public void testOrdinaryStringColumnStaysVarchar() throws Exception {
    File jsonFile = createJsonFile("plain.json",
        "[{\"name\": \"alice\"}, {\"name\": \"bob\"}]");
    JsonTable table = new JsonTable(Sources.of(jsonFile));

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    RelDataType nameType = rowType.getField("name", false, false).getType();
    assertEquals(SqlTypeName.VARCHAR, nameType.getSqlTypeName(),
        "ordinary text column must not be affected by date detection");
  }
}
