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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link FileFieldType}.
 */
@Tag("unit")
class FileFieldTypeTest {

  private static final JavaTypeFactory TYPE_FACTORY =
      new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  @Test void testOfString() {
    FileFieldType type = FileFieldType.of("String");
    assertEquals(FileFieldType.STRING, type);
  }

  @Test void testOfInt() {
    FileFieldType type = FileFieldType.of("int");
    assertEquals(FileFieldType.INT, type);
  }

  @Test void testOfInteger() {
    FileFieldType type = FileFieldType.of("Integer");
    assertEquals(FileFieldType.INT, type);
  }

  @Test void testOfLong() {
    FileFieldType type = FileFieldType.of("long");
    assertEquals(FileFieldType.LONG, type);
  }

  @Test void testOfDouble() {
    FileFieldType type = FileFieldType.of("double");
    assertEquals(FileFieldType.DOUBLE, type);
  }

  @Test void testOfFloat() {
    FileFieldType type = FileFieldType.of("float");
    assertEquals(FileFieldType.FLOAT, type);
  }

  @Test void testOfBoolean() {
    FileFieldType type = FileFieldType.of("boolean");
    assertEquals(FileFieldType.BOOLEAN, type);
  }

  @Test void testOfByte() {
    FileFieldType type = FileFieldType.of("byte");
    assertEquals(FileFieldType.BYTE, type);
  }

  @Test void testOfChar() {
    FileFieldType type = FileFieldType.of("char");
    assertEquals(FileFieldType.CHAR, type);
  }

  @Test void testOfShort() {
    FileFieldType type = FileFieldType.of("short");
    assertEquals(FileFieldType.SHORT, type);
  }

  @Test void testOfDate() {
    FileFieldType type = FileFieldType.of("Date");
    assertEquals(FileFieldType.DATE, type);
  }

  @Test void testOfTime() {
    FileFieldType type = FileFieldType.of("Time");
    assertEquals(FileFieldType.TIME, type);
  }

  @Test void testOfTimestamp() {
    FileFieldType type = FileFieldType.of("timestamp");
    assertEquals(FileFieldType.TIMESTAMP, type);
  }

  @Test void testOfTimestampWithLocalTimeZone() {
    FileFieldType type = FileFieldType.of("timestamptz");
    assertEquals(FileFieldType.TIMESTAMP_WITH_LOCAL_TIME_ZONE, type);
  }

  @Test void testOfTimestampWithLocalTimeZoneAlias() {
    FileFieldType type = FileFieldType.of("TimestampWithLocalTimeZone");
    assertEquals(FileFieldType.TIMESTAMP_WITH_LOCAL_TIME_ZONE, type);
  }

  @Test void testOfNonExistent() {
    FileFieldType type = FileFieldType.of("nonexistent");
    assertNull(type);
  }

  @Test void testToTypeString() {
    RelDataType relType = FileFieldType.STRING.toType(TYPE_FACTORY);
    assertNotNull(relType);
  }

  @Test void testToTypeInt() {
    RelDataType relType = FileFieldType.INT.toType(TYPE_FACTORY);
    assertNotNull(relType);
  }

  @Test void testToTypeLong() {
    RelDataType relType = FileFieldType.LONG.toType(TYPE_FACTORY);
    assertNotNull(relType);
  }

  @Test void testToTypeDouble() {
    RelDataType relType = FileFieldType.DOUBLE.toType(TYPE_FACTORY);
    assertNotNull(relType);
  }

  @Test void testToTypeBoolean() {
    RelDataType relType = FileFieldType.BOOLEAN.toType(TYPE_FACTORY);
    assertNotNull(relType);
  }

  @Test void testToTypeDate() {
    RelDataType relType = FileFieldType.DATE.toType(TYPE_FACTORY);
    assertNotNull(relType);
  }

  @Test void testToTypeTimestamp() {
    RelDataType relType = FileFieldType.TIMESTAMP.toType(TYPE_FACTORY);
    assertNotNull(relType);
  }

  @Test void testToTypeTimestampWithLocalTimeZone() {
    RelDataType relType = FileFieldType.TIMESTAMP_WITH_LOCAL_TIME_ZONE.toType(TYPE_FACTORY);
    assertNotNull(relType);
  }

  @Test void testEnumValues() {
    FileFieldType[] values = FileFieldType.values();
    assertEquals(13, values.length);
  }
}
