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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

/**
 * Type of a field in a Web (HTML) table.
 *
 * <p>Usually, and unless specified explicitly in the header row, a field is
 * of type {@link #STRING}. But specifying the field type in the fields
 * makes it easier to write SQL.
 */
public enum FileFieldType {
  STRING(null, String.class),
  BOOLEAN(Primitive.BOOLEAN),
  BYTE(Primitive.BYTE),
  CHAR(Primitive.CHAR),
  SHORT(Primitive.SHORT),
  INT(Primitive.INT),
  LONG(Primitive.LONG),
  FLOAT(Primitive.FLOAT),
  DOUBLE(Primitive.DOUBLE),
  DATE(null, java.sql.Date.class),
  TIME(null, java.sql.Time.class),
  TIMESTAMP(null, java.sql.Timestamp.class),
  TIMESTAMP_WITH_LOCAL_TIME_ZONE(null, java.sql.Timestamp.class);

  private final @Nullable Primitive primitive;
  private final Class clazz;

  private static final Map<String, FileFieldType> MAP;

  static {
    ImmutableMap.Builder<String, FileFieldType> builder =
        ImmutableMap.builder();
    for (FileFieldType value : values()) {
      // Don't add duplicate keys for types with the same class
      // TIMESTAMP_WITH_LOCAL_TIME_ZONE shares java.sql.Timestamp with TIMESTAMP
      if (value != TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
        builder.put(value.clazz.getSimpleName(), value);
      }

      if (value.primitive != null) {
        builder.put(value.primitive.getPrimitiveName(), value);
      }
    }
    // Add explicit aliases for timestamp types
    builder.put("timestamp", TIMESTAMP);
    builder.put("timestamptz", TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    builder.put("TimestampWithLocalTimeZone", TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    MAP = builder.build();
  }

  FileFieldType(Primitive primitive) {
    this(primitive, primitive.getBoxClass());
  }

  FileFieldType(@Nullable Primitive primitive, Class clazz) {
    this.primitive = primitive;
    this.clazz = clazz;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    return typeFactory.createJavaType(clazz);
  }

  public static FileFieldType of(String typeString) {
    return MAP.get(typeString);
  }
}
