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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The partition columns of an Iceberg table that the table stores as text and its schema
 * declares as an integer (a {@code year} written {@code '2023'}).
 *
 * <p>The DuckDB engine serves each as its declared type, cast from the stored text. A filter on
 * the cast value is rewritten to the stored text ({@link StoredTextPartitionFilter}),
 * so DuckDB still prunes partitions by it; DuckDB cannot prune through the cast.
 */
public final class StoredTextPartitions {

  /** No column of the table is stored as text and declared as an integer. */
  static final StoredTextPartitions NONE = new StoredTextPartitions(ImmutableMap.of());

  /** Integer types a stored-text partition column may be declared as. */
  private static final Map<String, SqlTypeName> INTEGER_TYPES =
      ImmutableMap.<String, SqlTypeName>builder()
          .put("TINYINT", SqlTypeName.TINYINT)
          .put("SMALLINT", SqlTypeName.SMALLINT)
          .put("INT", SqlTypeName.INTEGER)
          .put("INTEGER", SqlTypeName.INTEGER)
          .put("BIGINT", SqlTypeName.BIGINT)
          .build();

  /** One such column: its position in the stored row type, declared type and stored width. */
  static final class Column {
    final String name;
    final SqlTypeName declared;
    /** Digits of every stored value, zero-padded (a year: 4); null for unpadded decimal text. */
    final @Nullable Integer digits;

    Column(String name, SqlTypeName declared, @Nullable Integer digits) {
      this.name = name;
      this.declared = declared;
      this.digits = digits;
    }

    /**
     * The stored text of the integer {@code value}, or null when no stored value can hold it,
     * which leaves a comparison with it to the cast. {@code value} is exact: a literal of the
     * declared integer type.
     */
    @Nullable String text(BigDecimal value) {
      final BigInteger integer;
      try {
        integer = value.toBigIntegerExact();
      } catch (ArithmeticException notAnInteger) {
        return null;
      }
      if (integer.signum() < 0) {
        return null;
      }
      String plain = integer.toString();
      if (digits == null) {
        return plain;
      }
      if (plain.length() > digits) {
        return null;
      }
      StringBuilder padded = new StringBuilder(digits);
      for (int i = plain.length(); i < digits; i++) {
        padded.append('0');
      }
      return padded.append(plain).toString();
    }
  }

  private final Map<Integer, Column> byIndex;

  private StoredTextPartitions(Map<Integer, Column> byIndex) {
    this.byIndex = byIndex;
  }

  /**
   * The columns of {@code stored}, the table's row type as stored, that {@code declared} names
   * with an integer type and that are stored as text.
   */
  static StoredTextPartitions of(RelDataType stored,
      List<PartitionedTableConfig.ColumnDefinition> declared) {
    ImmutableMap.Builder<Integer, Column> columns = ImmutableMap.builder();
    for (PartitionedTableConfig.ColumnDefinition definition : declared) {
      SqlTypeName type = INTEGER_TYPES.get(definition.getType().toUpperCase(Locale.ROOT));
      RelDataTypeField field = stored.getField(definition.getName(), false, false);
      if (type != null && field != null
          && field.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
        columns.put(field.getIndex(),
            new Column(field.getName(), type, definition.getStoredDigits()));
      }
    }
    Map<Integer, Column> byIndex = columns.build();
    return byIndex.isEmpty() ? NONE : new StoredTextPartitions(byIndex);
  }

  boolean isEmpty() {
    return byIndex.isEmpty();
  }

  @Nullable Column column(int index) {
    return byIndex.get(index);
  }

  /** {@code stored} with each of these columns at its declared type. */
  RelDataType declaredRowType(RelDataTypeFactory typeFactory, RelDataType stored) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (RelDataTypeField field : stored.getFieldList()) {
      Column column = byIndex.get(field.getIndex());
      builder.add(field.getName(),
          column == null
              ? field.getType()
              : typeFactory.createTypeWithNullability(
                  typeFactory.createSqlType(column.declared), field.getType().isNullable()));
    }
    return builder.build();
  }
}
