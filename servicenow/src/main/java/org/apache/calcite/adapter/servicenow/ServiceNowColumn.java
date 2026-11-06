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
package org.apache.calcite.adapter.servicenow;

import org.apache.calcite.sql.type.SqlTypeName;

/**
 * One SQL column of a ServiceNow table.
 *
 * <p>A ServiceNow reference field becomes two columns: the field itself, holding the referenced
 * record's sys_id, and a second column named {@code <field>__display} ({@link #DISPLAY_SUFFIX})
 * holding the referenced record's display value (for a user, the name). The display column is
 * read only when a query selects it, because asking ServiceNow for display values is slower and
 * depends on the calling user's locale and time zone.
 */
final class ServiceNowColumn {

  /** Suffix of the display-value column that accompanies each reference field. */
  static final String DISPLAY_SUFFIX = "__display";

  /** How a column's text on the wire is turned into a value, and the SQL type it gets. */
  enum Kind {
    /** A 32-character sys_id. */
    GUID(SqlTypeName.VARCHAR),
    /** Free text; also glide_list, journal fields, durations and other unparsed text. */
    TEXT(SqlTypeName.VARCHAR),
    INTEGER(SqlTypeName.INTEGER),
    LONG(SqlTypeName.BIGINT),
    /** A decimal with two digits after the point. */
    DECIMAL(SqlTypeName.DECIMAL),
    /** Currency and price: a decimal with four digits after the point. */
    CURRENCY(SqlTypeName.DECIMAL),
    DOUBLE(SqlTypeName.DOUBLE),
    BOOLEAN(SqlTypeName.BOOLEAN),
    /** Date and time in UTC. */
    TIMESTAMP(SqlTypeName.TIMESTAMP),
    DATE(SqlTypeName.DATE),
    TIME(SqlTypeName.TIME),
    /** A reference to a record in another table; holds its sys_id. */
    REFERENCE(SqlTypeName.VARCHAR);

    final SqlTypeName sqlType;

    Kind(SqlTypeName sqlType) {
      this.sqlType = sqlType;
    }
  }

  /** SQL column name. */
  final String name;
  /** The ServiceNow field this column is read from. */
  final String field;
  /** ServiceNow's {@code internal_type} for the field. */
  final String glideType;
  final Kind kind;
  /** Declared maximum length from the dictionary, or 0 if none. */
  final int maxLength;
  /** True if this column is the display value of the reference field {@link #field}. */
  final boolean display;

  private ServiceNowColumn(String name, String field, String glideType, Kind kind, int maxLength,
      boolean display) {
    this.name = name;
    this.field = field;
    this.glideType = glideType;
    this.kind = kind;
    this.maxLength = maxLength;
    this.display = display;
  }

  static ServiceNowColumn field(String field, String glideType, Kind kind, int maxLength) {
    return new ServiceNowColumn(field, field, glideType, kind, maxLength, false);
  }

  /** The display-value column of a reference field. */
  static ServiceNowColumn displayOf(ServiceNowColumn reference) {
    return new ServiceNowColumn(reference.field + DISPLAY_SUFFIX, reference.field,
        reference.glideType, Kind.TEXT, 0, true);
  }

  @Override public String toString() {
    return name + " " + kind + (display ? " (display of " + field + ")" : " (" + glideType + ")");
  }
}
