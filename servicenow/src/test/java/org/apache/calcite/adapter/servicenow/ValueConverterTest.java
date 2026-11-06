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

import org.apache.calcite.adapter.servicenow.ServiceNowColumn.Kind;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Wire text to Calcite values, and the field type table. */
class ValueConverterTest {

  private static Object convert(Kind kind, String text) {
    return ValueConverter.convert(ServiceNowColumn.field("f", "t", kind, 0), text, "tbl", "id1");
  }

  @Test void emptyIsNullForEveryKind() {
    for (Kind kind : Kind.values()) {
      assertThat(convert(kind, ""), nullValue());
    }
  }

  @Test void parsesEachKind() {
    assertThat(convert(Kind.TEXT, "hello"), equalTo((Object) "hello"));
    assertThat(convert(Kind.GUID, "abc"), equalTo((Object) "abc"));
    assertThat(convert(Kind.INTEGER, "42"), equalTo((Object) 42));
    assertThat(convert(Kind.LONG, "4200000000"), equalTo((Object) 4200000000L));
    assertThat(convert(Kind.DECIMAL, "12.50"), equalTo((Object) new BigDecimal("12.50")));
    assertThat(convert(Kind.CURRENCY, "12.5000"), equalTo((Object) new BigDecimal("12.5000")));
    assertThat(convert(Kind.DOUBLE, "1.5"), equalTo((Object) 1.5d));
    assertThat(convert(Kind.BOOLEAN, "true"), equalTo((Object) Boolean.TRUE));
    assertThat(convert(Kind.BOOLEAN, "false"), equalTo((Object) Boolean.FALSE));
    // 2026-01-01 08:30:00 UTC
    assertThat(convert(Kind.TIMESTAMP, "2026-01-01 08:30:00"), equalTo((Object) 1767256200000L));
    // 2026-02-01 is 20485 days after the epoch
    assertThat(convert(Kind.DATE, "2026-02-01"), equalTo((Object) 20485));
    assertThat(convert(Kind.TIME, "1970-01-01 01:02:03"), equalTo((Object) 3723000));
  }

  @Test void anUnparsableValueNamesTableColumnRecordAndValue() {
    final ServiceNowException e =
        assertThrows(ServiceNowException.class, () -> convert(Kind.INTEGER, "forty"));
    assertThat(e.getMessage(), containsString("tbl.f"));
    assertThat(e.getMessage(), containsString("id1"));
    assertThat(e.getMessage(), containsString("'forty'"));
  }

  @Test void invalidValuesAreNotCoerced() {
    assertThrows(ServiceNowException.class, () -> convert(Kind.BOOLEAN, "1"));
    assertThrows(ServiceNowException.class, () -> convert(Kind.BOOLEAN, "True"));
    assertThrows(ServiceNowException.class, () -> convert(Kind.TIMESTAMP, "2026-01-01"));
    assertThrows(ServiceNowException.class, () -> convert(Kind.TIMESTAMP, "2026-13-01 00:00:00"));
    assertThrows(ServiceNowException.class, () -> convert(Kind.DATE, "2026-01-01 00:00:00"));
    assertThrows(ServiceNowException.class, () -> convert(Kind.TIME, "08:00:00"));
    assertThrows(ServiceNowException.class, () -> convert(Kind.DECIMAL, "USD;12.50"));
  }

  @Test void glideTypeTable() {
    assertThat(GlideTypes.known("GUID"), equalTo(Kind.GUID));
    assertThat(GlideTypes.known("glide_date_time"), equalTo(Kind.TIMESTAMP));
    assertThat(GlideTypes.known("due_date"), equalTo(Kind.TIMESTAMP));
    assertThat(GlideTypes.known("price"), equalTo(Kind.CURRENCY));
    assertThat(GlideTypes.known("journal_input"), equalTo(Kind.TEXT));
    assertThat(GlideTypes.known("no_such_type"), nullValue());
    assertThat(GlideTypes.scalar("datetime"), equalTo(Kind.TIMESTAMP));
    assertThat(GlideTypes.scalar("Integer"), equalTo(Kind.INTEGER));
    assertThat(GlideTypes.scalar("weird"), nullValue());
  }
}
