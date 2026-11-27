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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.RawCache;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The ref tables carry no row that is a whole copy of another. */
@Tag("unit")
class RefDuplicateRowTest {

  private static String holiday(String type) {
    return "{\"date\":\"2023-01-02\",\"localName\":\"Viti i Ri\",\"name\":\"New Year's Day\","
        + "\"fixed\":false,\"global\":true,\"counties\":null,\"launchYear\":null,"
        + "\"types\":[\"" + type + "\"]}";
  }

  @Test void holidaysDropsIdenticalSourceEntriesAndKeepsVariants() throws Exception {
    final String holidays =
        "[" + holiday("Public") + "," + holiday("Public") + "," + holiday("Bank") + "]";
    RawCache cache = new RawCache() {
      @Override public InputStream openStream(String url) {
        String body = url.endsWith("AvailableCountries")
            ? "[{\"countryCode\":\"AL\",\"name\":\"Albania\"}]" : holidays;
        return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
      }

      @Override public InputStream openStream(String key, ContentSupplier onMiss) {
        return openStream(key);
      }

      @Override public boolean isEnabled() {
        return true;
      }
    };
    Iterator<Map<String, Object>> rows = new HolidaysDataProvider()
        .fetch(null, Collections.singletonMap("year", "2023"), cache);
    assertEquals("Public", rows.next().get("holiday_types"));
    assertEquals("Bank", rows.next().get("holiday_types"));
    assertEquals(false, rows.hasNext());
  }

  @Test void einHubHoldsOneRowPerCikAndEin() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE filings (cik VARCHAR, irs_number VARCHAR)");
      st.execute("INSERT INTO filings VALUES ('0001320695', '362154936'), "
          + "('0001320695', '362154936'), ('0000000002', '362154936'), ('0000000003', ' ')");
      st.execute(EntityBridgeListener.einHubSql("filings"));
      try (ResultSet rs = st.executeQuery("SELECT count(*) FROM ein_hub")) {
        rs.next();
        assertEquals(2, rs.getInt(1));
      }
    }
  }
}
