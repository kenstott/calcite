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
package org.apache.calcite.adapter.govdata.econ;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests {@link TicForeignHoldingsTransformer} against copies of the Treasury TIC files. */
@Tag("unit")
class TicForeignHoldingsTransformerTest {

  private static String fixture(String name) throws IOException {
    try (InputStream in =
        TicForeignHoldingsTransformerTest.class.getResourceAsStream("/tic/" + name)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) != -1) {
        out.write(buf, 0, n);
      }
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
  }

  private static Double value(List<Map<String, Object>> rows, int year, int month,
      String holder) {
    Double found = null;
    for (Map<String, Object> r : rows) {
      if (r.get("year").equals(year) && r.get("month").equals(month)
          && r.get("holder").equals(holder)) {
        assertTrue(found == null, "duplicate " + year + "-" + month + " " + holder);
        found = (Double) r.get("holdings_usd_billions");
      }
    }
    return found;
  }

  @Test void mergesHistoryAndLatest() throws IOException {
    List<Map<String, Object>> rows =
        TicForeignHoldingsTransformer.parse(fixture("mfhhis01.txt"), fixture("slt_table5.txt"));
    // history-only month (Jan 2025) and a quoted country name
    assertEquals(Double.valueOf(1079.3), value(rows, 2025, 1, "Japan"));
    assertEquals(Double.valueOf(760.8), value(rows, 2025, 1, "China, Mainland"));
    assertEquals(Double.valueOf(3803.8), value(rows, 2025, 1, "Foreign Official"));
    // 2012 block is read, 2011 and earlier are not
    assertEquals(Double.valueOf(1220.4), value(rows, 2012, 12, "China, Mainland"));
    assertEquals(Double.valueOf(5573.8), value(rows, 2012, 12, "Grand Total"));
    for (Map<String, Object> r : rows) {
      assertTrue(((Integer) r.get("year")) >= 2012);
    }
    // latest-only month, and the current vintage overrides the history for overlapping months
    assertEquals(Double.valueOf(1063.1), value(rows, 2026, 9, "Japan"));
    assertEquals(Double.valueOf(3674.7), value(rows, 2026, 9, "Foreign Official"));
    assertEquals(Double.valueOf(863.2), value(rows, 2025, 12, "United Kingdom"));
    assertEquals(Double.valueOf(9270.5), value(rows, 2025, 12, "Grand Total"));
    Set<String> kinds = new HashSet<String>();
    for (Map<String, Object> r : rows) {
      kinds.add((String) r.get("holder_kind"));
    }
    assertEquals(6, kinds.size(), kinds.toString());
  }

  @Test void noFootnoteMarkersInHolderNames() throws IOException {
    for (Map<String, Object> r : TicForeignHoldingsTransformer.parseHistory(
        fixture("mfhhis01.txt"))) {
      assertTrue(!((String) r.get("holder")).matches(".*\\d/$"), (String) r.get("holder"));
    }
  }

  @Test void rejectsUnrecognizedFile() {
    assertThrows(IllegalStateException.class,
        () -> TicForeignHoldingsTransformer.parseLatest("<html>blocked</html>"));
    assertThrows(IllegalStateException.class,
        () -> TicForeignHoldingsTransformer.parseHistory("<html>blocked</html>"));
  }
}
