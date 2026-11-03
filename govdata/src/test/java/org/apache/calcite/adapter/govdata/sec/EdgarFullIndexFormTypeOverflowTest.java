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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * EDGAR's company.idx form-type column is 12 characters wide, so {@code SCHEDULE 13G/A} (14)
 * arrives as {@code SCHEDULE 13G} with the {@code /A} spilled into the CIK column. The parser
 * has to put the overflow back, or every amendment is stored as its base form type.
 */
@Tag("unit")
public class EdgarFullIndexFormTypeOverflowTest {

  private static String line(String company, String form, String cik, String date, String file) {
    // company 0-61, form type 62-73 (12 wide), CIK 74-90, date 91-102, file name 103-.
    // A longer form type runs on into the CIK column, exactly as EDGAR writes it.
    StringBuilder sb = new StringBuilder(String.format("%-62s", company));
    sb.append(form);
    while (sb.length() < 74) {
      sb.append(' ');
    }
    sb.append(form.length() > 12 ? "   " : "").append(cik);
    while (sb.length() < 91) {
      sb.append(' ');
    }
    sb.append(String.format("%-12s%s", date, file));
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  private static List<EdgarFullIndexCache.IndexEntry> parse(String content, String cik)
      throws Exception {
    EdgarFullIndexCache cache = new EdgarFullIndexCache(null, "unused", 2030, 2029);
    Method m = EdgarFullIndexCache.class.getDeclaredMethod("parseIndex", String.class, int.class,
        int.class);
    m.setAccessible(true);
    m.invoke(cache, content, 2025, 1);
    Field f = EdgarFullIndexCache.class.getDeclaredField("entriesByCik");
    f.setAccessible(true);
    return ((Map<String, List<EdgarFullIndexCache.IndexEntry>>) f.get(cache)).get(cik);
  }

  private static String index(String... lines) {
    StringBuilder sb = new StringBuilder("Form Type   Company Name\n----------\n");
    for (String l : lines) {
      sb.append(l).append('\n');
    }
    return sb.toString();
  }

  @Test void amendmentKeepsItsSlashA() throws Exception {
    String file = "edgar/data/1084869/0000921895-25-000123.txt";
    List<EdgarFullIndexCache.IndexEntry> e = parse(index(
        line("SOME HOLDER LP", "SCHEDULE 13G/A", "1084869", "2025-01-16", file)), "0001084869");
    assertEquals(1, e.size());
    assertEquals("SC 13G/A", e.get(0).formType);
    assertEquals("0001084869", e.get(0).cik);
  }

  @Test void baseFormsAreUnchanged() throws Exception {
    String file = "edgar/data/1084869/0000905148-25-000001.txt";
    List<EdgarFullIndexCache.IndexEntry> e = parse(index(
        line("SOME HOLDER LP", "SCHEDULE 13G", "1084869", "2025-02-14", file)), "0001084869");
    assertEquals("SC 13G", e.get(0).formType);
    assertEquals("0001084869", e.get(0).cik);
  }

  @Test void legacyAmendmentFitsTheColumn() throws Exception {
    String file = "edgar/data/320193/0000320193-19-000001.txt";
    List<EdgarFullIndexCache.IndexEntry> e = parse(index(
        line("APPLE INC", "SC 13D/A", "320193", "2019-02-14", file)), "0000320193");
    assertEquals("SC 13D/A", e.get(0).formType);
  }
}
