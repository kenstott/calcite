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
package org.apache.calcite.adapter.govdata.health;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.adapter.file.etl.RawCache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Quarter discovery against the shape data.cms.gov's data.json has today. */
@Tag("unit")
class CmsPbjNurseStaffingProviderTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static String dataset(String title, String start, String... urls) {
    StringBuilder d = new StringBuilder();
    for (String u : urls) {
      if (d.length() > 0) {
        d.append(',');
      }
      d.append("{\"downloadURL\":\"").append(u).append("\",\"temporal\":[{\"@type\":\"PeriodOfTime\","
          + "\"startDate\":\"").append(start).append("\",\"endDate\":\"x\"}]}");
    }
    return "{\"title\":\"" + title + "\",\"distribution\":[" + d + "]}";
  }

  private static List<CmsPbjNurseStaffingProvider.QuarterFile> parse(String... datasets)
      throws Exception {
    JsonNode catalog = MAPPER.readTree("{\"dataset\":[" + String.join(",", datasets) + "]}");
    return CmsPbjNurseStaffingProvider.quartersIn(catalog);
  }

  @Test void eachQuarterIsItsOwnDatasetWithADateSuffixedTitle() throws Exception {
    List<CmsPbjNurseStaffingProvider.QuarterFile> q = parse(
        dataset("Payroll Based Journal Daily Nurse Staffing : 2025-10-01", "2025-10-01", "http://h/q4.csv"),
        dataset("Payroll Based Journal Daily Nurse Staffing : 2026-01-01", "2026-01-01", "", "http://h/q1.csv"),
        dataset("Payroll Based Journal Daily Non-Nurse Staffing : 2026-01-01", "2026-01-01", "http://h/other.csv"),
        dataset("Payroll Based Journal Employee Detail Nursing Home Staffing : 2026-01-01", "2026-01-01", "http://h/emp.csv"));
    assertEquals(2, q.size());
    assertEquals("http://h/q1.csv", q.get(0).csvUrl); // newest first
    assertEquals(2026, q.get(0).year);
    assertEquals(1, q.get(0).quarter);
    assertEquals(4, q.get(1).quarter);
  }

  @Test void stringTemporalAndPlainTitleStillWork() throws Exception {
    List<CmsPbjNurseStaffingProvider.QuarterFile> q = parse(
        "{\"title\":\"Payroll Based Journal Daily Nurse Staffing\",\"distribution\":[{"
            + "\"downloadURL\":\"http://h/a.csv\",\"temporal\":\"2024-04-01/2024-06-30\"}]}");
    assertEquals(1, q.size());
    assertEquals(2, q.get(0).quarter);
  }

  @Test void aSecondDistributionForTheSameQuarterIsNotLoadedTwice() throws Exception {
    List<CmsPbjNurseStaffingProvider.QuarterFile> q = parse(
        dataset("Payroll Based Journal Daily Nurse Staffing : 2025-07-01", "2025-07-01", "http://h/a.csv"),
        dataset("Payroll Based Journal Daily Nurse Staffing : 2025-07-02", "2025-07-01", "http://h/b.csv"));
    assertEquals(1, q.size());
    assertEquals("http://h/a.csv", q.get(0).csvUrl);
  }

  @Test void unrelatedCatalogYieldsNothing() throws Exception {
    assertTrue(parse(dataset("Something else", "2025-01-01", "http://h/x.csv")).isEmpty());
  }

  /** Fails the first {@code failures} downloads the way a TLS record error does, then serves a CSV. */
  private static RawCache flaky(final int failures, final AtomicInteger calls) {
    return new RawCache() {
      @Override public InputStream openStream(String url) throws java.io.IOException {
        return openStream(url, null);
      }

      @Override public InputStream openStream(String key, ContentSupplier onMiss)
          throws java.io.IOException {
        if (calls.incrementAndGet() <= failures) {
          throw new javax.net.ssl.SSLException("(bad_record_mac) Tag mismatch");
        }
        return new ByteArrayInputStream(
            "provnum,workdate,mdscensus\n015009,20260101,40\n".getBytes("UTF-8"));
      }

      @Override public boolean isEnabled() {
        return true;
      }
    };
  }

  private static List<Map<String, Object>> drain(int failures, int maxRetries, AtomicInteger calls) {
    CmsPbjNurseStaffingProvider.MultiQuarterRowIterator it =
        new CmsPbjNurseStaffingProvider.MultiQuarterRowIterator(
            java.util.Collections.singletonList(
                new CmsPbjNurseStaffingProvider.QuarterFile(2026, 1, "http://h/q1.csv")),
            "ua", flaky(failures, calls), maxRetries, 1L);
    List<Map<String, Object>> rows = new ArrayList<>();
    while (it.hasNext()) {
      rows.add(it.next());
    }
    return rows;
  }

  @Test void aQuarterDownloadThatFailsMidwayIsFetchedAgain() {
    AtomicInteger calls = new AtomicInteger();
    List<Map<String, Object>> rows = drain(2, 3, calls);
    assertEquals(1, rows.size());
    assertEquals("015009", rows.get(0).get("ccn"));
    assertEquals(3, calls.get());
  }

  @Test void retriesAreBoundedAndThenFailLoudly() {
    AtomicInteger calls = new AtomicInteger();
    RuntimeException e = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
        () -> drain(100, 2, calls));
    assertTrue(e.getCause() instanceof javax.net.ssl.SSLException, String.valueOf(e.getCause()));
    assertEquals(3, calls.get());
  }
}
