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
package org.apache.calcite.adapter.govdata.cyber.vuln;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link CsafAdvisoryDimensionResolver#parseIndex}.
 *
 * <p>These tests exercise the line-parsing logic without any network access.
 */
@Tag("unit")
class CsafAdvisoryDimensionResolverTest {

  private static BufferedReader reader(String content) {
    return new BufferedReader(new StringReader(content));
  }

  @Test void testYearFilterSkipsHeaderAndOtherYears() throws Exception {
    // changes.csv style: "path,timestamp" lines plus a header.
    String sample =
        "path,date\n"
        + "2025/va-25-001-01.json,2025-01-01T00:00:00Z\n"
        + "2025/va-25-002-01.json,2025-02-01T00:00:00Z\n"
        + "2026/va-26-155-01.json,2026-06-04T15:31:57Z\n"
        + "2026/va-26-156-01.json,2026-06-05T09:00:00Z\n";

    List<String> paths = CsafAdvisoryDimensionResolver.parseIndex(reader(sample), "2026");

    assertEquals(2, paths.size());
    assertEquals("2026/va-26-155-01.json", paths.get(0));
    assertEquals("2026/va-26-156-01.json", paths.get(1));
  }

  @Test void testNoFilterReturnsAllSkippingHeaderAndBlanks() throws Exception {
    // index.txt style: bare paths, one per line, with a blank line and a header.
    String sample =
        "path\n"
        + "2025/va-25-001-01.json\n"
        + "\n"
        + "2026/va-26-155-01.json\n"
        + "2026/va-26-156-01.json\n";

    List<String> paths = CsafAdvisoryDimensionResolver.parseIndex(reader(sample), null);

    assertEquals(3, paths.size());
    assertEquals("2025/va-25-001-01.json", paths.get(0));
    assertEquals("2026/va-26-155-01.json", paths.get(1));
    assertEquals("2026/va-26-156-01.json", paths.get(2));
  }
}
