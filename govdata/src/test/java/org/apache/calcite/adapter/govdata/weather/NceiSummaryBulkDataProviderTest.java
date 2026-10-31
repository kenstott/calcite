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
package org.apache.calcite.adapter.govdata.weather;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** CSV line splitting for the NCEI GSOM/GSOY station files. */
@Tag("unit")
class NceiSummaryBulkDataProviderTest {

  @Test void quotedFieldsKeepTheirCommas() {
    List<String> f = NceiSummaryBulkDataProvider.parseCsvLine(
        "\"USW00094728\",\"2014-01\",\"40.77\",\"NEW YORK CENTRAL PARK, NY US\",\"-1.2\",\",,W\",\"\"");
    assertEquals(7, f.size());
    assertEquals("NEW YORK CENTRAL PARK, NY US", f.get(3));
    assertEquals(",,W", f.get(5));
    assertEquals("", f.get(6));
  }

  @Test void doubledQuoteIsAnEscapedQuote() {
    List<String> f = NceiSummaryBulkDataProvider.parseCsvLine("\"a\"\"b\",c");
    assertEquals("a\"b", f.get(0));
    assertEquals("c", f.get(1));
  }
}
