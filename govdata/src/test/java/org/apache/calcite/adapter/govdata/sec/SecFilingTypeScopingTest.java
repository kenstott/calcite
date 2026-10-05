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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** A table-scoped SEC run fetches only the filing types its tables are built from. */
@org.junit.jupiter.api.Tag("unit")
class SecFilingTypeScopingTest {

  private static final List<String> DEFAULT = Arrays.asList(
      "3", "4", "5", "10-K", "10K", "10-Q", "10Q", "8-K", "8K", "8-K/A", "8KA", "DEF 14A");

  private static Map<String, Object> table(String name, String... types) {
    Map<String, Object> t = new HashMap<String, Object>();
    t.put("name", name);
    if (types.length > 0) {
      Map<String, Object> dims = new HashMap<String, Object>();
      dims.put("filing_type", Arrays.asList(types));
      t.put("dimensions", dims);
    }
    return t;
  }

  private static final List<Map<String, Object>> TABLES = new ArrayList<Map<String, Object>>(
      Arrays.asList(
          table("earnings_transcripts", "8-K", "8-K/A"),
          table("insider_transactions", "3", "4", "5"),
          table("filing_metadata", "10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A"),
          table("stock_prices")));

  private static List<String> narrow(String... enabled) {
    return SecSchemaFactory.narrowFilingTypes(DEFAULT,
        new HashSet<String>(Arrays.asList(enabled)), TABLES);
  }

  @Test void earningsTranscriptsNeedsOnlyCurrentReports() {
    assertEquals(Arrays.asList("8-K", "8K", "8-K/A", "8KA"), narrow("earnings_transcripts"));
  }

  @Test void insiderTransactionsNeedsOnlyInsiderForms() {
    assertEquals(Arrays.asList("3", "4", "5"), narrow("insider_transactions"));
  }

  @Test void severalTablesTakeTheUnion() {
    assertEquals(Arrays.asList("3", "4", "5", "8-K", "8K", "8-K/A", "8KA"),
        narrow("earnings_transcripts", "insider_transactions"));
  }

  @Test void baseTypeMatchesItsAmendmentAndAliasesMatch() {
    assertEquals(Arrays.asList("10-K", "10K", "10-Q", "10Q", "8-K", "8K", "8-K/A", "8KA"),
        narrow("filing_metadata"));
  }

  @Test void aTableWithoutFilingTypesNeedsNoFilings() {
    assertEquals(Collections.emptyList(), narrow("stock_prices"));
  }
}
