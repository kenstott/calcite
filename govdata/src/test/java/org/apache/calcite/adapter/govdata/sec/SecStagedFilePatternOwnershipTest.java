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

import org.apache.calcite.adapter.file.iceberg.IcebergMaterializer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Every sec table claims exactly its own staged files, using the source patterns declared in
 * {@code sec-schema.yaml}.
 */
@Tag("unit")
class SecStagedFilePatternOwnershipTest {

  private static final String BASE = "s3://govdata-parquet-v1/sec";

  /** Table name to staged file type written into {@code sec/year=Y/<type>_batch_*}. */
  private static Map<String, String> fileTypes() {
    Map<String, String> types = new LinkedHashMap<String, String>();
    types.put("filing_metadata", "metadata");
    types.put("financial_line_items", "facts");
    types.put("filing_contexts", "contexts");
    types.put("mda_sections", "mda");
    types.put("risk_factor_sections", "risk_factors");
    types.put("xbrl_relationships", "relationships");
    types.put("insider_transactions", "insider");
    types.put("institutional_holdings", "13f");
    types.put("beneficial_ownership", "13dg");
    types.put("earnings_transcripts", "earnings");
    return types;
  }

  /** Reads each table's top-level {@code pattern:} from the schema YAML. */
  private static Map<String, String> tablePatterns() throws Exception {
    Map<String, String> patterns = new LinkedHashMap<String, String>();
    InputStream in =
        SecStagedFilePatternOwnershipTest.class.getResourceAsStream("/sec/sec-schema.yaml");
    Pattern name = Pattern.compile("^  - name: (\\w+)\\s*$");
    Pattern pat = Pattern.compile("^    pattern: \"([^\"]+)\"\\s*$");
    String current = null;
    try (Scanner scanner = new Scanner(in, StandardCharsets.UTF_8.name())) {
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        Matcher n = name.matcher(line);
        if (n.matches()) {
          current = n.group(1);
          continue;
        }
        Matcher p = pat.matcher(line);
        if (p.matches() && current != null && !patterns.containsKey(current)) {
          patterns.put(current, p.group(1));
        }
      }
    }
    return patterns;
  }

  private static String qualify(String pattern) {
    return BASE + "/" + pattern;
  }

  @Test void testEachTableClaimsExactlyItsOwnFilesAndStockPricesClaimsNone() throws Exception {
    Map<String, String> patterns = tablePatterns();
    Map<String, String> types = fileTypes();
    List<String> staged = new ArrayList<String>();
    for (String type : types.values()) {
      staged.add(BASE + "/year=2022/" + type + "_batch_0001_0000.parquet");
      staged.add(BASE + "/year=2022/" + type + "_batch_0002_0001.parquet");
    }
    staged.add(BASE + "/stock_prices/year=2022/part-0000.parquet");

    for (Map.Entry<String, String> table : types.entrySet()) {
      String pattern = patterns.get(table.getKey());
      assertFalse(pattern == null, "no pattern read for " + table.getKey());
      List<String> claimed =
          IcebergMaterializer.filterStagedFilesForBatch(staged, qualify(pattern), "2022");
      assertEquals(
          Arrays.asList(
              BASE + "/year=2022/" + table.getValue() + "_batch_0001_0000.parquet",
              BASE + "/year=2022/" + table.getValue() + "_batch_0002_0001.parquet"),
          claimed, table.getKey() + " must claim exactly its own files");
    }

    String prices = patterns.get("stock_prices");
    assertEquals(Arrays.asList(BASE + "/stock_prices/year=2022/part-0000.parquet"),
        IcebergMaterializer.filterStagedFilesForBatch(staged, qualify(prices), "2022"),
        "stock_prices must claim only files under its own directory");
  }
}
