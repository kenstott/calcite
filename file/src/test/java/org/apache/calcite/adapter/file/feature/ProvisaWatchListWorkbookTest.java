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
package org.apache.calcite.adapter.file.feature;

import org.apache.calcite.adapter.file.converters.ExcelReadHints;
import org.apache.calcite.adapter.file.converters.MultiTableExcelToJsonConverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The insider watch list workbook, whose sheets start with short-text data rows (ids, domains,
 * ISO timestamps), reads with one header row both automatically and under the headerRows hint.
 */
@Tag("unit")
public class ProvisaWatchListWorkbookTest {

  @TempDir
  public File tempDir;

  private Map<String, JsonNode> convert(ExcelReadHints hints) throws Exception {
    File out = new File(tempDir, "out-" + System.nanoTime());
    out.mkdirs();
    File book = new File(out, "watch_list.xlsx");
    Files.copy(getClass().getResourceAsStream("/excel/watch_list.xlsx"), book.toPath(),
        StandardCopyOption.REPLACE_EXISTING);
    MultiTableExcelToJsonConverter.convertFileToJson(book, out, true, "SMART_CASING",
        "SMART_CASING", out, null, hints);
    Map<String, JsonNode> tables = new TreeMap<>();
    File[] json = out.listFiles((dir, name) -> name.endsWith(".json") && !name.startsWith("."));
    for (File f : json) {
      tables.put(f.getName().replace(".json", ""), new ObjectMapper().readTree(f));
    }
    return tables;
  }

  private static List<String> columns(Map<String, JsonNode> tables, String sheet) {
    JsonNode rows = tables.get("watch_list__" + sheet);
    assertNotNull(rows, "table for sheet " + sheet + " in " + tables.keySet());
    List<String> names = new ArrayList<>();
    rows.get(0).fieldNames().forEachRemaining(names::add);
    return names;
  }

  private void assertHeaders(Map<String, JsonNode> tables) {
    assertEquals(
        Arrays.asList("deal_id", "code_name", "legal_name", "aliases", "ticker", "deal_type",
            "description", "announced_on"), columns(tables, "deals"));
    assertEquals(Arrays.asList("deal_id", "employee_id", "over_wall_at"),
        columns(tables, "deal_team"));
    assertEquals(Arrays.asList("message_id", "deal_id"), columns(tables, "screening"));
    assertEquals(Arrays.asList("deal_id", "domain"), columns(tables, "counterparties"));
  }

  @Test void shortTextRowsAreDataWithAutomaticHeaders() throws Exception {
    assertHeaders(convert(ExcelReadHints.AUTO));
  }

  @Test void shortTextRowsAreDataUnderTheHeaderRowsHint() throws Exception {
    assertHeaders(convert(
        ExcelReadHints.fromOperand(Collections.<String, Object>singletonMap("headerRows", 1))));
  }
}
