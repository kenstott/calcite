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

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class UsgsMineralCommodityStatsProviderTest {

  private static final String LISTING = "https://www.sciencebase.gov/catalog/items?parentId=p";
  private static final String CSV_URL = "https://www.sciencebase.gov/catalog/file/get/abc";

  private static EtlPipelineConfig config() {
    return EtlPipelineConfig.builder()
        .name("usgs_mineral_commodity_stats")
        .enabled(false)
        .source(HttpSourceConfig.builder().url(LISTING).build())
        .build();
  }

  @Test void readsListingItemAndCsvThroughTheClient() throws IOException {
    final List<String> requested = new ArrayList<String>();
    final String csv = "MCS chapter,Section,Commodity,Country,Statistics,Statistics_detail,"
        + "Unit,Year,Value,Notes,Is critical mineral 2025,Other notes\r\n"
        + "COPPER,Salient\u2014United States,Copper,United States,Production,Mine,"
        + "metric tons,2024,\"1,100\",,Yes,\r\n";
    UsgsMineralCommodityStatsProvider provider =
        new UsgsMineralCommodityStatsProvider(p -> new UsgsMineralCommodityStatsProvider.Client() {
          @Override public String fetchText(String url) {
            requested.add(url);
            if (url.equals(LISTING)) {
              return "{\"items\":[{\"id\":\"g\",\"title\":\"Compilation of geospatial data\"},"
                  + "{\"id\":\"abc\",\"title\":\"Mineral Commodity Summaries 2026 Data Release\"}]}";
            }
            return "{\"files\":[{\"name\":\"MCS2026_Commodities_Data.csv\",\"downloadUri\":\""
                + CSV_URL + "\"}]}";
          }

          @Override public byte[] fetchBytes(String url) {
            requested.add(url);
            return csv.getBytes(Charset.forName("windows-1252"));
          }
        });

    Iterator<Map<String, Object>> rows = provider.fetch(config(),
        Collections.<String, String>emptyMap(), RawCache.passthrough());
    Map<String, Object> row = rows.next();

    assertFalse(rows.hasNext());
    assertEquals("Salient\u2014United States", row.get("section"));
    assertEquals(Double.valueOf(1100), row.get("value_num"));
    assertEquals(Integer.valueOf(2026), row.get("year"));
    assertEquals(3, requested.size());
    assertEquals(CSV_URL, requested.get(2));
  }

  @Test void missingBrowserSettingsFailNamingTheParameter() {
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () ->
            new UsgsMineralCommodityStatsProvider().fetch(config(),
                Collections.<String, String>emptyMap(), RawCache.passthrough()));
    assertEquals("sciencebase: source parameter browserNodeCommand is missing from the table "
        + "source block", e.getMessage());
  }
}
