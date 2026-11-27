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
package org.apache.calcite.adapter.govdata.environment;

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.HooksConfig;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.govdata.ScienceBaseClient;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class UsgsWaterUseFileProviderTest {

  private static final String URL = "https://www.sciencebase.gov/catalog/file/get/abc?f=x";

  private static EtlPipelineConfig config() {
    return EtlPipelineConfig.builder()
        .name("water_use_county")
        .enabled(false)
        .source(HttpSourceConfig.builder().url(URL).build())
        .hooks(HooksConfig.builder()
            .responseTransformerClass(UsgsWaterUseCountyTransformer.class.getName())
            .build())
        .build();
  }

  private static UsgsWaterUseFileProvider provider(final List<String> requested,
      final String body) {
    return new UsgsWaterUseFileProvider(p -> new ScienceBaseClient() {
      @Override public String fetchText(String url) {
        throw new AssertionError("no text resource expected: " + url);
      }

      @Override public InputStream openStream(String url) {
        requested.add(url);
        return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
      }
    });
  }

  @Test void downloadsScienceBaseFileThroughTheClientAndTransformsIt() throws IOException {
    List<String> requested = new ArrayList<String>();
    String csv = "citation line,,\n"
        + "STATE,STATEFIPS,COUNTY,COUNTYFIPS,FIPS,YEAR,TO-Wtotl\n"
        + "AL,01,Autauga County,001,01001,2015,35.5\n";

    Iterator<Map<String, Object>> rows = provider(requested, csv).fetch(config(),
        Collections.<String, String>emptyMap(), RawCache.passthrough());
    Map<String, Object> row = rows.next();

    assertFalse(rows.hasNext());
    assertEquals(Collections.singletonList(URL), requested);
    assertEquals("01001", row.get("county_fips"));
    assertEquals(Integer.valueOf(2015), row.get("census_year"));
    assertEquals(Double.valueOf(35.5), row.get("total_withdrawals_mgd"));
  }

  @Test void bodyWithoutCountyRowsFails() {
    IOException e =
        assertThrows(IOException.class, () ->
            provider(new ArrayList<String>(), "<html>Just a moment...</html>").fetch(config(),
                Collections.<String, String>emptyMap(), RawCache.passthrough()));
    assertEquals("USGS water use: no county rows parsed from " + URL, e.getMessage());
  }
}
