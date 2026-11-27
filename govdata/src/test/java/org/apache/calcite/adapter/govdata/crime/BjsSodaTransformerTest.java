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
package org.apache.calcite.adapter.govdata.crime;

import org.apache.calcite.adapter.file.etl.HttpSource;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.etl.HooksConfig;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link BjsSodaTransformer}.
 */
@Tag("unit")
class BjsSodaTransformerTest {

  private final BjsSodaTransformer transformer = new BjsSodaTransformer();
  private final RequestContext context = RequestContext.builder()
      .url("https://api.ojp.gov/bjsdataset/v1/ms42-n765.json")
      .dimensionValues(Collections.singletonMap("endpoint_id", "ms42-n765"))
      .build();

  @Test void testPageIsEnrichedWithEndpointId() {
    assertEquals("[{\"a\":\"1\",\"endpoint_id\":\"ms42-n765\"}]",
        transformer.transform("[{\"a\":\"1\"}]", context));
  }

  @Test void testEmptyPageStaysEmpty() {
    assertEquals("[]", transformer.transform("[]", context));
  }

  @Test void testNonArrayResponsesFail() {
    assertThrows(RuntimeException.class, () -> transformer.transform("", context));
    assertThrows(RuntimeException.class,
        () -> transformer.transform("{\"error\":true,\"message\":\"throttled\"}", context));
    assertThrows(RuntimeException.class, () -> transformer.transform("<html>", context));
  }

  @Test void testPaginatedRawCacheReplaysEveryRecord(@TempDir File dir) throws Exception {
    File cache = new File(dir, "endpoint_id=ms42-n765/response.json");
    Files.createDirectories(cache.getParentFile().toPath());
    Files.write(cache.toPath(),
        "{\"results\":[{\"a\":\"1\"},{\"a\":\"2\"}]}".getBytes(StandardCharsets.UTF_8));

    Map<String, Object> pagination = new LinkedHashMap<String, Object>();
    pagination.put("type", "OFFSET");
    pagination.put("limitParam", "$limit");
    pagination.put("offsetParam", "$offset");
    pagination.put("pageSize", 50000);
    Map<String, Object> response = new LinkedHashMap<String, Object>();
    response.put("format", "json");
    response.put("pagination", pagination);
    Map<String, Object> source = new LinkedHashMap<String, Object>();
    source.put("url", "http://localhost:1/{endpoint_id}.json");
    source.put("response", response);
    source.put("rawCache", Collections.singletonMap("enabled", true));
    Map<String, Object> hooks = Collections.<String, Object>singletonMap(
        "responseTransformer", BjsSodaTransformer.class.getName());

    HttpSource httpSource =
        new HttpSource(HttpSourceConfig.fromMap(source), HooksConfig.fromMap(hooks),
            new LocalFileStorageProvider(), dir.getAbsolutePath());
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Iterator<Map<String, Object>> it =
        httpSource.fetch(Collections.singletonMap("endpoint_id", "ms42-n765"));
    while (it.hasNext()) {
      rows.add(it.next());
    }

    assertEquals(2, rows.size());
    assertEquals("2", rows.get(1).get("a"));
    assertEquals("ms42-n765", rows.get(1).get("endpoint_id"));
  }
}
