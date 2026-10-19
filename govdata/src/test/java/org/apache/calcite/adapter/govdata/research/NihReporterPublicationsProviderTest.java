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
package org.apache.calcite.adapter.govdata.research;

import org.apache.calcite.adapter.file.etl.ModelOperand;
import org.apache.calcite.adapter.file.etl.RawCache;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests HTTP-500 bisection and the schema-level appl_id exclusion list. */
@Tag("unit")
class NihReporterPublicationsProviderTest {
  private static final long POISON = 11269188L;
  private static final Pattern IDS = Pattern.compile("\"appl_ids\":\\[([0-9,]*)\\]");

  private HttpServer server;
  private final List<Integer> requestSizes = new CopyOnWriteArrayList<Integer>();

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/pubs", ex -> {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      InputStream in = ex.getRequestBody();
      byte[] buf = new byte[4096];
      int n;
      while ((n = in.read(buf)) > 0) {
        bos.write(buf, 0, n);
      }
      Matcher m = IDS.matcher(new String(bos.toByteArray(), StandardCharsets.UTF_8));
      if (!m.find()) {
        throw new IOException("no appl_ids in request");
      }
      String[] ids = m.group(1).split(",");
      requestSizes.add(ids.length);
      boolean poison = false;
      StringBuilder results = new StringBuilder();
      for (String id : ids) {
        if (Long.parseLong(id) == POISON) {
          poison = true;
        }
        if (results.length() > 0) {
          results.append(',');
        }
        results.append("{\"applid\":").append(id).append(",\"coreproject\":\"C").append(id)
            .append("\",\"pmid\":").append(Long.parseLong(id) + 1).append('}');
      }
      byte[] out;
      int status;
      if (poison) {
        status = 500;
        out = "{\"error\":\"boom\"}".getBytes(StandardCharsets.UTF_8);
      } else {
        status = 200;
        out = ("{\"meta\":{\"total\":" + ids.length + "},\"results\":[" + results + "]}")
            .getBytes(StandardCharsets.UTF_8);
      }
      ex.sendResponseHeaders(status, out.length);
      ex.getResponseBody().write(out);
      ex.close();
    });
    server.start();
  }

  @AfterEach void stop() {
    server.stop(0);
    ModelOperand.clear();
  }

  private NihReporterPublicationsProvider provider() {
    return new NihReporterPublicationsProvider(
        "http://127.0.0.1:" + server.getAddress().getPort() + "/pubs");
  }

  private static List<Long> ids() {
    return new ArrayList<Long>(Arrays.asList(1L, 2L, 3L, POISON, 5L, 6L, 7L, 8L));
  }

  @Test void poisonIdSplitsDownAndThrowsNamingTheSingleId() {
    IOException e = assertThrows(IOException.class,
        () -> provider().fetchPublicationsForBatch(ids(), "2026", RawCache.passthrough(), 0));
    assertTrue(e.getMessage().contains("appl_id " + POISON), e.getMessage());
    // 8 -> 4 (500) -> 2 (500) -> 1 (500): the full batch was split, not retried whole.
    assertTrue(requestSizes.contains(8) && requestSizes.contains(4) && requestSizes.contains(2)
        && requestSizes.contains(1), requestSizes.toString());
  }

  @Test void excludedIdIsFilteredAndRestOfBatchSucceeds() throws IOException {
    Map<String, Object> schema = new HashMap<String, Object>();
    Map<String, Object> tables = new HashMap<String, Object>();
    Map<String, Object> nih = new HashMap<String, Object>();
    nih.put("excludeApplIds", Collections.singletonList(POISON));
    tables.put("nih_publications", nih);
    schema.put("partitionedTables", tables);
    ModelOperand.capture("research", schema);

    Set<Long> excluded = NihReporterPublicationsProvider.loadExcludedApplIds();
    assertEquals(Collections.singleton(POISON), excluded);

    List<Long> kept = new ArrayList<Long>(ids());
    kept.removeAll(excluded);
    List<Map<String, Object>> rows =
        provider().fetchPublicationsForBatch(kept, "2026", RawCache.passthrough(), 0);
    assertEquals(7, rows.size());
    assertEquals(Collections.singletonList(7), requestSizes);
  }

  @Test void noExclusionConfiguredMeansEmptySet() {
    assertTrue(NihReporterPublicationsProvider.loadExcludedApplIds().isEmpty());
  }
}
