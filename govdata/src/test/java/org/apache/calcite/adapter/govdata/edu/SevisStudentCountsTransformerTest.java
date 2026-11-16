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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests {@link SevisStudentCountsTransformer} against a local server shaped like the DHS feed. */
@Tag("unit")
class SevisStudentCountsTransformerTest {

  /** Two periods; the first has no STEM arrays and two surplus trailing coc elements. */
  private static final String FEED = "{\"reporting_periods\":[\"April 2014\",\"October 2026\"],"
      + "\"counts\":{},"
      + "\"April 2014\":{\"codes\":[\"CN\",\"PS\"],\"continent\":[\"Asia\",\"Asia\"],"
      + "\"region\":[\"Eastern Asia\",\"Western Asia\",\"x\",\"y\"],"
      + "\"coc\":[\"CHINA\",\"WEST BANK\",\"x\",\"y\"],"
      + "\"active_students\":[10,0],\"male\":[6,0],\"female\":[4,0]," + edu(new int[] {10, 0}, "")
      + ",\"CA\":[7,0],\"NY\":[3,0]},"
      + "\"October 2026\":{\"codes\":[\"CN\",\"PS\"],\"continent\":[\"Asia\",\"Asia\"],"
      + "\"region\":[\"Eastern Asia\",\"Western Asia\"],\"coc\":[\"CHINA\",\"WEST BANK\"],"
      + "\"active_students\":[8,2],\"male\":[5,1],\"female\":[3,1]," + edu(new int[] {8, 2}, "")
      + "," + edu(new int[] {5, 0}, "stem_") + ",\"stem_male\":[3,0],\"stem_female\":[2,0],"
      + "\"CA\":[8,0],\"NY\":[0,2]}}";

  private static String edu(int[] first, String prefix) {
    String[] levels = {"associate", "bachelors", "masters", "doctorate", "language_training",
        "high_school", "primary", "secondary", "flight_school", "other_vocational_school",
        "other"};
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < levels.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      int a = i == 1 ? first[0] : 0;
      int b = i == 1 ? first[1] : 0;
      sb.append('"').append(prefix).append(levels[i]).append("\":[").append(a).append(',')
          .append(b).append(']');
    }
    return sb.toString();
  }

  private static HttpServer serve(String page, String feed) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/tool", ex -> reply(ex, page));
    server.createContext("/files/2026-10/OCTOBER_2026.json", ex -> reply(ex, feed));
    server.start();
    return server;
  }

  private static void reply(com.sun.net.httpserver.HttpExchange ex, String body)
      throws java.io.IOException {
    byte[] b = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(200, b.length);
    ex.getResponseBody().write(b);
    ex.close();
  }

  private static String run(HttpServer server, String page) {
    String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/tool";
    return new SevisStudentCountsTransformer().transform(page,
        RequestContext.builder().url(url).build());
  }

  @Test void parsesBreakdownsAndDropsZeros() throws Exception {
    String page = "<div data-json=\"/files/2026-10/OCTOBER_2026.json\"></div>";
    HttpServer server = serve(page, FEED);
    try {
      JsonNode rows = new ObjectMapper().readTree(run(server, page));
      int stem = 0;
      long chinaTotal2026 = -1;
      long chinaCa2014 = -1;
      for (JsonNode r : rows) {
        assertTrue(r.get("students").asLong() > 0);
        if (r.get("breakdown").asText().startsWith("stem_")) {
          stem++;
          assertEquals("2026-10-01", r.get("reporting_month").asText());
        }
        if ("CHINA".equals(r.get("country_name").asText())) {
          if ("total".equals(r.get("breakdown").asText())
              && "2026-10-01".equals(r.get("reporting_month").asText())) {
            chinaTotal2026 = r.get("students").asLong();
          }
          if ("CA".equals(r.get("category").asText())
              && "2014-04-01".equals(r.get("reporting_month").asText())) {
            chinaCa2014 = r.get("students").asLong();
          }
        }
        assertTrue(!"WEST BANK".equals(r.get("country_name").asText())
            || "2026-10-01".equals(r.get("reporting_month").asText()),
            "zero-count West Bank 2014 rows must not be emitted");
      }
      assertEquals(8, chinaTotal2026);
      assertEquals(7, chinaCa2014);
      // CHINA: stem_sex female+male (2) + stem_education_level bachelors (1) = 3 rows; stem_*
      // for WEST BANK are all zero.
      assertEquals(3, stem);
    } finally {
      server.stop(0);
    }
  }

  @Test void missingDataJsonAttributeFails() throws Exception {
    String page = "<div>no feed here</div>";
    HttpServer server = serve(page, FEED);
    try {
      assertThrows(RuntimeException.class, () -> run(server, page));
    } finally {
      server.stop(0);
    }
  }

  @Test void shortArrayFails() throws Exception {
    String page = "<div data-json=\"/files/2026-10/OCTOBER_2026.json\"></div>";
    HttpServer server = serve(page, FEED.replace("\"male\":[5,1]", "\"male\":[5]"));
    try {
      assertThrows(RuntimeException.class, () -> run(server, page));
    } finally {
      server.stop(0);
    }
  }

  @Test void parsesMonthLabel() {
    assertEquals("2014-04-01", SevisStudentCountsTransformer.parseMonth("April 2014").toString());
  }
}
