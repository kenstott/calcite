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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for the IPEDS 12-Month Enrollment transformers. */
@Tag("unit")
class IpedsTwelveMonthEnrollmentTransformerTest {

  /** UTF-8 byte order mark, as NCES writes it in the 2023 and 2024 files. */
  private static final String BOM = "ï»¿";

  private HttpServer server;
  private byte[] body;

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(body);
        }
      }
    });
    server.start();
  }

  @AfterEach void stopServer() {
    server.stop(0);
  }

  @Test void coarseLevelsOfEarlyVintagesMapToDetailCodes() throws IOException {
    serve("effy2019.csv", "UNITID,EFFYLEV,LSTUDY,XEYTOTLT,EFYTOTLT\n"
        + "100654,1,999,\"R\",6686\n"
        + "100654,4,3,\"R\",1156\n");

    List<Map<String, Object>> rows =
        read(new IpedsTwelveMonthEnrollmentResponseTransformer(), "2019");

    assertEquals(2, rows.size());
    assertEquals(1, rows.get(0).get("effyalev"));
    assertEquals(6686, rows.get(0).get("efytotlt"));
    assertEquals(2019, rows.get(0).get("year"));
    assertEquals(12, rows.get(1).get("effyalev"));
    assertEquals("graduate", rows.get(1).get("level_of_study"));
    assertFalse(rows.get(1).containsKey("effylev"));
    assertFalse(rows.get(1).containsKey("xeytotlt"));
  }

  @Test void detailLevelsDeriveAttendanceAndDotsAreNull() throws IOException {
    serve("EFFY2024.csv", BOM + "UNITID,EFFYALEV,EFFYLEV,LSTUDY,EFYTOTLT,EFYGUAN\n"
        + "100654,31,-2,1,40,.\n");

    List<Map<String, Object>> rows =
        read(new IpedsTwelveMonthEnrollmentResponseTransformer(), "2024");

    assertEquals(100654, rows.get(0).get("unitid"));
    assertEquals("full_time", rows.get(0).get("attendance"));
    assertEquals("undergraduate", rows.get(0).get("level_of_study"));
    assertEquals("Full-time, Undergraduate, Non-degree/certificate-seeking",
        rows.get(0).get("effyalev_label"));
    assertNull(rows.get(0).get("efyguan"));
  }

  @Test void unknownLevelFailsTheLoad() throws IOException {
    serve("effy2024.csv", "UNITID,EFFYALEV,EFYTOTLT\n100654,99,1\n");

    assertThrows(IllegalStateException.class,
        () -> read(new IpedsTwelveMonthEnrollmentResponseTransformer(), "2024"));
  }

  @Test void dualEnrollmentReadsTheRevisedHighSchoolFile() throws IOException {
    serve("EFFY2023_HS_RV.csv", "UNITID,EFYTOTLT,EFYTOTLM\n100663,114,33\n");

    List<Map<String, Object>> rows = read(new IpedsDualEnrollmentResponseTransformer(), "2023");

    assertEquals(114, rows.get(0).get("efytotlt"));
    assertEquals(33, rows.get(0).get("efytotlm"));
  }

  @Test void distanceEnrollmentLabelsTheLevel() throws IOException {
    serve("effy2024_dist.csv", "UNITID,EFFYDLEV,EFYDETOT,EFYDEEXC,EFYDESOM,EFYDENON\n"
        + "100654,12,923,143,36,744\n");

    List<Map<String, Object>> rows =
        read(new IpedsDistanceEnrollmentResponseTransformer(), "2024");

    assertEquals("Graduate", rows.get(0).get("effydlev_label"));
    assertEquals(143, rows.get(0).get("efydeexc"));
  }

  @Test void instructionalActivityKeepsFteColumns() throws IOException {
    serve("efia2024.csv", "UNITID,CDACTUA,CNACTUA,EFTEUG,FTEDPP,ACTTYPE\n"
        + "100654,151217,.,5041,.,2\n");

    List<Map<String, Object>> rows =
        read(new IpedsInstructionalActivityResponseTransformer(), "2024");

    assertEquals(5041, rows.get(0).get("efteug"));
    assertNull(rows.get(0).get("cnactua"));
    assertEquals(2, rows.get(0).get("acttype"));
  }

  private void serve(String entryName, String csv) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
      zip.putNextEntry(new ZipEntry(entryName));
      zip.write(csv.getBytes(StandardCharsets.ISO_8859_1));
      zip.closeEntry();
    }
    body = bytes.toByteArray();
  }

  private List<Map<String, Object>> read(IpedsBulkCsvTransformer transformer, String year)
      throws IOException {
    RequestContext context = RequestContext.builder()
        .url("http://127.0.0.1:" + server.getAddress().getPort() + "/file.zip")
        .dimensionValues(Collections.singletonMap("effective_year", year))
        .build();
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Iterator<Map<String, Object>> it = transformer.fetchAndTransform(context);
    while (it.hasNext()) {
      rows.add(it.next());
    }
    return rows;
  }
}
