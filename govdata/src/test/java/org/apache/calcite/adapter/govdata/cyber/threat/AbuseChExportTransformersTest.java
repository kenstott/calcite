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
package org.apache.calcite.adapter.govdata.cyber.threat;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** abuse.ch v2 plain-CSV exports: the key lives in the URL path and must never be echoed. */
@Tag("unit")
class AbuseChExportTransformersTest {
  private static final String KEY = "SECRETKEY123";

  private static final String URLHAUS =
      "################################################################\r\n"
      + "# abuse.ch URLhaus Database Dump (CSV - recent URLs only)      #\r\n"
      + "################################################################\r\n"
      + "#\r\n"
      + "# id,dateadded,url,url_status,last_online,threat,tags,urlhaus_link,reporter\r\n"
      + "\"3966310\",\"2026-10-23 13:47:07\",\"http://36.20.76.9:54560/i\",\"online\","
      + "\"2026-10-23 13:47:07\",\"malware_download\",\"None\","
      + "\"https://urlhaus.abuse.ch/url/3966310/\",\"adliwahid\"\r\n"
      + "\"3966308\",\"2026-10-23 13:41:20\",\"http://27.206.179.73:36227/i\",\"online\","
      + "\"2026-10-23 13:41:20\",\"malware_download\",\"32-bit,arm,elf,mirai,Mozi\","
      + "\"https://urlhaus.abuse.ch/url/3966308/\",\"geenensp\"\r\n";

  private static final String BAZAAR =
      "################################################################\r\n"
      + "# MalwareBazaar recent malware samples (CSV)                   #\r\n"
      + "################################################################\r\n"
      + "#\r\n"
      + "# \"first_seen_utc\",\"sha256_hash\",\"md5_hash\",\"sha1_hash\",\"reporter\"\r\n"
      + "\"2026-10-23 13:55:13\", \"6d6a677eea2174b07ce6565c4698e316ae46ddf56d7f97ec37eb2af05ba202b6\","
      + " \"5df33324e8c93029eae3002d2d84841b\", \"6d25bd92fd33911e5de117ed36a41e775ecd2b4f\","
      + " \"anonymous\", \"rebrrgb.dll\", \"exe\", \"application/vnd.microsoft.portable-executable\","
      + " \"n/a\", \"n/a\", \"n/a\", \"ada9a3db8d3f798ee257893b27c0d869\", \"12288:Labc\", \"T1abc\"\r\n";

  private HttpServer server;
  private volatile int status = 200;
  private volatile String body = "";

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", ex -> {
      byte[] out = body.getBytes(StandardCharsets.UTF_8);
      ex.sendResponseHeaders(status, out.length == 0 ? -1 : out.length);
      if (out.length > 0) {
        ex.getResponseBody().write(out);
      }
      ex.close();
    });
    server.start();
  }

  @AfterEach void stop() {
    server.stop(0);
  }

  private String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort()
        + "/v2/files/exports/" + KEY + "/recent.csv";
  }

  private RequestContext ctx() {
    return RequestContext.builder().url(url()).build();
  }

  @Test void urlhausParsesPlainCsvAndSkipsComments() throws Exception {
    body = URLHAUS;
    JsonNode rows = new ObjectMapper().readTree(
        new UrlhausResponseTransformer().transform("", ctx()));
    assertEquals(2, rows.size());
    assertEquals("3966310", rows.get(0).get("url_id").asText());
    assertEquals("http://36.20.76.9:54560/i", rows.get(0).get("url").asText());
    assertEquals("2026-10-23", rows.get(0).get("first_seen").asText());
    assertEquals("32-bit,arm,elf,mirai,Mozi", rows.get(1).get("tags").asText());
    assertEquals("urlhaus", rows.get(1).get("source").asText());
  }

  @Test void bazaarParsesSpaceSeparatedQuotedFields() throws Exception {
    body = BAZAAR;
    JsonNode rows = new ObjectMapper().readTree(
        new MalwareBazaarResponseTransformer().transform("", ctx()));
    assertEquals(1, rows.size());
    JsonNode r = rows.get(0);
    assertEquals("6d6a677eea2174b07ce6565c4698e316ae46ddf56d7f97ec37eb2af05ba202b6",
        r.get("sha256").asText());
    assertEquals("rebrrgb.dll", r.get("file_name").asText());
    assertEquals("2026-10-23", r.get("first_seen").asText());
    assertTrue(r.get("clamav").isNull());
    assertEquals("ada9a3db8d3f798ee257893b27c0d869", r.get("imphash").asText());
  }

  @Test void failureMessagesNeverContainTheAuthKey() {
    status = 410;
    RuntimeException a = assertThrows(RuntimeException.class,
        () -> new UrlhausResponseTransformer().transform("", ctx()));
    RuntimeException b = assertThrows(RuntimeException.class,
        () -> new MalwareBazaarResponseTransformer().transform("", ctx()));
    assertTrue(a.getMessage().contains("410"), a.getMessage());
    assertFalse(a.getMessage().contains(KEY), a.getMessage());
    assertFalse(b.getMessage().contains(KEY), b.getMessage());
  }
}
