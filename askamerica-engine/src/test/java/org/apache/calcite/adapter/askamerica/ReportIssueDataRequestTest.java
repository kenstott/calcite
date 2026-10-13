/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * When the LLM concludes askamerica does not have the data, it must file a sourcing request and
 * tell the user one was sent and that a new series typically takes 24-48 hours. These cover the
 * three places that rule lives: the server instructions, the report_issue tool description, and
 * the text report_issue returns — which must claim the request was sent only when the issues
 * endpoint actually accepted it.
 */
@Tag("unit")
@ResourceLock("askamerica.api.url")
class ReportIssueDataRequestTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String API_URL_PROP = "askamerica.api.url";

  private HttpServer server;
  private String previousApiUrl;
  private final AtomicReference<String> receivedBody = new AtomicReference<String>();

  private PrintStream previousLog;

  @BeforeEach
  void startServer() {
    previousApiUrl = System.getProperty(API_URL_PROP);
    previousLog = McpServer.log;
    McpServer.log = new PrintStream(new ByteArrayOutputStream());
  }

  @AfterEach
  void stopServer() {
    McpServer.log = previousLog;
    if (server != null) {
      server.stop(0);
    }
    if (previousApiUrl == null) {
      System.clearProperty(API_URL_PROP);
    } else {
      System.setProperty(API_URL_PROP, previousApiUrl);
    }
  }

  /** Starts an issues endpoint that answers every POST with the given status. */
  private void serveIssues(final int status) throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/v1/issues", exchange -> {
      InputStream in = exchange.getRequestBody();
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      byte[] chunk = new byte[1024];
      int n;
      while ((n = in.read(chunk)) > 0) {
        buf.write(chunk, 0, n);
      }
      receivedBody.set(new String(buf.toByteArray(), "UTF-8"));
      exchange.sendResponseHeaders(status, -1);
      exchange.close();
    });
    server.start();
    System.setProperty(API_URL_PROP, "http://127.0.0.1:" + server.getAddress().getPort());
  }

  @Test
  void instructionsRequireFilingAndTellingTheUser() {
    String instructions = McpServer.handleInitialize(MAPPER.createObjectNode().numberNode(1))
        .path("result").path("instructions").asText();
    assertTrue(instructions.contains("ALWAYS file a data request"), instructions);
    assertTrue(instructions.contains("report_issue with type 'sourcing'"), instructions);
    assertTrue(instructions.contains("24-48 hours"), instructions);
    assertTrue(instructions.contains("check back"), instructions);
    assertTrue(instructions.contains("ALWAYS file a defect report"), instructions);
    assertTrue(instructions.contains("report_issue with type 'defect'"), instructions);
    assertTrue(instructions.contains("fixes typically take 24-48 hours"), instructions);
    assertTrue(instructions.contains("unverified"), instructions);
  }

  @Test
  void toolDescriptionRequiresSourcingReportForMissingData() {
    JsonNode def = null;
    for (JsonNode t : McpServer.toolDefs()) {
      if ("report_issue".equals(t.path("name").asText())) {
        def = t;
      }
    }
    assertNotNull(def);
    String description = def.path("description").asText();
    assertTrue(description.contains("ALWAYS call it with type 'sourcing'"), description);
    assertTrue(description.contains("24-48 hours"), description);
    assertTrue(description.contains("ALWAYS call it with type 'defect'"), description);
  }

  @Test
  void acceptedSourcingReportTellsTheUserARequestWasSent() throws Exception {
    serveIssues(200);
    String text = McpServer.reportIssue("sourcing", "No salmon population data", "body");
    assertTrue(text.startsWith("Data request sent."), text);
    assertTrue(text.contains("24-48 hours"), text);
    assertTrue(text.contains("check back"), text);
    String sent = receivedBody.get();
    assertNotNull(sent, "the issues endpoint was never called");
    assertEquals("sourcing", MAPPER.readTree(sent).path("type").asText());
  }

  @Test
  void acceptedDefectReportTellsTheUserTheValueIsUnverified() throws Exception {
    serveIssues(200);
    String text = McpServer.reportIssue("defect", "Implausible total", "body");
    assertTrue(text.startsWith("Data-quality report sent."), text);
    assertTrue(text.contains("unverified"), text);
    assertTrue(text.contains("24-48 hours"), text);
    assertTrue(text.contains("check back"), text);
    assertEquals("defect", MAPPER.readTree(receivedBody.get()).path("type").asText());
  }

  @Test
  void rejectedDefectReportDoesNotClaimItWasSent() throws Exception {
    serveIssues(500);
    String text = McpServer.reportIssue("defect", "Implausible total", "body");
    assertTrue(text.contains("Nothing was filed"), text);
    assertFalse(text.contains("Data-quality report sent"), text);
  }

  @Test
  void rejectedSourcingReportDoesNotClaimARequestWasSent() throws Exception {
    serveIssues(500);
    String text = McpServer.reportIssue("sourcing", "No salmon population data", "body");
    assertTrue(text.contains("Nothing was filed"), text);
    assertFalse(text.contains("24-48 hours"), text);
    assertFalse(text.contains("Data request sent"), text);
  }
}
