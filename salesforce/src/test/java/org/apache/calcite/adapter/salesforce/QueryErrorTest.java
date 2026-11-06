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
package org.apache.calcite.adapter.salesforce;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A query Salesforce refuses reports Salesforce's own error, not a bare "Query failed".
 */
class QueryErrorTest {

  private static final String REFUSAL = "[{\"message\":\"No such column 'Foo__c' on entity "
      + "'Account'.\",\"errorCode\":\"INVALID_FIELD\"}]";

  private HttpServer server;
  private SalesforceConnection connection;

  @BeforeEach void serveARefusal() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/", exchange -> {
      byte[] body = REFUSAL.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(400, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    });
    server.start();
    String instanceUrl = "http://localhost:" + server.getAddress().getPort();
    connection = new SalesforceConnection(instanceUrl,
        SalesforceConnection.AuthConfig.accessToken("token", instanceUrl), "v59.0");
  }

  @AfterEach void stop() throws IOException {
    connection.close();
    server.stop(0);
  }

  @Test void aRefusedQueryCarriesSalesforcesErrorCodeAndMessage() {
    IOException e = assertThrows(IOException.class,
        () -> connection.query("SELECT Foo__c FROM Account"));
    assertThat(e.getMessage(), containsString("INVALID_FIELD"));
    assertThat(e.getMessage(), containsString("No such column 'Foo__c'"));
  }

  @Test void aRefusedQueryMoreCarriesSalesforcesError() {
    IOException e = assertThrows(IOException.class,
        () -> connection.queryMore("/services/data/v59.0/query/01g-2000"));
    assertThat(e.getMessage(), containsString("INVALID_FIELD"));
  }

  @Test void theEnumeratorReportsSalesforcesError() {
    SalesforceEnumerator enumerator = new SalesforceEnumerator(connection,
        "SELECT Foo__c FROM Account", Collections.emptyList());
    RuntimeException e = assertThrows(RuntimeException.class, enumerator::moveNext);
    assertThat(e.getMessage(), containsString("INVALID_FIELD"));
  }
}
