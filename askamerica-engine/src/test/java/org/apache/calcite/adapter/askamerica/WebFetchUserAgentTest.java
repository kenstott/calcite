/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** web_fetch sends its own User-Agent, not a browser's: Akamai refuses a Java client posing as
 *  Chrome. */
@Tag("unit")
class WebFetchUserAgentTest {
    @Test void sendsHonestUserAgent() throws Exception {
        McpServer.log = new java.io.PrintStream(new java.io.ByteArrayOutputStream());
        AtomicReference<String> seen = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", ex -> {
            seen.set(ex.getRequestHeaders().getFirst("User-Agent"));
            byte[] body = "<html><body>ok</body></html>".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/html");
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
            ex.close();
        });
        server.start();
        try {
            ObjectNode args = new ObjectMapper().createObjectNode();
            args.put("url", "http://127.0.0.1:" + server.getAddress().getPort() + "/ua");
            Method m = McpServer.class.getDeclaredMethod("webFetch", JsonNode.class);
            m.setAccessible(true);
            m.invoke(null, args);
        } finally {
            server.stop(0);
        }
        assertEquals(McpServer.FETCH_USER_AGENT, seen.get());
        assertFalse(seen.get().contains("Mozilla"), seen.get());
    }
}
