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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A refused web_fetch (401/403/429/451) tells the model to retry with the client's fetch. */
@Tag("unit")
class WebFetchBlockedStatusTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static String fetchStatus(int status) throws Exception {
        McpServer.log = new java.io.PrintStream(new java.io.ByteArrayOutputStream());
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", ex -> {
            ex.sendResponseHeaders(status, -1);
            ex.close();
        });
        server.start();
        try {
            ObjectNode args = MAPPER.createObjectNode();
            args.put("url", "http://127.0.0.1:" + server.getAddress().getPort() + "/p" + status);
            Method m = McpServer.class.getDeclaredMethod("webFetch", JsonNode.class);
            m.setAccessible(true);
            return (String) m.invoke(null, args);
        } finally {
            server.stop(0);
        }
    }

    @Test void blockedStatusesPointToClientFetch() throws Exception {
        for (int status : new int[] {401, 403, 429, 451}) {
            String msg = fetchStatus(status);
            assertTrue(msg.contains("HTTP " + status), msg);
            assertTrue(msg.contains("client-side web fetch"), msg);
        }
    }

    @Test void otherErrorStatusDoesNotPointToClientFetch() throws Exception {
        String msg = fetchStatus(404);
        assertTrue(msg.contains("HTTP 404"), msg);
        assertFalse(msg.contains("client-side web fetch"), msg);
    }
}
