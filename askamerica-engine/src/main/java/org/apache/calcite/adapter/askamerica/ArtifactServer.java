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
package org.apache.calcite.adapter.askamerica;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Serves generated charts over loopback so a response can carry a link instead of the artifact.
 *
 * <p>Returning a dashboard as inline SVG costs about 6,900 tokens, and it only becomes viewable
 * if the client re-emits all 6,900 into an artifact — another 6,900 out, for one chart, and only
 * if the model cooperates. A link costs about twenty tokens and works whether it cooperates or
 * not. The picture still rides along as a PNG image block for anyone who just wants to glance at
 * it; the link is for opening full size, zooming, and saving.
 *
 * <p>Deliberately local-only, because the alternative is uploading the user's query results
 * somewhere to make them viewable:
 *
 * <ul>
 *   <li>bound to <b>127.0.0.1</b>, never a wildcard address, so nothing off this machine can
 *       reach it;</li>
 *   <li>on an <b>ephemeral port</b> chosen by the OS, so it collides with nothing and is not a
 *       known target;</li>
 *   <li>reachable only at an <b>unguessable path</b> — 128 bits from {@link SecureRandom} — since
 *       any other process on the machine can talk to loopback, and sequential ids would let one
 *       enumerate every chart a session produced;</li>
 *   <li>a <b>lookup, never a file path</b>: the handler resolves ids in a map and has no notion
 *       of a filesystem, so there is nothing for path traversal to reach;</li>
 *   <li><b>read-only and bounded</b> — GET and HEAD only, the newest {@value #CAPACITY} artifacts,
 *       evicted oldest-first so a long session cannot grow without limit.</li>
 * </ul>
 *
 * <p>Artifacts live in memory and die with the process. That is the right lifetime: a chart is a
 * view of a query the caller just ran, not a document, and a link that outlives the conversation
 * would be a stale answer waiting to be quoted.
 */
final class ArtifactServer {

    private ArtifactServer() {}

    /** Most artifacts kept; older ones are evicted so a long session stays bounded. */
    private static final int CAPACITY = 64;

    private static final SecureRandom RANDOM = new SecureRandom();

    /** One served artifact: its bytes and how to label them. */
    private static final class Artifact {
        final byte[] body;
        final String contentType;

        Artifact(byte[] body, String contentType) {
            this.body = body;
            this.contentType = contentType;
        }
    }

    private static final Map<String, Artifact> ARTIFACTS =
        new LinkedHashMap<String, Artifact>(16, 0.75f, false) {
            @Override protected boolean removeEldestEntry(Map.Entry<String, Artifact> eldest) {
                return size() > CAPACITY;
            }
        };

    private static HttpServer server;
    private static String base;

    /**
     * Publishes bytes and returns their loopback URL, or null if a server cannot be started.
     *
     * <p>Null rather than an exception: a chart that cannot be linked is still a chart, and the
     * response already carries the PNG and the SVG. Failing the whole call because a convenience
     * could not start would trade a working answer for a broken one.
     */
    static synchronized String publish(byte[] body, String contentType, String extension) {
        if (!ensureStarted()) {
            return null;
        }
        byte[] token = new byte[16];
        RANDOM.nextBytes(token);
        StringBuilder id = new StringBuilder(32);
        for (byte b : token) {
            id.append(String.format("%02x", b));
        }
        String name = id + "." + extension;
        ARTIFACTS.put(name, new Artifact(body, contentType));
        return base + "/a/" + name;
    }

    /** Convenience for the common pair. */
    static String publishSvg(String svg) {
        return publish(svg.getBytes(StandardCharsets.UTF_8), "image/svg+xml; charset=utf-8",
            "svg");
    }

    static String publishPng(byte[] png) {
        return publish(png, "image/png", "png");
    }

    private static boolean ensureStarted() {
        if (server != null) {
            return true;
        }
        try {
            // Port 0 asks the OS for a free ephemeral port; loopback only, never a wildcard.
            server = HttpServer.create(
                new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
            server.createContext("/a/", ArtifactServer::handle);
            server.setExecutor(null);
            server.start();
            base = "http://127.0.0.1:" + server.getAddress().getPort();
            return true;
        } catch (IOException | RuntimeException e) {
            server = null;
            return false;
        }
    }

    private static void handle(HttpExchange exchange) throws IOException {
        try {
            String method = exchange.getRequestMethod();
            if (!"GET".equals(method) && !"HEAD".equals(method)) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            String path = exchange.getRequestURI().getPath();
            String name = path.startsWith("/a/") ? path.substring(3) : "";
            Artifact artifact;
            synchronized (ArtifactServer.class) {
                artifact = ARTIFACTS.get(name);
            }
            if (artifact == null) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            exchange.getResponseHeaders().set("Content-Type", artifact.contentType);
            // SVG can carry script and external references. Ours carries neither, and this says
            // so to the browser rather than relying on that staying true.
            exchange.getResponseHeaders().set("Content-Security-Policy",
                "default-src 'none'; style-src 'unsafe-inline'; img-src data:");
            exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            if ("HEAD".equals(method)) {
                exchange.sendResponseHeaders(200, -1);
                return;
            }
            exchange.sendResponseHeaders(200, artifact.body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(artifact.body);
            }
        } finally {
            exchange.close();
        }
    }
}
