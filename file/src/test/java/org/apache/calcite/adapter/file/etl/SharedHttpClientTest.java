/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.etl;

import com.sun.net.httpserver.HttpServer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.netty.util.AsciiString;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Local-server tests for {@link SharedHttpClient} and {@link RetryableHttp}: the client reaches
 * an HTTP/2-capable server over h2 and an HTTP/1.1-only server over HTTP/1.1, the response
 * wrapper exposes headers/error bodies, a stalled body read times out, and the 429
 * Retry-After retry and the {@code compressed: gzip} path still work.
 *
 * <p>The h2 server is Netty (already on the test classpath through the AWS SDK) speaking
 * cleartext h2c via the HTTP/1.1 {@code Upgrade} handshake the JDK client offers on
 * {@code http://} URLs. ALPN-over-TLS is the production negotiation path; it is not exercised here
 * because that needs a trusted certificate injected into the client.
 */
@Tag("unit")
class SharedHttpClientTest {

  private HttpServer http11;
  private String http11Url;
  private EventLoopGroup nettyGroup;
  private Channel h2Channel;
  private String h2Url;

  @BeforeEach void startServers() throws Exception {
    http11 = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    http11Url = "http://localhost:" + http11.getAddress().getPort();
    http11.start();

    nettyGroup = new NioEventLoopGroup(1);
    h2Channel = new ServerBootstrap()
        .group(nettyGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override protected void initChannel(SocketChannel ch) {
            HttpServerCodec codec = new HttpServerCodec();
            ch.pipeline().addLast(codec, new HttpServerUpgradeHandler(codec,
                protocol -> AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME,
                    protocol)
                    ? new Http2ServerUpgradeCodec(Http2FrameCodecBuilder.forServer().build(),
                        new Http2MultiplexHandler(new H2Stream()))
                    : null));
          }
        })
        .bind(new InetSocketAddress("localhost", 0)).sync().channel();
    h2Url = "http://localhost:" + ((InetSocketAddress) h2Channel.localAddress()).getPort();
  }

  @AfterEach void stopServers() throws Exception {
    http11.stop(0);
    h2Channel.close().sync();
    nettyGroup.shutdownGracefully(0, 1, java.util.concurrent.TimeUnit.SECONDS).sync();
  }

  /** Answers every h2 stream with 200 and a fixed body. */
  private static final class H2Stream extends ChannelInboundHandlerAdapter {
    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
      if (msg instanceof Http2HeadersFrame) {
        ByteBuf body = Unpooled.copiedBuffer("served-over-h2", StandardCharsets.UTF_8);
        ctx.write(new DefaultHttp2HeadersFrame(
            new DefaultHttp2Headers().status("200").set("etag", "\"h2-tag\""), false));
        ctx.writeAndFlush(new DefaultHttp2DataFrame(body, true));
      }
    }
  }

  private static String readAll(InputStream in) throws IOException {
    try (InputStream is = in) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[4096];
      int n;
      while ((n = is.read(buf)) != -1) {
        out.write(buf, 0, n);
      }
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
  }

  private static void respond(com.sun.net.httpserver.HttpExchange ex, int status, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
    if (bytes.length > 0) {
      try (OutputStream out = ex.getResponseBody()) {
        out.write(bytes);
      }
    }
    ex.close();
  }

  // (a) h2-capable server is reached over h2

  @Test void http2CapableServerIsReachedOverHttp2() throws Exception {
    try (SharedHttpClient.Response r =
        SharedHttpClient.execute(new SharedHttpClient.Request("GET", h2Url + "/x"))) {
      assertEquals(HttpClient.Version.HTTP_2, r.getVersion());
      assertEquals(200, r.getResponseCode());
      assertEquals("\"h2-tag\"", r.getHeaderField("ETag"));
      assertEquals("served-over-h2", readAll(r.getInputStream()));
    }
  }

  @Test void retryableHttpUsesHttp2WhenAvailable() throws Exception {
    SharedHttpClient.Response r =
        RetryableHttp.openWithRetry(h2Url + "/x", null, null, true);
    assertEquals(HttpClient.Version.HTTP_2, r.getVersion());
    assertEquals("served-over-h2", readAll(r.getInputStream()));
  }

  @Test void clientPrefersHttp2AndHasTwoRedirectModes() {
    assertEquals(HttpClient.Version.HTTP_2, SharedHttpClient.client(true).version());
    assertEquals(HttpClient.Redirect.NORMAL, SharedHttpClient.client(true).followRedirects());
    assertEquals(HttpClient.Redirect.NEVER, SharedHttpClient.client(false).followRedirects());
    assertSame(SharedHttpClient.client(true), SharedHttpClient.client(true));
  }

  // (b) HTTP/1.1-only server works through the same client; wrapper contract

  @Test void http11OnlyServerWorksAndWrapperExposesHeaders() throws Exception {
    http11.createContext("/res", ex -> {
      ex.getResponseHeaders().add("ETag", "\"abc\"");
      ex.getResponseHeaders().add("Last-Modified", "Tue, 15 Nov 1994 12:45:26 GMT");
      ex.getResponseHeaders().add("Content-Type", "text/plain");
      respond(ex, 200, "hello");
    });
    try (SharedHttpClient.Response r =
        SharedHttpClient.execute(new SharedHttpClient.Request("GET", http11Url + "/res"))) {
      assertEquals(HttpClient.Version.HTTP_1_1, r.getVersion());
      assertEquals(200, r.getResponseCode());
      assertEquals("\"abc\"", r.getHeaderField("etag"));
      assertEquals(5L, r.getContentLengthLong());
      assertEquals("text/plain", r.getContentType());
      assertEquals(784_903_526_000L, r.getHeaderFieldDate("Last-Modified", -1L));
      assertEquals(-1L, r.getHeaderFieldDate("Missing", -1L));
      assertNull(r.getHeaderField("Missing"));
      assertNull(r.getErrorStream());
      assertEquals("hello", readAll(r.getInputStream()));
    }
  }

  @Test void postBodyAndRequestHeadersReachTheServer() throws Exception {
    AtomicReference<String> seen = new AtomicReference<>();
    http11.createContext("/echo", ex -> {
      String body = readAll(ex.getRequestBody());
      seen.set(ex.getRequestMethod() + "|" + ex.getRequestHeaders().getFirst("X-Key") + "|"
          + ex.getRequestHeaders().getFirst("Content-Type") + "|" + body);
      respond(ex, 200, "ok");
    });
    SharedHttpClient.Request req = new SharedHttpClient.Request("POST", http11Url + "/echo")
        .setRequestProperty("X-Key", "k1")
        .setRequestProperty("content-type", "application/json")
        .setBody("{\"a\":1}".getBytes(StandardCharsets.UTF_8));
    try (SharedHttpClient.Response r = SharedHttpClient.execute(req)) {
      assertEquals("ok", readAll(r.getInputStream()));
    }
    assertEquals("POST|k1|application/json|{\"a\":1}", seen.get());
  }

  @Test void redirectsFollowedOnlyWhenRequested() throws Exception {
    http11.createContext("/target", ex -> respond(ex, 200, "arrived"));
    http11.createContext("/moved", ex -> {
      ex.getResponseHeaders().add("Location", "/target");
      respond(ex, 302, "");
    });
    try (SharedHttpClient.Response r = SharedHttpClient.execute(
        new SharedHttpClient.Request("GET", http11Url + "/moved").setFollowRedirects(true))) {
      assertEquals("arrived", readAll(r.getInputStream()));
    }
    try (SharedHttpClient.Response r = SharedHttpClient.execute(
        new SharedHttpClient.Request("GET", http11Url + "/moved").setFollowRedirects(false))) {
      assertEquals(302, r.getResponseCode());
    }
  }

  // (d) non-2xx error body is readable

  @Test void nonSuccessBodyIsReadableViaErrorStream() throws Exception {
    http11.createContext("/bad", ex -> respond(ex, 400, "{\"error\":\"bad param\"}"));
    try (SharedHttpClient.Response r =
        SharedHttpClient.execute(new SharedHttpClient.Request("GET", http11Url + "/bad"))) {
      assertEquals(400, r.getResponseCode());
      assertThrows(IOException.class, r::getInputStream);
      assertEquals("{\"error\":\"bad param\"}", readAll(r.getErrorStream()));
    }
  }

  @Test void nonRetryableStatusSurfacesAsHttpStatusException() {
    http11.createContext("/missing", ex -> respond(ex, 404, "nope"));
    RetryableHttp.HttpStatusException e = assertThrows(RetryableHttp.HttpStatusException.class,
        () -> RetryableHttp.openWithRetry(http11Url + "/missing", null, null, true));
    assertEquals(404, e.getStatus());
  }

  // (c) 429 + Retry-After still retried

  @Test void retryAfterOn429IsHonouredThenSucceeds() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    http11.createContext("/limited", ex -> {
      if (calls.incrementAndGet() == 1) {
        ex.getResponseHeaders().add("Retry-After", "1");
        respond(ex, 429, "slow down");
      } else {
        respond(ex, 200, "through");
      }
    });
    long start = System.nanoTime();
    SharedHttpClient.Response r =
        RetryableHttp.openWithRetry(http11Url + "/limited", null, null, true);
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
    assertEquals("through", readAll(r.getInputStream()));
    assertEquals(2, calls.get());
    assertTrue(elapsedMs >= 900, "Retry-After: 1 must delay the retry, took " + elapsedMs + "ms");
  }

  @Test void retriesExhaustedOn503Throws() {
    AtomicInteger calls = new AtomicInteger();
    http11.createContext("/down", ex -> {
      calls.incrementAndGet();
      ex.getResponseHeaders().add("Retry-After", "0");
      respond(ex, 503, "down");
    });
    Map<String, Object> rl = new LinkedHashMap<String, Object>();
    rl.put("maxRetries", 2);
    RetryableHttp.HttpStatusException e = assertThrows(RetryableHttp.HttpStatusException.class,
        () -> RetryableHttp.openWithRetry(http11Url + "/down", null,
            HttpSourceConfig.RateLimitConfig.fromMap(rl), true));
    assertEquals(503, e.getStatus());
    assertEquals(3, calls.get());
  }

  @Test void retryAfterParsesSecondsAndHttpDate() throws Exception {
    http11.createContext("/secs", ex -> {
      ex.getResponseHeaders().add("Retry-After", "7");
      respond(ex, 429, "x");
    });
    String future = DateTimeFormatter.RFC_1123_DATE_TIME.format(
        ZonedDateTime.now(java.time.ZoneOffset.UTC).plusSeconds(120));
    http11.createContext("/date", ex -> {
      ex.getResponseHeaders().add("Retry-After", future);
      respond(ex, 429, "x");
    });
    http11.createContext("/none", ex -> respond(ex, 429, "x"));
    try (SharedHttpClient.Response r =
        SharedHttpClient.execute(new SharedHttpClient.Request("GET", http11Url + "/secs"))) {
      assertEquals(7000L, RetryableHttp.parseRetryAfterMs(r));
    }
    try (SharedHttpClient.Response r =
        SharedHttpClient.execute(new SharedHttpClient.Request("GET", http11Url + "/date"))) {
      long ms = RetryableHttp.parseRetryAfterMs(r);
      assertTrue(ms > 100_000L && ms <= 120_000L, "date form delta was " + ms);
    }
    try (SharedHttpClient.Response r =
        SharedHttpClient.execute(new SharedHttpClient.Request("GET", http11Url + "/none"))) {
      assertEquals(-1L, RetryableHttp.parseRetryAfterMs(r));
    }
  }

  // read timeout: a stalled body read times out, a slow-but-flowing one does not

  @Test void stalledBodyReadTimesOut() throws Exception {
    http11.createContext("/stall", ex -> {
      ex.sendResponseHeaders(200, 100);
      OutputStream out = ex.getResponseBody();
      out.write("part".getBytes(StandardCharsets.UTF_8));
      out.flush();
      try {
        Thread.sleep(5_000);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      ex.close();
    });
    // The headers are awaited without a bound, so only the body read meets the 500ms guard;
    // execute() would apply the same 500ms to connect plus headers too.
    HttpResponse<InputStream> answered = SharedHttpClient.client(false).send(
        HttpRequest.newBuilder(URI.create(http11Url + "/stall")).build(),
        HttpResponse.BodyHandlers.ofInputStream());
    SharedHttpClient.Response r = new SharedHttpClient.Response(answered, 500);
    InputStream in = r.getInputStream();
    byte[] buf = new byte[4];
    assertEquals(4, in.read(buf));
    long start = System.nanoTime();
    assertThrows(SocketTimeoutException.class, () -> in.read());
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
    assertTrue(elapsedMs < 3_000, "stall guard should fire near 500ms, took " + elapsedMs + "ms");
  }

  @Test void slowButProgressingBodyIsNotCutOff() throws Exception {
    http11.createContext("/slow", ex -> {
      ex.sendResponseHeaders(200, 0);
      OutputStream out = ex.getResponseBody();
      try {
        for (int i = 0; i < 5; i++) {
          out.write(("c" + i).getBytes(StandardCharsets.UTF_8));
          out.flush();
          Thread.sleep(300);
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      ex.close();
    });
    try (SharedHttpClient.Response r = SharedHttpClient.execute(
        new SharedHttpClient.Request("GET", http11Url + "/slow").setReadTimeoutMs(1_000))) {
      assertEquals("c0c1c2c3c4", readAll(r.getInputStream()));
    }
  }

  @Test void headerTimeoutSurfacesAsSocketTimeout() {
    http11.createContext("/hang", ex -> {
      try {
        Thread.sleep(3_000);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      ex.close();
    });
    assertThrows(SocketTimeoutException.class, () -> SharedHttpClient.execute(
        new SharedHttpClient.Request("GET", http11Url + "/hang").setReadTimeoutMs(300)));
  }

  // (e) HttpSource over the shared client: gzip, POST body, non-2xx body

  private static HttpSourceConfig.ResponseConfig response(String compressed) {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    m.put("format", "JSON");
    if (compressed != null) {
      m.put("compressed", compressed);
    }
    return HttpSourceConfig.ResponseConfig.fromMap(m);
  }

  private static int drain(Iterator<Map<String, Object>> it) {
    int count = 0;
    while (it.hasNext()) {
      it.next();
      count++;
    }
    return count;
  }

  @Test void httpSourceReadsGzipResponse() throws Exception {
    http11.createContext("/gz", ex -> {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
        gz.write("[{\"id\":1},{\"id\":2},{\"id\":3}]".getBytes(StandardCharsets.UTF_8));
      }
      byte[] bytes = bos.toByteArray();
      ex.sendResponseHeaders(200, bytes.length);
      try (OutputStream out = ex.getResponseBody()) {
        out.write(bytes);
      }
    });
    HttpSourceConfig cfg = HttpSourceConfig.builder()
        .url(http11Url + "/gz").response(response("gzip")).build();
    assertEquals(3, drain(new HttpSource(cfg).fetch(Collections.<String, String>emptyMap())));
  }

  @Test void httpSourcePostSendsBodyAndHeadersOverSharedClient() throws Exception {
    AtomicReference<String> seen = new AtomicReference<>();
    http11.createContext("/post", ex -> {
      seen.set(ex.getRequestMethod() + "|" + ex.getRequestHeaders().getFirst("X-Api") + "|"
          + ex.getRequestHeaders().getFirst("Content-Type") + "|" + readAll(ex.getRequestBody()));
      respond(ex, 200, "[{\"id\":1}]");
    });
    Map<String, String> headers = new LinkedHashMap<String, String>();
    headers.put("X-Api", "secret");
    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("q", "v");
    HttpSourceConfig cfg = HttpSourceConfig.builder()
        .url(http11Url + "/post").method(HttpSourceConfig.HttpMethod.POST)
        .headers(headers).body(body).bodyFormat(HttpSourceConfig.BodyFormat.JSON)
        .response(response(null)).build();
    assertEquals(1, drain(new HttpSource(cfg).fetch(Collections.<String, String>emptyMap())));
    assertEquals("POST|secret|application/json|{\"q\":\"v\"}", seen.get());
  }

  @Test void httpSourceErrorBodyAppearsInFailure() {
    http11.createContext("/err", ex -> respond(ex, 400, "field 'x' is invalid"));
    HttpSourceConfig cfg = HttpSourceConfig.builder()
        .url(http11Url + "/err").response(response(null)).build();
    Exception e = assertThrows(Exception.class,
        () -> drain(new HttpSource(cfg).fetch(Collections.<String, String>emptyMap())));
    Throwable t = e;
    boolean found = false;
    while (t != null) {
      found |= String.valueOf(t.getMessage()).contains("field 'x' is invalid");
      t = t.getCause();
    }
    assertTrue(found, "error body must be surfaced, got: " + e);
  }

  @Test void httpSourceRetriesOn429HonouringRetryAfter() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    http11.createContext("/rl", ex -> {
      if (calls.incrementAndGet() == 1) {
        ex.getResponseHeaders().add("Retry-After", "1");
        respond(ex, 429, "slow");
      } else {
        respond(ex, 200, "[{\"id\":1}]");
      }
    });
    HttpSourceConfig cfg = HttpSourceConfig.builder()
        .url(http11Url + "/rl").response(response(null)).build();
    long start = System.nanoTime();
    assertEquals(1, drain(new HttpSource(cfg).fetch(Collections.<String, String>emptyMap())));
    assertEquals(2, calls.get());
    assertTrue((System.nanoTime() - start) / 1_000_000 >= 900, "Retry-After must delay the retry");
  }

  // mid-body truncation must still be classified retryable by HttpSource (commit bb2376b1f3)

  /** Serves one raw response then closes (optionally with a TCP reset); returns the thrown read. */
  private IOException readTruncated(String rawResponse, boolean reset) throws Exception {
    // The connection is cut only once the client has its response: cut before, the reset races
    // the headers -- Windows drops received bytes the client has not read yet, and the JDK
    // client may already have failed reading the body inside execute.
    final java.util.concurrent.CountDownLatch responded =
        new java.util.concurrent.CountDownLatch(1);
    try (java.net.ServerSocket raw = new java.net.ServerSocket(0, 1,
        java.net.InetAddress.getByName("localhost"))) {
      Thread t = new Thread(() -> {
        try (java.net.Socket sock = raw.accept()) {
          sock.getInputStream().read(new byte[4096]);
          sock.getOutputStream().write(rawResponse.getBytes(StandardCharsets.UTF_8));
          sock.getOutputStream().flush();
          if (!responded.await(30, java.util.concurrent.TimeUnit.SECONDS)) {
            throw new IllegalStateException("the client never had its response");
          }
          if (reset) {
            sock.setSoLinger(true, 0);
          }
        } catch (IOException ex) {
          throw new java.io.UncheckedIOException(ex);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      });
      t.start();
      SharedHttpClient.Response r = SharedHttpClient.execute(new SharedHttpClient.Request(
          "GET", "http://localhost:" + raw.getLocalPort() + "/t"));
      responded.countDown();
      IOException e = assertThrows(IOException.class, () -> readAll(r.getInputStream()));
      t.join();
      return e;
    }
  }

  private static boolean httpSourceRetries(IOException e) throws Exception {
    java.lang.reflect.Method m = HttpSource.class.getDeclaredMethod("shouldRetry",
        IOException.class, HttpSourceConfig.RateLimitConfig.class);
    m.setAccessible(true);
    HttpSource source = new HttpSource(
        HttpSourceConfig.builder().url("http://localhost/x").build());
    return (Boolean) m.invoke(source, e, HttpSourceConfig.RateLimitConfig.defaults());
  }

  @Test void shortFixedLengthBodyIsRetryable() throws Exception {
    IOException e = readTruncated(
        "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npartial", false);
    assertTrue(httpSourceRetries(e), "not retryable: " + e);
  }

  @Test void truncatedChunkedBodyIsRetryable() throws Exception {
    IOException e = readTruncated("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        + "a\r\n01234", false);
    assertTrue(httpSourceRetries(e), "not retryable: " + e);
  }

  @Test void connectionResetMidBodyIsRetryable() throws Exception {
    IOException e = readTruncated(
        "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npartial", true);
    assertTrue(httpSourceRetries(e), "not retryable: " + e);
  }
}
