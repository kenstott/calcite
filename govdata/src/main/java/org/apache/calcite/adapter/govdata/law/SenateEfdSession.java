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
package org.apache.calcite.adapter.govdata.law;
// storage-provider-guard:ignore-file - audited: the cache is a local temp directory, never an
// object-store URI.

import org.apache.calcite.adapter.govdata.GovDataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.CookieManager;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One scripted session against the Senate's eFD system (efdsearch.senate.gov). Every search and
 * view is gated behind a click-through agreement (a CSRF form posting {@code
 * prohibition_agreement=1}); {@link #acceptAgreement()} submits it once and the cookie jar carries
 * the session after that. Requests are serialized and spaced by {@link #REQUEST_SPACING_MS}, and a
 * 5xx or I/O failure is retried with a growing delay (10s, 20s, ... about 2.5 minutes in all, since
 * eFD answers runs of 503s for minutes at a time) before it is thrown.
 *
 * <p>Redirects are never followed: a view URL that answers 302 to the site root is a report the
 * Senate no longer serves (candidate reports expire a year after the candidacy ends), which
 * {@link #getView(String)} reports as {@link View#unavailable} rather than hiding behind the home
 * page's HTML.
 */
final class SenateEfdSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(SenateEfdSession.class);

  static final String SITE = "https://efdsearch.senate.gov";

  private static final long REQUEST_SPACING_MS = 350L;
  private static final int MAX_ATTEMPTS = 6;
  private static final String USER_AGENT = "Mozilla/5.0 (compatible; govdata-etl)";

  /** Result of {@link #getView(String)}. */
  static final class View {
    final String body;
    final boolean unavailable;

    View(String body, boolean unavailable) {
      this.body = body;
      this.unavailable = unavailable;
    }
  }

  /**
   * Local cache of report pages and past-year listings. A report page is immutable (an amendment
   * is a new filing with its own UUID) and an expired report stays expired, so both are cached
   * forever; a listing is cached only for a year that has ended. Every table of a year reads the
   * same pages, so without this each of the ten tables would crawl the site again.
   */
  private static final File CACHE_DIR =
      new File(System.getProperty("java.io.tmpdir"), "govdata-senate-efd");
  private static final String UNAVAILABLE_MARKER = "UNAVAILABLE";

  private final HttpClient client;
  private final CookieManager cookies = new CookieManager();
  private boolean agreed;
  private long lastRequestAt;

  SenateEfdSession() {
    this.client = HttpClient.newBuilder().cookieHandler(cookies)
        .followRedirects(HttpClient.Redirect.NEVER).connectTimeout(Duration.ofSeconds(30)).build();
  }

  /** Loads the agreement form and submits it; idempotent. */
  synchronized void acceptAgreement() throws IOException {
    if (agreed) {
      return;
    }
    String form = send(get("/search/home/")).body();
    String token = attribute(form, "csrfmiddlewaretoken");
    if (token == null) {
      throw new GovDataException(SITE + "/search/home/: no csrfmiddlewaretoken in the agreement form");
    }
    Map<String, String> fields = new LinkedHashMap<String, String>();
    fields.put("csrfmiddlewaretoken", token);
    fields.put("prohibition_agreement", "1");
    HttpResponse<String> response = send(post("/search/home/", fields, "/search/home/", null));
    if (response.statusCode() != 302 || !response.headers().firstValue("Location").orElse("")
        .endsWith("/search/")) {
      throw new GovDataException(SITE + "/search/home/: agreement POST answered "
          + response.statusCode() + " instead of a redirect to /search/");
    }
    agreed = true;
  }

  /** POSTs the DataTables listing query and returns the JSON body. */
  synchronized String listing(Map<String, String> params, boolean cacheable) throws IOException {
    File cached = new File(CACHE_DIR, "list-" + sha256(params.toString()) + ".json");
    if (cacheable && cached.isFile()) {
      return new String(Files.readAllBytes(cached.toPath()), StandardCharsets.UTF_8);
    }
    acceptAgreement();
    HttpResponse<String> response =
        send(post("/search/report/data/", params, "/search/", csrfCookie()));
    if (response.statusCode() != 200) {
      throw new GovDataException(SITE + "/search/report/data/: HTTP " + response.statusCode()
          + " (a 302 means the session's agreement lapsed)");
    }
    if (cacheable) {
      store(cached, response.body());
    }
    return response.body();
  }

  /** GETs a report view path such as {@code /search/view/annual/{uuid}/}. */
  synchronized View getView(String path) throws IOException {
    File cached = new File(CACHE_DIR, path.replaceAll("[^A-Za-z0-9]+", "_") + ".html");
    if (cached.isFile()) {
      String body = new String(Files.readAllBytes(cached.toPath()), StandardCharsets.UTF_8);
      return body.equals(UNAVAILABLE_MARKER) ? new View(null, true) : new View(body, false);
    }
    acceptAgreement();
    HttpResponse<String> response = send(get(path));
    if (response.statusCode() == 200) {
      store(cached, response.body());
      return new View(response.body(), false);
    }
    String location = response.headers().firstValue("Location").orElse("");
    if (response.statusCode() == 302 && (location.equals(SITE + "/") || location.equals("/"))) {
      store(cached, UNAVAILABLE_MARKER);
      return new View(null, true);
    }
    throw new GovDataException(SITE + path + ": HTTP " + response.statusCode() + " Location="
        + location);
  }

  private static String sha256(String text) {
    try {
      byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
          .digest(text.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder();
      for (byte b : digest) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new GovDataException(e);
    }
  }

  private static void store(File target, String body) throws IOException {
    Files.createDirectories(CACHE_DIR.toPath());
    File tmp = File.createTempFile("efd-", ".tmp", CACHE_DIR);
    Files.write(tmp.toPath(), body.getBytes(StandardCharsets.UTF_8));
    Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
  }

  private String csrfCookie() {
    for (java.net.HttpCookie cookie : cookies.getCookieStore().getCookies()) {
      if ("csrftoken".equals(cookie.getName())) {
        return cookie.getValue();
      }
    }
    throw new GovDataException(SITE + ": the session has no csrftoken cookie");
  }

  private static HttpRequest get(String path) {
    return HttpRequest.newBuilder(URI.create(SITE + path)).header("User-Agent", USER_AGENT)
        .timeout(Duration.ofMinutes(2)).GET().build();
  }

  private static HttpRequest post(String path, Map<String, String> fields, String referer,
      String csrfHeader) {
    StringBuilder body = new StringBuilder();
    for (Map.Entry<String, String> field : fields.entrySet()) {
      if (body.length() > 0) {
        body.append('&');
      }
      body.append(URLEncoder.encode(field.getKey(), StandardCharsets.UTF_8)).append('=')
          .append(URLEncoder.encode(field.getValue(), StandardCharsets.UTF_8));
    }
    HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(SITE + path))
        .header("User-Agent", USER_AGENT).header("Referer", SITE + referer)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .timeout(Duration.ofMinutes(2)).POST(HttpRequest.BodyPublishers.ofString(body.toString()));
    if (csrfHeader != null) {
      builder.header("X-CSRFToken", csrfHeader);
    }
    return builder.build();
  }

  private HttpResponse<String> send(HttpRequest request) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      long wait = lastRequestAt + REQUEST_SPACING_MS - System.currentTimeMillis();
      try {
        if (wait > 0) {
          Thread.sleep(wait);
        }
        HttpResponse<String> response =
            client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        lastRequestAt = System.currentTimeMillis();
        if (response.statusCode() < 500) {
          return response;
        }
        last = new IOException(request.uri() + ": HTTP " + response.statusCode());
      } catch (IOException e) {
        last = e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(request.uri() + ": interrupted", e);
      }
      LOGGER.warn("{} (attempt {}/{})", last.getMessage(), attempt, MAX_ATTEMPTS);
      try {
        Thread.sleep(10000L * attempt);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(request.uri() + ": interrupted", e);
      }
    }
    throw last;
  }

  /** Value of the first {@code <input name="...">} in the page, or null. */
  private static String attribute(String html, String name) {
    java.util.regex.Matcher m = java.util.regex.Pattern
        .compile("name=\"" + name + "\"\\s+value=\"([^\"]*)\"").matcher(html);
    return m.find() ? m.group(1) : null;
  }
}
