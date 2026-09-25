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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * Every request to supremecourt.gov goes through here.
 *
 * <p>The site answers a burst of requests with {@code 403}. Treating that as "the file is not
 * there", as a generic download helper does for 403, turns a block into a missing file, and a
 * caller that falls back to another file on "missing" then hides the block. So the statuses are
 * kept apart:
 * <ul>
 *   <li>{@code 404} and {@code 410} mean the resource is absent: {@link NotFoundException},
 *       not retried.</li>
 *   <li>{@code 403}, {@code 429} and {@code 5xx} mean the site is refusing or failing, not that
 *       the resource is absent: the request is retried after a growing pause, and if it still
 *       fails an {@link IOException} that says so is thrown — never a {@link NotFoundException}.
 *       </li>
 *   <li>any other status is an error at once.</li>
 * </ul>
 *
 * <p>Requests are also spaced: a minimum interval between the start of one request and the next,
 * across every thread, so the parallel batches of every table stay under the site's tolerance.
 */
final class SupremeCourtSite {

  private static final Logger LOGGER = LoggerFactory.getLogger(SupremeCourtSite.class);

  /** The instance every provider shares, so the spacing holds across tables and threads. */
  static final SupremeCourtSite DEFAULT =
      new SupremeCourtSite(400L, new long[] {5000L, 15000L, 45000L, 90000L});

  private static final String USER_AGENT = "Apache-Calcite-GovData/1.0";
  private static final int CONNECT_TIMEOUT_MS = 30000;
  private static final int READ_TIMEOUT_MS = 120000;
  private static final int MAX_HTML_BYTES = 32 * 1024 * 1024;

  /** The resource does not exist (HTTP 404 or 410). A block or a failure is never this. */
  static final class NotFoundException extends FileNotFoundException {
    private static final long serialVersionUID = 1L;

    final int status;

    NotFoundException(int status, String url) {
      super("HTTP " + status + " from " + url);
      this.status = status;
    }
  }

  /** A status that is neither success, absence nor a refusal; retrying will not help. */
  private static final class UnexpectedStatus extends IOException {
    private static final long serialVersionUID = 1L;

    UnexpectedStatus(String message) {
      super(message);
    }
  }

  private final long minIntervalMs;
  private final long[] backoffMs;
  private long nextRequestAtMs;

  /**
   * @param minIntervalMs least time between the start of two requests
   * @param backoffMs pause before each retry; the number of retries is its length
   */
  SupremeCourtSite(long minIntervalMs, long[] backoffMs) {
    this.minIntervalMs = minIntervalMs;
    this.backoffMs = backoffMs.clone();
  }

  /** Fetches an HTML page. */
  String getHtml(String url) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    fetch(url, false, new Reader() {
      @Override public void read(HttpURLConnection conn) throws IOException {
        out.reset();
        copy(conn.getInputStream(), out, MAX_HTML_BYTES);
      }
    });
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  /** Downloads a binary file (a PDF) to {@code dest}. */
  void download(String url, final File dest) throws IOException {
    fetch(url, true, new Reader() {
      @Override public void read(HttpURLConnection conn) throws IOException {
        try (OutputStream out = new FileOutputStream(dest)) {
          long written = copy(conn.getInputStream(), out, Long.MAX_VALUE);
          long expected = conn.getContentLengthLong();
          if (expected > 0 && written != expected) {
            throw new IOException("Truncated download: got " + written + " of " + expected
                + " bytes");
          }
        }
      }
    });
  }

  private interface Reader {
    void read(HttpURLConnection conn) throws IOException;
  }

  private void fetch(String url, boolean expectBinary, Reader reader) throws IOException {
    String problem = "no attempt made";
    boolean refused = false;
    for (int attempt = 1; attempt <= backoffMs.length + 1; attempt++) {
      pace();
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      try {
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        conn.setRequestProperty("User-Agent", USER_AGENT);
        int status = conn.getResponseCode();
        if (status == HttpURLConnection.HTTP_OK) {
          String type = conn.getContentType();
          if (expectBinary && type != null && type.contains("text/html")) {
            problem = "HTML where a file was expected (a block page?)";
            refused = true;
          } else {
            reader.read(conn);
            return;
          }
        } else if (status == HttpURLConnection.HTTP_NOT_FOUND
            || status == HttpURLConnection.HTTP_GONE) {
          throw new NotFoundException(status, url);
        } else if (status == HttpURLConnection.HTTP_FORBIDDEN || status == 429 || status >= 500) {
          problem = "HTTP " + status;
          refused = true;
        } else {
          throw new UnexpectedStatus("HTTP " + status + " from " + url);
        }
      } catch (NotFoundException | UnexpectedStatus e) {
        throw e;
      } catch (IOException e) {
        problem = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
      } finally {
        conn.disconnect();
      }
      if (attempt <= backoffMs.length) {
        LOGGER.warn("{}: {} (attempt {} of {}); retrying in {} ms", url, problem, attempt,
            backoffMs.length + 1, backoffMs[attempt - 1]);
        sleep(backoffMs[attempt - 1]);
      }
    }
    throw new IOException(url + " failed after " + (backoffMs.length + 1) + " attempts (last: "
        + problem + ")" + (refused ? "; supremecourt.gov is blocking or rate limiting requests, "
        + "not reporting the resource as absent" : ""));
  }

  /** Waits until the minimum interval since the previous request has passed, across threads. */
  private synchronized void pace() throws IOException {
    long now = System.currentTimeMillis();
    long wait = nextRequestAtMs - now;
    if (wait > 0) {
      sleep(wait);
      now = System.currentTimeMillis();
    }
    nextRequestAtMs = now + minIntervalMs;
  }

  private static void sleep(long ms) throws IOException {
    if (ms <= 0) {
      return;
    }
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting to call supremecourt.gov", e);
    }
  }

  private static long copy(InputStream in, OutputStream out, long limit) throws IOException {
    byte[] buffer = new byte[65536];
    long total = 0;
    try (InputStream source = in) {
      int n;
      while ((n = source.read(buffer)) != -1) {
        total += n;
        if (total > limit) {
          throw new IOException("Response larger than " + limit + " bytes");
        }
        out.write(buffer, 0, n);
      }
    }
    return total;
  }
}
