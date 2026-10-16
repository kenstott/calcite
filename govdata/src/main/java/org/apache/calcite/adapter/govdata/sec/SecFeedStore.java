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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.etl.DocumentSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Filing documents unpacked from EDGAR's daily feed, answered locally instead of fetched.
 *
 * <p>The feed holds every document of every submission of a day. {@link EdgarFeedUnpacker} lays
 * the ones this adapter reads out as {@code <dir>/<accession without dashes>/<filename>} (plus a
 * synthesized {@code index.json}, and {@code <accession>.txt} for Forms 3/4/5). A request for
 * {@code https://www.sec.gov/Archives/edgar/data/<cik>/<accession>/<filename>} is answered from
 * that tree whichever CIK it names, which also settles ownership filings that EDGAR lists once per
 * CIK involved. A request the tree cannot answer returns null and is fetched as before.
 *
 * <p>The tree is transient: unpacked for the run, deleted after it.
 */
final class SecFeedStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(SecFeedStore.class);

  static final String ARCHIVE_PREFIX = "https://www.sec.gov/Archives/edgar/data/";
  private static final Pattern DOC_URL =
      Pattern.compile("^https://www\\.sec\\.gov/Archives/edgar/data/\\d+/(\\d{18})/(.+)$");
  private static final Pattern DIR_URL =
      Pattern.compile("^https://www\\.sec\\.gov/Archives/edgar/data/(\\d+)/(\\d{18})/?$");
  private static final Pattern INDEX_PAGE_URL = Pattern.compile(
      "^https://www\\.sec\\.gov/Archives/edgar/data/(\\d+)/(\\d{18})/\\d{10}-\\d{2}-\\d{6}-index\\.html?$");
  private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
      new com.fasterxml.jackson.databind.ObjectMapper();
  private static final Pattern FILING_SUMMARY_URL = Pattern.compile(
      "^https://www\\.sec\\.gov/Archives/edgar/data/\\d+/(\\d{18})/FilingSummary\\.xml$");
  private static final Pattern TXT_URL = Pattern.compile(
      "^https://www\\.sec\\.gov/Archives/edgar/data/\\d+/(\\d{10}-\\d{2}-\\d{6})\\.txt$");

  /** Misses logged by URL per JVM: enough to see what a feed leaves to sec.gov, without flooding. */
  private static final long MAX_MISS_LOGS = 200;

  private static volatile SecFeedStore instance;

  private final File dir;
  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();

  private SecFeedStore(File dir) {
    this.dir = dir;
  }

  /**
   * Installs the unpacked feed in {@code dir} (the {@code feedDir} operand), if one is set. A
   * configured directory that does not exist is an error, not a reason to quietly fetch everything.
   */
  static void install(String dir) {
    if (dir == null || dir.trim().isEmpty()) {
      LOGGER.info("EDGAR feed store: not configured (feedDir empty): every filing document is "
          + "fetched from sec.gov");
      return;
    }
    // justification: transient local tree unpacked from the daily feed, deleted after the run
    File root = new File(dir.trim());
    if (!root.isDirectory()) {
      throw new IllegalStateException("feedDir " + dir + " is configured but is not a directory");
    }
    final SecFeedStore store = new SecFeedStore(root);
    instance = store;
    final DocumentSource.LocalContentResolver resolver = new DocumentSource.LocalContentResolver() {
      @Override public String resolve(String url) throws IOException {
        return text(url);
      }
    };
    DocumentSource.registerLocalContentResolver(ARCHIVE_PREFIX, resolver);
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override public void run() {
        LOGGER.info("EDGAR feed store: run finished, {} documents answered locally, {} not held "
            + "(fetched live)", store.hits.get(), store.misses.get());
      }
    }, "feed-store-summary"));
    LOGGER.info("EDGAR feed store: using {}", root);
  }

  /** Clears the installed store (tests). */
  static void uninstall() {
    instance = null;
    DocumentSource.clearLocalContentResolvers();
  }

  /** The held bytes for {@code url}, or null when no store is installed or it does not hold it. */
  static byte[] bytes(String url) throws IOException {
    SecFeedStore store = instance;
    if (store == null || url == null || !url.startsWith(ARCHIVE_PREFIX)) {
      return null;
    }
    byte[] listing = store.listingFor(url);
    if (listing != null) {
      store.hits.incrementAndGet();
      return listing;
    }
    File f = store.fileFor(url);
    if (f == null || !f.isFile()) {
      long n = store.misses.incrementAndGet();
      if (n <= MAX_MISS_LOGS) {
        LOGGER.info("EDGAR feed store: not held, fetched live ({}): {}",
            store.holdsAccessionOf(url) ? "accession held, document missing" : "accession not held",
            url);
      }
      return null;
    }
    store.hits.incrementAndGet();
    return Files.readAllBytes(f.toPath());
  }

  /**
   * True when {@code url} is a filing's {@code FilingSummary.xml} that sec.gov cannot have: the
   * accession is held and nothing in it is XBRL. sec.gov renders {@code FilingSummary.xml} (with
   * the R files) only for a filing carrying XBRL, so a held accession whose index lists no
   * {@code EX-101.*} document and none of whose HTML documents has an inline-XBRL header answers a
   * request for it with 404 (a proxy statement or Schedule 13G, say). The caller then records it as
   * absent without the request. Anything less certain returns false and is fetched.
   */
  static boolean knownAbsent(String url) throws IOException {
    SecFeedStore store = instance;
    if (store == null || url == null) {
      return false;
    }
    Matcher m = FILING_SUMMARY_URL.matcher(url);
    if (!m.matches()) {
      return false;
    }
    File acc = new File(store.dir, m.group(1)); // justification: same transient local tree
    File index = new File(acc, "index.json");
    if (!index.isFile() || store.fileFor(url).isFile()) {
      return false;
    }
    for (com.fasterxml.jackson.databind.JsonNode item
        : MAPPER.readTree(index).path("directory").path("item")) {
      if (item.path("type").asText().startsWith("EX-101")) {
        return false;
      }
    }
    File[] files = acc.listFiles();
    if (files == null) {
      return false;
    }
    for (File f : files) {
      String n = f.getName().toLowerCase(java.util.Locale.ROOT);
      if ((n.endsWith(".htm") || n.endsWith(".html")) && hasInlineXbrlHeader(f)) {
        return false;
      }
    }
    long n = store.hits.incrementAndGet();
    if (n <= MAX_MISS_LOGS) {
      LOGGER.info("EDGAR feed store: answered locally as absent (the filing carries no XBRL): {}",
          url);
    }
    return true;
  }

  /** As {@link #knownAbsent}, for callers that cannot throw a checked exception. */
  static boolean knownAbsentUnchecked(String url) {
    try {
      return knownAbsent(url);
    } catch (IOException e) {
      throw new IllegalStateException("could not read the EDGAR feed store for " + url, e);
    }
  }

  /** Whether the HTML document carries an inline-XBRL {@code <ix:header>}. */
  private static boolean hasInlineXbrlHeader(File f) throws IOException {
    byte[] needle = "<ix:header".getBytes(StandardCharsets.US_ASCII);
    byte[] buf = new byte[1 << 20];
    int keep = 0;
    try (InputStream in = Files.newInputStream(f.toPath())) {
      int n;
      while ((n = in.read(buf, keep, buf.length - keep)) > 0) {
        int end = keep + n;
        for (int i = 0; i + needle.length <= end; i++) {
          int j = 0;
          while (j < needle.length && (buf[i + j] | 0x20) == needle[j]) {
            j++;
          }
          if (j == needle.length) {
            return true;
          }
        }
        keep = Math.min(needle.length - 1, end);
        System.arraycopy(buf, end - keep, buf, 0, keep);
      }
    }
    return false;
  }

  /** The held content for {@code url} as text, or null. */
  static String text(String url) throws IOException {
    byte[] b = bytes(url);
    return b == null ? null : new String(b, StandardCharsets.UTF_8);
  }

  /**
   * As {@link #text}, for callers that cannot throw a checked exception. A local read that fails
   * is a real error, not a reason to fetch the document from sec.gov.
   */
  static String textUnchecked(String url) {
    try {
      return text(url);
    } catch (IOException e) {
      throw new IllegalStateException("could not read " + url + " from the EDGAR feed store", e);
    }
  }

  /** The held content for {@code url} as a stream, or null. */
  static InputStream open(String url) throws IOException {
    byte[] b = bytes(url);
    return b == null ? null : new ByteArrayInputStream(b);
  }

  /**
   * EDGAR's directory listing and filing-index page for an accession, built from the index.json
   * the unpacker wrote. Both are tables with one row per document (sequence, description, linked
   * document name, type, size), the shape the converter's index parsers read; the feed itself
   * carries neither page. Null when the URL is neither or the accession is not held.
   */
  private byte[] listingFor(String url) throws IOException {
    Matcher m = INDEX_PAGE_URL.matcher(url);
    if (!m.matches()) {
      m = DIR_URL.matcher(url);
      if (!m.matches()) {
        return null;
      }
    }
    String cik = m.group(1);
    String acc = m.group(2);
    File index = new File(dir, acc + "/index.json"); // justification: same transient local tree
    if (!index.isFile()) {
      return null;
    }
    com.fasterxml.jackson.databind.JsonNode items =
        MAPPER.readTree(index).path("directory").path("item");
    StringBuilder html = new StringBuilder("<html><body><table summary=\"Document Format Files\">")
        .append("<tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>");
    int seq = 1;
    for (com.fasterxml.jackson.databind.JsonNode item : items) {
      String name = item.path("name").asText();
      html.append("<tr><td>").append(seq++).append("</td><td></td><td><a href=\"")
          .append(ARCHIVE_PREFIX.substring("https://www.sec.gov".length())).append(cik).append('/')
          .append(acc).append('/').append(escape(name)).append("\">").append(escape(name))
          .append("</a></td><td>").append(escape(item.path("type").asText())).append("</td><td>")
          .append(escape(item.path("size").asText())).append("</td></tr>");
    }
    html.append("</table></body></html>");
    return html.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static String escape(String s) {
    return s.replace("&", "&amp;").replace("<", "&lt;").replace("\"", "&quot;");
  }

  /** True when the accession named by {@code url} was unpacked: a miss then is a gap in the feed. */
  private boolean holdsAccessionOf(String url) {
    Matcher d = DOC_URL.matcher(url);
    String acc = null;
    if (d.matches()) {
      acc = d.group(1);
    } else {
      Matcher t = TXT_URL.matcher(url);
      if (t.matches()) {
        acc = t.group(1).replace("-", "");
      } else {
        Matcher dir = DIR_URL.matcher(url);
        if (dir.matches()) {
          acc = dir.group(2);
        }
      }
    }
    return acc != null && new File(dir, acc).isDirectory(); // justification: same transient tree
  }

  private File fileFor(String url) {
    Matcher d = DOC_URL.matcher(url);
    String rel;
    if (d.matches()) {
      rel = d.group(1) + "/" + d.group(2);
    } else {
      Matcher t = TXT_URL.matcher(url);
      if (!t.matches()) {
        return null;
      }
      rel = t.group(1).replace("-", "") + "/" + t.group(1) + ".txt";
    }
    if (rel.contains("..")) {
      return null;
    }
    return new File(dir, rel); // justification: same transient local tree
  }

  long hits() {
    return hits.get();
  }

  long misses() {
    return misses.get();
  }
}
