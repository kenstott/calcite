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

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.zip.GZIPInputStream;

/**
 * Streams one day of EDGAR's daily feed ({@code <YYYYMMDD>.nc.tar.gz}) and lays the documents of
 * the submissions this adapter processes out as a {@link SecFeedStore} tree.
 *
 * <p>One pass over a day file replaces the per-filing requests for every document of every
 * processed form. Nothing is kept from the feed itself: the day file is read as a stream and the
 * tree is transient. Binary documents (uuencoded images, spreadsheets, zips) are skipped, as are
 * forms {@link FormType} does not model.
 *
 * <p>Usage: {@code EdgarFeedUnpacker <feed_dir> <day_file_or_url>...}. A URL is fetched with a
 * descriptive User-Agent, one request per day.
 */
public final class EdgarFeedUnpacker {

  private static final Logger LOGGER = LoggerFactory.getLogger(EdgarFeedUnpacker.class);
  private static final String USER_AGENT = "Apache Calcite SEC Adapter apache-calcite@apache.org";
  private static final long MAX_ENTRY_BYTES = 200L * 1024 * 1024;

  /** What one pass wrote. */
  static final class Stats {
    long submissions;
    long processed;
    long documents;
    long bytes;
    long skippedLarge;

    @Override public String toString() {
      return submissions + " submissions, " + processed + " of processed forms, " + documents
          + " documents (" + bytes + " bytes), " + skippedLarge + " over the size cap skipped";
    }
  }

  private EdgarFeedUnpacker() {
  }

  /** Unpacks one day file given as a stream of {@code .tar.gz}. */
  static Stats unpack(InputStream tarGz, File feedDir) throws IOException {
    Stats stats = new Stats();
    try (TarArchiveInputStream tar = new TarArchiveInputStream(
        new GZIPInputStream(new BufferedInputStream(tarGz, 1 << 16)))) {
      TarArchiveEntry entry;
      while ((entry = tar.getNextEntry()) != null) {
        if (!entry.isFile()) {
          continue;
        }
        stats.submissions++;
        if (entry.getSize() > MAX_ENTRY_BYTES) {
          stats.skippedLarge++;
          continue;
        }
        byte[] nc = readEntry(tar, (int) entry.getSize());
        NcSubmission sub;
        try {
          sub = NcSubmission.parse(nc);
        } catch (IllegalArgumentException e) {
          LOGGER.warn("Skipping unparseable feed entry {}: {}", entry.getName(), e.getMessage());
          continue;
        }
        if (FormType.fromString(sub.formType) == FormType.FORM_OTHER) {
          continue;
        }
        write(sub, nc, feedDir, stats);
        stats.processed++;
      }
    }
    return stats;
  }

  private static byte[] readEntry(InputStream in, int size) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(size, 1024));
    byte[] buf = new byte[1 << 16];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  private static void write(NcSubmission sub, byte[] nc, File feedDir, Stats stats)
      throws IOException {
    // justification: transient local tree, see SecFeedStore
    File accDir = new File(feedDir, sub.accessionNoDash());
    StringBuilder index = new StringBuilder("{\"directory\":{\"name\":\"/Archives/edgar/data/0/")
        .append(sub.accessionNoDash()).append("\",\"item\":[");
    boolean first = true;
    for (NcSubmission.Doc d : sub.docs) {
      if (d.filename == null || d.filename.isEmpty() || d.filename.contains("..")) {
        continue;
      }
      if (!first) {
        index.append(',');
      }
      first = false;
      index.append("{\"name\":\"").append(json(d.filename)).append("\",\"type\":\"")
          .append(json(d.type == null ? "" : d.type)).append("\",\"size\":\"")
          .append(d.content == null ? 0 : d.content.length).append("\"}");
      if (d.content != null && !isRenderingOnly(d.filename) && isRead(d.type)) {
        File target = new File(accDir, d.filename);
        Files.createDirectories(target.getParentFile().toPath());
        Files.write(target.toPath(), d.content);
        stats.documents++;
        stats.bytes += d.content.length;
      }
    }
    index.append("]}}");
    Files.createDirectories(accDir.toPath());
    // the 13D/G converter reads the party CIKs from <accession>.hdr.sgml, which is this header
    Files.write(new File(accDir, sub.accession + ".hdr.sgml").toPath(), sub.header);
    Files.write(new File(accDir, "index.json").toPath(),
        index.toString().getBytes(StandardCharsets.UTF_8));
    FormType form = FormType.fromString(sub.formType);
    if (form.isInsiderForm()) {
      // the converter fetches <accession>.txt and cuts the ownership XML out of it
      Files.write(new File(accDir, sub.accession + ".txt").toPath(), nc);
      stats.bytes += nc.length;
    }
  }

  /**
   * The rendering files SEC generates beside the XBRL (R1.htm, R2.htm, the stylesheet and script
   * that show them, MetaLinks.json). Nothing here reads them, and they were over a third of the
   * bytes of a 10-Q's unpacked tree, so they are listed in index.json but not written.
   */
  private static boolean isRenderingOnly(String filename) {
    return filename.matches("R\\d+\\.htm") || "Show.js".equals(filename)
        || "report.css".equals(filename) || "MetaLinks.json".equals(filename);
  }

  /**
   * Whether the converter ever reads a document of this type: the filing's own documents (type =
   * the form), 8-K earnings exhibits (EX-99.x), the XBRL instance and the presentation, calculation
   * and definition linkbases, the 13F information table, and untyped XML such as FilingSummary.xml.
   * The label linkbase and schema (EX-101.LAB, EX-101.SCH, which the converter only filters out when
   * looking for the instance) and every other exhibit (contracts, certifications, subsidiaries) are
   * listed in index.json but not written. A request for one that is not held is fetched from
   * sec.gov, so skipping a document can cost a request but never a wrong answer.
   */
  private static boolean isRead(String type) {
    if (type == null || !type.startsWith("EX-")) {
      return true;
    }
    return type.startsWith("EX-99") || "EX-101.INS".equals(type) || "EX-101.PRE".equals(type)
        || "EX-101.CAL".equals(type) || "EX-101.DEF".equals(type);
  }

  private static String json(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: EdgarFeedUnpacker <feed_dir> <day_file_or_url>...");
      System.exit(1);
    }
    File feedDir = new File(args[0]); // justification: transient local tree
    Files.createDirectories(feedDir.toPath());
    for (int i = 1; i < args.length; i++) {
      String src = args[i];
      long t0 = System.currentTimeMillis();
      Stats stats;
      if (src.startsWith("http")) {
        HttpURLConnection conn = (HttpURLConnection) new URL(src).openConnection();
        conn.setRequestProperty("User-Agent", USER_AGENT);
        conn.setConnectTimeout(30000);
        conn.setReadTimeout(120000);
        int status = conn.getResponseCode();
        if (status == 404) {
          // a holiday or weekend has no feed file; a day not yet published has none either
          System.out.println("[edgar-feed] " + src + ": no file (HTTP 404), skipped");
          continue;
        }
        if (status != 200) {
          throw new IOException("HTTP " + status + " for " + src);
        }
        try (InputStream in = conn.getInputStream()) {
          stats = unpack(in, feedDir);
        }
      } else {
        try (InputStream in = new FileInputStream(src)) { // justification: local day file
          stats = unpack(in, feedDir);
        }
      }
      System.out.println("[edgar-feed] " + src + ": " + stats + " in "
          + (System.currentTimeMillis() - t0) / 1000 + "s");
    }
  }
}
