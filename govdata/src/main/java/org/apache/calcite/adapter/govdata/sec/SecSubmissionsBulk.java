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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Answers {@code https://data.sec.gov/submissions/CIK<cik>.json} (and its older-filings pages)
 * from SEC's nightly bulk file, {@code submissions.zip}, instead of one request per filer.
 *
 * <p>Each ETL run fetched the submissions JSON of every filer it touched: thousands of requests
 * against the host-wide 8 requests per second budget, with HTTP 429 and 403 answers in the worker
 * logs. The bulk file holds the same JSON for every filer, and its older-filings pages are
 * identical to the live ones. It is regenerated nightly, so the newest filings are missing from
 * it; a caller that misses must recheck live (see {@link DocumentSource#fetchUrlContentLive}).
 * A CIK absent from the file is returned as null, so the caller fetches it as before.
 *
 * <p>The zip is a transient download ({@code govdata/scripts/fetch-submissions-bulk.sh}); read in
 * place with random access, never unpacked, and deleted when the run is over.
 */
final class SecSubmissionsBulk implements DocumentSource.LocalContentResolver, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SecSubmissionsBulk.class);

  static final String URL_PREFIX = "https://data.sec.gov/submissions/";
  private static final Pattern ENTRY = Pattern.compile("CIK\\d{10}(-submissions-\\d{3})?\\.json");
  private static final long LOG_EVERY = 5000;

  private final ZipFile zip;
  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();

  private SecSubmissionsBulk(ZipFile zip) {
    this.zip = zip;
  }

  /**
   * Registers the bulk file named by the {@code bulkSubmissionsZip} operand, if one is set.
   * A configured path that is missing or unreadable is an error, not a reason to quietly fetch
   * every filer from sec.gov.
   */
  static void install(String zipPath) {
    if (zipPath == null || zipPath.trim().isEmpty()) {
      LOGGER.info("submissions bulk file: not configured (bulkSubmissionsZip empty): every "
          + "filer's submissions JSON is fetched from data.sec.gov");
      return;
    }
    try {
      // justification: transient local download of SEC's nightly file, read in place
      File file = new File(zipPath.trim());
      SecSubmissionsBulk bulk = new SecSubmissionsBulk(new ZipFile(file));
      DocumentSource.registerLocalContentResolver(URL_PREFIX, bulk);
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override public void run() {
          LOGGER.info("submissions bulk file: run finished, {} answered locally, {} not in the "
              + "file (fetched live)", bulk.hits(), bulk.misses());
        }
      }, "submissions-bulk-summary"));
      LOGGER.info("submissions bulk file: using {} ({} bytes, last modified {})", file,
          file.length(), Instant.ofEpochMilli(file.lastModified()));
    } catch (IOException e) {
      throw new IllegalStateException(
          "bulkSubmissionsZip " + zipPath + " is configured but could not be opened", e);
    }
  }

  /** Opens {@code file}; for tests and callers that manage the resolver themselves. */
  static SecSubmissionsBulk open(File file) throws IOException {
    return new SecSubmissionsBulk(new ZipFile(file)); // justification: transient local download
  }

  @Override public String resolve(String url) throws IOException {
    if (url == null || !url.startsWith(URL_PREFIX)) {
      return null;
    }
    String name = url.substring(URL_PREFIX.length());
    if (!ENTRY.matcher(name).matches()) {
      return null;
    }
    ZipEntry entry = zip.getEntry(name);
    if (entry == null) {
      note(false);
      return null;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream((int) Math.max(entry.getSize(), 1024));
    try (InputStream in = zip.getInputStream(entry)) {
      byte[] buf = new byte[65536];
      int n;
      while ((n = in.read(buf)) != -1) {
        out.write(buf, 0, n);
      }
    }
    note(true);
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  private void note(boolean hit) {
    long h = hit ? hits.incrementAndGet() : hits.get();
    long m = hit ? misses.get() : misses.incrementAndGet();
    if ((h + m) % LOG_EVERY == 0) {
      LOGGER.info("submissions bulk file: {} answered locally, {} not in the file (fetched "
          + "live)", hits.get(), misses.get());
    }
  }

  long hits() {
    return hits.get();
  }

  long misses() {
    return misses.get();
  }

  @Override public void close() throws IOException {
    zip.close();
  }
}
