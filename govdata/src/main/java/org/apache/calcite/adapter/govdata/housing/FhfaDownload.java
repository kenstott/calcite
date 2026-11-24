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
package org.apache.calcite.adapter.govdata.housing;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * Downloads one of FHFA's large annual HPI files to a scratch file. The ZIP5 workbook is 40 MB
 * (240 MB of sheet XML) and the tract file 90 MB, so they are spooled to disk and parsed from
 * there instead of being held in memory.
 */
final class FhfaDownload {

  private static final String REFERER = "https://www.fhfa.gov/data/hpi/datasets";
  private static final String USER_AGENT =
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          + "(KHTML, like Gecko) Chrome/120.0 Safari/537.36";
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 300_000;
  private static final int MAX_RETRIES = 3;

  private FhfaDownload() {
  }

  /** Downloads {@code url} to a new temp file; the caller deletes it. */
  static File toTempFile(String url, String suffix) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      File tmp = File.createTempFile("fhfa-hpi-", suffix);
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", USER_AGENT);
      conn.setRequestProperty("Referer", REFERER);
      conn.setRequestProperty("Accept", "*/*");
      boolean ok = false;
      try {
        NetworkOutageWait.connect(conn);
        int status = conn.getResponseCode();
        if (status == HttpURLConnection.HTTP_OK) {
          try (InputStream in = conn.getInputStream()) {
            Files.copy(in, tmp.toPath(), StandardCopyOption.REPLACE_EXISTING);
          }
          ok = true;
          return tmp;
        }
        if (status != 429 && status < 500) {
          throw new IOException("HTTP " + status + " from " + url);
        }
        last = new IOException("HTTP " + status + " from " + url);
      } finally {
        conn.disconnect();
        if (!ok) {
          Files.deleteIfExists(tmp.toPath());
        }
      }
      try {
        Thread.sleep(1000L * attempt);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted while downloading " + url, e);
      }
    }
    throw last;
  }

  /** FHFA's "." not-published sentinel and blanks are null; other non-numerics are errors. */
  static Double number(String s, String what) throws IOException {
    if (s == null) {
      return null;
    }
    String t = s.trim();
    if (t.isEmpty() || ".".equals(t)) {
      return null;
    }
    try {
      return Double.valueOf(t);
    } catch (NumberFormatException e) {
      throw new IOException("fhfa hpi: non-numeric " + what + " '" + t + "'", e);
    }
  }
}
