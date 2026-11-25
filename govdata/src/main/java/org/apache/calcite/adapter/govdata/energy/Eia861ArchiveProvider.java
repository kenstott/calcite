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
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.file.etl.SkippedBatchException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base provider for the tables built from the annual EIA-861 ZIP archive.
 *
 * <p>EIA does not publish the archives under one path: the newest year sits under
 * {@code zip/} and older years under {@code archive/zip/}, and a path that does not hold a year
 * redirects to an HTML page with status 200 at its target. {@code source.url} therefore names the
 * EIA-861 landing page, and the location of each year's archive is read from its links.
 *
 * <p>EIA re-posts corrected archives under the same name, so the raw-cache entry is keyed by the
 * file's {@code Last-Modified} and {@code Content-Length}: an unchanged archive is read from the
 * cache, and a re-posted one is downloaded again beside the earlier copy.
 */
public abstract class Eia861ArchiveProvider extends XlsxCellReader implements CachingDataProvider {

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  protected static final Logger LOGGER = LoggerFactory.getLogger(Eia861ArchiveProvider.class);

  /** A link to a final annual archive; the early-release file ({@code ...er.zip}) is excluded. */
  private static final Pattern ARCHIVE_LINK =
      Pattern.compile("href=\"((?:archive/)?zip/f861(\\d{4})\\.zip)\"");

  /**
   * Parses one year's archive into a JSON array of rows.
   *
   * @param archive the ZIP content; the caller closes it
   * @param year the data year the archive covers
   */
  protected abstract String parseArchive(InputStream archive, int year) throws Exception;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String effectiveYear = variables.get("effective_year");
    if (effectiveYear == null || effectiveYear.isEmpty()) {
      throw new IOException("EIA-861: effective_year dimension missing for " + config.getName());
    }
    int year = Integer.parseInt(effectiveYear);
    String pageUrl = config.getSource().getUrl();
    String archiveUrl =
        URI.create(pageUrl).resolve(archiveLink(readPage(pageUrl), year)).toString();

    String key = archiveUrl + "?version=" + version(archiveUrl);
    String json;
    try (InputStream archive = rawCache.openStream(key, () -> openArchive(archiveUrl))) {
      json = parseArchive(archive, year);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("EIA-861: failed to parse " + archiveUrl, e);
    }
    List<Map<String, Object>> rows =
        MAPPER.readValue(json, new TypeReference<List<Map<String, Object>>>() { });
    return rows.iterator();
  }

  /**
   * Returns the landing page's link to the final archive of {@code year}.
   *
   * @throws SkippedBatchException if {@code year} is later than every linked year: not published
   * @throws IOException if the page links no archive, or skips {@code year}
   */
  static String archiveLink(String page, int year) throws IOException {
    TreeMap<Integer, String> links = new TreeMap<Integer, String>();
    Matcher m = ARCHIVE_LINK.matcher(page);
    while (m.find()) {
      links.put(Integer.parseInt(m.group(2)), m.group(1));
    }
    if (links.isEmpty()) {
      throw new IOException("EIA-861: the landing page links no f861<year>.zip archive");
    }
    if (year > links.lastKey()) {
      throw new SkippedBatchException("EIA-861: " + year + " is not published yet (newest is "
          + links.lastKey() + ")");
    }
    String link = links.get(year);
    if (link == null) {
      throw new IOException("EIA-861: the landing page links no archive for " + year);
    }
    return link;
  }

  private static String readPage(String url) throws IOException {
    HttpURLConnection conn = request(url, "GET");
    try (InputStream in = conn.getInputStream()) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[65536];
      int n;
      while ((n = in.read(buf)) != -1) {
        out.write(buf, 0, n);
      }
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
  }

  /** Identifies the posted revision of an archive from its response headers. */
  private static String version(String url) throws IOException {
    HttpURLConnection conn = request(url, "HEAD");
    try {
      String modified = conn.getHeaderField("Last-Modified");
      long length = conn.getContentLengthLong();
      if (modified == null || length < 0) {
        throw new IOException("EIA-861: no Last-Modified or Content-Length from " + url);
      }
      return modified + "/" + length;
    } finally {
      conn.disconnect();
    }
  }

  /** Opens an archive, refusing content that is not a ZIP so it never enters the raw cache. */
  private static InputStream openArchive(String url) throws IOException {
    PushbackInputStream in = new PushbackInputStream(request(url, "GET").getInputStream(), 2);
    byte[] magic = new byte[2];
    int read = 0;
    while (read < magic.length) {
      int n = in.read(magic, read, magic.length - read);
      if (n < 0) {
        break;
      }
      read += n;
    }
    if (read < magic.length || magic[0] != 'P' || magic[1] != 'K') {
      in.close();
      throw new IOException("EIA-861: not a ZIP archive: " + url);
    }
    in.unread(magic);
    return in;
  }

  /** Sends a request and requires status 200; redirects are not followed. */
  private static HttpURLConnection request(String url, String method) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestMethod(method);
    conn.setInstanceFollowRedirects(false);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    return conn;
  }
}
