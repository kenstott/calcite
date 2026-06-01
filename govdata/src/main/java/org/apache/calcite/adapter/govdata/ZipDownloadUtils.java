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
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Shared utilities for downloading zip and gzip files from HTTP URLs.
 *
 * <p>Used by DataProvider implementations that do not extend AbstractGovDataDownloader
 * (e.g. GazetteerDataProvider, GhcndBulkDataProvider, TigerDataProvider).
 * Implementations that do extend AbstractGovDataDownloader should use its instance
 * methods (downloadZipToTempDir, downloadZipEntry, downloadGzipToFile) instead.
 */
public final class ZipDownloadUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZipDownloadUtils.class);

  private ZipDownloadUtils() { }

  /**
   * Download a ZIP from {@code url} and extract all entries to a new temp directory.
   * Caller is responsible for deleting the returned directory when done.
   *
   * @param url    URL of the ZIP file
   * @param headers optional request headers (may be null)
   * @param prefix temp directory name prefix for diagnostics
   * @return temp directory containing all extracted files
   */
  public static File downloadZipToTempDir(String url, Map<String, String> headers, String prefix)
      throws IOException {
    File tempZip = File.createTempFile(prefix + "-", ".zip");
    File tempDir = java.nio.file.Files.createTempDirectory(prefix + "-").toFile();
    try {
      downloadToFile(url, headers, tempZip);
      try (ZipInputStream zis = new ZipInputStream(new FileInputStream(tempZip))) {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
          File outFile = new File(tempDir, entry.getName());
          if (entry.isDirectory()) {
            outFile.mkdirs();
          } else {
            outFile.getParentFile().mkdirs();
            try (FileOutputStream fos = new FileOutputStream(outFile)) {
              byte[] buf = new byte[65536];
              int len;
              while ((len = zis.read(buf)) != -1) {
                fos.write(buf, 0, len);
              }
            }
          }
          zis.closeEntry();
        }
      }
      return tempDir;
    } finally {
      if (tempZip.exists()) {
        tempZip.delete();
      }
    }
  }

  /**
   * Download a GZIP file from {@code url} to {@code destFile} on local disk.
   * Stores the compressed .gz file (not decompressed) — caller decompresses on read.
   * Uses atomic rename via a .tmp sibling to avoid partial files on failure.
   *
   * <p>Skips download if {@code destFile} already exists and has non-zero size.
   */
  public static void downloadGzipToFile(String url, File destFile) throws IOException {
    if (destFile.exists() && destFile.length() > 0) {
      LOGGER.debug("Gzip cache hit: {}", destFile.getName());
      return;
    }
    destFile.getParentFile().mkdirs();
    File tmp = new File(destFile.getParentFile(), destFile.getName() + ".tmp");
    try {
      downloadToFile(url, null, tmp);
      if (!tmp.renameTo(destFile)) {
        tmp.delete();
        throw new IOException("Failed to rename temp file to " + destFile.getAbsolutePath());
      }
    } catch (IOException e) {
      tmp.delete();
      throw e;
    }
  }

  /**
   * Open a GZIP file for reading. Caller is responsible for closing the stream.
   */
  public static GZIPInputStream openGzip(File gzFile) throws IOException {
    return new GZIPInputStream(new BufferedInputStream(new FileInputStream(gzFile), 65536));
  }

  /**
   * Core HTTP download: GET {@code url} with optional {@code headers}, stream to {@code dest}.
   * Logs progress every 5 seconds for large files.
   */
  public static void downloadToFile(String url, Map<String, String> headers, File dest)
      throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(600000);
    conn.setRequestProperty("User-Agent", "Apache-Calcite-GovData/1.0");
    if (headers != null) {
      for (Map.Entry<String, String> h : headers.entrySet()) {
        conn.setRequestProperty(h.getKey(), h.getValue());
      }
    }
    int status = conn.getResponseCode();
    if (status != HttpURLConnection.HTTP_OK) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    String contentType = conn.getContentType();
    if (contentType != null && contentType.contains("text/html")) {
      throw new IOException("Expected binary but got HTML from: " + url
          + " — may be WAF-blocked or unavailable");
    }
    long contentLength = conn.getContentLengthLong();
    LOGGER.info("Downloading {} ({} MB)", dest.getName(), contentLength / (1024 * 1024));
    try (BufferedInputStream in = new BufferedInputStream(conn.getInputStream(), 65536);
         FileOutputStream out = new FileOutputStream(dest)) {
      byte[] buf = new byte[65536];
      int len;
      long total = 0;
      long lastLog = System.currentTimeMillis();
      while ((len = in.read(buf)) != -1) {
        out.write(buf, 0, len);
        total += len;
        long now = System.currentTimeMillis();
        if (contentLength > 0 && now - lastLog > 5000) {
          LOGGER.info("Download progress: {}% ({} MB / {} MB)",
              total * 100 / contentLength,
              total / (1024 * 1024),
              contentLength / (1024 * 1024));
          lastLog = now;
        }
      }
    } finally {
      conn.disconnect();
    }
    LOGGER.info("Download complete: {} ({} MB)", dest.getName(), dest.length() / (1024 * 1024));
  }

  /**
   * Download a ZIP, extract all entries to a temp dir, caching via StorageProvider.
   *
   * <p>On first call: downloads from {@code url}, extracts to temp dir, writes each
   * file to {@code cachePath} via {@code storageProvider}. On subsequent calls:
   * restores files from cache to a temp dir without downloading.
   *
   * @param url        source URL
   * @param headers    optional request headers
   * @param prefix     temp dir name prefix
   * @param cachePath  base storage path for caching extracted files
   * @param provider   storage provider to use (null = use StorageProviderFactory default)
   * @return temp dir with extracted files (caller must delete when done)
   */
  public static File downloadZipToTempDirCached(String url, Map<String, String> headers,
      String prefix, String cachePath, StorageProvider provider) throws IOException {
    if (provider == null) {
      provider = StorageProviderFactory.createForGovDataCache();
    }
    final StorageProvider sp = provider;

    // Check cache first
    try {
      List<StorageProvider.FileEntry> cached = sp.listFiles(cachePath, true);
      if (!cached.isEmpty()) {
        LOGGER.info("Cache hit: {} ({} files) — restoring to temp dir", cachePath, cached.size());
        File tempDir = java.nio.file.Files.createTempDirectory(prefix + "-cached-").toFile();
        for (StorageProvider.FileEntry entry : cached) {
          if (entry.isDirectory()) continue;
          String entryPath = entry.getPath();
          int stripLen = cachePath.endsWith("/") ? cachePath.length() : cachePath.length() + 1;
          String relative = stripLen <= entryPath.length()
              ? entryPath.substring(stripLen) : entryPath;
          File dest = new File(tempDir, relative);
          dest.getParentFile().mkdirs();
          try (InputStream in = sp.openInputStream(entry.getPath());
               FileOutputStream out = new FileOutputStream(dest)) {
            byte[] buf = new byte[65536];
            int len;
            while ((len = in.read(buf)) != -1) out.write(buf, 0, len);
          }
        }
        return tempDir;
      }
    } catch (IOException e) {
      LOGGER.debug("Cache check failed for {}: {}", cachePath, e.getMessage());
    }

    // Download and cache
    File tempDir = downloadZipToTempDir(url, headers, prefix);
    try {
      writeDirectoryToStorage(tempDir, cachePath, sp);
    } catch (Exception e) {
      LOGGER.warn("Failed to write to cache {}: {}", cachePath, e.getMessage());
    }
    return tempDir;
  }

  /**
   * Download a text/CSV from {@code url}, caching via StorageProvider.
   * Returns cached content as a String if available, downloads otherwise.
   */
  public static String downloadTextCached(String url, Map<String, String> headers,
      String cachePath, StorageProvider provider) throws IOException {
    if (provider == null) {
      provider = StorageProviderFactory.createForGovDataCache();
    }
    final StorageProvider sp = provider;

    // Check cache
    try {
      if (sp.exists(cachePath)) {
        LOGGER.info("Cache hit: {}", cachePath);
        try (InputStream in = sp.openInputStream(cachePath)) {
          return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Cache check failed for {}: {}", cachePath, e.getMessage());
    }

    // Download
    File tempFile = File.createTempFile("text-download-", ".tmp");
    try {
      downloadToFile(url, headers, tempFile);
      byte[] data = java.nio.file.Files.readAllBytes(tempFile.toPath());
      try (InputStream in = new java.io.ByteArrayInputStream(data)) {
        sp.writeFile(cachePath, in);
      }
      return new String(data, java.nio.charset.StandardCharsets.UTF_8);
    } finally {
      tempFile.delete();
    }
  }

  /**
   * Recursively delete a directory. Best-effort — logs warning on failure.
   */
  public static void deleteDirectory(File dir) {
    if (dir == null || !dir.exists()) return;
    try {
      java.nio.file.Files.walk(dir.toPath())
          .sorted(java.util.Comparator.reverseOrder())
          .map(java.nio.file.Path::toFile)
          .forEach(File::delete);
    } catch (IOException e) {
      LOGGER.warn("Failed to delete temp directory {}: {}", dir, e.getMessage());
    }
  }

  private static void writeDirectoryToStorage(File dir, String basePath, StorageProvider sp)
      throws IOException {
    File[] files = dir.listFiles();
    if (files == null) return;
    for (File file : files) {
      if (file.isDirectory()) {
        writeDirectoryToStorage(file, sp.resolvePath(basePath, file.getName()), sp);
      } else {
        String destPath = sp.resolvePath(basePath, file.getName());
        try (InputStream in = new FileInputStream(file)) {
          sp.writeFile(destPath, in);
        }
      }
    }
  }
}
