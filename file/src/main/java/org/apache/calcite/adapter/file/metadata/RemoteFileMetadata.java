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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Locale;

/**
 * Metadata for remote files to detect changes without downloading full content.
 */
public class RemoteFileMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteFileMetadata.class);

  private final String url;
  private final @Nullable String etag;
  private final @Nullable String lastModified;
  private final long contentLength;
  private final @Nullable String contentHash;
  private final Instant checkTime;

  private RemoteFileMetadata(String url, @Nullable String etag, @Nullable String lastModified,
                            long contentLength, @Nullable String contentHash) {
    this.url = url;
    this.etag = etag;
    this.lastModified = lastModified;
    this.contentLength = contentLength;
    this.contentHash = contentHash;
    this.checkTime = Instant.now();
  }

  /**
   * Fetches metadata for a remote file.
   */
  public static RemoteFileMetadata fetch(Source source) throws IOException {
    String protocol = source.protocol();
    String urlString = source.url() != null ? source.url().toString() : source.path();

    switch (protocol) {
    case "http":
    case "https":
      return fetchHttpMetadata(urlString);
    case "s3":
      return fetchS3Metadata(source.path());
    case "ftp":
      return fetchFtpMetadata(urlString);
    default:
      // For unsupported protocols, return minimal metadata
      return new RemoteFileMetadata(urlString, null, null, -1, null);
    }
  }

  /**
   * Fetches metadata for HTTP/HTTPS URLs using HEAD request.
   */
  private static RemoteFileMetadata fetchHttpMetadata(String urlString) throws IOException {
    URL url;
    try {
      url = new URI(urlString).toURL();
    } catch (Exception e) {
      throw new IOException("Invalid URL: " + urlString, e);
    }
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.setRequestMethod("HEAD");
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(5000);

      int responseCode = conn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        LOGGER.warn("HTTP HEAD request failed for " + urlString + ": " + responseCode);
        // Fall back to GET request metadata
        conn.disconnect();
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
      }

      String etag = conn.getHeaderField("ETag");
      String lastModified = conn.getHeaderField("Last-Modified");
      long contentLength = conn.getContentLengthLong();

      return new RemoteFileMetadata(urlString, etag, lastModified, contentLength, null);
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Fetches metadata for S3 objects.
   */
  private static RemoteFileMetadata fetchS3Metadata(String s3Uri) throws IOException {
    // S3 metadata support requires AWS SDK which is in the core module
    // For now, we'll rely on content-based change detection
    // When content is downloaded, we can compute and cache a hash

    // Future enhancement: Use reflection to call S3 HeadObject if available
    // or move this logic to core module where AWS SDK is available

    LOGGER.info("S3 metadata checking not yet implemented, using content-based detection for: "
        + s3Uri);
    return new RemoteFileMetadata(s3Uri, null, null, -1, null);
  }

  /**
   * Fetches metadata for FTP URLs.
   */
  private static RemoteFileMetadata fetchFtpMetadata(String ftpUrl) throws IOException {
    // FTP metadata support is limited
    // Would need to use FTP commands like MDTM (modification time) and SIZE
    // For now, return minimal metadata
    return new RemoteFileMetadata(ftpUrl, null, null, -1, null);
  }

  /**
   * Checks if the remote file has changed compared to previous metadata.
   */
  public boolean hasChanged(RemoteFileMetadata previous) {
    // First check ETag (most reliable for HTTP)
    if (etag != null && previous.etag != null) {
      return !etag.equals(previous.etag);
    }

    // Check Last-Modified header
    if (lastModified != null && previous.lastModified != null) {
      return !lastModified.equals(previous.lastModified);
    }

    // Check content length as a quick indicator
    if (contentLength > 0 && previous.contentLength > 0
        && contentLength != previous.contentLength) {
      return true;
    }

    // If we have content hashes, compare them
    if (contentHash != null && previous.contentHash != null) {
      return !contentHash.equals(previous.contentHash);
    }

    // If we can't determine changes, assume changed to be safe
    return true;
  }

  /**
   * Computes MD5 hash of content for change detection.
   */
  public static String computeHash(InputStream inputStream) throws IOException {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] buffer = new byte[8192];
      int bytesRead;

      while ((bytesRead = inputStream.read(buffer)) != -1) {
        md.update(buffer, 0, bytesRead);
      }

      byte[] digest = md.digest();
      StringBuilder sb = new StringBuilder();
      for (byte b : digest) {
        sb.append(String.format(Locale.ROOT, "%02x", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("MD5 algorithm not available", e);
    }
  }

  /**
   * Creates metadata with computed content hash.
   */
  public RemoteFileMetadata withContentHash(String hash) {
    return new RemoteFileMetadata(url, etag, lastModified, contentLength, hash);
  }

  /**
   * Creates metadata for testing purposes.
   */
  public static RemoteFileMetadata createForTesting(String url, @Nullable String etag,
      @Nullable String lastModified, long contentLength,
      @Nullable String contentHash) {
    return new RemoteFileMetadata(url, etag, lastModified, contentLength, contentHash);
  }

  // Getters
  public String getUrl() {
    return url;
  }
  public @Nullable String getEtag() {
    return etag;
  }
  public @Nullable String getLastModified() {
    return lastModified;
  }
  public long getContentLength() {
    return contentLength;
  }
  public @Nullable String getContentHash() {
    return contentHash;
  }
  public Instant getCheckTime() {
    return checkTime;
  }

  @Override public String toString() {
    return "RemoteFileMetadata{url=" + url
           + ", etag=" + etag
           + ", lastModified=" + lastModified
           + ", contentLength=" + contentLength
           + ", contentHash=" + contentHash + "}";
  }
}
