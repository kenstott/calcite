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
package org.apache.calcite.adapter.file.storage;
// storage-provider-guard:allow-scheme - storage-dispatch layer: inspecting a URI scheme here is the legitimate job (provider dispatch / S3 path handling / endpoint SSL config), not a consumer branching local-vs-remote.

import org.apache.calcite.adapter.file.storage.cache.PersistentStorageCache;
import org.apache.calcite.adapter.file.storage.cache.StorageCacheManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.BucketLifecycleConfiguration;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.LifecycleExpiration;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Storage provider implementation for Amazon S3, on AWS SDK v2 ({@link S3Client}).
 *
 * <p>This is the single JVM-side S3 stack for the file adapter — it replaces the AWS
 * SDK v1 {@code AmazonS3} client and, together with Iceberg's {@code S3FileIO} (also
 * AWS SDK v2), removes the need for hadoop-aws's {@code S3AFileSystem} (AWS SDK v1).
 */
public class S3StorageProvider implements StorageProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3StorageProvider.class);

  private final S3Client s3Client;

  // Persistent cache for restart-survivable caching
  private final PersistentStorageCache persistentCache;

  // Base S3 path from directory operand (e.g., "s3://bucket/prefix/")
  private final String baseS3Path;

  // S3 configuration for DuckDB access (credentials, endpoint, region)
  private final java.util.Map<String, String> s3Config;

  public S3StorageProvider(S3Client s3Client) {
    this(s3Client, null);
  }

  /**
   * Constructor with explicit configuration.
   * Expects config map with: bucket, region, accessKeyId, secretAccessKey, directory
   */
  public S3StorageProvider(java.util.Map<String, Object> config) {
    this(null, config);
  }

  /**
   * Internal constructor with both s3Client and config.
   */
  private S3StorageProvider(S3Client s3Client, java.util.Map<String, Object> config) {
    // Build or use provided S3 client
    if (s3Client == null) {
      if (config == null) {
        throw new IllegalArgumentException(
            "S3StorageProvider requires explicit configuration via model.json operand. "
            + "Provide 'accessKeyId' and 'secretAccessKey' in the storageConfig section.");
      }

      // Require explicit credentials from config - no environment variable fallbacks
      String accessKeyId = (String) config.get("accessKeyId");
      String secretAccessKey = (String) config.get("secretAccessKey");

      if (accessKeyId == null || secretAccessKey == null) {
        throw new IllegalArgumentException(
            "S3StorageProvider requires 'accessKeyId' and 'secretAccessKey' in model.json "
            + "storageConfig. Environment variable fallbacks are not supported. "
            + "Provided config keys: " + config.keySet());
      }

      // Endpoint from config only (no env var fallback)
      String endpoint = (String) config.get("endpoint");

      // Region from config only, default to us-east-1 if absent
      String region = (String) config.get("region");
      if (region == null || region.isEmpty() || "auto".equalsIgnoreCase(region)) {
        region = "us-east-1";
      }

      // Configure the HTTP client with longer timeouts for large file uploads (e.g. 100MB+
      // parquet files) and a large connection pool for heavy concurrent ETL. Socket timeout
      // 15 min (data transfer over slow links); connection timeout 60s (DNS + TCP handshake);
      // max 200 connections (default is too low for heavy ETL). Re-validate a pooled
      // connection after 2s idle — under heavy load the server (e.g. MinIO) silently drops
      // idle keep-alive sockets, and reusing a stale one throws "Connection reset".
      ApacheHttpClient.Builder httpClient = ApacheHttpClient.builder()
          .socketTimeout(Duration.ofMinutes(15))
          .connectionTimeout(Duration.ofSeconds(60))
          .connectionMaxIdleTime(Duration.ofSeconds(60))
          .connectionAcquisitionTimeout(Duration.ofSeconds(60))
          .maxConnections(200);

      // Retry transient failures — 5xx, throttling, and connection-level errors such as
      // "Connection reset" — with the SDK's exponential backoff. Works for any S3 server
      // (MinIO, R2, AWS). The default is too few under heavy concurrent ETL load.
      ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
          .retryPolicy(RetryPolicy.builder().numRetries(8).build())
          .build();

      software.amazon.awssdk.services.s3.S3ClientBuilder builder = S3Client.builder()
          .httpClientBuilder(httpClient)
          .overrideConfiguration(overrideConfig)
          .region(Region.of(region))
          .credentialsProvider(
              StaticCredentialsProvider.create(
                  AwsBasicCredentials.create(accessKeyId, secretAccessKey)));

      // If custom endpoint is provided, use it with path-style access (MinIO/R2).
      if (endpoint != null) {
        builder.endpointOverride(URI.create(endpoint))
            .serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .build());
      }

      this.s3Client = builder.build();
    } else {
      this.s3Client = s3Client;
    }

    // Extract base S3 path from config (directory operand)
    if (config != null && config.get("directory") != null) {
      String directory = (String) config.get("directory");
      // Ensure it ends with /
      this.baseS3Path = directory.endsWith("/") ? directory : directory + "/";

      // Ensure bucket exists - create if needed
      ensureBucketExists(this.baseS3Path);
    } else {
      this.baseS3Path = null;
    }

    // Initialize persistent cache if cache manager is available
    PersistentStorageCache cache = null;
    try {
      cache = StorageCacheManager.getInstance().getCache("s3");
    } catch (IllegalStateException e) {
      // Cache manager not initialized, persistent cache will be null
    }
    this.persistentCache = cache;

    // Store S3 config for DuckDB access
    if (config != null) {
      LOGGER.info("S3StorageProvider: Initializing with config containing {} keys: {}",
          config.size(), config.keySet());

      java.util.Map<String, String> s3ConfigMap = new java.util.HashMap<>();
      if (config.get("accessKeyId") != null) {
        s3ConfigMap.put("accessKeyId", (String) config.get("accessKeyId"));
      }
      if (config.get("secretAccessKey") != null) {
        s3ConfigMap.put("secretAccessKey", (String) config.get("secretAccessKey"));
      }
      if (config.get("region") != null) {
        s3ConfigMap.put("region", (String) config.get("region"));
      }
      if (config.get("endpoint") != null) {
        s3ConfigMap.put("endpoint", (String) config.get("endpoint"));
      }
      this.s3Config = s3ConfigMap.isEmpty() ? null : s3ConfigMap;

      if (s3ConfigMap.isEmpty()) {
        LOGGER.warn("S3StorageProvider: Config was provided but no recognized keys found. "
            + "Expected keys: accessKeyId, secretAccessKey, region, endpoint. "
            + "Provided keys: {}", config.keySet());
      } else {
        LOGGER.info("S3StorageProvider: Stored {} credentials for DuckDB/Iceberg access",
            s3ConfigMap.size());
      }
    } else {
      LOGGER.info("S3StorageProvider: No config provided, credentials will be null");
      this.s3Config = null;
    }
  }

  /**
   * Ensures that the S3 bucket exists, creating it if necessary.
   *
   * @param s3Path Full S3 path (e.g., "s3://bucket-name/prefix/")
   */
  private void ensureBucketExists(String s3Path) {
    try {
      S3Uri s3Uri = parseS3Uri(s3Path);
      String bucketName = s3Uri.bucket;

      // Check if bucket exists via HEAD; create it if absent.
      try {
        s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
        LOGGER.debug("S3 bucket already exists: {}", bucketName);
      } catch (NoSuchBucketException notFound) {
        LOGGER.info("Creating S3 bucket: {}", bucketName);
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        LOGGER.info("Successfully created S3 bucket: {}", bucketName);
      }
    } catch (S3Exception e) {
      // Log but don't fail - the bucket might exist but we don't have permission to check,
      // or it might be created by another process. Let subsequent operations fail if needed.
      LOGGER.warn("Unable to verify or create S3 bucket from path {}: {} ({})",
          s3Path, e.getMessage(), e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "");
    } catch (IOException e) {
      LOGGER.warn("Unable to parse S3 path for bucket creation: {}", s3Path, e);
    }
  }

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    // Convert relative path to full S3 URI if needed
    String fullPath = toFullPath(path);
    S3Uri s3Uri = parseS3Uri(fullPath);
    List<FileEntry> entries = new ArrayList<>();

    ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
        .bucket(s3Uri.bucket)
        .prefix(s3Uri.key);
    if (!recursive) {
      requestBuilder.delimiter("/");
    }

    String continuationToken = null;
    int page = 0;
    ListObjectsV2Response result;
    do {
      if (continuationToken != null) {
        requestBuilder.continuationToken(continuationToken);
      }
      result = s3Client.listObjectsV2(requestBuilder.build());
      page++;

      for (S3Object summary : result.contents()) {
        if (!summary.key().equals(s3Uri.key)) { // Skip the directory itself
          entries.add(
              new FileEntry(
              "s3://" + s3Uri.bucket + "/" + summary.key(),
              getFileName(summary.key()),
              false,
              summary.size(),
              summary.lastModified() != null ? summary.lastModified().toEpochMilli() : 0));
        }
      }

      // Add directories when not recursive
      if (!recursive && result.commonPrefixes() != null) {
        for (CommonPrefix cp : result.commonPrefixes()) {
          String prefix = cp.prefix();
          entries.add(
              new FileEntry(
              "s3://" + s3Uri.bucket + "/" + prefix,
              getFileName(prefix.endsWith("/") ?
                  prefix.substring(0, prefix.length() - 1) : prefix),
              true,
              0,
              0));
        }
      }

      if (Boolean.TRUE.equals(result.isTruncated()) && page % 5 == 0) {
        LOGGER.info("S3 LIST {}: {} entries so far (page {})", s3Uri.key, entries.size(), page);
      }

      continuationToken = result.nextContinuationToken();
    } while (Boolean.TRUE.equals(result.isTruncated()));

    return entries;
  }

  @Override public FileMetadata getMetadata(String path) throws IOException {
    // Convert relative path to full S3 URI if needed
    String fullPath = toFullPath(path);
    S3Uri s3Uri = parseS3Uri(fullPath);

    HeadObjectResponse metadata = s3Client.headObject(
        HeadObjectRequest.builder().bucket(s3Uri.bucket).key(s3Uri.key).build());

    return new FileMetadata(
        path,
        metadata.contentLength(),
        metadata.lastModified() != null ? metadata.lastModified().toEpochMilli() : 0,
        metadata.contentType(),
        metadata.eTag());
  }

  @Override public InputStream openInputStream(String path) throws IOException {
    // Check persistent cache first if available
    // Use original path as cache key for consistency
    if (persistentCache != null) {
      byte[] cachedData = persistentCache.getCachedData(path);
      FileMetadata cachedMetadata = persistentCache.getCachedMetadata(path);

      if (cachedData != null && cachedMetadata != null) {
        // Check if cached data is still fresh
        try {
          if (!hasChanged(path, cachedMetadata)) {
            return new java.io.ByteArrayInputStream(cachedData);
          }
        } catch (IOException e) {
          // If we can't check freshness, use cached data anyway
          return new java.io.ByteArrayInputStream(cachedData);
        }
      }
    }

    // Convert relative path to full S3 URI if needed
    String fullPath = toFullPath(path);
    S3Uri s3Uri = parseS3Uri(fullPath);
    GetObjectRequest request =
        GetObjectRequest.builder().bucket(s3Uri.bucket).key(s3Uri.key).build();
    ResponseInputStream<GetObjectResponse> object = s3Client.getObject(request);

    // If persistent cache is available, read data and cache it
    if (persistentCache != null) {
      GetObjectResponse resp = object.response();
      byte[] data = readAllBytes(object);
      object.close();

      // Use original path as cache key for consistency
      FileMetadata metadata =
          new FileMetadata(path, resp.contentLength(),
          resp.lastModified() != null ? resp.lastModified().toEpochMilli() : 0,
          resp.contentType(),
          resp.eTag());
      persistentCache.cacheData(path, data, metadata, 0); // No TTL for S3

      return new java.io.ByteArrayInputStream(data);
    }

    return object;
  }

  @Override public byte[] readRange(String path, long offset, long length) throws IOException {
    String fullPath = toFullPath(path);
    S3Uri s3Uri = parseS3Uri(fullPath);
    GetObjectRequest request = GetObjectRequest.builder()
        .bucket(s3Uri.bucket)
        .key(s3Uri.key)
        .range("bytes=" + offset + "-" + (offset + length - 1))
        .build();
    try (InputStream is = s3Client.getObject(request)) {
      return readAllBytes(is);
    }
  }

  @Override public Reader openReader(String path) throws IOException {
    return new InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
  }

  @Override public boolean exists(String path) throws IOException {
    try {
      // Convert relative path to full S3 URI if needed
      String fullPath = toFullPath(path);
      S3Uri s3Uri = parseS3Uri(fullPath);

      // Check if path contains glob patterns
      if (s3Uri.key.contains("*")) {
        // Use prefix-based listing for glob patterns.
        String globPattern = s3Uri.key;

        int wildcardIndex = globPattern.indexOf('*');
        String beforeWildcard = globPattern.substring(0, wildcardIndex);
        String afterWildcard = globPattern.substring(wildcardIndex + 1);

        int nextSlash = afterWildcard.indexOf('/');
        String constantPart = "";
        if (nextSlash >= 0) {
          String filenamePattern = afterWildcard.substring(nextSlash + 1);
          int filenameWildcard = filenamePattern.indexOf('*');
          if (filenameWildcard > 0) {
            constantPart = filenamePattern.substring(0, filenameWildcard);
          }
        }

        // Convert glob pattern to regex for matching
        String regex = globPattern
            .replace(".", "\\.")
            .replace("*", ".*");

        // Try each year from 2016 to current year
        int currentYear = java.time.Year.now(java.time.ZoneOffset.UTC).getValue();
        for (int year = currentYear; year >= 2016; year--) {
          String yearPrefix = beforeWildcard + year + afterWildcard.substring(0, nextSlash >= 0 ? nextSlash + 1 : 0) + constantPart;

          ListObjectsV2Response result = s3Client.listObjectsV2(
              ListObjectsV2Request.builder()
                  .bucket(s3Uri.bucket)
                  .prefix(yearPrefix)
                  .maxKeys(10)
                  .build());
          for (S3Object obj : result.contents()) {
            if (obj.key().matches(regex)) {
              LOGGER.debug("S3 exists check (glob): {} -> true (matched {})", path, obj.key());
              return true;
            }
          }
        }
        LOGGER.debug("S3 exists check (glob): {} -> false", path);
        return false;
      }

      // Standard exact path check via HEAD
      boolean exists = objectExists(s3Uri.bucket, s3Uri.key);
      LOGGER.debug("S3 exists check: {} -> {}", path, exists);
      return exists;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      // Do NOT silently assume the object is absent. A transient/connection error (e.g. a
      // "Connection reset" that survived the SDK retries) would otherwise be read as "not cached" —
      // causing a needless re-download and polluting the pipeline's error list. Surface it.
      throw new IOException("S3 exists check failed for " + path, e);
    }
  }

  /** HEAD-based object existence check (v2 has no doesObjectExist). */
  private boolean objectExists(String bucket, String key) {
    try {
      s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
      return true;
    } catch (NoSuchKeyException notFound) {
      return false;
    }
  }

  @Override public boolean isDirectory(String path) throws IOException {
    // Convert relative path to full S3 URI if needed
    String fullPath = toFullPath(path);
    S3Uri s3Uri = parseS3Uri(fullPath);

    // In S3, directories are conceptual. Check if there are objects with this prefix
    ListObjectsV2Response result = s3Client.listObjectsV2(
        ListObjectsV2Request.builder()
            .bucket(s3Uri.bucket)
            .prefix(s3Uri.key.endsWith("/") ? s3Uri.key : s3Uri.key + "/")
            .maxKeys(1)
            .build());
    return result.keyCount() != null && result.keyCount() > 0;
  }

  @Override public String getStorageType() {
    return "s3";
  }

  @Override public java.util.Map<String, String> getS3Config() {
    return s3Config;
  }

  @Override public String resolvePath(String basePath, String relativePath) {
    // If relativePath is already a full S3 URI, return it unchanged
    if (relativePath.startsWith("s3://") || relativePath.startsWith("s3a://")) {
      return relativePath;
    }

    // If basePath doesn't end with /, it might be a file
    // Strip the filename part to get the directory
    if (!basePath.endsWith("/")) {
      int lastSlash = basePath.lastIndexOf('/');
      if (lastSlash > "s3://".length()) {
        // Check if the part after the last slash looks like a file (has extension)
        String lastPart = basePath.substring(lastSlash + 1);
        if (lastPart.contains(".")) {
          // It's likely a file, use the directory part
          basePath = basePath.substring(0, lastSlash + 1);
        } else {
          // It's likely a directory without trailing slash, add one
          basePath = basePath + "/";
        }
      } else {
        basePath = basePath + "/";
      }
    }

    return basePath + relativePath;
  }

  private S3Uri parseS3Uri(String uri) throws IOException {
    // Accept both s3:// and s3a:// (Hadoop S3A FileSystem) URI schemes
    if (!uri.startsWith("s3://") && !uri.startsWith("s3a://")) {
      throw new IOException("Invalid S3 URI: " + uri);
    }

    try {
      // URL-encode spaces and other special characters in the path before parsing
      // S3 keys can contain spaces, but java.net.URI requires them to be encoded
      String encodedUri = uri.replace(" ", "%20");

      URI parsed = new URI(encodedUri);
      String bucket = parsed.getHost();
      String key = parsed.getPath();
      if (key.startsWith("/")) {
        key = key.substring(1);
      }
      // Decode the key back for S3 API calls
      key = java.net.URLDecoder.decode(key, "UTF-8");
      return new S3Uri(bucket, key);
    } catch (Exception e) {
      throw new IOException("Failed to parse S3 URI: " + uri, e);
    }
  }

  private String getFileName(String key) {
    int lastSlash = key.lastIndexOf('/');
    if (lastSlash >= 0 && lastSlash < key.length() - 1) {
      return key.substring(lastSlash + 1);
    }
    return key;
  }

  private byte[] readAllBytes(InputStream inputStream) throws IOException {
    java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
    byte[] data = new byte[8192];
    int nRead;
    while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, nRead);
    }
    return buffer.toByteArray();
  }

  /**
   * Converts a relative path to a full S3 URI using the base S3 path.
   * If the path is already a full S3 URI (s3:// or s3a://), returns it unchanged.
   */
  private String toFullPath(String path) throws IOException {
    if (path.startsWith("s3://") || path.startsWith("s3a://")) {
      return path;
    }

    if (baseS3Path == null) {
      throw new IOException("Cannot resolve relative path '" + path
          + "' without base S3 path. Please provide 'directory' in configuration.");
    }

    // Combine base S3 path with relative path
    return baseS3Path + path;
  }

  @Override public void writeFile(String path, byte[] content) throws IOException {
    // Convert relative path to full S3 URI if needed
    String fullPath = toFullPath(path);
    S3Uri s3Uri = parseS3Uri(fullPath);

    PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
        .bucket(s3Uri.bucket)
        .key(s3Uri.key)
        .contentLength((long) content.length);
    String contentType = guessContentType(path);
    if (contentType != null) {
      requestBuilder.contentType(contentType);
    }

    int maxRetries = 3;
    for (int attempt = 0; attempt < maxRetries; attempt++) {
      try {
        s3Client.putObject(requestBuilder.build(), RequestBody.fromBytes(content));
        return;
      } catch (S3Exception e) {
        if (isRetryableS3Error(e) && attempt < maxRetries - 1) {
          long delay = 1000L * (1L << attempt);
          LOGGER.warn("S3 write failed for {} (HTTP {}): {} — retrying in {}ms (attempt {}/{})",
              path, e.statusCode(), errorCode(e), delay, attempt + 1, maxRetries);
          sleepQuietly(delay);
        } else {
          throw new IOException("Failed to write file to S3: " + path, e);
        }
      }
    }
  }

  @Override public void writeFile(String path, InputStream content) throws IOException {
    streamMultipartUpload(path, content);
  }

  /**
   * Streams an InputStream directly to S3 using multipart upload.
   * Each part is buffered to exactly {@code partSize} bytes (16 MB) before uploading,
   * so peak memory usage is O(partSize) regardless of total file size.
   */
  private void streamMultipartUpload(String path, InputStream content) throws IOException {
    String fullPath = toFullPath(path);
    S3Uri s3Uri = parseS3Uri(fullPath);
    final int partSize = 16 * 1024 * 1024; // 16 MB parts

    String contentType = guessContentType(path);

    // Read first part to check if content is empty
    byte[] partBuf = new byte[partSize];
    int firstPartLen = readFully(content, partBuf);

    if (firstPartLen <= 0) {
      // Zero-byte object: simple put
      PutObjectRequest.Builder pb = PutObjectRequest.builder()
          .bucket(s3Uri.bucket).key(s3Uri.key).contentLength(0L);
      if (contentType != null) {
        pb.contentType(contentType);
      }
      s3Client.putObject(pb.build(), RequestBody.fromBytes(new byte[0]));
      return;
    }

    // If content fits in one part and is < 5MB, use simple put (multipart min is 5MB)
    boolean streamDone = firstPartLen < partSize;
    if (streamDone && firstPartLen < 5 * 1024 * 1024) {
      PutObjectRequest.Builder pb = PutObjectRequest.builder()
          .bucket(s3Uri.bucket).key(s3Uri.key).contentLength((long) firstPartLen);
      if (contentType != null) {
        pb.contentType(contentType);
      }
      s3Client.putObject(pb.build(),
          RequestBody.fromInputStream(new ByteArrayInputStream(partBuf, 0, firstPartLen), firstPartLen));
      return;
    }

    // Multipart upload — stream parts as we read them
    CreateMultipartUploadRequest.Builder initBuilder = CreateMultipartUploadRequest.builder()
        .bucket(s3Uri.bucket).key(s3Uri.key);
    if (contentType != null) {
      initBuilder.contentType(contentType);
    }
    CreateMultipartUploadResponse initResponse =
        s3Client.createMultipartUpload(initBuilder.build());
    String uploadId = initResponse.uploadId();

    try {
      List<CompletedPart> completedParts = new ArrayList<>();
      int partNumber = 1;

      // Upload first part
      int firstPart = partNumber++;
      String firstETag = s3Client.uploadPart(
          UploadPartRequest.builder()
              .bucket(s3Uri.bucket).key(s3Uri.key).uploadId(uploadId)
              .partNumber(firstPart).contentLength((long) firstPartLen).build(),
          RequestBody.fromInputStream(new ByteArrayInputStream(partBuf, 0, firstPartLen), firstPartLen))
          .eTag();
      completedParts.add(CompletedPart.builder().partNumber(firstPart).eTag(firstETag).build());

      // Stream remaining parts
      while (!streamDone) {
        int partLen = readFully(content, partBuf);
        if (partLen <= 0) {
          break;
        }
        streamDone = partLen < partSize;

        int pn = partNumber++;
        String etag = s3Client.uploadPart(
            UploadPartRequest.builder()
                .bucket(s3Uri.bucket).key(s3Uri.key).uploadId(uploadId)
                .partNumber(pn).contentLength((long) partLen).build(),
            RequestBody.fromInputStream(new ByteArrayInputStream(partBuf, 0, partLen), partLen))
            .eTag();
        completedParts.add(CompletedPart.builder().partNumber(pn).eTag(etag).build());
      }

      // Complete
      s3Client.completeMultipartUpload(
          CompleteMultipartUploadRequest.builder()
              .bucket(s3Uri.bucket).key(s3Uri.key).uploadId(uploadId)
              .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
              .build());

    } catch (Exception e) {
      try {
        s3Client.abortMultipartUpload(
            AbortMultipartUploadRequest.builder()
                .bucket(s3Uri.bucket).key(s3Uri.key).uploadId(uploadId).build());
      } catch (Exception abortEx) {
        LOGGER.warn("Failed to abort multipart upload for s3://{}/{}: {}",
            s3Uri.bucket, s3Uri.key, abortEx.toString());
      }
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException("Failed streaming multipart upload to S3: " + path, e);
    }
  }

  /**
   * Reads from an InputStream into a buffer, filling as much as possible.
   * Returns the total bytes read, or -1 if the stream is exhausted.
   */
  private static int readFully(InputStream in, byte[] buf) throws IOException {
    int total = 0;
    while (total < buf.length) {
      int n = in.read(buf, total, buf.length - total);
      if (n < 0) {
        break;
      }
      total += n;
    }
    return total;
  }

  /**
   * Returns whether an S3 error is transient and worth retrying.
   * Covers server errors (500, 502, 503, 504), throttling (429),
   * and known transient error codes from S3/R2.
   */
  private static boolean isRetryableS3Error(S3Exception e) {
    int status = e.statusCode();
    if (status == 429 || status == 500 || status == 502 || status == 503 || status == 504) {
      return true;
    }
    String code = errorCode(e);
    if (code == null) {
      return false;
    }
    return "InternalError".equals(code)
        || "NoSuchUpload".equals(code)
        || "RequestTimeout".equals(code)
        || "SlowDown".equals(code)
        || "ServiceUnavailable".equals(code);
  }

  private static String errorCode(S3Exception e) {
    return e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null;
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  @Override public void createDirectories(String path) throws IOException {
    // S3 doesn't have real directories, they're just prefixes. For compatibility we create
    // an empty marker object with a trailing slash.

    // Convert relative path to full S3 URI if needed
    String fullPath = toFullPath(path);
    if (!fullPath.endsWith("/")) {
      fullPath = fullPath + "/";
    }

    S3Uri s3Uri = parseS3Uri(fullPath);

    try {
      s3Client.putObject(
          PutObjectRequest.builder()
              .bucket(s3Uri.bucket).key(s3Uri.key).contentLength(0L).build(),
          RequestBody.fromBytes(new byte[0]));
    } catch (S3Exception e) {
      // Ignore if it already exists
      if (e.statusCode() != 409) { // 409 = Conflict
        throw new IOException("Failed to create directory marker in S3: " + path, e);
      }
    }
  }

  @Override public boolean delete(String path) throws IOException {
    // Convert relative path to full S3 URI if needed
    String fullPath = toFullPath(path);
    S3Uri s3Uri = parseS3Uri(fullPath);

    try {
      // Check if an object exists first
      if (!objectExists(s3Uri.bucket, s3Uri.key)) {
        return false;
      }

      // Delete the object
      s3Client.deleteObject(
          DeleteObjectRequest.builder().bucket(s3Uri.bucket).key(s3Uri.key).build());
      return true;
    } catch (S3Exception e) {
      throw new IOException("Failed to delete S3 object: " + path, e);
    }
  }

  /**
   * Batch delete using S3 DeleteObjects API (up to 1000 objects per request).
   * Much more efficient than individual deletes for large numbers of files.
   */
  @Override public int deleteBatch(List<String> paths) throws IOException {
    if (paths == null || paths.isEmpty()) {
      return 0;
    }

    int totalDeleted = 0;

    // Group paths by bucket (all should be same bucket, but be safe)
    java.util.Map<String, List<ObjectIdentifier>> bucketKeys = new java.util.HashMap<>();

    for (String path : paths) {
      // Convert relative path to full S3 URI if needed
      String fullPath = toFullPath(path);
      S3Uri s3Uri = parseS3Uri(fullPath);
      bucketKeys.computeIfAbsent(s3Uri.bucket, k -> new ArrayList<>())
          .add(ObjectIdentifier.builder().key(s3Uri.key).build());
    }

    // Delete in batches of 1000 (S3 limit)
    for (java.util.Map.Entry<String, List<ObjectIdentifier>> entry : bucketKeys.entrySet()) {
      String bucket = entry.getKey();
      List<ObjectIdentifier> keys = entry.getValue();

      for (int i = 0; i < keys.size(); i += 1000) {
        int end = Math.min(i + 1000, keys.size());
        List<ObjectIdentifier> batch = keys.subList(i, end);

        try {
          DeleteObjectsResponse result = s3Client.deleteObjects(
              DeleteObjectsRequest.builder()
                  .bucket(bucket)
                  .delete(Delete.builder().objects(batch).quiet(true).build())
                  .build());
          // With quiet=true, deleted objects are not returned; count errors instead.
          int failed = result.errors() == null ? 0 : result.errors().size();
          totalDeleted += batch.size() - failed;
        } catch (S3Exception e) {
          LOGGER.warn("Batch delete partially failed for bucket {}: {}", bucket, e.getMessage());
          // Continue with remaining batches
        }
      }
    }

    return totalDeleted;
  }

  /**
   * Ensures a lifecycle rule exists for auto-expiring objects with a given prefix.
   *
   * @param prefix The S3 key prefix (e.g., "source=econ/type=regional_income/_temp_reorg/")
   * @param expirationDays Number of days after which objects expire (minimum 1)
   */
  @Override public void ensureLifecycleRule(String prefix, int expirationDays) throws IOException {
    if (baseS3Path == null || baseS3Path.isEmpty()) {
      LOGGER.warn("Cannot set lifecycle rule: no base S3 path configured");
      return;
    }

    S3Uri baseUri = parseS3Uri(baseS3Path);
    String bucket = baseUri.bucket;
    String fullPrefix = baseUri.key.isEmpty() ? prefix : baseUri.key + "/" + prefix;
    String ruleId = "auto-expire-" + prefix.replace("/", "-").replaceAll("[^a-zA-Z0-9-]", "");
    if (ruleId.length() > 255) {
      ruleId = ruleId.substring(0, 255);
    }

    try {
      // Get existing lifecycle configuration (empty if none set)
      List<LifecycleRule> rules = new ArrayList<>();
      try {
        rules.addAll(s3Client.getBucketLifecycleConfiguration(
            b -> b.bucket(bucket)).rules());
      } catch (S3Exception e) {
        // NoSuchLifecycleConfiguration → start with an empty rule set.
        if (!"NoSuchLifecycleConfiguration".equals(errorCode(e))) {
          throw e;
        }
      }

      // Check if rule already exists
      for (LifecycleRule rule : rules) {
        if (ruleId.equals(rule.id())) {
          LOGGER.debug("Lifecycle rule '{}' already exists for prefix '{}'", ruleId, fullPrefix);
          return;
        }
      }

      // Create new rule
      LifecycleRule newRule = LifecycleRule.builder()
          .id(ruleId)
          .filter(LifecycleRuleFilter.builder().prefix(fullPrefix).build())
          .expiration(LifecycleExpiration.builder().days(expirationDays).build())
          .status(ExpirationStatus.ENABLED)
          .build();

      List<LifecycleRule> newRules = new ArrayList<>(rules);
      newRules.add(newRule);

      s3Client.putBucketLifecycleConfiguration(b -> b
          .bucket(bucket)
          .lifecycleConfiguration(BucketLifecycleConfiguration.builder().rules(newRules).build()));
      LOGGER.info("Created lifecycle rule '{}': prefix='{}' expires in {} days",
          ruleId, fullPrefix, expirationDays);
    } catch (S3Exception e) {
      // Log warning but don't fail - lifecycle is a cleanup optimization, not critical
      LOGGER.warn("Failed to set lifecycle rule for prefix '{}': {}", fullPrefix, e.getMessage());
    }
  }

  /**
   * Gets a staging directory for temporary files with automatic cleanup via S3 lifecycle rules.
   *
   * @param purpose Subdirectory name to isolate different staging uses (e.g., "iceberg", "etl")
   * @return S3 path to staging directory with auto-cleanup guarantee
   * @throws IOException If an I/O error occurs
   */
  @Override public String getStagingDirectory(String purpose) throws IOException {
    if (baseS3Path == null || baseS3Path.isEmpty()) {
      // Fall back to default (system temp directory) if no S3 path configured
      String tempDir = System.getProperty("java.io.tmpdir");
      String stagingPath = tempDir + "/.staging/" + purpose;
      createDirectories(stagingPath);
      return stagingPath;
    }

    // Build staging path under .staging/ prefix
    String stagingPrefix = ".staging/" + purpose;
    String stagingPath = resolvePath(baseS3Path, stagingPrefix);

    // Ensure lifecycle rule exists for auto-cleanup (1 day expiration)
    ensureLifecycleRule(".staging/", 1);

    // Create the staging directory marker
    createDirectories(stagingPath);

    LOGGER.debug("S3 staging directory: {} (auto-expires in 1 day)", stagingPath);
    return stagingPath;
  }

  @Override public void copyFile(String source, String destination) throws IOException {
    // Convert relative paths to full S3 URIs if needed
    String fullSource = toFullPath(source);
    String fullDest = toFullPath(destination);
    S3Uri sourceUri = parseS3Uri(fullSource);
    S3Uri destUri = parseS3Uri(fullDest);

    try {
      // Check if source exists
      if (!objectExists(sourceUri.bucket, sourceUri.key)) {
        throw new IOException("Source file does not exist in S3: " + source);
      }

      // Perform the copy
      s3Client.copyObject(CopyObjectRequest.builder()
          .sourceBucket(sourceUri.bucket).sourceKey(sourceUri.key)
          .destinationBucket(destUri.bucket).destinationKey(destUri.key)
          .build());
    } catch (S3Exception e) {
      throw new IOException("Failed to copy S3 object from " + source + " to " + destination, e);
    }
  }

  /**
   * Guess content type based on file extension.
   */
  private String guessContentType(String path) {
    String lowercasePath = path.toLowerCase();
    if (lowercasePath.endsWith(".json")) {
      return "application/json";
    } else if (lowercasePath.endsWith(".csv")) {
      return "text/csv";
    } else if (lowercasePath.endsWith(".parquet")) {
      return "application/x-parquet";
    } else if (lowercasePath.endsWith(".xml")) {
      return "application/xml";
    } else if (lowercasePath.endsWith(".txt")) {
      return "text/plain";
    } else if (lowercasePath.endsWith(".yaml") || lowercasePath.endsWith(".yml")) {
      return "application/x-yaml";
    }
    return "application/octet-stream";
  }

  private static class S3Uri {
    final String bucket;
    final String key;

    S3Uri(String bucket, String key) {
      this.bucket = bucket;
      this.key = key;
    }
  }
}
