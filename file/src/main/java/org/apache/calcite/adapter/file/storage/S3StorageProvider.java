/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.storage;

import org.apache.calcite.adapter.file.storage.cache.PersistentStorageCache;
import org.apache.calcite.adapter.file.storage.cache.StorageCacheManager;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Storage provider implementation for Amazon S3.
 */
public class S3StorageProvider implements StorageProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3StorageProvider.class);

  private final AmazonS3 s3Client;

  // Persistent cache for restart-survivable caching
  private final PersistentStorageCache persistentCache;

  // Base S3 path from directory operand (e.g., "s3://bucket/prefix/")
  private final String baseS3Path;

  // S3 configuration for DuckDB access (credentials, endpoint, region)
  private final java.util.Map<String, String> s3Config;

  public S3StorageProvider() {
    this((AmazonS3) null, null);
  }

  public S3StorageProvider(AmazonS3 s3Client) {
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
  private S3StorageProvider(AmazonS3 s3Client, java.util.Map<String, Object> config) {
    // Build or use provided S3 client
    if (s3Client == null) {
      // Configure client with longer timeouts for large file uploads (e.g., 100MB+ parquet files)
      // Socket timeout: 15 minutes (sufficient for large files over slow connections)
      // Connection timeout: 60 seconds (DNS + TCP handshake)
      // Max connections: 200 (default 50 is too low for heavy ETL with many concurrent requests)
      ClientConfiguration clientConfig = new ClientConfiguration();
      clientConfig.setSocketTimeout(15 * 60 * 1000);    // 15 minutes for data transfer
      clientConfig.setConnectionTimeout(60 * 1000);     // 60 seconds for TCP handshake
      clientConfig.setConnectionMaxIdleMillis(60_000);  // Close idle connections after 1 minute
      clientConfig.setMaxConnections(200);              // Support heavy ETL workloads

      AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
          .withClientConfiguration(clientConfig);

      if (config != null) {
        // Use provided credentials if available, otherwise fall back to default chain
        String accessKeyId = (String) config.get("accessKeyId");
        String secretAccessKey = (String) config.get("secretAccessKey");

        if (accessKeyId != null && secretAccessKey != null) {
          builder.withCredentials(
              new com.amazonaws.auth.AWSStaticCredentialsProvider(
              new com.amazonaws.auth.BasicAWSCredentials(accessKeyId, secretAccessKey)));
        } else {
          builder.withCredentials(new DefaultAWSCredentialsProviderChain());
        }

        // Check for custom endpoint (e.g., MinIO, Wasabi, or other S3-compatible services)
        // Priority: config > AWS_ENDPOINT_OVERRIDE environment variable
        String endpoint = (String) config.get("endpoint");
        if (endpoint == null) {
          endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
        }

        String region = (String) config.get("region");
        if (region == null) {
          region = System.getenv("AWS_REGION");
        }
        if (region == null) {
          try {
            region = new DefaultAwsRegionProviderChain().getRegion();
          } catch (Exception e) {
            region = "us-east-1"; // Default for custom endpoints
          }
        }

        // If custom endpoint is provided, use endpoint configuration
        if (endpoint != null) {
          builder.withEndpointConfiguration(new EndpointConfiguration(endpoint, region));
          // Enable path-style access for S3-compatible services like MinIO
          builder.withPathStyleAccessEnabled(true);
        } else {
          // Standard AWS S3 - use region only
          builder.withRegion(region);
        }
      } else {
        builder.withCredentials(new DefaultAWSCredentialsProviderChain());

        // Check for AWS_ENDPOINT_OVERRIDE even without config
        String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");

        String region;
        try {
          region = new DefaultAwsRegionProviderChain().getRegion();
        } catch (Exception e) {
          region = "us-east-1";
        }

        if (endpoint != null) {
          builder.withEndpointConfiguration(new EndpointConfiguration(endpoint, region));
          builder.withPathStyleAccessEnabled(true);
        } else {
          builder.withRegion(region);
        }
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
        LOGGER.debug("S3StorageProvider: Found accessKeyId (length={})",
            ((String) config.get("accessKeyId")).length());
      }
      if (config.get("secretAccessKey") != null) {
        s3ConfigMap.put("secretAccessKey", (String) config.get("secretAccessKey"));
        LOGGER.debug("S3StorageProvider: Found secretAccessKey (length={})",
            ((String) config.get("secretAccessKey")).length());
      }
      if (config.get("region") != null) {
        s3ConfigMap.put("region", (String) config.get("region"));
        LOGGER.debug("S3StorageProvider: Found region={}", config.get("region"));
      }
      if (config.get("endpoint") != null) {
        s3ConfigMap.put("endpoint", (String) config.get("endpoint"));
        LOGGER.debug("S3StorageProvider: Found endpoint={}", config.get("endpoint"));
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

      // Check if bucket exists
      if (!s3Client.doesBucketExistV2(bucketName)) {
        LOGGER.info("Creating S3 bucket: {}", bucketName);
        s3Client.createBucket(bucketName);
        LOGGER.info("Successfully created S3 bucket: {}", bucketName);
      } else {
        LOGGER.debug("S3 bucket already exists: {}", bucketName);
      }
    } catch (AmazonServiceException e) {
      // Log but don't fail - the bucket might exist but we don't have permission to check,
      // or it might be created by another process. Let subsequent operations fail if needed.
      LOGGER.warn("Unable to verify or create S3 bucket from path {}: {} ({})",
          s3Path, e.getMessage(), e.getErrorCode());
    } catch (IOException e) {
      LOGGER.warn("Unable to parse S3 path for bucket creation: {}", s3Path, e);
    }
  }

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    // Convert relative path to full S3 URI if needed
    String fullPath = toFullPath(path);
    S3Uri s3Uri = parseS3Uri(fullPath);
    List<FileEntry> entries = new ArrayList<>();

    ListObjectsV2Request request = new ListObjectsV2Request()
        .withBucketName(s3Uri.bucket)
        .withPrefix(s3Uri.key);

    if (!recursive) {
      request.withDelimiter("/");
    }

    ListObjectsV2Result result;
    do {
      result = s3Client.listObjectsV2(request);

      for (S3ObjectSummary summary : result.getObjectSummaries()) {
        if (!summary.getKey().equals(s3Uri.key)) { // Skip the directory itself
          entries.add(
              new FileEntry(
              "s3://" + s3Uri.bucket + "/" + summary.getKey(),
              getFileName(summary.getKey()),
              false,
              summary.getSize(),
              summary.getLastModified().getTime()));
        }
      }

      // Add directories when not recursive
      if (!recursive && result.getCommonPrefixes() != null) {
        for (String prefix : result.getCommonPrefixes()) {
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

      request.setContinuationToken(result.getNextContinuationToken());
    } while (result.isTruncated());

    return entries;
  }

  @Override public FileMetadata getMetadata(String path) throws IOException {
    // Convert relative path to full S3 URI if needed
    String fullPath = toFullPath(path);
    S3Uri s3Uri = parseS3Uri(fullPath);

    com.amazonaws.services.s3.model.ObjectMetadata metadata =
        s3Client.getObjectMetadata(s3Uri.bucket, s3Uri.key);

    return new FileMetadata(
        path,
        metadata.getContentLength(),
        metadata.getLastModified().getTime(),
        metadata.getContentType(),
        metadata.getETag());
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
    GetObjectRequest request = new GetObjectRequest(s3Uri.bucket, s3Uri.key);
    S3Object object = s3Client.getObject(request);

    // If persistent cache is available, read data and cache it
    if (persistentCache != null) {
      byte[] data = readAllBytes(object.getObjectContent());
      object.close();

      // Get file metadata for caching (use S3 object metadata)
      // Use original path as cache key for consistency
      FileMetadata metadata =
          new FileMetadata(path, object.getObjectMetadata().getContentLength(),
          object.getObjectMetadata().getLastModified().getTime(),
          object.getObjectMetadata().getContentType(),
          object.getObjectMetadata().getETag());
      persistentCache.cacheData(path, data, metadata, 0); // No TTL for S3

      return new java.io.ByteArrayInputStream(data);
    }

    return object.getObjectContent();
  }

  @Override public Reader openReader(String path) throws IOException {
    return new InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
  }

  @Override public boolean exists(String path) throws IOException {
    try {
      // Convert relative path to full S3 URI if needed
      String fullPath = toFullPath(path);
      S3Uri s3Uri = parseS3Uri(fullPath);
      boolean exists = s3Client.doesObjectExist(s3Uri.bucket, s3Uri.key);
      LOGGER.debug("S3 exists check: {} -> {}", path, exists);
      return exists;
    } catch (Exception e) {
      LOGGER.warn("S3 exists check failed for {}: {} - assuming does not exist", path, e.getMessage());
      return false;
    }
  }

  @Override public boolean isDirectory(String path) throws IOException {
    // Convert relative path to full S3 URI if needed
    String fullPath = toFullPath(path);
    S3Uri s3Uri = parseS3Uri(fullPath);

    // In S3, directories are conceptual. Check if there are objects with this prefix
    ListObjectsV2Request request = new ListObjectsV2Request()
        .withBucketName(s3Uri.bucket)
        .withPrefix(s3Uri.key.endsWith("/") ? s3Uri.key : s3Uri.key + "/")
        .withMaxKeys(1);

    ListObjectsV2Result result = s3Client.listObjectsV2(request);
    return result.getKeyCount() > 0;
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

    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(content.length);

    // Set content type based on file extension
    String contentType = guessContentType(path);
    if (contentType != null) {
      metadata.setContentType(contentType);
    }

    try (InputStream input = new ByteArrayInputStream(content)) {
      PutObjectRequest request = new PutObjectRequest(s3Uri.bucket, s3Uri.key, input, metadata);
      s3Client.putObject(request);
    } catch (AmazonServiceException e) {
      throw new IOException("Failed to write file to S3: " + path, e);
    }
  }

  @Override public void writeFile(String path, InputStream content) throws IOException {
    // Stream upload using S3 multipart upload to avoid buffering large payloads in memory
    String fullPath = toFullPath(path);
    S3Uri s3Uri = parseS3Uri(fullPath);

    // Choose part size (must be >= 5 MB for multipart). Use 16 MB by default.
    final int partSize = 16 * 1024 * 1024;
    final byte[] buf = new byte[partSize];

    // Prepare metadata; we don't know content length in advance
    ObjectMetadata objectMetadata = new ObjectMetadata();
    String contentType = guessContentType(path);
    if (contentType != null) {
      objectMetadata.setContentType(contentType);
    }

    List<PartETag> partETags = new ArrayList<>();
    String uploadId = null;

    try {
      // Initiate multipart upload
      InitiateMultipartUploadRequest initRequest =
          new InitiateMultipartUploadRequest(s3Uri.bucket, s3Uri.key)
              .withObjectMetadata(objectMetadata);
      InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
      uploadId = initResponse.getUploadId();

      int partNumber = 1;
      long total = 0L;
      while (true) {
        int bytesRead = fillBuffer(content, buf);
        if (bytesRead <= 0) {
          break; // EOF
        }
        total += bytesRead;

        UploadPartRequest uploadRequest = new UploadPartRequest()
            .withBucketName(s3Uri.bucket)
            .withKey(s3Uri.key)
            .withUploadId(uploadId)
            .withPartNumber(partNumber++)
            .withInputStream(new java.io.ByteArrayInputStream(buf, 0, bytesRead))
            .withPartSize(bytesRead);

        partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());
      }

      if (partETags.isEmpty()) {
        // Zero-byte object: put a small empty object instead of multipart
        try (InputStream empty = new java.io.ByteArrayInputStream(new byte[0])) {
          PutObjectRequest req = new PutObjectRequest(s3Uri.bucket, s3Uri.key, empty, objectMetadata);
          s3Client.putObject(req);
        }
        return;
      }

      // Complete multipart upload
      CompleteMultipartUploadRequest compRequest =
          new CompleteMultipartUploadRequest(s3Uri.bucket, s3Uri.key, uploadId, partETags);
      s3Client.completeMultipartUpload(compRequest);
    } catch (AmazonServiceException e) {
      // Abort multipart upload on failure
      if (uploadId != null) {
        try {
          s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(s3Uri.bucket, s3Uri.key, uploadId));
        } catch (Exception abortEx) {
          // log and continue
          LoggerFactory.getLogger(S3StorageProvider.class).warn("Failed to abort multipart upload for s3://{}/{}: {}",
              s3Uri.bucket, s3Uri.key, abortEx.toString());
        }
      }
      throw new IOException("Failed to write file to S3: " + path, e);
    }
  }

  // Reads from 'in' into 'buffer' until either buffer is full or EOF; returns bytes read or -1 for EOF
  private static int fillBuffer(InputStream in, byte[] buffer) throws IOException {
    int off = 0;
    int len = buffer.length;
    while (off < len) {
      int r = in.read(buffer, off, len - off);
      if (r < 0) break;
      off += r;
    }
    return off == 0 ? -1 : off;
  }

  @Override public void createDirectories(String path) throws IOException {
    // S3 doesn't have real directories, they're just prefixes
    // We can create a marker object if needed, but it's often not necessary
    // For compatibility, we'll create an empty object with a trailing slash

    // Convert relative path to full S3 URI if needed
    String fullPath = toFullPath(path);
    if (!fullPath.endsWith("/")) {
      fullPath = fullPath + "/";
    }

    S3Uri s3Uri = parseS3Uri(fullPath);

    // Create an empty marker object
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(0);

    try (InputStream emptyContent = new ByteArrayInputStream(new byte[0])) {
      PutObjectRequest request = new PutObjectRequest(s3Uri.bucket, s3Uri.key, emptyContent, metadata);
      s3Client.putObject(request);
    } catch (AmazonServiceException e) {
      // Ignore if it already exists
      if (e.getStatusCode() != 409) { // 409 = Conflict
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
      if (!s3Client.doesObjectExist(s3Uri.bucket, s3Uri.key)) {
        return false;
      }

      // Delete the object
      DeleteObjectRequest request = new DeleteObjectRequest(s3Uri.bucket, s3Uri.key);
      s3Client.deleteObject(request);
      return true;
    } catch (AmazonServiceException e) {
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
    java.util.Map<String, List<DeleteObjectsRequest.KeyVersion>> bucketKeys =
        new java.util.HashMap<>();

    for (String path : paths) {
      // Convert relative path to full S3 URI if needed
      String fullPath = toFullPath(path);
      S3Uri s3Uri = parseS3Uri(fullPath);
      bucketKeys.computeIfAbsent(s3Uri.bucket, k -> new ArrayList<>())
          .add(new DeleteObjectsRequest.KeyVersion(s3Uri.key));
    }

    // Delete in batches of 1000 (S3 limit)
    for (java.util.Map.Entry<String, List<DeleteObjectsRequest.KeyVersion>> entry
        : bucketKeys.entrySet()) {
      String bucket = entry.getKey();
      List<DeleteObjectsRequest.KeyVersion> keys = entry.getValue();

      for (int i = 0; i < keys.size(); i += 1000) {
        int end = Math.min(i + 1000, keys.size());
        List<DeleteObjectsRequest.KeyVersion> batch = keys.subList(i, end);

        try {
          DeleteObjectsRequest request = new DeleteObjectsRequest(bucket)
              .withKeys(batch)
              .withQuiet(true);  // Don't return list of deleted objects
          DeleteObjectsResult result = s3Client.deleteObjects(request);
          totalDeleted += batch.size() - (result.getDeletedObjects() == null ? 0
              : batch.size() - result.getDeletedObjects().size());
        } catch (AmazonServiceException e) {
          LOGGER.warn("Batch delete partially failed for bucket {}: {}", bucket, e.getMessage());
          // Continue with remaining batches
        }
      }
    }

    return totalDeleted;
  }

  /**
   * Ensures a lifecycle rule exists for auto-expiring objects with a given prefix.
   * Used to auto-clean temp files without manual deletion.
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
      // Get existing lifecycle configuration
      BucketLifecycleConfiguration config = s3Client.getBucketLifecycleConfiguration(bucket);
      List<BucketLifecycleConfiguration.Rule> rules;

      if (config == null || config.getRules() == null) {
        rules = new ArrayList<>();
      } else {
        rules = new ArrayList<>(config.getRules());
      }

      // Check if rule already exists
      boolean ruleExists = false;
      for (BucketLifecycleConfiguration.Rule rule : rules) {
        if (ruleId.equals(rule.getId())) {
          ruleExists = true;
          LOGGER.debug("Lifecycle rule '{}' already exists for prefix '{}'", ruleId, fullPrefix);
          break;
        }
      }

      if (!ruleExists) {
        // Create new rule
        BucketLifecycleConfiguration.Rule newRule = new BucketLifecycleConfiguration.Rule()
            .withId(ruleId)
            .withFilter(new LifecycleFilter(new LifecyclePrefixPredicate(fullPrefix)))
            .withExpirationInDays(expirationDays)
            .withStatus(BucketLifecycleConfiguration.ENABLED);

        rules.add(newRule);

        BucketLifecycleConfiguration newConfig = new BucketLifecycleConfiguration().withRules(rules);
        s3Client.setBucketLifecycleConfiguration(bucket, newConfig);
        LOGGER.info("Created lifecycle rule '{}': prefix='{}' expires in {} days",
            ruleId, fullPrefix, expirationDays);
      }
    } catch (AmazonServiceException e) {
      // Log warning but don't fail - lifecycle is a cleanup optimization, not critical
      LOGGER.warn("Failed to set lifecycle rule for prefix '{}': {}", fullPrefix, e.getMessage());
    }
  }

  /**
   * Gets a staging directory for temporary files with automatic cleanup via S3 lifecycle rules.
   *
   * <p>For S3 storage, this returns a .staging/ prefix under the base path and ensures
   * a lifecycle rule exists to auto-expire objects after 1 day. This provides a safety
   * net for orphaned staging files.
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
      if (!s3Client.doesObjectExist(sourceUri.bucket, sourceUri.key)) {
        throw new IOException("Source file does not exist in S3: " + source);
      }

      // Perform the copy
      CopyObjectRequest copyRequest =
          new CopyObjectRequest(sourceUri.bucket, sourceUri.key,
          destUri.bucket, destUri.key);

      s3Client.copyObject(copyRequest);
    } catch (AmazonServiceException e) {
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
