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

import org.apache.calcite.adapter.file.iceberg.IcebergStorageProvider;

import software.amazon.awssdk.services.s3.S3Client;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating storage provider instances based on URL or storage type.
 */
public class StorageProviderFactory {

  private static final Map<String, StorageProvider> PROVIDER_CACHE = new ConcurrentHashMap<>();

  /**
   * Creates a storage provider based on the URL scheme.
   *
   * @param url The URL to access
   * @return Appropriate storage provider
   */
  public static StorageProvider createFromUrl(String url) {
    if (url == null || url.isEmpty()) {
      return new LocalFileStorageProvider();
    }

    // Extract scheme
    int schemeEnd = url.indexOf("://");
    if (schemeEnd < 0) {
      // No scheme, assume local file
      return getCachedProvider("local", LocalFileStorageProvider::new);
    }

    String scheme = url.substring(0, schemeEnd).toLowerCase(Locale.ROOT);

    switch (scheme) {
      case "file":
        return getCachedProvider("local", LocalFileStorageProvider::new);

      case "http":
      case "https":
        return getCachedProvider("http", HttpStorageProvider::new);

      case "s3":
        throw new IllegalArgumentException(
            "S3 storage requires explicit credentials via model.json storageConfig. "
            + "Cannot create S3 provider from URL alone.");

      case "hdfs":
        return getCachedProvider("hdfs", () -> {
          try {
            return new HDFSStorageProvider();
          } catch (Exception e) {
            throw new RuntimeException("Failed to create HDFS storage provider", e);
          }
        });

      case "ftp":
      case "ftps":
        return getCachedProvider("ftp", FtpStorageProvider::new);

      case "sftp":
        return getCachedProvider("sftp", SftpStorageProvider::new);

      case "iceberg":
        // Iceberg URLs would be like: iceberg://catalog/namespace/table
        // But typically Iceberg is accessed via storageType, not URL
        return new IcebergStorageProvider(new java.util.HashMap<>());

      default:
        throw new IllegalArgumentException("Unsupported URL scheme: " + scheme);
    }
  }

  /**
   * Creates a storage provider based on explicit storage type.
   *
   * @param storageType The storage type (local, s3, http, ftp, sharepoint)
   * @param config Configuration parameters for the provider
   * @return Appropriate storage provider
   */
  public static StorageProvider createFromType(String storageType, Map<String, Object> config) {
    if (storageType == null || storageType.isEmpty()) {
      storageType = "local";
    }

    switch (storageType.toLowerCase(Locale.ROOT)) {
      case "local":
      case "file":
        return getCachedProvider("local", LocalFileStorageProvider::new);

      case "http":
      case "https":
        if (config != null && (config.containsKey("method") || config.containsKey("body")
            || config.containsKey("headers") || config.containsKey("mimeType"))) {
          // Create configured HTTP provider
          HttpConfig httpConfig = HttpConfig.fromMap(config);
          return httpConfig.createStorageProvider();
        }
        return getCachedProvider("http", HttpStorageProvider::new);

      case "s3":
        if (config != null && config.containsKey("s3Client")) {
          // Custom S3 client provided
          return new S3StorageProvider((S3Client) config.get("s3Client"));
        } else if (config != null && !config.isEmpty()) {
          // Configuration map provided with credentials/region
          return new S3StorageProvider(config);
        }
        throw new IllegalArgumentException(
            "S3 storage requires explicit credentials via model.json storageConfig. "
            + "Provide 'accessKeyId' and 'secretAccessKey' in the config map.");

      case "hdfs":
        if (config != null && config.containsKey("hadoopConfig")) {
          // Custom Hadoop configuration provided
          try {
            org.apache.hadoop.conf.Configuration hadoopConfig =
                (org.apache.hadoop.conf.Configuration) config.get("hadoopConfig");
            return new HDFSStorageProvider(hadoopConfig);
          } catch (Exception e) {
            throw new RuntimeException("Failed to create HDFS storage provider with custom config", e);
          }
        }
        return getCachedProvider("hdfs", () -> {
          try {
            return new HDFSStorageProvider();
          } catch (Exception e) {
            throw new RuntimeException("Failed to create HDFS storage provider", e);
          }
        });

      case "ftp":
      case "ftps":
        return getCachedProvider("ftp", FtpStorageProvider::new);

      case "sftp":
        if (config != null) {
          String username = (String) config.get("username");
          String password = (String) config.get("password");
          String privateKeyPath = (String) config.get("privateKeyPath");
          Boolean strictHostKey = (Boolean) config.get("strictHostKeyChecking");

          return new SftpStorageProvider(
              username,
              password,
              privateKeyPath,
              strictHostKey != null ? strictHostKey : false);
        }
        return getCachedProvider("sftp", SftpStorageProvider::new);

      case "sharepoint":
        if (config == null || !config.containsKey("siteUrl")) {
          throw new IllegalArgumentException("SharePoint storage requires 'siteUrl' in config");
        }

        String siteUrl = (String) config.get("siteUrl");

        // Check for different authentication methods
        SharePointTokenManager tokenManager = null;

        if (config.containsKey("accessToken")) {
          // Static token (backward compatibility)
          String accessToken = (String) config.get("accessToken");
          tokenManager = new SharePointTokenManager(accessToken, siteUrl);

        } else if (config.containsKey("clientId") && config.containsKey("clientSecret")) {
          // Client credentials flow (app-only)
          String tenantId = (String) config.get("tenantId");
          String clientId = (String) config.get("clientId");
          String clientSecret = (String) config.get("clientSecret");

          if (tenantId == null) {
            throw new IllegalArgumentException(
                "SharePoint client credentials auth requires 'tenantId'");
          }

          tokenManager = new SharePointTokenManager(tenantId, clientId, clientSecret, siteUrl);

        } else if (config.containsKey("refreshToken")) {
          // Refresh token flow
          String tenantId = (String) config.get("tenantId");
          String clientId = (String) config.get("clientId");
          String refreshToken = (String) config.get("refreshToken");

          if (tenantId == null || clientId == null) {
            throw new IllegalArgumentException(
                "SharePoint refresh token auth requires 'tenantId' and 'clientId'");
          }

          tokenManager = new SharePointTokenManager(tenantId, clientId, refreshToken, siteUrl, true);

        } else if (config.containsKey("certificatePath") && config.containsKey("certificatePassword")) {
          // Certificate authentication - still needs tenant and client IDs
          String tenantId = (String) config.get("tenantId");
          String clientId = (String) config.get("clientId");

          if (tenantId == null || clientId == null) {
            throw new IllegalArgumentException(
                "Certificate authentication requires 'tenantId' and 'clientId'");
          }
          // Create a dummy token manager - certificate handling is done below
          tokenManager = new SharePointTokenManager(tenantId, clientId, "CERTIFICATE", siteUrl);
        } else {
          throw new IllegalArgumentException(
              "SharePoint storage requires one of: 'accessToken', " +
              "'clientId+clientSecret', 'refreshToken', or 'certificatePath+certificatePassword' for authentication");
        }

        // Determine which API to use
        boolean useGraphApi = config.containsKey("useGraphApi")
            && Boolean.TRUE.equals(config.get("useGraphApi"));
        boolean useLegacyAuth = config.containsKey("useLegacyAuth")
            && Boolean.TRUE.equals(config.get("useLegacyAuth"));

        if (useGraphApi) {
          // Use Microsoft Graph API
          MicrosoftGraphTokenManager graphTokenManager;
          if (tokenManager.clientSecret != null) {
            graphTokenManager =
                new MicrosoftGraphTokenManager(tokenManager.tenantId, tokenManager.clientId,
                tokenManager.clientSecret, tokenManager.getSiteUrl());
          } else if (tokenManager.refreshToken != null) {
            graphTokenManager =
                new MicrosoftGraphTokenManager(tokenManager.tenantId, tokenManager.clientId,
                tokenManager.refreshToken, tokenManager.getSiteUrl(), true);
          } else {
            // Static token
            graphTokenManager =
                new MicrosoftGraphTokenManager(tokenManager.accessToken, tokenManager.getSiteUrl());
          }
          return new MicrosoftGraphStorageProvider(graphTokenManager);
        } else if (useLegacyAuth && tokenManager.clientSecret != null) {
          // Use legacy SharePoint authentication (works with client secret)
          // Note: Legacy auth uses realm instead of tenantId
          String realm = config.containsKey("realm") ? (String) config.get("realm") : null;
          SharePointLegacyTokenManager legacyTokenManager =
              realm != null
              ? new SharePointLegacyTokenManager(tokenManager.clientId,
                  tokenManager.clientSecret, tokenManager.getSiteUrl(), realm)
              : new SharePointLegacyTokenManager(tokenManager.clientId,
                  tokenManager.clientSecret, tokenManager.getSiteUrl());
          return new SharePointRestStorageProvider(legacyTokenManager);
        } else {
          // Use SharePoint REST API with modern auth
          // Check for certificate authentication
          if (config.containsKey("certificatePath") && config.containsKey("certificatePassword")) {
            try {
              String certificatePath = (String) config.get("certificatePath");
              String certificatePassword = (String) config.get("certificatePassword");
              SharePointCertificateTokenManager certTokenManager =
                  new SharePointCertificateTokenManager(tokenManager.tenantId, tokenManager.clientId,
                      certificatePath, certificatePassword, tokenManager.getSiteUrl());
              return new SharePointRestStorageProvider(certTokenManager);
            } catch (Exception e) {
              throw new RuntimeException("Failed to initialize certificate authentication", e);
            }
          }

          // Fall back to client secret (will fail with REST API but might work with Graph)
          SharePointRestTokenManager restTokenManager;
          if (tokenManager.clientSecret != null) {
            restTokenManager =
                new SharePointRestTokenManager(tokenManager.tenantId, tokenManager.clientId,
                tokenManager.clientSecret, tokenManager.getSiteUrl());
          } else if (tokenManager.refreshToken != null) {
            restTokenManager =
                new SharePointRestTokenManager(tokenManager.tenantId, tokenManager.clientId,
                tokenManager.refreshToken, tokenManager.getSiteUrl(), true);
          } else {
            // Static token
            restTokenManager =
                new SharePointRestTokenManager(tokenManager.accessToken, tokenManager.getSiteUrl());
          }
          return new SharePointRestStorageProvider(restTokenManager);
        }

      case "iceberg":
        // Create Iceberg storage provider with configuration
        return new IcebergStorageProvider(config != null ? config : new java.util.HashMap<>());

      default:
        throw new IllegalArgumentException("Unsupported storage type: " + storageType);
    }
  }

  /**
   * Normalizes a storage path for Hadoop/Iceberg, which requires the {@code s3a://} scheme.
   * Converts {@code s3://bucket/...} to {@code s3a://bucket/...}; all other paths pass through.
   */
  public static String normalizeForHadoop(String path) {
    if (path != null && path.startsWith("s3://")) {
      return "s3a://" + path.substring(5);
    }
    return path;
  }

  /**
   * Returns the govdata cache root directory from {@code GOVDATA_CACHE_DIR}, falling back to
   * the JVM temp directory when the variable is unset or empty.
   */
  public static String getGovDataCacheDir() {
    String dir = System.getenv("GOVDATA_CACHE_DIR");
    return (dir != null && !dir.isEmpty()) ? dir : System.getProperty("java.io.tmpdir");
  }

  /**
   * Creates a storage provider for the govdata cache directory.
   *
   * <p>When {@code GOVDATA_CACHE_DIR} points to an {@code s3://} URL, credentials are read
   * from process environment ({@code AWS_ACCESS_KEY_ID}, {@code AWS_SECRET_ACCESS_KEY},
   * {@code AWS_ENDPOINT_OVERRIDE}, {@code AWS_REGION}) and an S3 provider is built directly
   * — the govdata cache is process-owned, so model.json storageConfig is not in scope.
   * Other schemes delegate to {@link #createFromUrl(String)}.
   */
  public static StorageProvider createForGovDataCache() {
    String cacheDir = getGovDataCacheDir();
    if (cacheDir != null && cacheDir.startsWith("s3://")) {
      return getCachedProvider("govdata-s3:" + cacheDir, () -> {
        Map<String, Object> s3Config = new HashMap<>();
        String keyId = System.getenv("AWS_ACCESS_KEY_ID");
        String secret = System.getenv("AWS_SECRET_ACCESS_KEY");
        String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
        String region = System.getenv("AWS_REGION");
        if (keyId != null && !keyId.isEmpty()) {
          s3Config.put("accessKeyId", keyId);
        }
        if (secret != null && !secret.isEmpty()) {
          s3Config.put("secretAccessKey", secret);
        }
        if (endpoint != null && !endpoint.isEmpty()) {
          s3Config.put("endpoint", endpoint);
        }
        s3Config.put("region", (region != null && !region.isEmpty()) ? region : "auto");
        s3Config.put("directory", cacheDir);
        return createFromType("s3", s3Config);
      });
    }
    return createFromUrl(cacheDir);
  }

  /**
   * Clears the provider cache. Useful for testing.
   */
  public static void clearCache() {
    PROVIDER_CACHE.clear();
  }

  private static StorageProvider getCachedProvider(String key,
                                                   java.util.function.Supplier<StorageProvider> creator) {
    return PROVIDER_CACHE.computeIfAbsent(key, k -> creator.get());
  }

  private StorageProviderFactory() {
    // Utility class
  }
}
