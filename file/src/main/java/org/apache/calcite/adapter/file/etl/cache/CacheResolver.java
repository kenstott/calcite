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
package org.apache.calcite.adapter.file.etl.cache;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Resolves raw cache entries through a three-tier hierarchy.
 *
 * <p>Lookup order:
 * <ol>
 *   <li><b>Tier 1 — Local filesystem:</b> Check {@code <localCacheDir>/<sourceKey>}</li>
 *   <li><b>Tier 2 — S3 bundle/object:</b> Look up in {@link BundleIndex}, extract
 *       via byte-range GET (bundled) or direct GET (individual object), promote to local</li>
 *   <li><b>Tier 3 — Origin:</b> Return null, caller falls through to origin fetch</li>
 * </ol>
 *
 * <p>The index is loaded lazily on first access and cached for the lifetime
 * of this resolver instance.
 *
 * @see BundleIndex
 * @see BundleIndexLoader
 * @see BundleArchiver
 */
public class CacheResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(CacheResolver.class);

  private final String localCacheDir;
  private final StorageProvider storageProvider;
  private final String schemaName;
  private volatile BundleIndex bundleIndex;
  private volatile boolean indexLoadAttempted;

  /**
   * Creates a CacheResolver.
   *
   * @param localCacheDir Local cache directory (e.g., {@code .aperio/<schema>/cache/raw})
   * @param storageProvider S3 storage provider for bundle access (may be null)
   * @param schemaName Schema name for S3 path prefix
   */
  public CacheResolver(String localCacheDir, StorageProvider storageProvider,
      String schemaName) {
    this.localCacheDir = localCacheDir;
    this.storageProvider = storageProvider;
    this.schemaName = schemaName;
    this.bundleIndex = null;
    this.indexLoadAttempted = false;
  }

  /**
   * Resolves a source key to a local file path.
   *
   * <p>Checks local cache first, then S3 bundle/objects, promoting to local on hit.
   * Returns null if not found in any tier (caller should fetch from origin).
   *
   * @param sourceKey Source key identifying the cached entry
   * @return Local file path, or null if not found
   */
  public String resolve(String sourceKey) {
    // Tier 1: local filesystem
    String localPath = localCacheDir + "/" + sourceKey;
    File localFile = new File(localPath);
    if (localFile.exists() && localFile.length() > 0) {
      return localPath;
    }

    // Tier 2: S3 bundle/object
    if (storageProvider == null) {
      return null;
    }

    BundleIndex index = getOrLoadIndex();
    if (index == null) {
      return null;
    }

    BundleEntry entry = index.get(sourceKey);
    if (entry == null) {
      return null;
    }

    try {
      byte[] content;
      if (entry.isBundled()) {
        // Byte-range GET from bundle
        String bundlePath = "bundles/" + schemaName + "/" + entry.getBundleFile();
        content =
            storageProvider.readRange(bundlePath, entry.getOffset(), entry.getLength());
        LOGGER.debug("Extracted from bundle: {} ({}B from {})",
            sourceKey, content.length, entry.getBundleFile());
      } else {
        // Direct GET of individual object
        String objectPath = "objects/" + schemaName + "/" + sourceKey;
        try (InputStream is = storageProvider.openInputStream(objectPath)) {
          java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
          byte[] buf = new byte[8192];
          int read;
          while ((read = is.read(buf)) != -1) {
            bos.write(buf, 0, read);
          }
          content = bos.toByteArray();
        }
        LOGGER.debug("Retrieved individual object: {} ({}B)", sourceKey, content.length);
      }

      // Promote to tier 1
      Path path = Paths.get(localPath);
      Files.createDirectories(path.getParent());
      Files.write(path, content);
      return localPath;

    } catch (IOException e) {
      LOGGER.warn("Failed to retrieve from S3 bundle for key '{}': {}",
          sourceKey, e.getMessage());
      return null;
    }
  }

  /** Lazily loads the bundle index from S3. */
  private BundleIndex getOrLoadIndex() {
    if (bundleIndex != null) {
      return bundleIndex;
    }
    if (indexLoadAttempted) {
      return null;
    }
    synchronized (this) {
      if (bundleIndex != null) {
        return bundleIndex;
      }
      indexLoadAttempted = true;
      try {
        BundleIndex loaded = BundleIndexLoader.load(storageProvider, schemaName);
        if (loaded.size() > 0) {
          bundleIndex = loaded;
          return bundleIndex;
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to load bundle index for '{}': {}", schemaName, e.getMessage());
      }
      return null;
    }
  }
}
