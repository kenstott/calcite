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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Archives local raw cache files to S3 as bundles.
 *
 * <p>After an ETL session completes, the archiver scans the local cache directory,
 * classifies files by size (small files are bundled into a single .bin, large files
 * like shapefiles are uploaded as individual S3 objects), builds a JSONL index,
 * and uploads everything to S3.
 *
 * <p>This is a fire-and-forget operation — failures are logged but do not affect
 * ETL results. The local cache remains intact regardless.
 *
 * @see BundleIndex
 * @see BundleEntry
 * @see CacheResolver
 */
public class BundleArchiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(BundleArchiver.class);

  /** Default size threshold: files larger than this are stored individually. */
  public static final long DEFAULT_SIZE_THRESHOLD = 1L * 1024 * 1024; // 1MB

  private BundleArchiver() {
    // utility class
  }

  /**
   * Archives the local raw cache to S3.
   *
   * @param localCacheDir Local cache directory (e.g., {@code .aperio/<schema>/cache/raw})
   * @param storageProvider S3 storage provider for uploading
   * @param schemaName Schema name for S3 path prefix
   * @param bundleId Bundle identifier (e.g., {@code run-20260310T1423})
   */
  public static void archive(String localCacheDir, StorageProvider storageProvider,
      String schemaName, String bundleId) {
    archive(localCacheDir, storageProvider, schemaName, bundleId, DEFAULT_SIZE_THRESHOLD);
  }

  /**
   * Archives the local raw cache to S3 with a custom size threshold.
   *
   * @param localCacheDir Local cache directory
   * @param storageProvider S3 storage provider for uploading
   * @param schemaName Schema name for S3 path prefix
   * @param bundleId Bundle identifier
   * @param sizeThreshold Files larger than this (bytes) are stored as individual objects
   */
  public static void archive(String localCacheDir, StorageProvider storageProvider,
      String schemaName, String bundleId, long sizeThreshold) {
    File cacheRoot = new File(localCacheDir);
    if (!cacheRoot.exists() || !cacheRoot.isDirectory()) {
      LOGGER.debug("No local cache directory to archive: {}", localCacheDir);
      return;
    }

    // Collect all files
    List<File> allFiles = new ArrayList<File>();
    collectFiles(cacheRoot, allFiles);
    if (allFiles.isEmpty()) {
      LOGGER.debug("No files to archive in {}", localCacheDir);
      return;
    }

    LOGGER.info("Archiving {} raw cache files from {}", allFiles.size(), localCacheDir);

    // Classify into small (bundle) and large (individual)
    List<File> smallFiles = new ArrayList<File>();
    List<File> largeFiles = new ArrayList<File>();
    for (File f : allFiles) {
      if (f.length() > sizeThreshold) {
        largeFiles.add(f);
      } else {
        smallFiles.add(f);
      }
    }

    String bundlePrefix = "bundles/" + schemaName + "/";
    String objectPrefix = "objects/" + schemaName + "/";
    BundleIndex index = new BundleIndex();
    String bundleFileName = bundleId + ".bin";

    try {
      // Bundle small files
      if (!smallFiles.isEmpty()) {
        bundleSmallFiles(smallFiles, cacheRoot, storageProvider,
            bundlePrefix, bundleFileName, index);
      }

      // Upload large files individually
      if (!largeFiles.isEmpty()) {
        uploadLargeFiles(largeFiles, cacheRoot, storageProvider,
            objectPrefix, index);
      }

      // Write index
      String indexContent = index.toJsonl();
      String indexPath = bundlePrefix + bundleId + ".idx.jsonl";
      storageProvider.writeFile(indexPath, indexContent.getBytes(StandardCharsets.UTF_8));

      LOGGER.info("Archive complete: {} bundled + {} individual objects, index at {}",
          smallFiles.size(), largeFiles.size(), indexPath);

    } catch (Exception e) {
      LOGGER.warn("Failed to archive raw cache (non-fatal): {}", e.getMessage());
    }
  }

  private static void bundleSmallFiles(List<File> files, File cacheRoot,
      StorageProvider storageProvider, String bundlePrefix, String bundleFileName,
      BundleIndex index) throws IOException {
    ByteArrayOutputStream bundleData = new ByteArrayOutputStream();
    long offset = 0;

    for (File f : files) {
      String sourceKey = relativize(cacheRoot, f);
      byte[] content = Files.readAllBytes(f.toPath());
      bundleData.write(content);

      index.put(sourceKey,
          BundleEntry.bundled(bundleFileName, offset, content.length,
              f.lastModified() / 1000));
      offset += content.length;
    }

    // Upload bundle
    String bundlePath = bundlePrefix + bundleFileName;
    storageProvider.writeFile(bundlePath, bundleData.toByteArray());
    LOGGER.info("Uploaded bundle: {} ({} files, {} bytes)", bundlePath, files.size(), offset);
  }

  private static void uploadLargeFiles(List<File> files, File cacheRoot,
      StorageProvider storageProvider, String objectPrefix,
      BundleIndex index) throws IOException {
    for (File f : files) {
      String sourceKey = relativize(cacheRoot, f);
      String objectPath = objectPrefix + sourceKey;
      try {
        byte[] content = Files.readAllBytes(f.toPath());
        storageProvider.writeFile(objectPath, content);
        index.put(sourceKey,
            BundleEntry.individual(content.length, f.lastModified() / 1000));
        LOGGER.info("Uploaded individual object: {} ({} bytes)", objectPath, content.length);
      } catch (IOException e) {
        LOGGER.warn("Failed to upload large file {} (skipping): {}", objectPath, e.getMessage());
      }
    }
  }

  /** Computes the relative path of a file within the cache root. */
  private static String relativize(File root, File file) {
    String rootPath = root.getAbsolutePath();
    String filePath = file.getAbsolutePath();
    if (filePath.startsWith(rootPath)) {
      String rel = filePath.substring(rootPath.length());
      if (rel.startsWith("/") || rel.startsWith(File.separator)) {
        rel = rel.substring(1);
      }
      return rel;
    }
    return file.getName();
  }

  /** Recursively collects all files under a directory. */
  private static void collectFiles(File dir, List<File> result) {
    File[] children = dir.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isDirectory()) {
        collectFiles(child, result);
      } else if (child.isFile() && child.length() > 0) {
        result.add(child);
      }
    }
  }
}
