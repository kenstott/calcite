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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Archives local raw cache files to S3 as bundles.
 *
 * <p>After an ETL session completes, the archiver scans the local cache directory,
 * classifies files by size (small files are bundled into chunked .bin files, large files
 * like shapefiles are uploaded as individual S3 objects), builds a JSONL index,
 * and uploads everything to S3.
 *
 * <p>Small files are packed into chunks of up to {@link #DEFAULT_CHUNK_SIZE} bytes each.
 * Each chunk is uploaded as a separate .bin file (e.g., {@code run-xxx-001.bin},
 * {@code run-xxx-002.bin}). The index records which chunk file and byte offset
 * each entry lives in, so the {@link CacheResolver} can retrieve any entry via
 * byte-range GET on the correct chunk.
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

  /** Default chunk size for bundled files: 256 MB. */
  public static final long DEFAULT_CHUNK_SIZE = 256L * 1024 * 1024;

  private BundleArchiver() {
    // utility class
  }

  /**
   * Checks whether a complete archive exists for the given schema.
   *
   * <p>An archive is considered complete if at least one index file
   * ({@code .idx.jsonl} or {@code .idx-NNN.jsonl}) exists in the
   * schema's bundle prefix. A partial archive (chunks uploaded but
   * no index) is considered incomplete.
   *
   * @param storageProvider S3 storage provider
   * @param schemaName Schema name for S3 path prefix
   * @return true if a complete archive exists
   */
  public static boolean hasCompleteArchive(StorageProvider storageProvider, String schemaName) {
    String prefix = "bundles/" + schemaName + "/";
    try {
      List<StorageProvider.FileEntry> files = storageProvider.listFiles(prefix, false);
      for (StorageProvider.FileEntry entry : files) {
        if (entry.getPath().endsWith(".jsonl") && entry.getPath().contains(".idx")) {
          return true;
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to check archive status for '{}': {}", schemaName, e.getMessage());
    }
    return false;
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

    try {
      // Bundle small files into chunks
      if (!smallFiles.isEmpty()) {
        bundleSmallFiles(smallFiles, cacheRoot, storageProvider,
            bundlePrefix, bundleId, DEFAULT_CHUNK_SIZE, index);
      }

      // Upload large files individually
      if (!largeFiles.isEmpty()) {
        uploadLargeFiles(largeFiles, cacheRoot, storageProvider,
            objectPrefix, index);
      }

      // Write index in chunks to avoid building a multi-GB string
      String indexPrefix = bundlePrefix + bundleId + ".idx";
      int indexParts = writeIndexChunked(index, storageProvider, indexPrefix);

      LOGGER.info("Archive complete: {} bundled + {} individual objects, index in {} parts",
          smallFiles.size(), largeFiles.size(), indexParts);

    } catch (Exception e) {
      LOGGER.warn("Failed to archive raw cache (non-fatal): {}", e.getMessage());
    }
  }

  private static void bundleSmallFiles(List<File> files, File cacheRoot,
      StorageProvider storageProvider, String bundlePrefix, String bundleId,
      long chunkSize, BundleIndex index) throws IOException {
    ByteArrayOutputStream chunkData = new ByteArrayOutputStream();
    long offsetInChunk = 0;
    int chunkNumber = 1;
    int filesInChunk = 0;
    String chunkFileName = formatChunkName(bundleId, chunkNumber);

    for (File f : files) {
      String sourceKey = relativize(cacheRoot, f);
      byte[] content = Files.readAllBytes(f.toPath());

      // Flush current chunk if adding this file would exceed the limit
      if (offsetInChunk > 0 && offsetInChunk + content.length > chunkSize) {
        flushChunk(storageProvider, bundlePrefix, chunkFileName,
            chunkData, filesInChunk, offsetInChunk);
        chunkData.reset();
        offsetInChunk = 0;
        filesInChunk = 0;
        chunkNumber++;
        chunkFileName = formatChunkName(bundleId, chunkNumber);
      }

      chunkData.write(content);
      index.put(sourceKey,
          BundleEntry.bundled(chunkFileName, offsetInChunk, content.length,
              f.lastModified() / 1000));
      offsetInChunk += content.length;
      filesInChunk++;
    }

    // Flush final chunk
    if (offsetInChunk > 0) {
      flushChunk(storageProvider, bundlePrefix, chunkFileName,
          chunkData, filesInChunk, offsetInChunk);
    }

    LOGGER.info("Bundled {} files into {} chunks", files.size(), chunkNumber);
  }

  private static void flushChunk(StorageProvider storageProvider, String bundlePrefix,
      String chunkFileName, ByteArrayOutputStream chunkData, int fileCount,
      long byteCount) throws IOException {
    String chunkPath = bundlePrefix + chunkFileName;
    storageProvider.writeFile(chunkPath, chunkData.toByteArray());
    LOGGER.info("Uploaded chunk: {} ({} files, {} bytes)", chunkPath, fileCount, byteCount);
  }

  private static String formatChunkName(String bundleId, int chunkNumber) {
    return bundleId + "-" + String.format("%03d", chunkNumber) + ".bin";
  }

  /**
   * Writes the index in chunked parts to avoid building a multi-GB string.
   * Each part is uploaded as a separate .idx-NNN.jsonl file.
   * For small indexes (under 50K entries), writes a single .idx.jsonl file.
   */
  private static int writeIndexChunked(BundleIndex index, StorageProvider storageProvider,
      String indexPrefix) throws IOException {
    int entriesPerPart = 50000;
    int totalEntries = index.size();

    if (totalEntries <= entriesPerPart) {
      // Small index — single file, use original method
      String content = index.toJsonl();
      storageProvider.writeFile(indexPrefix + ".jsonl",
          content.getBytes(StandardCharsets.UTF_8));
      LOGGER.info("Uploaded index: {} ({} entries)", indexPrefix + ".jsonl", totalEntries);
      return 1;
    }

    // Large index — write in parts
    int partNumber = 0;
    int entriesInPart = 0;
    StringBuilder sb = new StringBuilder();

    for (java.util.Map.Entry<String, BundleEntry> entry : index.getEntries().entrySet()) {
      String key = entry.getKey();
      BundleEntry be = entry.getValue();
      sb.append("{\"key\":\"").append(escapeJson(key)).append("\"");
      if (be.isBundled()) {
        sb.append(",\"bundleFile\":\"").append(escapeJson(be.getBundleFile())).append("\"");
        sb.append(",\"offset\":").append(be.getOffset());
      } else {
        sb.append(",\"storage\":\"object\"");
      }
      sb.append(",\"length\":").append(be.getLength());
      sb.append(",\"ts\":").append(be.getTimestamp());
      sb.append("}\n");
      entriesInPart++;

      if (entriesInPart >= entriesPerPart) {
        partNumber++;
        String partPath = indexPrefix + "-" + String.format("%03d", partNumber) + ".jsonl";
        storageProvider.writeFile(partPath, sb.toString().getBytes(StandardCharsets.UTF_8));
        LOGGER.info("Uploaded index part: {} ({} entries)", partPath, entriesInPart);
        sb.setLength(0);
        entriesInPart = 0;
      }
    }

    // Flush remaining
    if (entriesInPart > 0) {
      partNumber++;
      String partPath = indexPrefix + "-" + String.format("%03d", partNumber) + ".jsonl";
      storageProvider.writeFile(partPath, sb.toString().getBytes(StandardCharsets.UTF_8));
      LOGGER.info("Uploaded index part: {} ({} entries)", partPath, entriesInPart);
    }

    return partNumber;
  }

  private static String escapeJson(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static void uploadLargeFiles(List<File> files, File cacheRoot,
      StorageProvider storageProvider, String objectPrefix,
      BundleIndex index) throws IOException {
    for (File f : files) {
      String sourceKey = relativize(cacheRoot, f);
      String objectPath = objectPrefix + sourceKey;
      try (InputStream in = Files.newInputStream(f.toPath())) {
        long length = f.length();
        storageProvider.writeFile(objectPath, in);
        index.put(sourceKey,
            BundleEntry.individual(length, f.lastModified() / 1000));
        LOGGER.info("Uploaded individual object: {} ({} bytes)", objectPath, length);
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
