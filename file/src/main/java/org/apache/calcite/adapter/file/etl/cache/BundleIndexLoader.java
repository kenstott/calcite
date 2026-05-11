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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Loads and merges bundle index files from S3 into a {@link BundleIndex}.
 *
 * <p>Lists all {@code .idx.jsonl} files under the schema's bundle prefix,
 * sorts them by name (which sorts by timestamp since bundle IDs are
 * {@code run-YYYYMMDDTHHmm}), and merges them in order. Later entries
 * override earlier ones for the same source key.
 *
 * @see BundleIndex
 * @see CacheResolver
 */
public class BundleIndexLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(BundleIndexLoader.class);

  private BundleIndexLoader() {
    // utility class
  }

  /**
   * Loads all bundle indexes for a schema from S3.
   *
   * @param storageProvider S3 storage provider
   * @param schemaName Schema name (e.g., "census")
   * @return Merged BundleIndex, or empty index if none found
   */
  public static BundleIndex load(StorageProvider storageProvider, String schemaName) {
    BundleIndex index = new BundleIndex();
    String prefix = "bundles/" + schemaName + "/";

    try {
      // List all index files
      List<StorageProvider.FileEntry> files = storageProvider.listFiles(prefix, false);
      List<String> indexFiles = new ArrayList<String>();
      for (StorageProvider.FileEntry entry : files) {
        if (entry.getPath().endsWith(".jsonl")
            && entry.getPath().contains(".idx")) {
          indexFiles.add(entry.getPath());
        }
      }

      if (indexFiles.isEmpty()) {
        LOGGER.debug("No bundle indexes found for schema: {}", schemaName);
        return index;
      }

      // Sort by name (timestamps sort lexicographically)
      Collections.sort(indexFiles);

      // Merge each index file
      for (String indexFile : indexFiles) {
        try {
          String content = readAsString(storageProvider, indexFile);
          // Derive bundle filename from index filename:
          // run-20260310T1423.idx.jsonl -> run-20260310T1423.bin
          String fileName = indexFile;
          int lastSlash = fileName.lastIndexOf('/');
          if (lastSlash >= 0) {
            fileName = fileName.substring(lastSlash + 1);
          }
          // Strip index suffix: .idx.jsonl or .idx-NNN.jsonl → .bin
          String bundleFile = fileName.replaceAll("\\.idx(-\\d+)?\\.jsonl$", ".bin");
          index.mergeFromJsonl(content, bundleFile);
          LOGGER.debug("Merged bundle index: {} ({} total entries)", indexFile, index.size());
        } catch (IOException e) {
          LOGGER.warn("Failed to load bundle index {}: {}", indexFile, e.getMessage());
        }
      }

      LOGGER.info("Loaded {} bundle index entries for schema '{}'", index.size(), schemaName);

    } catch (IOException e) {
      LOGGER.warn("Failed to list bundle indexes for schema '{}': {}", schemaName, e.getMessage());
    }

    return index;
  }

  private static String readAsString(StorageProvider storageProvider, String path)
      throws IOException {
    try (InputStream is = storageProvider.openInputStream(path);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(is, StandardCharsets.UTF_8))) {
      StringBuilder sb = new StringBuilder();
      char[] buf = new char[8192];
      int read;
      while ((read = reader.read(buf)) != -1) {
        sb.append(buf, 0, read);
      }
      return sb.toString();
    }
  }
}
