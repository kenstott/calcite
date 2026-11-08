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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Durable per-request store of the responses a forced reprocess has already fetched, so a run that
 * is stopped part-way (a daily API quota, a kill) replays the finished requests from storage and
 * spends calls only on the rest. It is handed to a {@link StreamingResponseTransformer} through
 * {@link RequestContext#getResumeStore()} for sources whose request budget is smaller than one
 * full run.
 *
 * <p>An entry is keyed by the request, so a request whose URL changed (a redefined query) never
 * reads an older response. An entry is visible only once its completion marker is written, so a
 * body that was being written when the process died is never read. The pipeline clears the whole
 * store after the table's commit succeeds, which scopes the stored responses to one forced run:
 * the next forced run starts empty.
 */
public final class FetchResumeStore {

  private static final String BODY = ".body";
  private static final String DONE = ".done";

  private final StorageProvider storage;
  private final String basePath;

  /**
   * @param storage provider the entries are written through
   * @param basePath directory (in {@code storage}) that holds this table's entries
   */
  public FetchResumeStore(StorageProvider storage, String basePath) {
    if (storage == null || basePath == null || basePath.isEmpty()) {
      throw new IllegalArgumentException("FetchResumeStore needs a storage provider and a path");
    }
    this.storage = storage;
    this.basePath = basePath.endsWith("/") ? basePath.substring(0, basePath.length() - 1) : basePath;
  }

  /** Returns the storage key for a request URL. */
  public static String keyFor(String url) {
    try {
      java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(url.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private String path(String key, String suffix) {
    return basePath + "/" + key + suffix;
  }

  /** True when the response for {@code key} was stored completely. */
  public boolean has(String key) throws IOException {
    return storage.exists(path(key, DONE));
  }

  /** Opens the stored response for {@code key}; the caller closes the stream. */
  public InputStream open(String key) throws IOException {
    return storage.openInputStream(path(key, BODY));
  }

  /** Stores the response file under {@code key}, replacing any earlier entry. */
  public void put(String key, Path file) throws IOException {
    storage.delete(path(key, DONE));
    // local temp spool owned by the caller, streamed up through the provider (not a storage path)
    try (InputStream in = Files.newInputStream(file)) {
      storage.writeFile(path(key, BODY), in);
    }
    storage.writeFile(path(key, DONE), new byte[] {'1'});
  }

  /** Stores {@code content} under {@code key}, replacing any earlier entry. */
  public void put(String key, byte[] content) throws IOException {
    storage.delete(path(key, DONE));
    storage.writeFile(path(key, BODY), content);
    storage.writeFile(path(key, DONE), new byte[] {'1'});
  }

  /** Removes every entry of this store. */
  public void clear() throws IOException {
    if (!storage.exists(basePath)) {
      return;
    }
    List<String> paths = new ArrayList<String>();
    for (StorageProvider.FileEntry entry : storage.listFiles(basePath, true)) {
      if (!entry.isDirectory()) {
        paths.add(entry.getPath());
      }
    }
    // the completion marker goes first so a clear cut short never leaves a marker over a missing body
    Collections.sort(paths, Collections.reverseOrder());
    storage.deleteBatch(paths);
  }
}
