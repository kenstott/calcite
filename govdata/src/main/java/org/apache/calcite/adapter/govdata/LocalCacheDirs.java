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
// storage-provider-guard:allow-scheme - storage-dispatch layer: inspecting a URI scheme here is the legitimate job (provider dispatch / S3 path handling / endpoint SSL config), not a consumer branching local-vs-remote.
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import java.io.File;

/**
 * Resolves a guaranteed local-filesystem directory for artifacts that cannot live
 * in object storage.
 *
 * <p>The govdata cache directory ({@code GOVDATA_CACHE_DIR}) is frequently an
 * object-store URI such as {@code s3://govdata-raw-v1}. Most cache traffic flows
 * through the {@code StorageProvider} abstraction, which understands such URIs.
 * A few artifacts, however, are <em>inherently local</em> and must be backed by a
 * real path on the local filesystem:
 * <ul>
 *   <li>DuckDB database files (DuckDB opens its catalog as a local file);</li>
 *   <li>downloaded archives that must be unzipped / memory-mapped (e.g. the Stooq
 *       bulk price zip queried via DuckDB's zipfs extension).</li>
 * </ul>
 *
 * <p>Handing an {@code s3://…} string to {@link File} or {@code Paths.get} does not
 * fail — it silently produces a relative path, creating a literal {@code s3:/…}
 * directory tree under the process working directory. This helper maps any
 * scheme-bearing URI to a deterministic location under the JVM temp directory
 * ({@code java.io.tmpdir}), preserving the path tail so caches remain stable across
 * runs. Paths that are already local are returned unchanged.
 */
public final class LocalCacheDirs {

  /** Subdirectory under {@code java.io.tmpdir} that roots all derived local caches. */
  private static final String ROOT = "govdata-cache";

  private LocalCacheDirs() {
  }

  /**
   * Maps a possibly-remote cache directory to a local-filesystem path.
   *
   * @param cacheDir configured cache directory; may be a local path, an object-store
   *                 URI ({@code s3://}, {@code r2://}, {@code gs://}, {@code http(s)://}),
   *                 or null/empty
   * @return an absolute local path safe for {@link File}/{@code Paths.get}; never null
   */
  public static String toLocal(String cacheDir) {
    File root = new File(System.getProperty("java.io.tmpdir"), ROOT);
    if (cacheDir == null || cacheDir.isEmpty()) {
      return root.getAbsolutePath();
    }
    int scheme = cacheDir.indexOf("://");
    if (scheme < 0) {
      return cacheDir;  // already a local filesystem path
    }
    // Preserve the path after the scheme (bucket + key) so the derived local cache
    // is deterministic and per-bucket distinct: s3://b/sec/sec -> <tmp>/govdata-cache/b/sec/sec
    String tail = cacheDir.substring(scheme + 3).replace('\\', '/');
    return new File(root, tail).getAbsolutePath();
  }
}
