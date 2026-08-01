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
// storage-provider-guard:ignore-file
// Every filesystem op here targets the LOCAL cache directory, which is by definition a
// machine-local artifact — the whole point is to avoid the StorageProvider/object-store round
// trip. Remote reads in this file go through the delegate FileIO, never raw filesystem calls.
package org.apache.calcite.adapter.file.iceberg;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.Map;

/**
 * A local, on-disk cache for immutable Iceberg metadata objects.
 *
 * <p>This is a hand-rolled equivalent of DuckDB's {@code cache_httpfs} for the JAVA read path.
 * DuckDB's cache cannot help here: Calcite resolves table metadata through {@link S3FileIOTables}
 * and Iceberg's {@code S3FileIO}, so those reads never reach DuckDB and never enter its cache.
 * Measured cost of that gap: a cold metadata pull over 24 schemas took ~9.2s, essentially all of
 * it per-table object-store round trips; the same pull warm (in-process memo) takes ~16ms.
 *
 * <h2>What is cached, and why only that</h2>
 *
 * <p>ONLY {@code metadata/v{N}.metadata.json} is cached. That object is immutable: Iceberg writes
 * a new file with an incremented N on every commit and never rewrites an existing one, so a hit
 * cannot be stale — the bytes for a given path are the same forever.
 *
 * <p>{@code metadata/version-hint.text} is deliberately NOT cached. It is mutable, and it is the
 * pointer that says which N is current; caching it would let a connection pin an old snapshot and
 * silently read stale data. {@code DuckDBJdbcSchemaFactory.purgeStaleIcebergMetadataFromCache}
 * exists because exactly that leaked into the httpfs cache before. Reading the hint live costs one
 * small GET per table and keeps snapshot selection always correct.
 *
 * <p>Manifests and data files are delegated untouched — the paths inside the metadata are
 * {@code s3a://} and must continue to resolve through the real {@link FileIO}.
 *
 * <p>Cache directory resolution, mirroring the httpfs cache:
 * <ol>
 *   <li>System property {@code iceberg.metadata.cache.directory}</li>
 *   <li>{@code {user.home}/.aperio/.iceberg_metadata_cache}</li>
 * </ol>
 *
 * <p>A miss falls through to the object store and populates the cache, so this is purely an
 * accelerator: an absent or partial cache degrades to today's behaviour rather than failing.
 */
public final class IcebergMetadataCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergMetadataCache.class);

  /** Set false via {@code iceberg.metadata.cache.enabled=false} to bypass entirely. */
  private static final boolean ENABLED =
      !"false".equalsIgnoreCase(System.getProperty("iceberg.metadata.cache.enabled"));

  private IcebergMetadataCache() {
  }

  /** Returns the cache directory, creating it on first use. */
  static File cacheDir() {
    String configured = System.getProperty("iceberg.metadata.cache.directory");
    File dir;
    if (configured != null && !configured.isEmpty()) {
      dir = new File(configured);
    } else {
      dir = new File(System.getProperty("user.home")
          + File.separator + ".aperio" + File.separator + ".iceberg_metadata_cache");
    }
    if (!dir.exists()) {
      dir.mkdirs();
    }
    return dir;
  }

  /**
   * True for objects that are immutable once written and therefore safe to serve from disk.
   * Only versioned table metadata qualifies — see the class javadoc for why version-hint does not.
   */
  static boolean isCacheable(String path) {
    return ENABLED && path != null && path.endsWith(".metadata.json");
  }

  /** Maps a remote object path to its local cache file. */
  static File cacheFileFor(String path) {
    return new File(cacheDir(), sha256(path) + ".metadata.json");
  }

  private static String sha256(String s) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder(64);
      for (byte b : digest) {
        sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
      }
      return sb.toString();
    // fallback-guard: allow sha256's catch is documented as unreachable for SHA-256; degrades only the cache filename derivation, not correctness
    } catch (Exception e) {
      // Cannot happen for SHA-256; degrade to a path-derived name rather than fail the read.
      return Integer.toHexString(s.hashCode());
    }
  }

  /**
   * Wraps a {@link FileIO} so reads of immutable metadata objects are served from (and populate)
   * the local cache. Everything else passes straight through.
   */
  public static FileIO wrap(FileIO delegate) {
    if (!ENABLED) {
      return delegate;
    }
    return new CachingFileIO(delegate);
  }

  /** Delegating FileIO that short-circuits immutable metadata reads to local disk. */
  private static final class CachingFileIO implements FileIO {
    private static final long serialVersionUID = 1L;

    private final FileIO delegate;

    CachingFileIO(FileIO delegate) {
      this.delegate = delegate;
    }

    @Override public InputFile newInputFile(String path) {
      if (!isCacheable(path)) {
        return delegate.newInputFile(path);
      }
      File local = cacheFileFor(path);
      if (local.isFile() && local.length() > 0) {
        return org.apache.iceberg.Files.localInput(local);
      }
      try {
        populate(delegate.newInputFile(path), local);
        return org.apache.iceberg.Files.localInput(local);
      // fallback-guard: allow comment documents a cache write failure must never fail a read that would otherwise succeed; fallback is the real delegate read
      } catch (IOException | RuntimeException e) {
        // A cache write failure must never fail a read that would otherwise succeed.
        LOGGER.debug("Iceberg metadata cache miss-and-populate failed for {}: {}",
            path, e.toString());
        return delegate.newInputFile(path);
      }
    }

    @Override public InputFile newInputFile(String path, long length) {
      return isCacheable(path) ? newInputFile(path) : delegate.newInputFile(path, length);
    }

    @Override public OutputFile newOutputFile(String path) {
      return delegate.newOutputFile(path);
    }

    @Override public void deleteFile(String path) {
      if (isCacheable(path)) {
        File local = cacheFileFor(path);
        if (local.isFile()) {
          local.delete();
        }
      }
      delegate.deleteFile(path);
    }

    @Override public Map<String, String> properties() {
      return delegate.properties();
    }

    @Override public void initialize(Map<String, String> properties) {
      delegate.initialize(properties);
    }

    @Override public void close() {
      delegate.close();
    }

    /**
     * Copies the remote object to the cache via a temp file + atomic rename, so a concurrent
     * reader never observes a half-written entry.
     */
    private void populate(InputFile remote, File target) throws IOException {
      File tmp = File.createTempFile("icebergmeta", ".tmp", cacheDir());
      try (InputStream in = remote.newStream();
           OutputStream out = Files.newOutputStream(tmp.toPath())) {
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
          out.write(buf, 0, n);
        }
      } catch (IOException e) {
        tmp.delete();
        throw e;
      }
      try {
        Files.move(tmp.toPath(), target.toPath(),
            StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (IOException e) {
        // ATOMIC_MOVE is unsupported on some filesystems; a plain replace is still safe here
        // because the content for a given path is immutable.
        Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }
}
