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
// Filesystem access here targets the LOCAL keyed cache file, which is a machine-local artifact by
// design — the point is to avoid the object-store round trip entirely. Every REMOTE operation in
// this file goes through S3StorageProvider (HEAD/GET/PUT/COPY), never a raw filesystem call.
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.storage.S3StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keyed, durable cache of per-table Iceberg SCHEMA — column names, types, nullability, and
 * comments — so enumerating a catalog costs no object-store round trips.
 *
 * <h2>Why this exists</h2>
 *
 * <p>Measured on 24 schemas / 325 Iceberg tables against MinIO: enumerating the catalog cost
 * ~9.3s, and instrumentation attributed ALL 325 per-table Iceberg loads to a single call site —
 * {@code DuckDBJdbcSchema.refreshStaleIcebergViewIfNeeded}, which calls
 * {@code fsTable.getRowType(...).getFieldCount()} to detect view drift. Every other consumer then
 * hits the in-process memo. So one cache, serving {@link IcebergTable#getRowType}, removes the
 * whole cost.
 *
 * <h2>What is cached, and what deliberately is not</h2>
 *
 * <ul>
 *   <li><b>Row type and comments — cached.</b> The stored artifact is the Iceberg schema as
 *       {@link SchemaParser} JSON, which round-trips exactly: nested types, decimal precision,
 *       required/optional, and per-field {@code doc}. Nothing is re-derived or approximated, so a
 *       cache hit produces byte-for-byte the same {@code RelDataType} as a live read. Table-level
 *       and property-based column comments ride along, because they come from the same
 *       object-store read and would otherwise re-trigger it on their own.</li>
 *   <li><b>Statistics — NOT cached.</b> Row counts grow with every daily ingest and feed the
 *       planner; a stale cardinality produces bad join orders. {@code getStatistic()} keeps
 *       reading live, and is not on the catalog path anyway.</li>
 * </ul>
 *
 * <h2>Validity — trusted only after an explicit match</h2>
 *
 * <p>The cache is keyed by table root path and gated as a whole. On first use it performs ONE
 * object-store {@code HEAD} of the published copy at {@link #PUBLISHED_KEY} and compares the
 * object's ETag (the MD5 of a single-part upload) against {@link #localFileDigest()}:
 *
 * <ul>
 *   <li>digests match — the local file is the published file; entries become trusted;</li>
 *   <li>digests differ — {@code GET} the published file, replace the local copy, then trust it;</li>
 *   <li>no published object, no credentials, or any error — the cache stays UNTRUSTED.</li>
 * </ul>
 *
 * <p>Untrusted means every lookup reports a miss, and a miss falls through to the live Iceberg
 * read. That is the whole safety argument: this cache can only ever make a correct start faster,
 * never make a wrong start. It cannot serve an empty or partial schema, because it serves nothing
 * at all until it has proven it holds exactly the published bytes.
 *
 * <p>A multipart ETag (containing {@code -}) is treated as unknown and forces a download, since
 * it is not an MD5 of the content. Publishing goes through a single {@code PUT}, so this is a
 * defensive path rather than an expected one.
 *
 * <p>Lookups after that are a hash hit — the file is parsed once per JVM.
 */
public final class IcebergSchemaCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergSchemaCache.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Cache file name, written under the cache directory and shipped in the seed. */
  public static final String FILE_NAME = "iceberg-schema-cache.json";

  /** Key of the published copy, relative to the warehouse bucket root. */
  public static final String PUBLISHED_KEY = "_catalog/" + FILE_NAME;

  /** Temp key used while publishing, so a reader never observes a partial object. */
  private static final String PUBLISH_TMP_KEY = "_catalog/." + FILE_NAME + ".tmp";

  /** Set false via {@code iceberg.schema.cache.enabled=false} to bypass entirely. */
  private static final boolean ENABLED =
      !"false".equalsIgnoreCase(System.getProperty("iceberg.schema.cache.enabled"));

  private static final Map<String, TableSchema> ENTRIES = new ConcurrentHashMap<>();
  private static volatile boolean loaded;
  private static volatile boolean trusted;
  private static volatile boolean syncAttempted;

  /**
   * Warehouse coordinates observed on the live read path, so publishing needs no plumbing.
   *
   * <p>Every table that is read live hands over its own root path and credentials, which is
   * exactly what addressing the published object requires. Remembering them here means the seed
   * builder can publish with a no-argument call instead of threading an object-store config
   * through the verification runner that has no other use for one.
   */
  private static volatile String warehouseTablePath;
  private static volatile Map<String, String> warehouseS3Config;

  private IcebergSchemaCache() {
  }

  /**
   * One table's cached schema. Public fields are the on-disk JSON contract.
   */
  public static final class TableSchema {
    /** Table root path, e.g. {@code s3://bucket/schema/table}. The cache key. */
    public String path;
    /** Iceberg schema as {@link SchemaParser} JSON — lossless, including per-field doc. */
    public String schemaJson;
    /** Iceberg table property {@code comment}, or null when the table has none. */
    public String tableComment;
    /** Iceberg table property {@code column.comments} (a JSON map), or null. */
    public String columnCommentsJson;

    public TableSchema() {
    }
  }

  /** On-disk document: the keyed table entries, nothing else. */
  public static final class Document {
    public Map<String, TableSchema> tables = new LinkedHashMap<>();
  }

  // ---------------------------------------------------------------------------------------------
  // Local file
  // ---------------------------------------------------------------------------------------------

  /** Resolves the cache directory, mirroring {@link IcebergMetadataCache}. */
  static File cacheDir() {
    String configured = System.getProperty("iceberg.metadata.cache.directory");
    File dir = (configured != null && !configured.isEmpty())
        ? new File(configured)
        : new File(System.getProperty("user.home")
            + File.separator + ".aperio" + File.separator + ".iceberg_metadata_cache");
    if (!dir.exists()) {
      dir.mkdirs();
    }
    return dir;
  }

  static File cacheFile() {
    return new File(cacheDir(), FILE_NAME);
  }

  /** Parses the cache file once per JVM. An absent or unreadable file leaves the cache empty. */
  private static void ensureLoaded() {
    if (loaded) {
      return;
    }
    synchronized (IcebergSchemaCache.class) {
      if (loaded) {
        return;
      }
      loaded = true;
      File f = cacheFile();
      if (!f.isFile()) {
        return;
      }
      try {
        Document doc = MAPPER.readValue(f, Document.class);
        if (doc.tables != null) {
          ENTRIES.putAll(doc.tables);
        }
        LOGGER.info("Iceberg schema cache loaded: {} tables from {}",
            ENTRIES.size(), f.getAbsolutePath());
      } catch (IOException e) {
        // A corrupt cache must never fail a connection — fall through to the live path.
        ENTRIES.clear();
        LOGGER.warn("Ignoring unreadable Iceberg schema cache {}: {}", f, e.getMessage());
      }
    }
  }

  /**
   * MD5 of the local cache FILE's bytes — the comparison token against the published copy.
   *
   * <p>Hashing the file rather than the parsed entries is deliberate: the published object and the
   * local file are the same artifact, so comparing their bytes needs no canonical serialization,
   * no stable map ordering, and no agreement between writer and reader on how to rebuild a digest.
   * A mismatch simply means "download the published file".
   *
   * @return lowercase hex MD5, or null when there is no readable local cache
   */
  public static String localFileDigest() {
    File f = cacheFile();
    if (!f.isFile()) {
      return null;
    }
    try {
      return md5Hex(java.nio.file.Files.readAllBytes(f.toPath()));
    // fallback-guard: allow localFileDigest's javadoc documents null meaning no readable local cache, forcing the safe download path
    } catch (IOException e) {
      LOGGER.warn("Could not digest schema cache {}: {}", f, e.getMessage());
      return null;
    }
  }

  private static String md5Hex(byte[] bytes) {
    try {
      byte[] d = java.security.MessageDigest.getInstance("MD5").digest(bytes);
      StringBuilder hex = new StringBuilder(32);
      for (byte b : d) {
        hex.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
      }
      return hex.toString();
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 unavailable", e);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Lookup / record — the read path
  // ---------------------------------------------------------------------------------------------

  /**
   * Returns the cached schema for a table root, or null on a miss.
   *
   * <p>Performs the one-time sync against the published copy on first call. Null is returned for
   * every table until that sync proves the local file matches what was published, so a caller can
   * treat null as "read it live" without ever risking a stale answer.
   *
   * @param tablePath table root, e.g. {@code s3://bucket/schema/table}
   * @param s3Config the {@code fs.s3a.*} credential map, or null for non-object-store tables
   */
  public static TableSchema lookup(String tablePath, Map<String, String> s3Config) {
    if (!ENABLED || tablePath == null) {
      return null;
    }
    syncIfNeeded(tablePath, s3Config);
    if (!trusted) {
      return null;
    }
    ensureLoaded();
    return ENTRIES.get(tablePath);
  }

  /**
   * Records a table's live-read schema in memory. Persisting is explicit — see {@link #save} — so
   * an ordinary connection pays nothing beyond a map write, and only seed generation writes a file.
   *
   * @param tablePath table root; ignored when null
   * @param schema the Iceberg schema just read
   * @param properties the Iceberg table properties just read (may be null)
   * @param s3Config the credential map for this table, or null for non-object-store tables
   */
  public static void record(String tablePath, Schema schema, Map<String, String> properties,
      Map<String, String> s3Config) {
    if (!ENABLED || tablePath == null || schema == null) {
      return;
    }
    ensureLoaded();
    if (s3Config != null && !s3Config.isEmpty() && bucketRoot(tablePath) != null) {
      warehouseTablePath = tablePath;
      warehouseS3Config = s3Config;
    }
    TableSchema entry = new TableSchema();
    entry.path = tablePath;
    entry.schemaJson = SchemaParser.toJson(schema);
    if (properties != null) {
      entry.tableComment = properties.get("comment");
      entry.columnCommentsJson = properties.get("column.comments");
    }
    ENTRIES.put(tablePath, entry);
  }

  /**
   * Drops a single table's cached schema, forcing the next {@link #lookup} to miss and the
   * caller to fall back to a live read. Used when a caller has independent proof the cached
   * entry is stale — e.g. DuckDB itself reports a bound view's column list no longer matches
   * live reality — see {@code EtagRetryConnection}'s view-type-drift repair.
   *
   * @param tablePath table root, e.g. {@code s3://bucket/schema/table}; ignored when null
   */
  public static void invalidate(String tablePath) {
    if (tablePath != null) {
      ENTRIES.remove(tablePath);
    }
  }

  /** Number of entries currently held — used by seed generation to report what it materialized. */
  public static int size() {
    ensureLoaded();
    return ENTRIES.size();
  }

  /**
   * Whether {@link #record} has fired at least once this JVM — i.e. some table's schema was
   * actually read live rather than served from a trusted published cache.
   *
   * <p>Lets a caller of {@link #publishToWarehouse} tell "nothing needed publishing because the
   * published cache already covered every table this run touched" (expected, not a failure) apart
   * from "a live read happened but the publish itself failed" (a real problem worth failing on) —
   * {@link #publishToWarehouse} returns {@code false} for both, since from inside the cache they
   * look the same.
   */
  public static boolean anyLiveRead() {
    return warehouseTablePath != null;
  }

  /**
   * Publishes to the warehouse this JVM has been reading from, as observed by {@link #record}.
   *
   * <p>This is the seed-generation entry point: run a full catalog pull, then call this. Returns
   * false both when nothing was read live (the published cache already covered every table this
   * run touched, so there is nothing new to publish — check {@link #anyLiveRead} to tell that
   * apart from an actual write failure) and when the write itself failed.
   *
   * @return true when the published object was replaced
   */
  public static boolean publishToWarehouse() {
    String path = warehouseTablePath;
    Map<String, String> s3Config = warehouseS3Config;
    if (path == null || s3Config == null) {
      LOGGER.info("Nothing to publish to the Iceberg schema cache: no table was read live in "
          + "this JVM (every lookup this run was served from the already-published cache)");
      return false;
    }
    return publish(path, s3Config);
  }

  // ---------------------------------------------------------------------------------------------
  // Sync — one HEAD (plus a GET on mismatch) per JVM
  // ---------------------------------------------------------------------------------------------

  private static void syncIfNeeded(String tablePath, Map<String, String> s3Config) {
    if (syncAttempted) {
      return;
    }
    synchronized (IcebergSchemaCache.class) {
      if (syncAttempted) {
        return;
      }
      syncAttempted = true;
      String root = bucketRoot(tablePath);
      if (root == null || s3Config == null || s3Config.isEmpty()) {
        LOGGER.debug("Iceberg schema cache not synced (no object-store root for {}) — "
            + "every lookup will read live", tablePath);
        return;
      }
      try {
        StorageProvider provider = providerFor(s3Config);
        String publishedPath = root + PUBLISHED_KEY;
        String publishedDigest = etagDigest(provider, publishedPath);
        if (publishedDigest == null) {
          LOGGER.info("No published Iceberg schema cache at {} — reading schemas live",
              publishedPath);
          return;
        }
        if (!publishedDigest.equals(localFileDigest())) {
          download(provider, publishedPath);
        }
        // Re-digest after the download: trust requires the local bytes to BE the published bytes.
        if (publishedDigest.equals(localFileDigest())) {
          trusted = true;
          ensureLoaded();
          LOGGER.info("Iceberg schema cache trusted ({} tables, digest {})",
              ENTRIES.size(), publishedDigest);
        } else {
          LOGGER.warn("Iceberg schema cache digest still differs from published {} — reading live",
              publishedPath);
        }
      } catch (IOException | RuntimeException e) {
        LOGGER.info("Iceberg schema cache sync failed ({}) — reading schemas live", e.toString());
      }
    }
  }

  /**
   * HEADs the published object and returns its content MD5, or null when it is absent or the ETag
   * is not an MD5 (multipart upload), which forces the caller down the download path.
   */
  private static String etagDigest(StorageProvider provider, String path) {
    try {
      StorageProvider.FileMetadata meta = provider.getMetadata(path);
      String etag = meta.getEtag();
      if (etag == null) {
        return null;
      }
      etag = etag.replace("\"", "").trim();
      if (etag.isEmpty() || etag.indexOf('-') >= 0) {
        // Multipart ETag is not the content MD5; report unknown so we re-download and re-compare.
        return null;
      }
      return etag.toLowerCase(java.util.Locale.ROOT);
    // fallback-guard: allow etagDigest's javadoc documents null on HEAD failure/non-MD5 ETag, forcing a safe re-download-and-recompare
    } catch (IOException | RuntimeException e) {
      LOGGER.debug("HEAD of {} failed: {}", path, e.toString());
      return null;
    }
  }

  /** GETs the published cache and replaces the local file atomically. */
  private static void download(StorageProvider provider, String publishedPath) throws IOException {
    byte[] bytes;
    try (InputStream in = provider.openInputStream(publishedPath)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) > 0) {
        out.write(buf, 0, n);
      }
      bytes = out.toByteArray();
    }
    File dir = cacheDir();
    File tmp = File.createTempFile("schemacache", ".tmp", dir);
    java.nio.file.Files.write(tmp.toPath(), bytes);
    replace(tmp, cacheFile());
    // The in-memory view must come from the bytes we just installed, not a prior parse.
    ENTRIES.clear();
    loaded = false;
    LOGGER.info("Downloaded published Iceberg schema cache: {} bytes from {}",
        bytes.length, publishedPath);
  }

  private static void replace(File tmp, File target) throws IOException {
    try {
      java.nio.file.Files.move(tmp.toPath(), target.toPath(),
          java.nio.file.StandardCopyOption.REPLACE_EXISTING,
          java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      java.nio.file.Files.move(tmp.toPath(), target.toPath(),
          java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Publish — seed generation and schema-drift republish
  // ---------------------------------------------------------------------------------------------

  /**
   * Writes the in-memory cache to the local file and publishes it to the warehouse.
   *
   * <p>The upload goes to a temp key and is then server-side copied onto {@link #PUBLISHED_KEY},
   * so a concurrent reader either sees the previous complete object or the new complete object,
   * never a partially written one.
   *
   * @param tablePath any table root in the warehouse, used to derive the bucket
   * @param s3Config the {@code fs.s3a.*} credential map
   * @return true when the published object was replaced
   */
  public static boolean publish(String tablePath, Map<String, String> s3Config) {
    String root = bucketRoot(tablePath);
    if (root == null || s3Config == null || s3Config.isEmpty()) {
      LOGGER.warn("Cannot publish Iceberg schema cache: no object-store root for {}", tablePath);
      return false;
    }
    if (!save()) {
      return false;
    }
    try {
      byte[] bytes = java.nio.file.Files.readAllBytes(cacheFile().toPath());
      StorageProvider provider = providerFor(s3Config);
      provider.writeFile(root + PUBLISH_TMP_KEY, bytes);
      provider.copyFile(root + PUBLISH_TMP_KEY, root + PUBLISHED_KEY);
      provider.delete(root + PUBLISH_TMP_KEY);
      LOGGER.info("Published Iceberg schema cache: {} tables, {} bytes, digest {} -> {}",
          ENTRIES.size(), bytes.length, md5Hex(bytes), root + PUBLISHED_KEY);
      return true;
    // fallback-guard: allow publish() returns false on failure, a properly distinguishable boolean signal from true
    } catch (IOException | RuntimeException e) {
      LOGGER.warn("Failed to publish Iceberg schema cache to {}: {}",
          root + PUBLISHED_KEY, e.toString());
      return false;
    }
  }

  /**
   * Drops the published cache, forcing every connection back onto the live read until it is
   * republished. Called when schema drift is detected: the cached schema is known-wrong at that
   * moment, and an unpublished cache is simply slow, whereas a wrong one is incorrect.
   *
   * @param tablePath any table root in the warehouse, used to derive the bucket
   * @param s3Config the {@code fs.s3a.*} credential map
   */
  public static void unpublish(String tablePath, Map<String, String> s3Config) {
    String root = bucketRoot(tablePath);
    if (root == null || s3Config == null || s3Config.isEmpty()) {
      return;
    }
    try {
      providerFor(s3Config).delete(root + PUBLISHED_KEY);
      LOGGER.info("Withdrew published Iceberg schema cache at {} after schema drift",
          root + PUBLISHED_KEY);
    } catch (IOException | RuntimeException e) {
      LOGGER.warn("Could not withdraw published Iceberg schema cache at {}: {}",
          root + PUBLISHED_KEY, e.toString());
    }
  }

  /**
   * Writes the cache file atomically.
   *
   * @return true when the file was written
   */
  public static boolean save() {
    ensureLoaded();
    File target = cacheFile();
    try {
      Document doc = new Document();
      doc.tables.putAll(ENTRIES);
      File tmp = File.createTempFile("schemacache", ".tmp", cacheDir());
      MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmp, doc);
      replace(tmp, target);
      LOGGER.info("Iceberg schema cache written: {} tables -> {}",
          ENTRIES.size(), target.getAbsolutePath());
      return true;
    // fallback-guard: allow save() returns false on IOException, the same distinguishable boolean success/failure contract
    } catch (IOException e) {
      LOGGER.warn("Could not write Iceberg schema cache {}: {}", target, e.getMessage());
      return false;
    }
  }

  /**
   * Installs a cache shipped inside the JAR, so a first-ever connection can skip the download.
   *
   * <p>Installs once per bundled artifact, gated on a marker holding the digest of the copy last
   * installed here. An absent marker means no JAR has seeded this cache yet; a differing one means
   * the JAR was upgraded and ships a cache the local file has never seen. Between upgrades the
   * marker matches and the local file is left alone, so a copy the warehouse sync has since
   * refreshed is never overwritten by the older bundled one.
   *
   * <p>A plain "skip whenever a local file exists" test cannot express that. It reads an existing
   * file as necessarily newer than the JAR's, which holds right up until the JAR is upgraded —
   * and then the freshly shipped cache is locked out permanently, leaving every connection to
   * resolve schemas the slow way against a cache that predates the release.
   *
   * <p>Installing does not make the cache trusted. The normal sync still HEADs the published
   * object and compares digests — a bundled copy that no longer matches the warehouse is
   * downloaded over, exactly as a stale downloaded copy would be. So a JAR that has drifted from
   * the warehouse costs one GET, never a wrong schema.
   *
   * @param bundled the resource stream; closed by the caller
   * @return true when the bundled cache was installed
   */
  public static boolean installBundled(InputStream bundled) {
    if (!ENABLED || bundled == null) {
      return false;
    }
    File target = cacheFile();
    try {
      byte[] bytes = readFully(bundled);
      String bundledDigest = md5Hex(bytes);
      File marker = bundledMarkerFile();
      if (target.isFile() && bundledDigest.equals(readMarker(marker))) {
        LOGGER.debug("Bundled Iceberg schema cache {} already installed; keeping local copy",
            bundledDigest);
        return false;
      }
      File tmp = File.createTempFile("schemacache", ".tmp", cacheDir());
      java.nio.file.Files.write(tmp.toPath(), bytes);
      replace(tmp, target);
      java.nio.file.Files.write(marker.toPath(),
          bundledDigest.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      LOGGER.info("Installed bundled Iceberg schema cache -> {} ({} bytes, digest {})",
          target.getAbsolutePath(), target.length(), bundledDigest);
      return true;
    // fallback-guard: allow installBundled() returns false on failure; javadoc clarifies this is a cold-start optimization, never an override
    } catch (IOException e) {
      LOGGER.warn("Could not install bundled Iceberg schema cache: {}", e.getMessage());
      return false;
    }
  }

  /** Records the digest of the bundled cache last installed here; see {@link #installBundled}. */
  private static File bundledMarkerFile() {
    return new File(cacheDir(), FILE_NAME + ".bundled");
  }

  /** Reads the bundled-install marker, or null when it is absent or unreadable. */
  private static String readMarker(File marker) {
    if (!marker.isFile()) {
      return null;
    }
    try {
      return new String(java.nio.file.Files.readAllBytes(marker.toPath()),
          java.nio.charset.StandardCharsets.UTF_8).trim();
    // fallback-guard: allow an unreadable marker reads as absent, which reinstalls the bundled cache — the same safe outcome as a first-ever install
    } catch (IOException e) {
      LOGGER.warn("Could not read bundled-cache marker {}: {}", marker, e.getMessage());
      return null;
    }
  }

  /** Reads a stream fully into memory; the bundled cache is a bounded JAR resource. */
  private static byte[] readFully(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  /** Drops all state — the maintenance flush, and the hook used by tests. */
  public static void invalidate() {
    ENTRIES.clear();
    loaded = false;
    trusted = false;
    syncAttempted = false;
    warehouseTablePath = null;
    warehouseS3Config = null;
    File f = cacheFile();
    if (f.isFile() && !f.delete()) {
      LOGGER.warn("Could not delete Iceberg schema cache {}", f);
    }
    // The marker must go with it: left behind, it would report the bundled cache as already
    // installed and stop the next connection from re-seeding the file just deleted.
    File marker = bundledMarkerFile();
    if (marker.isFile() && !marker.delete()) {
      LOGGER.warn("Could not delete bundled-cache marker {}", marker);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /**
   * Derives the bucket root, e.g. {@code s3://bucket/}, from a table path.
   *
   * <p>This decomposes a URI; it does not decide local-vs-remote. That decision is already made by
   * the caller, which passes a non-null credential map only for object-store-backed tables — the
   * same storage signal {@link IcebergTable} uses to select the S3FileIO loader. A path with no
   * scheme or no authority simply has no bucket to address, so there is nowhere to publish.
   *
   * @return the root with a trailing slash, or null when the path names no bucket
   */
  static String bucketRoot(String tablePath) {
    if (tablePath == null) {
      return null;
    }
    try {
      java.net.URI uri = new java.net.URI(tablePath);
      String scheme = uri.getScheme();
      String authority = uri.getAuthority();
      if (scheme == null || authority == null) {
        return null;
      }
      return scheme + "://" + authority + "/";
    // fallback-guard: allow bucketRoot's javadoc documents null when the path names no bucket, a safe degrade shared with the not-a-bucket-URI case
    } catch (java.net.URISyntaxException e) {
      return null;
    }
  }

  /**
   * Builds an S3 provider from the same credential map the Iceberg loader uses, accepting either
   * the hadoop {@code fs.s3a.*} keys or the plain {@code accessKeyId}/{@code secretAccessKey} ones.
   */
  private static StorageProvider providerFor(Map<String, String> s3Config) {
    Map<String, Object> config = new HashMap<>();
    copyFirst(config, "accessKeyId", s3Config, "fs.s3a.access.key", "accessKeyId");
    copyFirst(config, "secretAccessKey", s3Config, "fs.s3a.secret.key", "secretAccessKey");
    // Short-lived credentials sign with a session token; a provider built without it
    // fails every request with 403 SignatureDoesNotMatch.
    copyFirst(config, "sessionToken", s3Config, "fs.s3a.session.token", "sessionToken");
    copyFirst(config, "endpoint", s3Config, "fs.s3a.endpoint", "endpoint");
    copyFirst(config, "region", s3Config, "fs.s3a.endpoint.region", "region");
    if (!config.containsKey("region")) {
      // AWS SDK v2 requires a region even against a custom endpoint; MinIO/R2 ignore it.
      config.put("region", "us-east-1");
    }
    config.put("pathStyleAccess", "true");
    return new S3StorageProvider(config);
  }

  private static void copyFirst(Map<String, Object> dst, String dstKey,
      Map<String, String> src, String... srcKeys) {
    for (String k : srcKeys) {
      String v = src.get(k);
      if (v != null && !v.isEmpty()) {
        dst.put(dstKey, v);
        return;
      }
    }
  }
}
