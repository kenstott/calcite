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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link StorageProvider} wrapper that stages parquet writes locally and
 * batch-merges them before uploading to the delegate (R2/S3) storage.
 *
 * <p>{@link XbrlToParquetConverter} writes 5-20 individual parquet files per SEC filing
 * directly to R2, generating millions of Class A operations. This wrapper:
 * <ol>
 *   <li>Intercepts {@code writeAvroParquet} calls and writes to a local staging directory.</li>
 *   <li>Groups staged files by table type + year partition
 *       (e.g., {@code facts} within {@code year=2024}).</li>
 *   <li>When a group reaches {@code flushThreshold} files, merges them with DuckDB
 *       into one parquet file and uploads as a single R2 write.</li>
 *   <li>All other operations (reads, exists, writeFile, etc.) delegate to the real provider.</li>
 * </ol>
 *
 * <p>Controlled by environment variable {@code ETL_BATCH_FLUSH_SIZE} (default: 100).
 * Staging directory: {@code $ETL_LOCAL_RAW_CACHE/sec/staging}
 * (default: {@code /tmp/etl-sec-staging} when {@code ETL_LOCAL_RAW_CACHE} is unset).
 *
 * <p>storage-provider-guard:ignore-file — by design this wrapper stages writes on the
 * local filesystem before merge-uploading through the delegate provider; local
 * java.io.File / java.nio.file usage is its entire purpose.
 */
public class LocalStagingStorageProvider implements StorageProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalStagingStorageProvider.class);

  /**
   * Hadoop Configuration that routes {@code file://} through {@link
   * org.apache.hadoop.fs.RawLocalFileSystem} instead of the default checksum-wrapping
   * {@code LocalFileSystem}. Without this, every staged parquet gets a hidden
   * {@code .<name>.parquet.crc} sidecar that {@link java.io.File#delete()} cannot see, so
   * flush cleanup orphans one crc per staged filing — ~1M tiny files over a full SEC
   * backfill, which exhausts the tmpfs inode table and fails ETL with "No space left on
   * device" while bytes remain free. RawLocalFileSystem writes no checksums, so none leak.
   */
  private static final Configuration NO_CHECKSUM_CONF = new Configuration();
  static {
    NO_CHECKSUM_CONF.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    NO_CHECKSUM_CONF.setBoolean("fs.file.impl.disable.cache", true);
  }

  /**
   * Matches the table-type suffix in a per-filing parquet filename.
   * Example: {@code 0000320193-24-000006_facts.parquet} → group 1 = {@code facts}
   */
  private static final Pattern TABLE_TYPE_PATTERN =
      Pattern.compile("_([a-z0-9]+)\\.parquet$", Pattern.CASE_INSENSITIVE);

  private final StorageProvider delegate;
  private final File stagingDir;
  private final int flushThreshold;
  private final AtomicInteger batchCounter = new AtomicInteger(0);

  /** Lock for all access to stagedFiles and groupKeyToR2Dir. */
  private final Object lock = new Object();

  /** Map from group key ({parentR2Path}/{tableType}) to staged local parquet files. */
  private final Map<String, List<File>> stagedFiles = new LinkedHashMap<String, List<File>>();

  /** Maps each group key to the parent R2 directory where the merged file will be uploaded. */
  private final Map<String, String> groupKeyToR2ParentDir =
      new LinkedHashMap<String, String>();

  /** Maps each group key to the table type string (for merged filename). */
  private final Map<String, String> groupKeyToTableType =
      new LinkedHashMap<String, String>();

  public LocalStagingStorageProvider(StorageProvider delegate) {
    this(delegate, resolveStagingDir());
  }

  public LocalStagingStorageProvider(StorageProvider delegate, File stagingDir) {
    this.delegate = delegate;
    this.stagingDir = stagingDir;
    this.flushThreshold = resolveFlushThreshold();
    LOGGER.info("LocalStagingStorageProvider initialized: stagingDir={}, flushThreshold={}",
        stagingDir.getAbsolutePath(), flushThreshold);
    // Flush staged-but-unbatched files on JVM shutdown (e.g. SIGTERM from a graceful pool
    // restart when the jar is rebuilt mid-run), so an interrupted worker uploads its tail
    // instead of stranding it. Idempotent: a clean completion already drained the map, so the
    // hook then sees nothing to flush.
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override public void run() {
        try {
          flushAll();
        } catch (Exception e) {
          LOGGER.warn("Shutdown flush failed: {}", e.getMessage());
        }
      }
    }, "local-staging-flush"));
  }

  private static File resolveStagingDir() {
    String env = System.getenv("ETL_LOCAL_RAW_CACHE");
    if (env != null && !env.isEmpty()) {
      return new File(env, "sec/staging");
    }
    return new File(System.getProperty("java.io.tmpdir"), "etl-sec-staging");
  }

  private static int resolveFlushThreshold() {
    String env = System.getenv("ETL_BATCH_FLUSH_SIZE");
    if (env != null && !env.isEmpty()) {
      try {
        return Integer.parseInt(env.trim());
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid ETL_BATCH_FLUSH_SIZE='{}', using default 100", env);
      }
    }
    return 100;
  }

  // -----------------------------------------------------------------------
  // writeAvroParquet — intercepted: write to local staging, batch-upload
  // -----------------------------------------------------------------------

  /**
   * Intercepts parquet writes: stages locally and batch-uploads to R2 when
   * {@code flushThreshold} files accumulate for a given table type + partition.
   */
  @Override
  public void writeAvroParquet(String path, Schema schema,
      List<GenericRecord> records, String recordType) throws IOException {

    String tableType = extractTableType(path);
    String r2ParentDir = extractParentPath(path);
    String groupKey = r2ParentDir + "/" + tableType;

    // Write parquet data to a local staging file
    File localFile = stageLocally(groupKey, schema, records, recordType);

    // Add to staging map and check flush threshold
    List<File> toFlush = null;
    synchronized (lock) {
      List<File> files = stagedFiles.get(groupKey);
      if (files == null) {
        files = new ArrayList<File>();
        stagedFiles.put(groupKey, files);
        groupKeyToR2ParentDir.put(groupKey, r2ParentDir);
        groupKeyToTableType.put(groupKey, tableType);
      }
      files.add(localFile);

      if (files.size() >= flushThreshold) {
        toFlush = new ArrayList<File>(files);
        files.clear();
      }
    }

    if (toFlush != null) {
      flushFiles(groupKey, toFlush);
    }
  }

  /**
   * Flushes all staged files to R2.
   * Call this after all filings have been processed to upload the final partial batch.
   *
   * @throws IOException if any flush operation fails
   */
  public void flushAll() throws IOException {
    Map<String, List<File>> snapshot = new LinkedHashMap<String, List<File>>();
    synchronized (lock) {
      for (Map.Entry<String, List<File>> entry : stagedFiles.entrySet()) {
        if (!entry.getValue().isEmpty()) {
          snapshot.put(entry.getKey(), new ArrayList<File>(entry.getValue()));
          entry.getValue().clear();
        }
      }
    }

    int totalGroups = snapshot.size();
    int totalFiles = 0;
    for (List<File> files : snapshot.values()) {
      totalFiles += files.size();
    }
    if (totalFiles == 0) {
      LOGGER.info("flushAll: no staged files to flush");
      return;
    }
    LOGGER.info("flushAll: flushing {} files across {} groups", totalFiles, totalGroups);

    for (Map.Entry<String, List<File>> entry : snapshot.entrySet()) {
      flushFiles(entry.getKey(), entry.getValue());
    }
  }

  // -----------------------------------------------------------------------
  // Internal staging and flushing
  // -----------------------------------------------------------------------

  /**
   * Writes parquet data to a local staging file and returns that file.
   */
  @SuppressWarnings({"deprecation", "UnusedVariable"})
  private File stageLocally(String groupKey, Schema schema,
      List<GenericRecord> records, String recordType) throws IOException {
    // Sanitize groupKey for use as a path segment
    String safeDirName = groupKey.replaceAll("[^a-zA-Z0-9=_-]", "_");
    File groupDir = new File(stagingDir, safeDirName);
    // Idempotent + race-safe: createDirectories() makes parents and silently succeeds if the dir
    // already exists (concurrent pool threads stage into the same group dir), throwing IOException
    // only on a genuine failure. This avoids the check-then-create race in the old
    // !exists() && !mkdirs() form, where a lost mkdirs() race returned false ambiguously and
    // dropped insider forms — which emptied beneficial_ownership.
    java.nio.file.Files.createDirectories(groupDir.toPath());
    File localFile = new File(groupDir, UUID.randomUUID().toString() + ".parquet");

    // Write parquet using Parquet/Avro writer
    try (org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(new Path(localFile.toURI()))
            .withConf(NO_CHECKSUM_CONF)
            .withSchema(schema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }
    LOGGER.debug("Staged {} records to local file: {}", records.size(),
        localFile.getAbsolutePath());
    return localFile;
  }

  /**
   * Merges the given staged files with DuckDB and uploads the result to R2 as one file.
   */
  private void flushFiles(String groupKey, List<File> files) throws IOException {
    if (files.isEmpty()) {
      return;
    }

    String r2ParentDir = groupKeyToR2ParentDir.get(groupKey);
    String tableType = groupKeyToTableType.get(groupKey);
    int batchNum = batchCounter.getAndIncrement();
    String r2TargetPath = String.format(Locale.ROOT, "%s/%s_batch_%04d.parquet",
        r2ParentDir, tableType, batchNum);

    LOGGER.info("Flushing batch {}: merging {} staged '{}' files → {}",
        batchNum, files.size(), tableType, r2TargetPath);

    File mergedFile = null;
    try {
      if (files.size() == 1) {
        // Skip DuckDB merge for single file
        mergedFile = files.get(0);
      } else {
        mergedFile = mergeWithDuckDB(files, tableType, batchNum);
      }

      try (InputStream is = new FileInputStream(mergedFile)) {
        delegate.writeFile(r2TargetPath, is);
      }
      LOGGER.info("Uploaded batch {} to R2: {} ({} source files)",
          batchNum, r2TargetPath, files.size());

    } finally {
      // Clean up: delete staged files and merged temp file (if it's different from a staged file)
      for (File f : files) {
        if (!f.equals(mergedFile)) {
          deleteQuietly(f);
        }
      }
      if (mergedFile != null && !files.contains(mergedFile)) {
        deleteQuietly(mergedFile);
      } else if (mergedFile != null && files.size() == 1) {
        // Single file case: merged IS the staged file; delete after upload
        deleteQuietly(mergedFile);
      }
    }
  }

  /**
   * Uses DuckDB to merge multiple local parquet files into one.
   */
  private File mergeWithDuckDB(List<File> filesToMerge, String tableType, int batchNum)
      throws IOException {
    File mergedFile = File.createTempFile(
        "sec_merged_" + tableType + "_" + batchNum + "_", ".parquet", stagingDir);
    // Delete so DuckDB can create it fresh
    if (!mergedFile.delete()) {
      LOGGER.warn("Could not pre-delete temp merge file: {}", mergedFile.getAbsolutePath());
    }

    StringBuilder fileList = new StringBuilder("[");
    for (int i = 0; i < filesToMerge.size(); i++) {
      if (i > 0) {
        fileList.append(", ");
      }
      fileList.append("'")
          .append(filesToMerge.get(i).getAbsolutePath().replace("'", "''"))
          .append("'");
    }
    fileList.append("]");

    String sql = String.format(Locale.ROOT,
        "COPY (SELECT * FROM read_parquet(%s)) TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
        fileList,
        mergedFile.getAbsolutePath().replace("'", "''"));

    LOGGER.debug("DuckDB merge SQL: {}", sql);

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {
      stmt.execute("PRAGMA temp_directory='" + stagingDir.getAbsolutePath().replace("'", "''") + "'");
      stmt.execute(sql);
    } catch (Exception e) {
      deleteQuietly(mergedFile);
      throw new IOException("DuckDB merge failed for batch " + batchNum
          + " (tableType=" + tableType + "): " + e.getMessage(), e);
    }

    LOGGER.debug("DuckDB merged {} files → {} ({} bytes)",
        filesToMerge.size(), mergedFile.getName(), mergedFile.length());
    return mergedFile;
  }

  private static void deleteQuietly(File file) {
    if (file == null) {
      return;
    }
    if (file.exists() && !file.delete()) {
      LOGGER.warn("Could not delete temp file: {}", file.getAbsolutePath());
    }
    // Also remove Hadoop's hidden checksum sidecar (.{name}.crc) if one exists. New writes go
    // through RawLocalFileSystem and never create it, but this reaps any pre-existing sidecar so
    // flush cleanup can never orphan a crc and slowly exhaust the tmpfs inode table.
    File crc = new File(file.getParentFile(), "." + file.getName() + ".crc");
    if (crc.exists() && !crc.delete()) {
      LOGGER.warn("Could not delete checksum sidecar: {}", crc.getAbsolutePath());
    }
  }

  // -----------------------------------------------------------------------
  // Path utilities
  // -----------------------------------------------------------------------

  /**
   * Extracts the table type from the parquet filename suffix.
   * E.g., {@code .../year=2024/0001234567-24-000001_facts.parquet} → {@code facts}
   */
  private static String extractTableType(String path) {
    if (path == null) {
      return "unknown";
    }
    // Get the filename part only
    int lastSlash = path.lastIndexOf('/');
    String filename = lastSlash >= 0 ? path.substring(lastSlash + 1) : path;

    Matcher m = TABLE_TYPE_PATTERN.matcher(filename);
    if (m.find()) {
      return m.group(1).toLowerCase(Locale.ROOT);
    }
    // Fallback: use full filename without extension
    int dot = filename.lastIndexOf('.');
    return dot > 0 ? filename.substring(0, dot).toLowerCase(Locale.ROOT) : filename;
  }

  /**
   * Returns the parent directory portion of a path.
   * E.g., {@code s3://bucket/sec/parquet/year=2024/file.parquet}
   *     → {@code s3://bucket/sec/parquet/year=2024}
   */
  private static String extractParentPath(String path) {
    if (path == null) {
      return "";
    }
    int lastSlash = path.lastIndexOf('/');
    return lastSlash > 0 ? path.substring(0, lastSlash) : path;
  }

  // -----------------------------------------------------------------------
  // All other StorageProvider methods delegate to the underlying provider
  // -----------------------------------------------------------------------

  @Override
  public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    return delegate.listFiles(path, recursive);
  }

  @Override
  public FileMetadata getMetadata(String path) throws IOException {
    return delegate.getMetadata(path);
  }

  @Override
  public InputStream openInputStream(String path) throws IOException {
    return delegate.openInputStream(path);
  }

  @Override
  public Reader openReader(String path) throws IOException {
    return delegate.openReader(path);
  }

  @Override
  public byte[] readRange(String path, long offset, long length) throws IOException {
    return delegate.readRange(path, offset, length);
  }

  @Override
  public boolean exists(String path) throws IOException {
    return delegate.exists(path);
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return delegate.isDirectory(path);
  }

  @Override
  public String getStorageType() {
    return "staging+" + delegate.getStorageType();
  }

  @Override
  public String resolvePath(String basePath, String relativePath) {
    return delegate.resolvePath(basePath, relativePath);
  }

  @Override
  public void writeFile(String path, byte[] content) throws IOException {
    delegate.writeFile(path, content);
  }

  @Override
  public void writeFile(String path, InputStream content) throws IOException {
    delegate.writeFile(path, content);
  }

  @Override
  public void createDirectories(String path) throws IOException {
    delegate.createDirectories(path);
  }

  @Override
  public boolean delete(String path) throws IOException {
    return delegate.delete(path);
  }

  @Override
  public int deleteBatch(List<String> paths) throws IOException {
    return delegate.deleteBatch(paths);
  }

  @Override
  public void ensureLifecycleRule(String prefix, int expirationDays) throws IOException {
    delegate.ensureLifecycleRule(prefix, expirationDays);
  }

  @Override
  public String getStagingDirectory(String purpose) throws IOException {
    return delegate.getStagingDirectory(purpose);
  }

  @Override
  public void copyFile(String source, String destination) throws IOException {
    delegate.copyFile(source, destination);
  }

  @Override
  public boolean hasChanged(String path, FileMetadata cachedMetadata) throws IOException {
    return delegate.hasChanged(path, cachedMetadata);
  }

  @Override
  public void cleanupMacosMetadata(String directoryPath) throws IOException {
    delegate.cleanupMacosMetadata(directoryPath);
  }

  @Override
  public Map<String, String> getS3Config() {
    return delegate.getS3Config();
  }
}