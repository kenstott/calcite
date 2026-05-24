/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.govdata.etl;

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.sec.SecDataFetcher;
import org.apache.calcite.adapter.govdata.sec.XbrlToParquetConverter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One-time DQ patch job for filing_metadata parquet files in R2.
 *
 * <p>Reads each {@code *_metadata.parquet} under {@code govdata-parquet-v1/sec/},
 * skips non-patchable filings, applies DQ normalization (filing_type, period_of_report,
 * fiscal_year_end, ticker, sic_code, fiscal_year), and rewrites in place.
 *
 * <p>Supports resumable checkpointing: if a {@code checkpointDir} is supplied, each
 * processed file path is appended to {@code <checkpointDir>/patch_<year>.ckpt}. On
 * restart, already-checkpointed paths are skipped so the sweep picks up where it left off.
 *
 * <p>Run modes:
 * <ul>
 *   <li>{@code --sample CIK1,CIK2,...} — patch only listed CIKs (validation phase)</li>
 *   <li>{@code --year YYYY} — patch all filings for one year</li>
 *   <li>{@code --all} — full sweep (all years)</li>
 *   <li>{@code --dry-run} — report what would change without writing</li>
 * </ul>
 */
public class FilingMetadataPatchJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilingMetadataPatchJob.class);

  private static final String BUCKET = "govdata-parquet-v1";
  private static final String SEC_PREFIX = "sec/";

  private static final Set<String> PATCHABLE_TYPES = new HashSet<>(Arrays.asList(
      "10-K", "10-Q", "10-K/A", "10-Q/A",
      "10K",  "10Q",  "10K/A",  "10Q/A",  "10KA", "10QA",
      "8-K",  "8-K/A", "8K", "8KA"
  ));

  private final StorageProvider storage;
  private final boolean dryRun;
  private final File checkpointDir;
  private int filesScanned = 0;
  private int filesPatched = 0;
  private int filesSkipped = 0;

  public int getFilesScanned() { return filesScanned; }
  public int getFilesPatched() { return filesPatched; }
  public int getFilesSkipped() { return filesSkipped; }

  public FilingMetadataPatchJob(StorageProvider storage, boolean dryRun) {
    this(storage, dryRun, null);
  }

  public FilingMetadataPatchJob(StorageProvider storage, boolean dryRun, File checkpointDir) {
    this.storage = storage;
    this.dryRun = dryRun;
    this.checkpointDir = checkpointDir;
    if (checkpointDir != null) {
      checkpointDir.mkdirs();
    }
  }

  /** Patches only filings for the given CIKs (across all years). */
  public void patchSampleCiks(List<String> ciks) throws IOException {
    for (int y = 2000; y <= 2026; y++) {
      String yearDir = "s3://" + BUCKET + "/" + SEC_PREFIX + "year=" + y + "/";
      List<StorageProvider.FileEntry> entries;
      try {
        entries = storage.listFiles(yearDir, false);
      } catch (Exception e) {
        continue;
      }
      for (StorageProvider.FileEntry entry : entries) {
        if (!entry.getName().endsWith("_metadata.parquet")) {
          continue;
        }
        for (String cik : ciks) {
          if (entry.getName().startsWith(cik)) {
            patchFile(entry.getPath());
            break;
          }
        }
      }
    }
  }

  /** Patches all metadata files for a given year, skipping already-checkpointed paths. */
  @SuppressWarnings("UnusedVariable")
  public void patchYear(String year) throws IOException {
    String yearDir = "s3://" + BUCKET + "/" + SEC_PREFIX + "year=" + year + "/";
    LOGGER.info("Scanning year={}", year);

    Set<String> done = loadCheckpoint(year);
    if (!done.isEmpty()) {
      LOGGER.info("Checkpoint: resuming year={} ({} already done)", year, done.size());
    }

    List<StorageProvider.FileEntry> entries;
    try {
      entries = storage.listFiles(yearDir, false);
    } catch (Exception e) {
      LOGGER.warn("Could not list year={}: {}", year, e.getMessage());
      return;
    }

    for (StorageProvider.FileEntry entry : entries) {
      if (!entry.getName().endsWith("_metadata.parquet")) {
        continue;
      }
      if (done.contains(entry.getPath())) {
        filesSkipped++;
        continue;
      }
      PatchResult result = patchFile(entry.getPath());
      // Record every processed path so restarts skip it regardless of outcome.
      appendCheckpoint(year, entry.getPath());
    }
  }

  /** Reads, applies DQ fixes, and rewrites a single metadata parquet file. */
  public PatchResult patchFile(String s3Path) throws IOException {
    filesScanned++;
    File tempIn = File.createTempFile("patch_in_", ".parquet");
    File tempOut = File.createTempFile("patch_out_", ".parquet");
    try {
      // Download
      try (InputStream in = storage.openInputStream(s3Path);
           OutputStream out = Files.newOutputStream(tempIn.toPath())) {
        byte[] buf = new byte[65536];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
      }

      // Read
      Configuration conf = new Configuration();
      Schema schema;
      GenericRecord original;
      try (ParquetReader<GenericRecord> reader = AvroParquetReader
          .<GenericRecord>builder(HadoopInputFile.fromPath(
              new Path(tempIn.toURI()), conf))
          .withConf(conf)
          .build()) {
        original = reader.read();
        if (original == null) {
          filesSkipped++;
          return PatchResult.SKIPPED_EMPTY;
        }
        schema = original.getSchema();
      }

      // Check filing type
      Object ftObj = original.get("filing_type");
      String rawFilingType = ftObj != null ? ftObj.toString() : null;
      if (!isPatchable(rawFilingType)) {
        filesSkipped++;
        return PatchResult.SKIPPED_NOT_10KQ;
      }

      // Build patched record
      String cik = getStr(original, "cik");
      Map<String, String> edgarInfo = SecDataFetcher.getCompanyInfoForCik(cik);
      List<String> tickers = SecDataFetcher.getTickersForCik(cik);

      GenericRecord patched = new GenericData.Record(schema);
      for (Schema.Field field : schema.getFields()) {
        patched.put(field.name(), original.get(field.name()));
      }

      // Apply DQ fixes
      String normalizedFilingType = XbrlToParquetConverter.normalizeFilingType(rawFilingType);
      patched.put("filing_type", normalizedFilingType);

      String rawPeriod = getStr(original, "period_of_report");
      String normalizedPeriod = XbrlToParquetConverter.normalizeDateToIso(rawPeriod);
      if (normalizedPeriod != null) {
        patched.put("period_of_report", normalizedPeriod);
      }

      String rawFye = getStr(original, "fiscal_year_end");
      String normalizedFye = normalizeFiscalYearEndFromSubmissions(rawFye, edgarInfo);
      if (normalizedFye != null) {
        patched.put("fiscal_year_end", normalizedFye);
      }

      if (getStr(original, "ticker") == null) {
        String ticker = !tickers.isEmpty() ? tickers.get(0) : edgarInfo.get("ticker");
        patched.put("ticker", ticker);
      }

      if (getStr(original, "sic_code") == null) {
        patched.put("sic_code", edgarInfo.get("sic_code"));
      }

      // Derive fiscal_year from period_of_report if missing
      Object fyObj = original.get("fiscal_year");
      if (fyObj == null && normalizedPeriod != null && normalizedPeriod.length() >= 4) {
        try {
          int fy = Integer.parseInt(normalizedPeriod.substring(0, 4));
          patched.put("fiscal_year", fy);
        } catch (NumberFormatException ignored) {
          // leave null
        }
      }

      // Check if anything actually changed
      if (!recordChanged(original, patched, schema)) {
        filesSkipped++;
        return PatchResult.SKIPPED_NO_CHANGE;
      }

      if (dryRun) {
        LOGGER.info("[DRY-RUN] Would patch: {} filing_type={}->{} period={}->{} ticker={} sic={}",
            s3Path, rawFilingType, normalizedFilingType,
            rawPeriod, normalizedPeriod,
            patched.get("ticker"), patched.get("sic_code"));
        filesPatched++;
        return PatchResult.PATCHED_DRY_RUN;
      }

      // Write patched parquet to temp file (delete first — AvroParquetWriter won't overwrite)
      tempOut.delete();
      @SuppressWarnings("deprecation")
      ParquetWriter<GenericRecord> writer = AvroParquetWriter
          .<GenericRecord>builder(new Path(tempOut.toURI()))
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .build();
      try {
        writer.write(patched);
      } finally {
        writer.close();
      }

      // Upload back to R2
      try (InputStream in = Files.newInputStream(tempOut.toPath())) {
        storage.writeFile(s3Path, in);
      }

      filesPatched++;
      LOGGER.info("Patched: {} filing_type={}->{} period={}->{} ticker={} sic={}",
          s3Path, rawFilingType, normalizedFilingType,
          rawPeriod, normalizedPeriod,
          patched.get("ticker"), patched.get("sic_code"));
      return PatchResult.PATCHED;

    } finally {
      tempIn.delete();
      tempOut.delete();
    }
  }

  // ---- Checkpoint helpers ----

  private File checkpointFile(String year) {
    return new File(checkpointDir, "patch_" + year + ".ckpt");
  }

  private Set<String> loadCheckpoint(String year) {
    Set<String> done = new HashSet<>();
    if (checkpointDir == null) {
      return done;
    }
    File f = checkpointFile(year);
    if (!f.exists()) {
      return done;
    }
    try {
      for (String line : Files.readAllLines(f.toPath(), StandardCharsets.UTF_8)) {
        String trimmed = line.trim();
        if (!trimmed.isEmpty()) {
          done.add(trimmed);
        }
      }
    } catch (IOException e) {
      LOGGER.warn("Could not read checkpoint for year={}: {}", year, e.getMessage());
    }
    return done;
  }

  private void appendCheckpoint(String year, String s3Path) {
    if (checkpointDir == null) {
      return;
    }
    try (BufferedWriter w = new BufferedWriter(
        new java.io.OutputStreamWriter(
            new java.io.FileOutputStream(checkpointFile(year), true), StandardCharsets.UTF_8))) {
      w.write(s3Path);
      w.newLine();
    } catch (IOException e) {
      LOGGER.warn("Could not write checkpoint for {}: {}", s3Path, e.getMessage());
    }
  }

  // ---- Private helpers ----

  private static boolean isPatchable(String filingType) {
    return filingType != null && PATCHABLE_TYPES.contains(filingType.trim());
  }

  private static String getStr(GenericRecord record, String field) {
    Object v = record.get(field);
    if (v == null) {
      return null;
    }
    String s = v.toString().trim();
    return s.isEmpty() ? null : s;
  }

  private static String normalizeFiscalYearEndFromSubmissions(
      String rawFye, Map<String, String> edgarInfo) {
    // If already in --MM-DD format, return as-is
    if (rawFye != null && rawFye.startsWith("--")) {
      return rawFye;
    }
    // Try EDGAR submissions.json value (already in MMDD format)
    String submissionsFye = edgarInfo.get("fiscal_year_end_mmdd");
    if (submissionsFye != null) {
      return XbrlToParquetConverter.normalizeFiscalYearEnd(submissionsFye);
    }
    // Try to normalize the raw value (e.g. "September 30", "0930")
    return XbrlToParquetConverter.normalizeFiscalYearEnd(rawFye);
  }

  private static boolean recordChanged(GenericRecord original, GenericRecord patched,
      Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      Object o = original.get(field.name());
      Object p = patched.get(field.name());
      if (o == null && p == null) {
        continue;
      }
      if (o == null || p == null) {
        return true;
      }
      if (!o.toString().equals(p.toString())) {
        return true;
      }
    }
    return false;
  }

  /** Result of patching a single file. */
  public enum PatchResult {
    PATCHED,
    PATCHED_DRY_RUN,
    SKIPPED_EMPTY,
    SKIPPED_NOT_10KQ,
    SKIPPED_NO_CHANGE
  }
}
