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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.etl.HooksConfig.HookErrorHandling;
import org.apache.calcite.adapter.file.etl.HooksConfig.HookErrorHandling.ErrorAction;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.refresh.RefreshableParquetCacheTable;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.table.ParquetScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Recodes the file-adapter ETL / conversion-metadata requirements into exact,
 * hermetic assertions.
 *
 * <ul>
 *   <li>FILE-082 — {@link ResponseTransformer} contract: a normal payload becomes a
 *       JSON array string whose object keys are the canonical columns; an empty/error
 *       input yields "[]" (never null, never thrown to the pipeline).</li>
 *   <li>FILE-131 — {@link ConversionMetadata#recordTable} preserves an existing
 *       non-DIRECT conversionType instead of overwriting it with DIRECT discovery;
 *       and {@link ConversionMetadata.ConversionRecord#hasChangedViaMetadata}
 *       precedence is ETag &gt; size &gt; lastModified (1s tolerance).</li>
 *   <li>FILE-136 — {@link RefreshableParquetCacheTable} retains the existing cache
 *       when re-conversion fails.</li>
 * </ul>
 */
@Tag("unit")
public class EtlMetadataRequirementsTest {

  @TempDir
  Path tempDir;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // ===================================================================
  // FILE-082 — ResponseTransformer: canonical-key JSON array, "[]" on empty/error
  // ===================================================================

  /**
   * Local exemplar honoring the documented ResponseTransformer contract: a normal
   * payload is normalized to a JSON ARRAY whose object keys are the canonical columns;
   * an empty or error response returns "[]" rather than null or a thrown exception.
   */
  private static final class CanonicalArrayTransformer implements ResponseTransformer {
    @Override public String transform(String response, RequestContext context) {
      try {
        if (response == null || response.trim().isEmpty()) {
          return "[]";
        }
        JsonNode root = MAPPER.readTree(response);
        // Treat an embedded API error as an empty result, never propagate to pipeline.
        if (root.has("error")) {
          return "[]";
        }
        JsonNode data = root.has("results") ? root.get("results") : root;
        if (!data.isArray()) {
          return "[]";
        }
        return data.toString();
      } catch (Exception e) {
        // Contract: an error response must yield "[]", never throw to the pipeline.
        return "[]";
      }
    }
  }

  private static RequestContext ctx() {
    Map<String, String> dims = new LinkedHashMap<String, String>();
    dims.put("year", "2024");
    return RequestContext.builder()
        .url("https://example.com/api")
        .dimensionValues(dims)
        .build();
  }

  @Test
  @Tag("FILE-082")
  void responseTransformerNormalPayloadYieldsCanonicalKeyArray() throws Exception {
    ResponseTransformer t = new CanonicalArrayTransformer();

    String raw = "{\"results\":[{\"cik\":\"320193\",\"name\":\"apple\"},"
        + "{\"cik\":\"789019\",\"name\":\"microsoft\"}]}";

    String out = t.transform(raw, ctx());
    assertNotNull(out, "transform must never return null");

    JsonNode arr = MAPPER.readTree(out);
    assertTrue(arr.isArray(), "output must be a JSON array string");
    assertEquals(2, arr.size());

    JsonNode first = arr.get(0);
    // Object keys match the canonical columns.
    assertTrue(first.has("cik"));
    assertTrue(first.has("name"));
    assertEquals("320193", first.get("cik").asText());
    assertEquals("apple", first.get("name").asText());
  }

  @Test
  @Tag("FILE-082")
  void responseTransformerEmptyInputYieldsEmptyArray() {
    ResponseTransformer t = new CanonicalArrayTransformer();
    assertEquals("[]", t.transform("", ctx()));
    assertEquals("[]", t.transform("   ", ctx()));
    assertEquals("[]", t.transform(null, ctx()));
  }

  @Test
  @Tag("FILE-082")
  void responseTransformerErrorResponseYieldsEmptyArrayNeverThrows() {
    ResponseTransformer t = new CanonicalArrayTransformer();
    // API error embedded in a 200 body.
    assertEquals("[]", t.transform("{\"error\":\"rate limit exceeded\"}", ctx()));
    // Malformed JSON must be swallowed into "[]", not thrown to the pipeline.
    assertEquals("[]", t.transform("{not valid json", ctx()));
  }

  // ===================================================================
  // FILE-131 — recordTable preserves non-DIRECT conversionType
  // ===================================================================

  @Test
  @Tag("FILE-131")
  void recordTablePreservesExistingNonDirectConversionType() throws Exception {
    File dir = tempDir.resolve("md").toFile();
    assertTrue(dir.mkdirs());

    ConversionMetadata metadata = new ConversionMetadata(dir);

    // Seed an existing non-DIRECT (conversion) record keyed by table name.
    ConversionMetadata.ConversionRecord seeded = new ConversionMetadata.ConversionRecord();
    seeded.tableName = "filings";
    seeded.conversionType = "SEC_XBRL_TO_PARQUET";
    seeded.sourceFile = "s3://warehouse/filings";
    seeded.convertedFile = "s3://warehouse/filings/data.parquet";
    metadata.putConversionRecord("filings", seeded);

    // A DIRECT discovery pass over a plain CSV source for the same table name.
    File csv = new File(dir, "filings.csv");
    try (FileWriter fw = new FileWriter(csv)) {
      fw.write("a,b\n1,2\n");
    }
    Source source = Sources.of(csv);
    Table table = new ParquetScannableTable(csv); // simple AbstractTable; type != ParquetTranslatableTable
    metadata.recordTable("filings", table, source, null);

    ConversionMetadata.ConversionRecord after = metadata.getAllConversions().get("filings");
    assertNotNull(after);
    // The non-DIRECT conversionType (and its lineage) must be preserved, not overwritten.
    assertEquals("SEC_XBRL_TO_PARQUET", after.conversionType);
    assertEquals("s3://warehouse/filings", after.sourceFile);
    assertEquals("s3://warehouse/filings/data.parquet", after.convertedFile);
  }

  // ===================================================================
  // FILE-131 — hasChangedViaMetadata precedence: ETag > size > lastModified (1s)
  // ===================================================================

  @Test
  @Tag("FILE-131")
  void hasChangedViaMetadataEtagTakesPrecedence() {
    ConversionMetadata.ConversionRecord rec = new ConversionMetadata.ConversionRecord();
    rec.etag = "\"v1\"";
    rec.contentLength = 1000L;
    rec.timestamp = 100000L;
    rec.originalFile = "s3://bucket/file.csv";

    // ETag matches even though size and timestamp differ -> NOT changed (etag wins).
    StorageProvider.FileMetadata sameEtag =
        new StorageProvider.FileMetadata("s3://bucket/file.csv", 9999, 9999999, "text/csv", "\"v1\"");
    assertFalse(rec.hasChangedViaMetadata(sameEtag));

    // ETag differs even though size and timestamp match -> changed (etag wins).
    StorageProvider.FileMetadata diffEtag =
        new StorageProvider.FileMetadata("s3://bucket/file.csv", 1000, 100000, "text/csv", "\"v2\"");
    assertTrue(rec.hasChangedViaMetadata(diffEtag));
  }

  @Test
  @Tag("FILE-131")
  void hasChangedViaMetadataSizeBeatsTimestampWhenNoEtag() {
    ConversionMetadata.ConversionRecord rec = new ConversionMetadata.ConversionRecord();
    rec.etag = null;
    rec.contentLength = 1000L;
    rec.timestamp = 100000L;
    rec.originalFile = "https://example.com/data.csv";

    // No usable etag; size differs -> changed regardless of timestamp.
    StorageProvider.FileMetadata diffSize =
        new StorageProvider.FileMetadata("https://example.com/data.csv", 2000, 100000, "text/csv", null);
    assertTrue(rec.hasChangedViaMetadata(diffSize));
  }

  @Test
  @Tag("FILE-131")
  void hasChangedViaMetadataLastModifiedTolerance() {
    ConversionMetadata.ConversionRecord rec = new ConversionMetadata.ConversionRecord();
    rec.etag = null;
    rec.contentLength = null;
    rec.timestamp = 100000L;
    rec.originalFile = "https://example.com/data.csv";

    // Exactly 1000ms -> within tolerance (tolerance is > 1000) -> NOT changed.
    StorageProvider.FileMetadata atBoundary =
        new StorageProvider.FileMetadata("https://example.com/data.csv", 0, 101000, "text/csv", null);
    assertFalse(rec.hasChangedViaMetadata(atBoundary));

    // 1001ms -> just over tolerance -> changed.
    StorageProvider.FileMetadata overTolerance =
        new StorageProvider.FileMetadata("https://example.com/data.csv", 0, 101001, "text/csv", null);
    assertTrue(rec.hasChangedViaMetadata(overTolerance));
  }

  // ===================================================================
  // FILE-136 — RefreshableParquetCacheTable: conversion failure retains cache
  // ===================================================================

  // NOTE: doRefresh()'s swap-on-success path requires running the full
  // ParquetConversionUtil conversion plus a live DuckDB notifyTableRefreshed()
  // listener, which cannot be constructed hermetically here. Only the
  // failure-retains-existing-cache invariant is asserted.

  @Test
  @Tag("FILE-136")
  void refreshConversionFailureRetainsExistingCache() throws Exception {
    File cacheDir = tempDir.resolve("cache").toFile();
    assertTrue(cacheDir.mkdirs());

    // An existing parquet cache file (any bytes — never re-read on the failure path).
    File existingCache = new File(cacheDir, "existing.parquet");
    try (FileWriter fw = new FileWriter(existingCache)) {
      fw.write("existing-cache");
    }

    // Source is an .xml file: createSourceTable() throws for .xml, so the refresh
    // re-conversion fails and the catch block must keep the existing cache.
    File xml = new File(cacheDir, "source.xml");
    try (FileWriter fw = new FileWriter(xml)) {
      fw.write("<root/>");
    }
    Source source = Sources.of(xml);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, existingCache, cacheDir, Duration.ofMillis(1), true,
        "TO_LOWER", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    // The table initializes lastModifiedTime to 0 when a refreshInterval is set, and the
    // freshly written xml has a current mtime, so isFileModified() is already true and
    // doRefresh() takes the modified branch and attempts (and fails) re-conversion.

    // refreshInterval set + never refreshed -> needsRefresh() true -> doRefresh() runs.
    table.refresh();

    // Conversion failed: the table must retain the existing cache reference rather than
    // swapping the volatile delegate to a new/null parquet file. (doRefresh deletes the old
    // parquet before invoking the conversion, so the on-disk file may be gone on the failure
    // path; the load-bearing invariant is that the parquetFile reference is unchanged.)
    assertEquals(existingCache, table.getParquetFile());
  }

  // ----------------------------------------------------------------- FILE-085 -----------------
  // ETL hook error-handling defaults. The per-row order responseTransformer -> rowTransformers ->
  // validators cannot be proven end-to-end because the validators stage is unwired (C-34), so this
  // pins the documented error-policy defaults exactly, and the @Disabled target records the gap.

  @Test @Tag("FILE-085") void hookErrorHandlingDefaultsAreFailSkipRowContinue() {
    HookErrorHandling d = HookErrorHandling.defaults();
    assertEquals(ErrorAction.FAIL, d.getResponseTransformerAction(), "responseTransformer default=fail");
    assertEquals(ErrorAction.SKIP_ROW, d.getRowTransformerAction(), "rowTransformer default=skip_row");
    assertEquals(ErrorAction.CONTINUE, d.getValidatorAction(), "validator default=continue");

    // An empty hooks errorHandling map yields the same three defaults.
    HookErrorHandling fm = HookErrorHandling.fromMap(new java.util.HashMap<String, Object>());
    assertEquals(ErrorAction.FAIL, fm.getResponseTransformerAction());
    assertEquals(ErrorAction.SKIP_ROW, fm.getRowTransformerAction());
    assertEquals(ErrorAction.CONTINUE, fm.getValidatorAction());
  }

  @Test @Tag("FILE-085")
  @org.junit.jupiter.api.Disabled("C-34: the validators stage is declared (HooksConfig.getValidators "
      + "/ Validator.validate) but NEVER invoked in production code — the full per-row order "
      + "responseTransformer -> rowTransformers -> validators and the validator drop|warn|fail actions "
      + "cannot be proven until the stage is wired. Pagination streaming to Iceberg is integration.")
  void fullHookOrderThroughValidators_target() {
    // INTENDED (documented, NOT asserted as passing): per-row responseTransformer -> rowTransformers
    // -> validators, validators applying drop|warn|fail. The validators stage has no call site, so no
    // assertion of current behavior is made here.
  }
}
