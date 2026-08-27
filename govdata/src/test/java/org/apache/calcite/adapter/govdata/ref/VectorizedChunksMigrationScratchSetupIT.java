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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.iceberg.IcebergTableWriter;
import org.apache.calcite.adapter.file.storage.S3StorageProvider;

import org.apache.iceberg.Table;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * One-off setup, NOT part of the real migration: builds a tiny scratch
 * {@code ref.vectorized_chunks}-shaped table in the DQ bucket, pre-migration shape (no
 * ingest_ts), with a deliberate duplicate chunk_id — so
 * {@link VectorizedChunksIngestTsMigrationIT} can be dry-run against it end-to-end before ever
 * pointing at the real 25M-row production table. Run once, then point the migration test's
 * MIGRATE_WAREHOUSE_PATH / MIGRATE_SOURCE_ICEBERG_PATH env vars at the same scratch location.
 */
@Tag("integration")
public class VectorizedChunksMigrationScratchSetupIT {

  private static final String WAREHOUSE_PATH = "s3a://govdata-parquet-v1-dq/scratch-migration-test";
  private static final String TABLE_ID = "vectorized_chunks";

  @Test
  public void createScratchTable() throws Exception {
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
    assumeTrue(accessKey != null && !accessKey.isEmpty(), "AWS_ACCESS_KEY_ID not set");
    assumeTrue(secretKey != null && !secretKey.isEmpty(), "AWS_SECRET_ACCESS_KEY not set");
    assumeTrue(endpoint != null && !endpoint.isEmpty(), "AWS_ENDPOINT_OVERRIDE not set");

    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("accessKeyId", accessKey);
    storageConfig.put("secretAccessKey", secretKey);
    storageConfig.put("endpoint", endpoint);
    storageConfig.put("region", "auto");
    S3StorageProvider storageProvider = new S3StorageProvider(storageConfig);

    List<IcebergCatalogManager.ColumnDef> columns = Arrays.asList(
        new IcebergCatalogManager.ColumnDef("chunk_id", "string"),
        new IcebergCatalogManager.ColumnDef("source_schema", "string"),
        new IcebergCatalogManager.ColumnDef("source_table", "string"),
        new IcebergCatalogManager.ColumnDef("stringified_fk", "string"),
        new IcebergCatalogManager.ColumnDef("sequence", "long"),
        new IcebergCatalogManager.ColumnDef("source_type", "string"),
        new IcebergCatalogManager.ColumnDef("chunk_text", "string"),
        new IcebergCatalogManager.ColumnDef("enriched_text", "string"),
        new IcebergCatalogManager.ColumnDef("section", "string"),
        new IcebergCatalogManager.ColumnDef("subsection", "string"),
        new IcebergCatalogManager.ColumnDef("section_path", "string"),
        new IcebergCatalogManager.ColumnDef("paragraph_continuation", "boolean"),
        new IcebergCatalogManager.ColumnDef("speaker_name", "string"),
        new IcebergCatalogManager.ColumnDef("speaker_role", "string"),
        new IcebergCatalogManager.ColumnDef("exhibit_number", "string"),
        new IcebergCatalogManager.ColumnDef("financial_concepts", "string"),
        new IcebergCatalogManager.ColumnDef("paragraph_number", "long"),
        new IcebergCatalogManager.ColumnDef("content_type", "string"),
        new IcebergCatalogManager.ColumnDef("cik", "string"),
        new IcebergCatalogManager.ColumnDef("accession_number", "string"),
        new IcebergCatalogManager.ColumnDef("filing_date", "string"),
        new IcebergCatalogManager.ColumnDef("ref_naics_code", "string"),
        new IcebergCatalogManager.ColumnDef("fedregister_document_number", "string"));

    Map<String, Object> catalogConfig = new HashMap<>();
    catalogConfig.put("warehousePath", WAREHOUSE_PATH);
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.s3a.access.key", accessKey);
    hadoopConfig.put("fs.s3a.secret.key", secretKey);
    hadoopConfig.put("fs.s3a.endpoint", endpoint);
    hadoopConfig.put("fs.s3a.path.style.access", "true");
    catalogConfig.put("hadoopConfig", hadoopConfig);

    Table table = IcebergCatalogManager.createTableFromColumns(catalogConfig, TABLE_ID, columns,
        Collections.singletonList("source_schema"));
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> secRows = new ArrayList<>();
    secRows.add(chunkRow("dup-chunk-1", "sec", "vectorized_chunks", "0000320193:0001-A", 0L,
        "row_concat", "first copy of a duplicated chunk", "0000320193", "0001-A"));
    secRows.add(chunkRow("dup-chunk-1", "sec", "vectorized_chunks", "0000320193:0001-A", 0L,
        "row_concat", "SAME chunk_id written a second time (simulated reprocess duplicate)",
        "0000320193", "0001-A"));
    secRows.add(chunkRow("unique-chunk-2", "sec", "vectorized_chunks", "0000320193:0002-B", 0L,
        "row_concat", "a normal, non-duplicated chunk", "0000320193", "0002-B"));
    org.apache.iceberg.DataFile secFile =
        writer.writeRecords(secRows, Collections.singletonMap("source_schema", "sec"));

    List<Map<String, Object>> refRows = new ArrayList<>();
    refRows.add(chunkRow("ref-chunk-1", "ref", "gleif_entities", "LEI123", 0L,
        "row_concat", "a ref-schema chunk, never duplicated", null, null));
    org.apache.iceberg.DataFile refFile =
        writer.writeRecords(refRows, Collections.singletonMap("source_schema", "ref"));

    // writeRecords only writes the parquet file and returns its DataFile handle -- it is not
    // linked into the table (and so not visible to any scan) until committed.
    writer.commitDataFiles(Arrays.asList(secFile, refFile), null);

    System.err.println("[scratch-setup] created " + WAREHOUSE_PATH + "/" + TABLE_ID
        + " with " + (secRows.size() + refRows.size()) + " row(s), including one duplicate chunk_id");
  }

  private static Map<String, Object> chunkRow(String chunkId, String sourceSchema,
      String sourceTable, String stringifiedFk, Long sequence, String sourceType,
      String chunkText, String cik, String accessionNumber) {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("chunk_id", chunkId);
    row.put("source_schema", sourceSchema);
    row.put("source_table", sourceTable);
    row.put("stringified_fk", stringifiedFk);
    row.put("sequence", sequence);
    row.put("source_type", sourceType);
    row.put("chunk_text", chunkText);
    row.put("enriched_text", chunkText);
    row.put("cik", cik);
    row.put("accession_number", accessionNumber);
    return row;
  }
}
