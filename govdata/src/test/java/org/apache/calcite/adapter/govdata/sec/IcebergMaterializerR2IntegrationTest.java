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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.iceberg.IcebergMaterializer;
import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.file.storage.S3StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test that exercises {@link IcebergMaterializer} end-to-end against
 * the live R2 bucket using real credentials. This test validates that the DuckDB
 * {@code CREATE SECRET} API works for reading parquet files from R2 via the Java
 * JDBC path (NOT the DuckDB CLI).
 *
 * <p>Required environment variables:
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID} — R2 access key</li>
 *   <li>{@code AWS_SECRET_ACCESS_KEY} — R2 secret key</li>
 *   <li>{@code AWS_ENDPOINT_OVERRIDE} — R2 endpoint URL (e.g. {@code https://xxx.r2.cloudflarestorage.com})</li>
 * </ul>
 *
 * <p>The test writes to an isolated CI path under {@code s3://govdata-parquet-v1/ci-test/}
 * and reads Apple (CIK 0000320193) filings from {@code year=2024} only, keeping the
 * run fast and side-effect-free for production data.
 */
@Tag("integration")
public class IcebergMaterializerR2IntegrationTest {

  private static final String BUCKET = "govdata-parquet-v1";
  // Use CIK prefix in glob to read only Apple filings — avoids scanning all year=2024 files.
  // Files are named {cik}_{accession}_metadata.parquet, so 0000320193_* matches only Apple.
  private static final String SOURCE_PATTERN =
      "s3://" + BUCKET + "/source=sec/year=2024/0000320193_*_metadata.parquet";
  private static final String WAREHOUSE =
      "s3a://" + BUCKET + "/ci-test/iceberg-r2-validate";
  private static final String TABLE_ID = "filing_metadata_ci_test";
  private static final String BATCH_TABLE_ID = "filing_metadata_ci_batch_test";

  // Apple Inc. — small, well-known, reliably has 2024 filings.
  private static final String ROW_FILTER = "cik = '0000320193'";

  private String accessKey;
  private String secretKey;
  private String endpoint;

  @BeforeEach
  public void requireCredentials() {
    accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");

    assumeTrue(accessKey != null && !accessKey.isEmpty(),
        "AWS_ACCESS_KEY_ID not set — skipping R2 integration test");
    assumeTrue(secretKey != null && !secretKey.isEmpty(),
        "AWS_SECRET_ACCESS_KEY not set — skipping R2 integration test");
    assumeTrue(endpoint != null && !endpoint.isEmpty(),
        "AWS_ENDPOINT_OVERRIDE not set — skipping R2 integration test");
  }

  @Test
  public void testMaterializeFilingMetadataFromR2() throws IOException {
    // Build S3StorageProvider with R2 credentials — same path as production worker
    Map<String, Object> storageConfig = new HashMap<String, Object>();
    storageConfig.put("accessKeyId", accessKey);
    storageConfig.put("secretAccessKey", secretKey);
    storageConfig.put("endpoint", endpoint);
    storageConfig.put("region", "auto");
    S3StorageProvider storageProvider = new S3StorageProvider(storageConfig);

    // Build a minimal filing_metadata column set (all columns from sec-schema.yaml)
    List<IcebergCatalogManager.ColumnDef> tableColumns = Arrays.asList(
        new IcebergCatalogManager.ColumnDef("cik", "string"),
        new IcebergCatalogManager.ColumnDef("accession_number", "string"),
        new IcebergCatalogManager.ColumnDef("filing_type", "string"),
        new IcebergCatalogManager.ColumnDef("filing_date", "string"),
        new IcebergCatalogManager.ColumnDef("year", "int"),
        new IcebergCatalogManager.ColumnDef("primary_document", "string"),
        new IcebergCatalogManager.ColumnDef("company_name", "string"),
        new IcebergCatalogManager.ColumnDef("period_of_report", "string"),
        new IcebergCatalogManager.ColumnDef("acceptance_datetime", "string"),
        new IcebergCatalogManager.ColumnDef("file_size", "long"),
        new IcebergCatalogManager.ColumnDef("fiscal_year", "int"),
        new IcebergCatalogManager.ColumnDef("state_of_incorporation", "string"),
        new IcebergCatalogManager.ColumnDef("fiscal_year_end", "string"),
        new IcebergCatalogManager.ColumnDef("business_address", "string"),
        new IcebergCatalogManager.ColumnDef("mailing_address", "string"),
        new IcebergCatalogManager.ColumnDef("phone", "string"),
        new IcebergCatalogManager.ColumnDef("sic_code", "string"),
        new IcebergCatalogManager.ColumnDef("irs_number", "string"),
        new IcebergCatalogManager.ColumnDef("ticker", "string")
    );

    // Partition by year (matches production config)
    List<PartitionedTableConfig.ColumnDefinition> partitionColumns =
        Collections.singletonList(new PartitionedTableConfig.ColumnDefinition("year", "int"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern(SOURCE_PATTERN)
            .sourceFormat(IcebergMaterializer.SourceFormat.PARQUET)
            .targetTableId(TABLE_ID)
            .sourceTableName("filing_metadata")
            .tableColumns(tableColumns)
            .partitionColumns(partitionColumns)
            .batchPartitionColumns(Collections.singletonList("year"))
            .incrementalKeys(Collections.singletonList("year"))
            .yearRange(2024, 2024)
            .rowFilter(ROW_FILTER)
            .icebergTableLocation(WAREHOUSE + "/" + TABLE_ID)
            .accessionColumn("accession_number")
            .description("filing_metadata CI test")
            .build();

    IcebergMaterializer materializer =
        new IcebergMaterializer(WAREHOUSE, storageProvider, IncrementalTracker.NOOP);

    IcebergMaterializer.MaterializationResult result = materializer.materialize(config);

    assertTrue(result.getSuccessCount() > 0,
        "Expected at least one successful batch, but got successCount="
            + result.getSuccessCount()
            + " failedCount=" + result.getFailedCount()
            + " skippedCount=" + result.getSkippedCount());
  }

  /**
   * Verifies the row-batching path ({@code processWithRowBatchingToIceberg}) works end-to-end.
   * Uses rowBatchSize=5 so Apple's ~13 filings are processed in 3 LIMIT/OFFSET pages,
   * confirming the OOM-prevention mechanism introduced for the production first run.
   */
  @Test
  public void testMaterializeWithRowBatching() throws IOException {
    Map<String, Object> storageConfig = new HashMap<String, Object>();
    storageConfig.put("accessKeyId", accessKey);
    storageConfig.put("secretAccessKey", secretKey);
    storageConfig.put("endpoint", endpoint);
    storageConfig.put("region", "auto");
    S3StorageProvider storageProvider = new S3StorageProvider(storageConfig);

    List<IcebergCatalogManager.ColumnDef> tableColumns = Arrays.asList(
        new IcebergCatalogManager.ColumnDef("cik", "string"),
        new IcebergCatalogManager.ColumnDef("accession_number", "string"),
        new IcebergCatalogManager.ColumnDef("filing_type", "string"),
        new IcebergCatalogManager.ColumnDef("filing_date", "string"),
        new IcebergCatalogManager.ColumnDef("year", "int"),
        new IcebergCatalogManager.ColumnDef("primary_document", "string"),
        new IcebergCatalogManager.ColumnDef("company_name", "string"),
        new IcebergCatalogManager.ColumnDef("period_of_report", "string"),
        new IcebergCatalogManager.ColumnDef("acceptance_datetime", "string"),
        new IcebergCatalogManager.ColumnDef("file_size", "long"),
        new IcebergCatalogManager.ColumnDef("fiscal_year", "int"),
        new IcebergCatalogManager.ColumnDef("state_of_incorporation", "string"),
        new IcebergCatalogManager.ColumnDef("fiscal_year_end", "string"),
        new IcebergCatalogManager.ColumnDef("business_address", "string"),
        new IcebergCatalogManager.ColumnDef("mailing_address", "string"),
        new IcebergCatalogManager.ColumnDef("phone", "string"),
        new IcebergCatalogManager.ColumnDef("sic_code", "string"),
        new IcebergCatalogManager.ColumnDef("irs_number", "string"),
        new IcebergCatalogManager.ColumnDef("ticker", "string")
    );

    List<PartitionedTableConfig.ColumnDefinition> partitionColumns =
        Collections.singletonList(new PartitionedTableConfig.ColumnDefinition("year", "int"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern(SOURCE_PATTERN)
            .sourceFormat(IcebergMaterializer.SourceFormat.PARQUET)
            .targetTableId(BATCH_TABLE_ID)
            .sourceTableName("filing_metadata")
            .tableColumns(tableColumns)
            .partitionColumns(partitionColumns)
            .batchPartitionColumns(Collections.singletonList("year"))
            .incrementalKeys(Collections.singletonList("year"))
            .yearRange(2024, 2024)
            .rowFilter(ROW_FILTER)
            .rowBatchSize(5)  // Forces LIMIT/OFFSET paging: 3 pages over Apple's ~13 rows
            .icebergTableLocation(WAREHOUSE + "/" + BATCH_TABLE_ID)
            .accessionColumn("accession_number")
            .description("filing_metadata row-batch CI test")
            .build();

    IcebergMaterializer materializer =
        new IcebergMaterializer(WAREHOUSE, storageProvider, IncrementalTracker.NOOP);

    IcebergMaterializer.MaterializationResult result = materializer.materialize(config);

    assertEquals(0, result.getFailedCount(),
        "Expected no failed batches with row batching enabled");
    assertTrue(result.getSuccessCount() > 0,
        "Expected at least one successful batch with row batching, got successCount="
            + result.getSuccessCount());
  }
}
