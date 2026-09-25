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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * A document-source pipeline hands its {@link DataWriter} no row iterator, so a writer that
 * reports the "use default MaterializationWriter" sentinel ({@code -1}) wrote nothing.
 */
@Tag("unit")
class DocumentSourceRowCountTest {

  @TempDir
  Path tempDir;

  @Test void documentSourceWithSentinelWriter_reportsZeroRows() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2023)
        .end(2024)
        .build());
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("doc_row_count_pipeline")
        .sourceType(EtlPipelineConfig.SOURCE_TYPE_DOCUMENT)
        .source(HttpSourceConfig.builder().url("https://example.invalid/api").build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null,
            new DeferredItemsTest.MemoryTracker(), DataProvider.DEFAULT, DataWriter.DEFAULT);
    EtlResult result = pipeline.execute();

    assertFalse(result.isFailed(), "pipeline must succeed: " + result.getFailureMessage());
    assertEquals(2, result.getSuccessfulBatches());
    assertEquals(0, result.getTotalRows(),
        "sentinel -1 from the default writer must not be summed into the row count");
  }
}
