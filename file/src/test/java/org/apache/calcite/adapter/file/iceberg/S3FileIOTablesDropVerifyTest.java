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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileInfo;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link IcebergCatalogManager#dropTable} routes every other S3-warehouse operation
 * (load/create/exists) through {@link S3FileIOTables}, but until this fix it always dropped
 * through {@code HadoopCatalog} instead — a separate client stack from the one that reads,
 * writes and creates the same table. Its single-shot recursive delete has no defense against
 * MinIO's S3 LIST lagging a delete (the same lag {@link S3FileIOTablesVersionHintRaceTest}
 * covers for reads), so a file the list missed silently survives the "drop" and coexists with
 * the fresh lineage the following create writes into the same directory — reproducing exactly
 * the interleaved-lineage defect (kenstott/govdata-ops#351).
 *
 * <p>These tests exercise {@link S3FileIOTables#dropPrefix}, the delete+verify loop that closes
 * that gap, against a mocked {@code S3FileIO} (real S3FileIO instantiation needs a live/mock S3
 * endpoint neither available nor needed to prove the retry logic itself).
 */
@Tag("unit")
class S3FileIOTablesDropVerifyTest {

  private static final String PREFIX = "s3://bucket/schema/table/";

  private static FileInfo fileInfo(String location) {
    FileInfo info = mock(FileInfo.class);
    when(info.location()).thenReturn(location);
    return info;
  }

  @Test void emptyAfterFirstDeleteReturnsWithoutRetrying() {
    S3FileIO io = mock(S3FileIO.class);
    when(io.listPrefix(anyString())).thenReturn(Collections.<FileInfo>emptyList());

    S3FileIOTables.dropPrefix(io, PREFIX);

    verify(io, times(1)).deletePrefix(PREFIX);
    verify(io, times(1)).listPrefix(PREFIX);
  }

  @Test void aLeftoverObjectOnFirstListIsSweptUpByARetriedDeletePlusVerify() {
    S3FileIO io = mock(S3FileIO.class);
    List<FileInfo> leftover = Collections.singletonList(fileInfo(PREFIX + "metadata/v3.metadata.json"));
    when(io.listPrefix(anyString()))
        .thenReturn(leftover, Collections.<FileInfo>emptyList());

    S3FileIOTables.dropPrefix(io, PREFIX);

    verify(io, times(2)).deletePrefix(PREFIX);
    verify(io, times(2)).listPrefix(PREFIX);
  }

  @Test void objectsStillListedAfterMaxAttemptsThrowsInsteadOfReturningSilently() {
    S3FileIO io = mock(S3FileIO.class);
    List<FileInfo> leftover = Collections.singletonList(fileInfo(PREFIX + "metadata/v3.metadata.json"));
    when(io.listPrefix(anyString())).thenReturn(leftover);

    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> S3FileIOTables.dropPrefix(io, PREFIX));
    assertEquals(true, e.getMessage().contains(PREFIX));
  }
}
