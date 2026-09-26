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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.Table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link IcebergTableWriter#pruneMetadataFiles} lists the metadata directory only once
 * per table per JVM and deletes by version key afterwards.
 */
@Tag("unit")
public class IcebergPruneMetadataFilesTest {

  @TempDir
  Path tempDir;

  private Table table;
  private StorageProvider storage;
  private String metadataDir;

  @BeforeEach
  void setUp() {
    table = mock(Table.class);
    storage = mock(StorageProvider.class);
    when(table.location()).thenReturn(tempDir.toString());
    when(table.name()).thenReturn("prune_table");
    metadataDir = tempDir + "/metadata/";
  }

  private String versionPath(int version) {
    return metadataDir + "v" + version + ".metadata.json";
  }

  private void hintAt(int version) throws IOException {
    when(storage.openInputStream(metadataDir + "version-hint.text")).thenAnswer(
        inv -> new ByteArrayInputStream(String.valueOf(version).getBytes(StandardCharsets.UTF_8)));
  }

  private List<StorageProvider.FileEntry> versionsUpTo(int newest) {
    List<StorageProvider.FileEntry> entries = new ArrayList<>();
    for (int v = 1; v <= newest; v++) {
      entries.add(new StorageProvider.FileEntry(versionPath(v), "v" + v + ".metadata.json",
          false, 10L, 0L));
    }
    return entries;
  }

  @Test void firstPruneListsThenLaterPrunesDeleteByKey() throws IOException {
    IcebergTableWriter writer = new IcebergTableWriter(table, storage);
    hintAt(30);
    when(storage.listFiles(metadataDir, false)).thenReturn(versionsUpTo(30));
    when(storage.delete(anyString())).thenReturn(true);

    assertEquals(20, writer.pruneMetadataFiles(10));
    verify(storage, times(1)).listFiles(metadataDir, false);
    for (int v = 1; v <= 20; v++) {
      verify(storage).delete(versionPath(v));
    }

    hintAt(35);
    assertEquals(5, writer.pruneMetadataFiles(10));
    verify(storage, times(1)).listFiles(anyString(), anyBoolean());
    for (int v = 21; v <= 25; v++) {
      verify(storage).delete(versionPath(v));
    }
    verify(storage, never()).delete(versionPath(26));
  }

  @Test void failedDeleteIsRetriedByNextPrune() throws IOException {
    IcebergTableWriter writer = new IcebergTableWriter(table, storage);
    hintAt(12);
    when(storage.listFiles(metadataDir, false)).thenReturn(versionsUpTo(12));
    when(storage.delete(anyString())).thenReturn(true);
    when(storage.delete(versionPath(2))).thenThrow(new IOException("boom"));

    assertEquals(2, writer.pruneMetadataFiles(10));

    doReturn(true).when(storage).delete(versionPath(2));
    hintAt(12);
    assertEquals(1, writer.pruneMetadataFiles(10));
    verify(storage, times(1)).listFiles(anyString(), anyBoolean());
    verify(storage, times(2)).delete(versionPath(2));
  }
}
