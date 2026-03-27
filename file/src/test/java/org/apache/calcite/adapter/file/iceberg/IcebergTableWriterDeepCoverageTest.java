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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link IcebergTableWriter} targeting uncovered code paths:
 * constructor variants, commitDataFiles with empty list, commitDataFiles with
 * partition filter, bulkCommitDataFiles, stageFiles with empty staging directory,
 * ensureVersionHint, and error handling.
 */
@Tag("unit")
public class IcebergTableWriterDeepCoverageTest {

  @TempDir
  Path tempDir;

  private Table mockTable;
  private StorageProvider mockStorage;

  @BeforeEach
  void setUp() {
    mockTable = mock(Table.class);
    mockStorage = mock(StorageProvider.class);
    when(mockTable.location()).thenReturn(tempDir.toString());
    when(mockTable.name()).thenReturn("test_table");
  }

  // --- Constructor variants ---

  @Test
  void testConstructorTwoArg() {
    IcebergTableWriter writer = new IcebergTableWriter(mockTable, mockStorage);
    assertNotNull(writer);
  }

  @Test
  void testConstructorThreeArg() {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    IcebergTableWriter writer = new IcebergTableWriter(mockTable, mockStorage, conf);
    assertNotNull(writer);
  }

  // --- commitDataFiles with empty list ---

  @Test
  void testCommitDataFilesEmptyList() {
    IcebergTableWriter writer = new IcebergTableWriter(mockTable, mockStorage);
    // Should return immediately without calling table.newAppend()
    writer.commitDataFiles(Collections.<DataFile>emptyList(), null);
    verify(mockTable, never()).newAppend();
  }

  // --- commitDataFiles with partition filter ---

  @Test
  void testCommitDataFilesWithPartitionFilter() {
    IcebergTableWriter writer = new IcebergTableWriter(mockTable, mockStorage);

    AppendFiles mockAppend = mock(AppendFiles.class);
    when(mockTable.newAppend()).thenReturn(mockAppend);

    // Need to handle ensureVersionHint call
    when(mockTable.location()).thenReturn(tempDir.toString());

    DataFile mockDataFile = mock(DataFile.class);
    List<DataFile> dataFiles = new ArrayList<>();
    dataFiles.add(mockDataFile);

    Map<String, Object> partitionFilter = new HashMap<>();
    partitionFilter.put("year", "2024");

    writer.commitDataFiles(dataFiles, partitionFilter);

    verify(mockAppend).appendFile(mockDataFile);
    verify(mockAppend).commit();
  }

  // --- commitDataFiles without partition filter ---

  @Test
  void testCommitDataFilesWithoutPartitionFilter() {
    IcebergTableWriter writer = new IcebergTableWriter(mockTable, mockStorage);

    AppendFiles mockAppend = mock(AppendFiles.class);
    when(mockTable.newAppend()).thenReturn(mockAppend);

    DataFile mockDataFile = mock(DataFile.class);
    List<DataFile> dataFiles = new ArrayList<>();
    dataFiles.add(mockDataFile);

    writer.commitDataFiles(dataFiles, null);

    verify(mockAppend).appendFile(mockDataFile);
    verify(mockAppend).commit();
  }

  @Test
  void testCommitDataFilesEmptyPartitionFilter() {
    IcebergTableWriter writer = new IcebergTableWriter(mockTable, mockStorage);

    AppendFiles mockAppend = mock(AppendFiles.class);
    when(mockTable.newAppend()).thenReturn(mockAppend);

    DataFile mockDataFile = mock(DataFile.class);
    List<DataFile> dataFiles = new ArrayList<>();
    dataFiles.add(mockDataFile);

    writer.commitDataFiles(dataFiles, Collections.<String, Object>emptyMap());

    verify(mockAppend).appendFile(mockDataFile);
    verify(mockAppend).commit();
  }

  // --- bulkCommitDataFiles ---

  @Test
  void testBulkCommitDataFilesEmpty() {
    IcebergTableWriter writer = new IcebergTableWriter(mockTable, mockStorage);
    // Empty list should not throw
    writer.bulkCommitDataFiles(Collections.<DataFile>emptyList());
    verify(mockTable, never()).newAppend();
  }

  @Test
  void testBulkCommitDataFilesMultipleFiles() {
    IcebergTableWriter writer = new IcebergTableWriter(mockTable, mockStorage);

    AppendFiles mockAppend = mock(AppendFiles.class);
    when(mockTable.newAppend()).thenReturn(mockAppend);

    DataFile mockFile1 = mock(DataFile.class);
    DataFile mockFile2 = mock(DataFile.class);
    DataFile mockFile3 = mock(DataFile.class);

    List<DataFile> dataFiles = new ArrayList<>();
    dataFiles.add(mockFile1);
    dataFiles.add(mockFile2);
    dataFiles.add(mockFile3);

    writer.bulkCommitDataFiles(dataFiles);

    verify(mockAppend).appendFile(mockFile1);
    verify(mockAppend).appendFile(mockFile2);
    verify(mockAppend).appendFile(mockFile3);
    verify(mockAppend).commit();
  }

  // --- stageFiles with empty staging directory ---

  @Test
  void testStageFilesEmptyDirectory() throws IOException {
    IcebergTableWriter writer = new IcebergTableWriter(mockTable, mockStorage);

    String stagingPath = tempDir.resolve("empty-staging").toString();
    java.io.File stagingDir = new java.io.File(stagingPath);
    stagingDir.mkdirs();

    // Mock storage provider to handle createDirectories
    doNothing().when(mockStorage).createDirectories(anyString());

    // Mock listFiles to return empty
    when(mockStorage.listFiles(anyString(), anyBoolean())).thenReturn(Collections.<StorageProvider.FileEntry>emptyList());

    List<DataFile> result = writer.stageFiles(stagingPath);
    assertTrue(result.isEmpty());
  }

  // --- commitFromStaging with empty staging ---

  @Test
  void testCommitFromStagingEmptyDirectory() throws IOException {
    IcebergTableWriter writer = new IcebergTableWriter(mockTable, mockStorage);

    String stagingPath = tempDir.resolve("empty-staging-2").toString();
    java.io.File stagingDir = new java.io.File(stagingPath);
    stagingDir.mkdirs();

    doNothing().when(mockStorage).createDirectories(anyString());
    when(mockStorage.listFiles(anyString(), anyBoolean())).thenReturn(Collections.<StorageProvider.FileEntry>emptyList());

    // commitFromStaging with empty files should not call commitDataFiles
    writer.commitFromStaging(stagingPath, null);
    verify(mockTable, never()).newAppend();
  }
}
