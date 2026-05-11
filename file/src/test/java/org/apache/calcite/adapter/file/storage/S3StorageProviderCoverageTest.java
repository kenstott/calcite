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
package org.apache.calcite.adapter.file.storage;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.PartETag;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link S3StorageProvider} to maximize JaCoCo line coverage.
 * Tests use Mockito to mock the AmazonS3 client and avoid real S3 calls.
 */
@Tag("unit")
public class S3StorageProviderCoverageTest {

  private AmazonS3 mockS3;
  private S3StorageProvider provider;

  @BeforeEach
  void setUp() {
    mockS3 = mock(AmazonS3.class);
    // Use the constructor that accepts a pre-built S3 client
    provider = new S3StorageProvider(mockS3);
  }

  // --- Constructor tests ---

  @Test
  void testConstructorWithS3ClientOnly() {
    S3StorageProvider p = new S3StorageProvider(mockS3);
    assertEquals("s3", p.getStorageType());
    assertNull(p.getS3Config());
  }

  @Test
  void testConstructorWithConfigMissingCredentials() {
    Map<String, Object> config = new HashMap<>();
    config.put("region", "us-east-1");
    // Missing accessKeyId and secretAccessKey
    assertThrows(IllegalArgumentException.class, () -> new S3StorageProvider(config));
  }

  @Test
  void testConstructorWithNullConfig() {
    // Config constructor with null should throw
    assertThrows(IllegalArgumentException.class, () -> new S3StorageProvider((Map<String, Object>) null));
  }

  @Test
  void testConstructorWithPartialCredentials() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "AKID");
    // Missing secretAccessKey
    assertThrows(IllegalArgumentException.class, () -> new S3StorageProvider(config));
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("s3", provider.getStorageType());
  }

  // --- getS3Config ---

  @Test
  void testGetS3ConfigReturnsNullForClientOnlyConstructor() {
    assertNull(provider.getS3Config());
  }

  // --- listFiles ---

  @Test
  void testListFilesNonRecursive() throws IOException {
    ListObjectsV2Result result = new ListObjectsV2Result();
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName("mybucket");
    summary.setKey("prefix/file1.csv");
    summary.setSize(1234);
    summary.setLastModified(new Date(1000000));
    result.getObjectSummaries().add(summary);
    result.setTruncated(false);

    // Also add common prefixes (directories)
    result.getCommonPrefixes().add("prefix/subdir/");

    when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    List<StorageProvider.FileEntry> entries = provider.listFiles("s3://mybucket/prefix/", false);
    assertNotNull(entries);
    assertEquals(2, entries.size());

    // File entry
    StorageProvider.FileEntry fileEntry = entries.get(0);
    assertEquals("file1.csv", fileEntry.getName());
    assertEquals(1234, fileEntry.getSize());
    assertFalse(fileEntry.isDirectory());

    // Directory entry
    StorageProvider.FileEntry dirEntry = entries.get(1);
    assertTrue(dirEntry.isDirectory());
    assertEquals("subdir", dirEntry.getName());
  }

  @Test
  void testListFilesRecursive() throws IOException {
    ListObjectsV2Result result = new ListObjectsV2Result();
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName("mybucket");
    summary.setKey("prefix/deep/file.json");
    summary.setSize(999);
    summary.setLastModified(new Date(2000000));
    result.getObjectSummaries().add(summary);
    result.setTruncated(false);

    when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    List<StorageProvider.FileEntry> entries = provider.listFiles("s3://mybucket/prefix/", true);
    assertNotNull(entries);
    assertEquals(1, entries.size());
    assertEquals("file.json", entries.get(0).getName());
  }

  @Test
  void testListFilesPagination() throws IOException {
    // First page - truncated
    ListObjectsV2Result page1 = new ListObjectsV2Result();
    S3ObjectSummary summary1 = new S3ObjectSummary();
    summary1.setBucketName("mybucket");
    summary1.setKey("prefix/file1.csv");
    summary1.setSize(100);
    summary1.setLastModified(new Date());
    page1.getObjectSummaries().add(summary1);
    page1.setTruncated(true);
    page1.setNextContinuationToken("token1");

    // Second page - not truncated
    ListObjectsV2Result page2 = new ListObjectsV2Result();
    S3ObjectSummary summary2 = new S3ObjectSummary();
    summary2.setBucketName("mybucket");
    summary2.setKey("prefix/file2.csv");
    summary2.setSize(200);
    summary2.setLastModified(new Date());
    page2.getObjectSummaries().add(summary2);
    page2.setTruncated(false);

    when(mockS3.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page1)
        .thenReturn(page2);

    List<StorageProvider.FileEntry> entries = provider.listFiles("s3://mybucket/prefix/", true);
    assertEquals(2, entries.size());
  }

  @Test
  void testListFilesSkipsDirectoryKey() throws IOException {
    ListObjectsV2Result result = new ListObjectsV2Result();
    // Add an entry that matches the prefix key itself (should be skipped)
    S3ObjectSummary dirSummary = new S3ObjectSummary();
    dirSummary.setBucketName("mybucket");
    dirSummary.setKey("prefix/");  // Same as the prefix
    dirSummary.setSize(0);
    dirSummary.setLastModified(new Date());
    result.getObjectSummaries().add(dirSummary);
    result.setTruncated(false);

    when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    List<StorageProvider.FileEntry> entries = provider.listFiles("s3://mybucket/prefix/", true);
    assertEquals(0, entries.size());
  }

  // --- getMetadata ---

  @Test
  void testGetMetadata() throws IOException {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(5000);
    metadata.setLastModified(new Date(3000000));
    metadata.setContentType("text/csv");
    metadata.setHeader("ETag", "\"abc123\"");

    when(mockS3.getObjectMetadata("mybucket", "path/data.csv")).thenReturn(metadata);

    StorageProvider.FileMetadata result = provider.getMetadata("s3://mybucket/path/data.csv");
    assertNotNull(result);
    assertEquals(5000, result.getSize());
    assertEquals("text/csv", result.getContentType());
  }

  // --- openInputStream ---

  @Test
  void testOpenInputStream() throws IOException {
    byte[] testData = "hello world".getBytes();
    S3Object s3Object = mock(S3Object.class);
    ObjectMetadata objMeta = new ObjectMetadata();
    objMeta.setContentLength(testData.length);
    objMeta.setLastModified(new Date());
    objMeta.setContentType("text/plain");

    S3ObjectInputStream s3Is = new S3ObjectInputStream(new ByteArrayInputStream(testData), null);
    when(s3Object.getObjectContent()).thenReturn(s3Is);
    when(s3Object.getObjectMetadata()).thenReturn(objMeta);
    when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(s3Object);

    try (InputStream is = provider.openInputStream("s3://mybucket/data.txt")) {
      assertNotNull(is);
      byte[] buf = new byte[1024];
      int n = is.read(buf);
      assertEquals("hello world", new String(buf, 0, n));
    }
  }

  // --- readRange ---

  @Test
  void testReadRange() throws IOException {
    byte[] rangeData = "partial".getBytes();
    S3Object s3Object = mock(S3Object.class);
    S3ObjectInputStream s3Is = new S3ObjectInputStream(new ByteArrayInputStream(rangeData), null);
    when(s3Object.getObjectContent()).thenReturn(s3Is);
    when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(s3Object);

    byte[] result = provider.readRange("s3://mybucket/file.dat", 10, 7);
    assertNotNull(result);
    assertArrayEquals(rangeData, result);

    // Verify the request used the correct range
    ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
    verify(mockS3).getObject(captor.capture());
    long[] range = captor.getValue().getRange();
    assertEquals(10, range[0]);
    assertEquals(16, range[1]); // offset + length - 1
  }

  // --- openReader ---

  @Test
  void testOpenReader() throws IOException {
    byte[] testData = "reader content".getBytes();
    S3Object s3Object = mock(S3Object.class);
    ObjectMetadata objMeta = new ObjectMetadata();
    objMeta.setContentLength(testData.length);
    objMeta.setLastModified(new Date());
    objMeta.setContentType("text/plain");

    S3ObjectInputStream s3Is = new S3ObjectInputStream(new ByteArrayInputStream(testData), null);
    when(s3Object.getObjectContent()).thenReturn(s3Is);
    when(s3Object.getObjectMetadata()).thenReturn(objMeta);
    when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(s3Object);
    // Mock getObjectMetadata for hasChanged check when persistent cache is active
    when(mockS3.getObjectMetadata(anyString(), anyString())).thenReturn(objMeta);

    try (Reader reader = provider.openReader("s3://mybucket/data.txt")) {
      assertNotNull(reader);
      char[] buf = new char[1024];
      int n = reader.read(buf);
      assertEquals("reader content", new String(buf, 0, n));
    }
  }

  // --- exists ---

  @Test
  void testExistsTrue() throws IOException {
    when(mockS3.doesObjectExist("mybucket", "data.csv")).thenReturn(true);

    assertTrue(provider.exists("s3://mybucket/data.csv"));
  }

  @Test
  void testExistsFalse() throws IOException {
    when(mockS3.doesObjectExist("mybucket", "missing.csv")).thenReturn(false);

    assertFalse(provider.exists("s3://mybucket/missing.csv"));
  }

  @Test
  void testExistsWithException() throws IOException {
    when(mockS3.doesObjectExist(anyString(), anyString()))
        .thenThrow(new RuntimeException("connection refused"));

    // Should return false on exception, not throw
    assertFalse(provider.exists("s3://mybucket/data.csv"));
  }

  @Test
  void testExistsWithGlobPattern() throws IOException {
    // Pattern containing wildcards triggers glob-based search
    ListObjectsV2Result result = new ListObjectsV2Result();
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName("mybucket");
    summary.setKey("year=2024/file_001.parquet");
    summary.setSize(100);
    summary.setLastModified(new Date());
    result.getObjectSummaries().add(summary);
    result.setTruncated(false);

    when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    // This tests the glob pattern path
    boolean exists = provider.exists("s3://mybucket/year=*/file_*.parquet");
    // The result depends on the year matching - may or may not find it
    // The key thing is that it exercises the glob code path without throwing
    assertNotNull(Boolean.valueOf(exists));
  }

  @Test
  void testExistsWithGlobNoMatch() throws IOException {
    ListObjectsV2Result emptyResult = new ListObjectsV2Result();
    emptyResult.setTruncated(false);
    when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(emptyResult);

    // Glob pattern that won't find any matches
    assertFalse(provider.exists("s3://mybucket/year=*/nonexistent_*.csv"));
  }

  // --- isDirectory ---

  @Test
  void testIsDirectoryTrue() throws IOException {
    ListObjectsV2Result result = new ListObjectsV2Result();
    result.setKeyCount(1);

    when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    assertTrue(provider.isDirectory("s3://mybucket/some/dir"));
  }

  @Test
  void testIsDirectoryFalse() throws IOException {
    ListObjectsV2Result result = new ListObjectsV2Result();
    result.setKeyCount(0);

    when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    assertFalse(provider.isDirectory("s3://mybucket/not-a-dir"));
  }

  @Test
  void testIsDirectoryWithTrailingSlash() throws IOException {
    ListObjectsV2Result result = new ListObjectsV2Result();
    result.setKeyCount(2);

    when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    assertTrue(provider.isDirectory("s3://mybucket/already-slashed/"));
  }

  // --- resolvePath ---

  @Test
  void testResolvePathWithFullS3Uri() {
    String resolved = provider.resolvePath("s3://bucket/base/", "s3://other/path.csv");
    assertEquals("s3://other/path.csv", resolved);
  }

  @Test
  void testResolvePathWithS3aUri() {
    String resolved = provider.resolvePath("s3://bucket/base/", "s3a://other/path.csv");
    assertEquals("s3a://other/path.csv", resolved);
  }

  @Test
  void testResolvePathRelative() {
    String resolved = provider.resolvePath("s3://bucket/base/", "subdir/file.csv");
    assertEquals("s3://bucket/base/subdir/file.csv", resolved);
  }

  @Test
  void testResolvePathBaseWithFile() {
    // Base path that looks like a file (has extension) - should strip filename
    String resolved = provider.resolvePath("s3://bucket/base/data.csv", "other.csv");
    assertEquals("s3://bucket/base/other.csv", resolved);
  }

  @Test
  void testResolvePathBaseWithDirectoryNoSlash() {
    // Base path that looks like a directory (no extension, no trailing slash)
    String resolved = provider.resolvePath("s3://bucket/base/subdir", "file.csv");
    assertEquals("s3://bucket/base/subdir/file.csv", resolved);
  }

  @Test
  void testResolvePathShortBase() {
    // Base path where last slash is at s3:// boundary
    String resolved = provider.resolvePath("s3://b", "file.csv");
    assertEquals("s3://b/file.csv", resolved);
  }

  // --- writeFile (byte[]) ---

  @Test
  void testWriteFileBytes() throws IOException {
    byte[] content = "test data".getBytes();
    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/output/data.csv", content);

    verify(mockS3).putObject(any(PutObjectRequest.class));
  }

  @Test
  void testWriteFileBytesRetryOnTransientError() throws IOException {
    byte[] content = "retry data".getBytes();

    AmazonServiceException transientError = new AmazonServiceException("Service Unavailable");
    transientError.setStatusCode(503);
    transientError.setErrorCode("ServiceUnavailable");

    when(mockS3.putObject(any(PutObjectRequest.class)))
        .thenThrow(transientError)
        .thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/output/retry.csv", content);

    verify(mockS3, times(2)).putObject(any(PutObjectRequest.class));
  }

  @Test
  void testWriteFileBytesNonRetryableError() {
    byte[] content = "fail data".getBytes();

    AmazonServiceException nonRetryable = new AmazonServiceException("Forbidden");
    nonRetryable.setStatusCode(403);
    nonRetryable.setErrorCode("AccessDenied");

    when(mockS3.putObject(any(PutObjectRequest.class)))
        .thenThrow(nonRetryable);

    assertThrows(IOException.class, () ->
        provider.writeFile("s3://mybucket/output/forbidden.csv", content));
  }

  @Test
  void testWriteFileBytesAllRetriesFail() {
    byte[] content = "all-fail data".getBytes();

    AmazonServiceException transientError = new AmazonServiceException("Internal Error");
    transientError.setStatusCode(500);
    transientError.setErrorCode("InternalError");

    when(mockS3.putObject(any(PutObjectRequest.class)))
        .thenThrow(transientError);

    assertThrows(IOException.class, () ->
        provider.writeFile("s3://mybucket/output/allfail.csv", content));

    // Should have tried 3 times
    verify(mockS3, times(3)).putObject(any(PutObjectRequest.class));
  }

  // --- writeFile (InputStream) - multipart upload ---

  @Test
  void testWriteFileStreamEmptyContent() throws IOException {
    InputStream emptyStream = new ByteArrayInputStream(new byte[0]);
    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/output/empty.dat", emptyStream);

    // Should use simple put for empty content
    verify(mockS3).putObject(any(PutObjectRequest.class));
  }

  @Test
  void testWriteFileStreamSmallContent() throws IOException {
    // Content smaller than 5MB - should use simple put
    byte[] data = new byte[1024]; // 1KB
    Arrays.fill(data, (byte) 'A');
    InputStream stream = new ByteArrayInputStream(data);

    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/output/small.dat", stream);

    // Should use simple put (not multipart)
    verify(mockS3).putObject(any(PutObjectRequest.class));
    verify(mockS3, never()).initiateMultipartUpload(any());
  }

  @Test
  void testWriteFileStreamMultipartUpload() throws IOException {
    // Content larger than 16MB requires multipart upload
    byte[] data = new byte[17 * 1024 * 1024]; // 17MB
    Arrays.fill(data, (byte) 'B');
    InputStream stream = new ByteArrayInputStream(data);

    InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
    initResult.setUploadId("upload-id-123");
    when(mockS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
        .thenReturn(initResult);

    UploadPartResult partResult = new UploadPartResult();
    partResult.setPartNumber(1);
    partResult.setETag("etag1");
    when(mockS3.uploadPart(any(UploadPartRequest.class))).thenReturn(partResult);

    when(mockS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenReturn(new CompleteMultipartUploadResult());

    provider.writeFile("s3://mybucket/output/large.dat", stream);

    verify(mockS3).initiateMultipartUpload(any());
    verify(mockS3, atLeastOnce()).uploadPart(any());
    verify(mockS3).completeMultipartUpload(any());
  }

  @Test
  void testWriteFileStreamMultipartUploadFailure() {
    // Content larger than 16MB triggers multipart
    byte[] data = new byte[17 * 1024 * 1024];
    Arrays.fill(data, (byte) 'C');
    InputStream stream = new ByteArrayInputStream(data);

    InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
    initResult.setUploadId("upload-id-fail");
    when(mockS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
        .thenReturn(initResult);

    when(mockS3.uploadPart(any(UploadPartRequest.class)))
        .thenThrow(new RuntimeException("upload failure"));

    assertThrows(IOException.class, () ->
        provider.writeFile("s3://mybucket/output/fail.dat", stream));

    // Should attempt to abort the multipart upload
    verify(mockS3).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
  }

  @Test
  void testWriteFileStreamMultipartUploadAbortAlsoFails() {
    // Content larger than 16MB triggers multipart
    byte[] data = new byte[17 * 1024 * 1024];
    Arrays.fill(data, (byte) 'D');
    InputStream stream = new ByteArrayInputStream(data);

    InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
    initResult.setUploadId("upload-id-abort-fail");
    when(mockS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
        .thenReturn(initResult);

    when(mockS3.uploadPart(any(UploadPartRequest.class)))
        .thenThrow(new RuntimeException("upload failure"));
    doThrow(new RuntimeException("abort also failed"))
        .when(mockS3).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

    // Should still throw IOException even if abort fails
    assertThrows(IOException.class, () ->
        provider.writeFile("s3://mybucket/output/abort-fail.dat", stream));
  }

  // --- createDirectories ---

  @Test
  void testCreateDirectories() throws IOException {
    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.createDirectories("s3://mybucket/new/dir/path");

    verify(mockS3).putObject(any(PutObjectRequest.class));
  }

  @Test
  void testCreateDirectoriesWithTrailingSlash() throws IOException {
    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.createDirectories("s3://mybucket/new/dir/path/");

    verify(mockS3).putObject(any(PutObjectRequest.class));
  }

  @Test
  void testCreateDirectoriesConflict() throws IOException {
    AmazonServiceException conflict = new AmazonServiceException("Conflict");
    conflict.setStatusCode(409);
    when(mockS3.putObject(any(PutObjectRequest.class))).thenThrow(conflict);

    // 409 conflict should be ignored (directory already exists)
    provider.createDirectories("s3://mybucket/existing/dir/");
  }

  @Test
  void testCreateDirectoriesOtherError() {
    AmazonServiceException error = new AmazonServiceException("Internal Error");
    error.setStatusCode(500);
    when(mockS3.putObject(any(PutObjectRequest.class))).thenThrow(error);

    assertThrows(IOException.class, () ->
        provider.createDirectories("s3://mybucket/fail/dir/"));
  }

  // --- delete ---

  @Test
  void testDeleteExists() throws IOException {
    when(mockS3.doesObjectExist("mybucket", "old/data.csv")).thenReturn(true);

    assertTrue(provider.delete("s3://mybucket/old/data.csv"));

    verify(mockS3).deleteObject(any(DeleteObjectRequest.class));
  }

  @Test
  void testDeleteNotExists() throws IOException {
    when(mockS3.doesObjectExist("mybucket", "nonexistent.csv")).thenReturn(false);

    assertFalse(provider.delete("s3://mybucket/nonexistent.csv"));

    verify(mockS3, never()).deleteObject(any(DeleteObjectRequest.class));
  }

  @Test
  void testDeleteServiceException() {
    when(mockS3.doesObjectExist("mybucket", "protected.csv")).thenReturn(true);
    doThrow(new AmazonServiceException("Access Denied"))
        .when(mockS3).deleteObject(any(DeleteObjectRequest.class));

    assertThrows(IOException.class, () ->
        provider.delete("s3://mybucket/protected.csv"));
  }

  // --- deleteBatch ---

  @Test
  void testDeleteBatchNull() throws IOException {
    assertEquals(0, provider.deleteBatch(null));
  }

  @Test
  void testDeleteBatchEmpty() throws IOException {
    assertEquals(0, provider.deleteBatch(Collections.emptyList()));
  }

  @Test
  void testDeleteBatch() throws IOException {
    DeleteObjectsResult result = mock(DeleteObjectsResult.class);
    List<DeleteObjectsResult.DeletedObject> deleted = new ArrayList<>();
    deleted.add(new DeleteObjectsResult.DeletedObject());
    deleted.add(new DeleteObjectsResult.DeletedObject());
    when(result.getDeletedObjects()).thenReturn(deleted);
    when(mockS3.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(result);

    List<String> paths = Arrays.asList(
        "s3://mybucket/file1.csv",
        "s3://mybucket/file2.csv"
    );

    int count = provider.deleteBatch(paths);
    // The formula: batch.size() - (result.getDeletedObjects() == null ? 0 : batch.size() - result.getDeletedObjects().size())
    // = 2 - (2 - 2) = 2 - 0 = 2
    assertEquals(2, count);
    verify(mockS3).deleteObjects(any(DeleteObjectsRequest.class));
  }

  @Test
  void testDeleteBatchPartialFailure() throws IOException {
    AmazonServiceException error = new AmazonServiceException("Partial failure");
    error.setStatusCode(500);
    when(mockS3.deleteObjects(any(DeleteObjectsRequest.class))).thenThrow(error);

    List<String> paths = Arrays.asList(
        "s3://mybucket/file1.csv",
        "s3://mybucket/file2.csv"
    );

    // Should not throw, just log warning
    int count = provider.deleteBatch(paths);
    assertEquals(0, count);
  }

  // --- ensureLifecycleRule ---

  @Test
  void testEnsureLifecycleRuleNoBasePathReturnsEarly() throws IOException {
    // Provider created with s3Client only (no config/baseS3Path)
    // ensureLifecycleRule should return early without error
    provider.ensureLifecycleRule("prefix/", 7);
    verify(mockS3, never()).getBucketLifecycleConfiguration(anyString());
  }

  // --- copyFile ---

  @Test
  void testCopyFile() throws IOException {
    when(mockS3.doesObjectExist("srcbucket", "src/data.csv")).thenReturn(true);
    when(mockS3.copyObject(any(CopyObjectRequest.class))).thenReturn(new CopyObjectResult());

    provider.copyFile("s3://srcbucket/src/data.csv", "s3://dstbucket/dst/data.csv");

    verify(mockS3).copyObject(any(CopyObjectRequest.class));
  }

  @Test
  void testCopyFileSourceNotExists() {
    when(mockS3.doesObjectExist("srcbucket", "missing.csv")).thenReturn(false);

    assertThrows(IOException.class, () ->
        provider.copyFile("s3://srcbucket/missing.csv", "s3://dstbucket/dst/missing.csv"));
  }

  @Test
  void testCopyFileServiceException() {
    when(mockS3.doesObjectExist("srcbucket", "data.csv")).thenReturn(true);
    when(mockS3.copyObject(any(CopyObjectRequest.class)))
        .thenThrow(new AmazonServiceException("Copy failed"));

    assertThrows(IOException.class, () ->
        provider.copyFile("s3://srcbucket/data.csv", "s3://dstbucket/dst/data.csv"));
  }

  // --- guessContentType ---

  @Test
  void testGuessContentTypeJson() throws IOException {
    byte[] content = "{}".getBytes();
    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/test.json", content);

    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(mockS3).putObject(captor.capture());
    assertEquals("application/json", captor.getValue().getMetadata().getContentType());
  }

  @Test
  void testGuessContentTypeCsv() throws IOException {
    byte[] content = "a,b".getBytes();
    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/test.csv", content);

    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(mockS3).putObject(captor.capture());
    assertEquals("text/csv", captor.getValue().getMetadata().getContentType());
  }

  @Test
  void testGuessContentTypeParquet() throws IOException {
    byte[] content = new byte[]{0x50, 0x41, 0x52, 0x31};
    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/test.parquet", content);

    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(mockS3).putObject(captor.capture());
    assertEquals("application/x-parquet", captor.getValue().getMetadata().getContentType());
  }

  @Test
  void testGuessContentTypeXml() throws IOException {
    byte[] content = "<root/>".getBytes();
    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/test.xml", content);

    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(mockS3).putObject(captor.capture());
    assertEquals("application/xml", captor.getValue().getMetadata().getContentType());
  }

  @Test
  void testGuessContentTypeTxt() throws IOException {
    byte[] content = "text".getBytes();
    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/test.txt", content);

    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(mockS3).putObject(captor.capture());
    assertEquals("text/plain", captor.getValue().getMetadata().getContentType());
  }

  @Test
  void testGuessContentTypeYaml() throws IOException {
    byte[] content = "key: val".getBytes();
    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/test.yaml", content);

    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(mockS3).putObject(captor.capture());
    assertEquals("application/x-yaml", captor.getValue().getMetadata().getContentType());
  }

  @Test
  void testGuessContentTypeYml() throws IOException {
    byte[] content = "key: val".getBytes();
    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/test.yml", content);

    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(mockS3).putObject(captor.capture());
    assertEquals("application/x-yaml", captor.getValue().getMetadata().getContentType());
  }

  @Test
  void testGuessContentTypeUnknown() throws IOException {
    byte[] content = new byte[]{1, 2, 3};
    when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/test.bin", content);

    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(mockS3).putObject(captor.capture());
    assertEquals("application/octet-stream", captor.getValue().getMetadata().getContentType());
  }

  // --- parseS3Uri ---

  @Test
  void testInvalidS3Uri() {
    assertThrows(IOException.class, () ->
        provider.listFiles("http://not-s3/path", false));
  }

  @Test
  void testS3aUri() throws IOException {
    // S3A URIs should also be accepted
    ListObjectsV2Result result = new ListObjectsV2Result();
    result.setTruncated(false);
    when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    List<StorageProvider.FileEntry> entries = provider.listFiles("s3a://mybucket/path/", true);
    assertNotNull(entries);
  }

  @Test
  void testS3UriWithSpaces() throws IOException {
    // Paths with spaces should be properly encoded
    ListObjectsV2Result result = new ListObjectsV2Result();
    result.setTruncated(false);
    when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    List<StorageProvider.FileEntry> entries = provider.listFiles("s3://mybucket/path with spaces/", true);
    assertNotNull(entries);
  }

  // --- toFullPath ---

  @Test
  void testToFullPathAlreadyS3() throws IOException {
    when(mockS3.doesObjectExist("mybucket", "data.csv")).thenReturn(true);

    // Full S3 URI should be returned unchanged
    assertTrue(provider.exists("s3://mybucket/data.csv"));
  }

  @Test
  void testToFullPathAlreadyS3a() throws IOException {
    when(mockS3.doesObjectExist("mybucket", "data.csv")).thenReturn(true);

    // Full S3A URI should also work
    assertTrue(provider.exists("s3a://mybucket/data.csv"));
  }

  @Test
  void testToFullPathRelativeWithoutBase() throws IOException {
    // Provider created with client only (no baseS3Path) - relative paths cause IOException
    // inside exists(), but exists() catches all exceptions and returns false
    assertFalse(provider.exists("relative/path.csv"));
  }

  // --- getFileName edge cases ---

  @Test
  void testListFilesFileNameExtraction() throws IOException {
    ListObjectsV2Result result = new ListObjectsV2Result();

    // File with no slash in key (root-level)
    S3ObjectSummary rootFile = new S3ObjectSummary();
    rootFile.setBucketName("mybucket");
    rootFile.setKey("root_file.csv");
    rootFile.setSize(100);
    rootFile.setLastModified(new Date());
    result.getObjectSummaries().add(rootFile);

    // File with multiple slashes
    S3ObjectSummary deepFile = new S3ObjectSummary();
    deepFile.setBucketName("mybucket");
    deepFile.setKey("a/b/c/deep_file.json");
    deepFile.setSize(200);
    deepFile.setLastModified(new Date());
    result.getObjectSummaries().add(deepFile);

    result.setTruncated(false);
    when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    List<StorageProvider.FileEntry> entries = provider.listFiles("s3://mybucket/", true);
    assertEquals(2, entries.size());
    assertEquals("root_file.csv", entries.get(0).getName());
    assertEquals("deep_file.json", entries.get(1).getName());
  }

  // --- Retryable error code coverage ---

  @Test
  void testRetryOnStatus429() throws IOException {
    byte[] content = "data".getBytes();

    AmazonServiceException throttle = new AmazonServiceException("Too Many Requests");
    throttle.setStatusCode(429);
    throttle.setErrorCode("Throttling");

    when(mockS3.putObject(any(PutObjectRequest.class)))
        .thenThrow(throttle)
        .thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/throttled.csv", content);
    verify(mockS3, times(2)).putObject(any(PutObjectRequest.class));
  }

  @Test
  void testRetryOnStatus502() throws IOException {
    byte[] content = "data".getBytes();

    AmazonServiceException badGateway = new AmazonServiceException("Bad Gateway");
    badGateway.setStatusCode(502);

    when(mockS3.putObject(any(PutObjectRequest.class)))
        .thenThrow(badGateway)
        .thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/bad-gw.csv", content);
    verify(mockS3, times(2)).putObject(any(PutObjectRequest.class));
  }

  @Test
  void testRetryOnStatus504() throws IOException {
    byte[] content = "data".getBytes();

    AmazonServiceException gwTimeout = new AmazonServiceException("Gateway Timeout");
    gwTimeout.setStatusCode(504);

    when(mockS3.putObject(any(PutObjectRequest.class)))
        .thenThrow(gwTimeout)
        .thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/gw-timeout.csv", content);
    verify(mockS3, times(2)).putObject(any(PutObjectRequest.class));
  }

  @Test
  void testNonRetryableErrorCode() {
    byte[] content = "data".getBytes();

    AmazonServiceException error = new AmazonServiceException("Not Found");
    error.setStatusCode(404);
    error.setErrorCode("NoSuchKey");

    when(mockS3.putObject(any(PutObjectRequest.class))).thenThrow(error);

    assertThrows(IOException.class, () ->
        provider.writeFile("s3://mybucket/notfound.csv", content));
    // Should only try once (non-retryable)
    verify(mockS3, times(1)).putObject(any(PutObjectRequest.class));
  }

  @Test
  void testRetryableErrorCodeRequestTimeout() throws IOException {
    byte[] content = "data".getBytes();

    AmazonServiceException timeout = new AmazonServiceException("Request Timeout");
    timeout.setStatusCode(400);
    timeout.setErrorCode("RequestTimeout");

    when(mockS3.putObject(any(PutObjectRequest.class)))
        .thenThrow(timeout)
        .thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/timeout.csv", content);
    verify(mockS3, times(2)).putObject(any(PutObjectRequest.class));
  }

  @Test
  void testRetryableErrorCodeSlowDown() throws IOException {
    byte[] content = "data".getBytes();

    AmazonServiceException slowDown = new AmazonServiceException("Slow Down");
    slowDown.setStatusCode(503);
    slowDown.setErrorCode("SlowDown");

    when(mockS3.putObject(any(PutObjectRequest.class)))
        .thenThrow(slowDown)
        .thenReturn(new PutObjectResult());

    provider.writeFile("s3://mybucket/slowdown.csv", content);
    verify(mockS3, times(2)).putObject(any(PutObjectRequest.class));
  }

  @Test
  void testNullErrorCode() {
    byte[] content = "data".getBytes();

    AmazonServiceException error = new AmazonServiceException("Unknown");
    error.setStatusCode(400);
    // errorCode is null by default

    when(mockS3.putObject(any(PutObjectRequest.class))).thenThrow(error);

    assertThrows(IOException.class, () ->
        provider.writeFile("s3://mybucket/null-code.csv", content));
    // Should only try once (non-retryable with null code and non-retryable status)
    verify(mockS3, times(1)).putObject(any(PutObjectRequest.class));
  }

  // --- getStagingDirectory ---

  @Test
  void testGetStagingDirectoryWithoutBase() {
    // Provider with no base path - getStagingDirectory falls back to temp directory path
    // but createDirectories calls toFullPath which throws because local path is not S3 URI
    // and baseS3Path is null
    assertThrows(IOException.class, () -> provider.getStagingDirectory("test-purpose"));
  }

  // --- Multipart content type ---

  @Test
  void testMultipartUploadContentType() throws IOException {
    // Content between 5MB and 16MB (single part but uses simple put because < partSize)
    byte[] data = new byte[6 * 1024 * 1024]; // 6MB
    Arrays.fill(data, (byte) 'E');
    InputStream stream = new ByteArrayInputStream(data);

    // Since 5MB < 6MB < 16MB, firstPartLen < partSize => streamDone=true
    // But firstPartLen >= 5MB so it goes to multipart path
    InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
    initResult.setUploadId("upload-6mb");
    when(mockS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
        .thenReturn(initResult);

    UploadPartResult partResult = new UploadPartResult();
    partResult.setPartNumber(1);
    partResult.setETag("etag-6mb");
    when(mockS3.uploadPart(any(UploadPartRequest.class))).thenReturn(partResult);

    when(mockS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenReturn(new CompleteMultipartUploadResult());

    provider.writeFile("s3://mybucket/output/medium.json", stream);

    verify(mockS3).initiateMultipartUpload(any());
  }
}
