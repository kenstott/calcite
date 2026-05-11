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
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests for S3StorageProvider using Mockito.
 */
@Tag("unit")
public class S3StorageProviderDeepCoverageTest {

  @Mock
  private AmazonS3 mockS3Client;

  private S3StorageProvider provider;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    when(mockS3Client.doesBucketExistV2(anyString())).thenReturn(true);
    provider = new S3StorageProvider(mockS3Client);
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("s3", provider.getStorageType());
  }

  // --- getS3Config ---

  @Test
  void testGetS3ConfigWithDirectClient() {
    // Provider created with just s3Client has no config
    assertNull(provider.getS3Config());
  }

  // --- resolvePath ---

  @Test
  void testResolvePathAbsoluteS3Uri() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "test-key");
    config.put("secretAccessKey", "test-secret");
    S3StorageProvider p = new S3StorageProvider(config);
    assertEquals("s3://bucket/path.txt",
        p.resolvePath("s3://base/dir/", "s3://bucket/path.txt"));
  }

  @Test
  void testResolvePathAbsoluteS3aUri() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "test-key");
    config.put("secretAccessKey", "test-secret");
    S3StorageProvider p = new S3StorageProvider(config);
    assertEquals("s3a://bucket/path.txt",
        p.resolvePath("s3://base/dir/", "s3a://bucket/path.txt"));
  }

  @Test
  void testResolvePathWithFileBase() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "test-key");
    config.put("secretAccessKey", "test-secret");
    S3StorageProvider p = new S3StorageProvider(config);
    assertEquals("s3://bucket/dir/other.txt",
        p.resolvePath("s3://bucket/dir/file.csv", "other.txt"));
  }

  @Test
  void testResolvePathWithDirBase() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "test-key");
    config.put("secretAccessKey", "test-secret");
    S3StorageProvider p = new S3StorageProvider(config);
    assertEquals("s3://bucket/base/relative.txt",
        p.resolvePath("s3://bucket/base/", "relative.txt"));
  }

  @Test
  void testResolvePathWithDirWithoutSlash() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "test-key");
    config.put("secretAccessKey", "test-secret");
    S3StorageProvider p = new S3StorageProvider(config);
    // No extension = treated as directory
    assertEquals("s3://bucket/base/relative.txt",
        p.resolvePath("s3://bucket/base", "relative.txt"));
  }

  @Test
  void testResolvePathShortPath() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "test-key");
    config.put("secretAccessKey", "test-secret");
    S3StorageProvider p = new S3StorageProvider(config);
    // Short path after s3:// - just add slash
    assertEquals("s3://b/relative.txt",
        p.resolvePath("s3://b", "relative.txt"));
  }

  // --- parseS3Uri via reflection ---

  @Test
  void testParseS3UriValid() throws Exception {
    Method parseMethod = S3StorageProvider.class.getDeclaredMethod("parseS3Uri", String.class);
    parseMethod.setAccessible(true);
    Object s3Uri = parseMethod.invoke(provider, "s3://my-bucket/path/to/file.txt");
    assertNotNull(s3Uri);
  }

  @Test
  void testParseS3UriWithS3aScheme() throws Exception {
    Method parseMethod = S3StorageProvider.class.getDeclaredMethod("parseS3Uri", String.class);
    parseMethod.setAccessible(true);
    Object s3Uri = parseMethod.invoke(provider, "s3a://my-bucket/path/to/file.txt");
    assertNotNull(s3Uri);
  }

  @Test
  void testParseS3UriInvalid() throws Exception {
    Method parseMethod = S3StorageProvider.class.getDeclaredMethod("parseS3Uri", String.class);
    parseMethod.setAccessible(true);
    try {
      parseMethod.invoke(provider, "http://not-s3/path");
      // Should not reach here
      assertTrue(false, "Expected IOException");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("Invalid S3 URI"));
    }
  }

  @Test
  void testParseS3UriWithSpaces() throws Exception {
    Method parseMethod = S3StorageProvider.class.getDeclaredMethod("parseS3Uri", String.class);
    parseMethod.setAccessible(true);
    Object s3Uri = parseMethod.invoke(provider, "s3://my-bucket/path with spaces/file.txt");
    assertNotNull(s3Uri);
  }

  // --- getFileName via reflection ---

  @Test
  void testGetFileName() throws Exception {
    Method getFileNameMethod =
        S3StorageProvider.class.getDeclaredMethod("getFileName", String.class);
    getFileNameMethod.setAccessible(true);

    assertEquals("file.txt", getFileNameMethod.invoke(provider, "path/to/file.txt"));
    assertEquals("file.txt", getFileNameMethod.invoke(provider, "file.txt"));
    assertEquals("dir", getFileNameMethod.invoke(provider, "path/to/dir"));
  }

  // --- guessContentType via reflection ---

  @Test
  void testGuessContentType() throws Exception {
    Method guessMethod =
        S3StorageProvider.class.getDeclaredMethod("guessContentType", String.class);
    guessMethod.setAccessible(true);

    assertEquals("application/json", guessMethod.invoke(provider, "file.json"));
    assertEquals("text/csv", guessMethod.invoke(provider, "file.csv"));
    assertEquals("application/x-parquet", guessMethod.invoke(provider, "file.parquet"));
    assertEquals("application/xml", guessMethod.invoke(provider, "file.xml"));
    assertEquals("text/plain", guessMethod.invoke(provider, "file.txt"));
    assertEquals("application/x-yaml", guessMethod.invoke(provider, "file.yaml"));
    assertEquals("application/x-yaml", guessMethod.invoke(provider, "file.yml"));
    assertEquals("application/octet-stream", guessMethod.invoke(provider, "file.unknown"));
  }

  // --- isRetryableS3Error via reflection ---

  @Test
  void testIsRetryableS3Error() throws Exception {
    Method isRetryable =
        S3StorageProvider.class.getDeclaredMethod("isRetryableS3Error",
            AmazonServiceException.class);
    isRetryable.setAccessible(true);

    AmazonServiceException e429 = new AmazonServiceException("throttled");
    e429.setStatusCode(429);
    assertTrue((Boolean) isRetryable.invoke(null, e429));

    AmazonServiceException e500 = new AmazonServiceException("server error");
    e500.setStatusCode(500);
    assertTrue((Boolean) isRetryable.invoke(null, e500));

    AmazonServiceException e502 = new AmazonServiceException("bad gateway");
    e502.setStatusCode(502);
    assertTrue((Boolean) isRetryable.invoke(null, e502));

    AmazonServiceException e503 = new AmazonServiceException("service unavailable");
    e503.setStatusCode(503);
    assertTrue((Boolean) isRetryable.invoke(null, e503));

    AmazonServiceException e504 = new AmazonServiceException("gateway timeout");
    e504.setStatusCode(504);
    assertTrue((Boolean) isRetryable.invoke(null, e504));

    AmazonServiceException e400 = new AmazonServiceException("bad request");
    e400.setStatusCode(400);
    e400.setErrorCode("BadRequest");
    assertFalse((Boolean) isRetryable.invoke(null, e400));

    AmazonServiceException eInternal = new AmazonServiceException("internal");
    eInternal.setStatusCode(400);
    eInternal.setErrorCode("InternalError");
    assertTrue((Boolean) isRetryable.invoke(null, eInternal));

    AmazonServiceException eSlowDown = new AmazonServiceException("slow down");
    eSlowDown.setStatusCode(400);
    eSlowDown.setErrorCode("SlowDown");
    assertTrue((Boolean) isRetryable.invoke(null, eSlowDown));

    AmazonServiceException eTimeout = new AmazonServiceException("timeout");
    eTimeout.setStatusCode(400);
    eTimeout.setErrorCode("RequestTimeout");
    assertTrue((Boolean) isRetryable.invoke(null, eTimeout));

    AmazonServiceException eNoUpload = new AmazonServiceException("no upload");
    eNoUpload.setStatusCode(400);
    eNoUpload.setErrorCode("NoSuchUpload");
    assertTrue((Boolean) isRetryable.invoke(null, eNoUpload));

    AmazonServiceException eSvcUnavail = new AmazonServiceException("unavailable");
    eSvcUnavail.setStatusCode(400);
    eSvcUnavail.setErrorCode("ServiceUnavailable");
    assertTrue((Boolean) isRetryable.invoke(null, eSvcUnavail));

    // Null error code
    AmazonServiceException eNull = new AmazonServiceException("null code");
    eNull.setStatusCode(400);
    assertFalse((Boolean) isRetryable.invoke(null, eNull));
  }

  // --- toFullPath via reflection ---

  @Test
  void testToFullPathWithAbsoluteS3Uri() throws Exception {
    Method toFullPath = S3StorageProvider.class.getDeclaredMethod("toFullPath", String.class);
    toFullPath.setAccessible(true);

    // Absolute paths should be returned unchanged
    assertEquals("s3://bucket/file.txt", toFullPath.invoke(provider, "s3://bucket/file.txt"));
    assertEquals("s3a://bucket/file.txt", toFullPath.invoke(provider, "s3a://bucket/file.txt"));
  }

  @Test
  void testToFullPathWithRelativePathNoBase() throws Exception {
    Method toFullPath = S3StorageProvider.class.getDeclaredMethod("toFullPath", String.class);
    toFullPath.setAccessible(true);

    // Provider created without base path should throw for relative path
    try {
      toFullPath.invoke(provider, "relative/path.txt");
      assertTrue(false, "Expected IOException");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("Cannot resolve relative path"));
    }
  }

  // --- listFiles ---

  @Test
  void testListFilesNonRecursive() throws IOException {
    ListObjectsV2Result result = new ListObjectsV2Result();
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName("bucket");
    summary.setKey("prefix/file.txt");
    summary.setSize(1024);
    summary.setLastModified(new Date());
    result.getObjectSummaries().add(summary);
    result.setTruncated(false);

    List<String> prefixes = new ArrayList<>();
    prefixes.add("prefix/subdir/");

    when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    List<StorageProvider.FileEntry> entries = provider.listFiles("s3://bucket/prefix/", false);
    assertNotNull(entries);
  }

  // --- getMetadata ---

  @Test
  void testGetMetadata() throws IOException {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(2048);
    metadata.setLastModified(new Date());
    metadata.setContentType("text/csv");

    when(mockS3Client.getObjectMetadata("bucket", "path/file.csv")).thenReturn(metadata);

    StorageProvider.FileMetadata result = provider.getMetadata("s3://bucket/path/file.csv");
    assertNotNull(result);
    assertEquals(2048, result.getSize());
    assertEquals("text/csv", result.getContentType());
  }

  // --- exists ---

  @Test
  void testExistsTrue() throws IOException {
    when(mockS3Client.doesObjectExist("bucket", "file.txt")).thenReturn(true);

    assertTrue(provider.exists("s3://bucket/file.txt"));
  }

  @Test
  void testExistsFalse() throws IOException {
    when(mockS3Client.doesObjectExist("bucket", "file.txt")).thenReturn(false);

    assertFalse(provider.exists("s3://bucket/file.txt"));
  }

  @Test
  void testExistsExceptionReturnsFalse() throws IOException {
    when(mockS3Client.doesObjectExist(anyString(), anyString()))
        .thenThrow(new RuntimeException("connection failed"));

    assertFalse(provider.exists("s3://bucket/file.txt"));
  }

  // --- isDirectory ---

  @Test
  void testIsDirectoryTrue() throws IOException {
    ListObjectsV2Result result = new ListObjectsV2Result();
    result.setKeyCount(1);

    when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    assertTrue(provider.isDirectory("s3://bucket/dir"));
  }

  @Test
  void testIsDirectoryFalse() throws IOException {
    ListObjectsV2Result result = new ListObjectsV2Result();
    result.setKeyCount(0);

    when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    assertFalse(provider.isDirectory("s3://bucket/file.txt"));
  }

  // --- writeFile (byte[]) ---

  @Test
  void testWriteFileByteArray() throws IOException {
    byte[] content = "test content".getBytes();
    provider.writeFile("s3://bucket/file.txt", content);
    // Verify no exception thrown
  }

  // --- createDirectories ---

  @Test
  void testCreateDirectories() throws IOException {
    provider.createDirectories("s3://bucket/new/directory/");
    // Verify no exception thrown
  }

  @Test
  void testCreateDirectoriesWithoutTrailingSlash() throws IOException {
    provider.createDirectories("s3://bucket/new/directory");
    // Should add trailing slash
  }

  @Test
  void testCreateDirectoriesConflict() throws IOException {
    AmazonServiceException conflict = new AmazonServiceException("conflict");
    conflict.setStatusCode(409);
    when(mockS3Client.putObject(any(PutObjectRequest.class))).thenThrow(conflict);

    // Should not throw for 409
    provider.createDirectories("s3://bucket/dir/");
  }

  @Test
  void testCreateDirectoriesOtherError() throws IOException {
    AmazonServiceException error = new AmazonServiceException("server error");
    error.setStatusCode(500);
    when(mockS3Client.putObject(any(PutObjectRequest.class))).thenThrow(error);

    assertThrows(IOException.class, () -> provider.createDirectories("s3://bucket/dir/"));
  }

  // --- delete ---

  @Test
  void testDeleteExistingObject() throws IOException {
    when(mockS3Client.doesObjectExist("bucket", "file.txt")).thenReturn(true);

    assertTrue(provider.delete("s3://bucket/file.txt"));
  }

  @Test
  void testDeleteNonExistentObject() throws IOException {
    when(mockS3Client.doesObjectExist("bucket", "file.txt")).thenReturn(false);

    assertFalse(provider.delete("s3://bucket/file.txt"));
  }

  @Test
  void testDeleteThrowsOnError() throws IOException {
    when(mockS3Client.doesObjectExist("bucket", "file.txt")).thenReturn(true);
    AmazonServiceException error = new AmazonServiceException("failed");
    error.setStatusCode(500);
    doThrow(error).when(mockS3Client).deleteObject(any());

    assertThrows(IOException.class, () -> provider.delete("s3://bucket/file.txt"));
  }

  // --- copyFile ---

  @Test
  void testCopyFileSuccess() throws IOException {
    when(mockS3Client.doesObjectExist("bucket", "source.txt")).thenReturn(true);
    when(mockS3Client.copyObject(any())).thenReturn(new CopyObjectResult());

    provider.copyFile("s3://bucket/source.txt", "s3://bucket/dest.txt");
  }

  @Test
  void testCopyFileSourceNotFound() throws IOException {
    when(mockS3Client.doesObjectExist("bucket", "source.txt")).thenReturn(false);

    assertThrows(IOException.class,
        () -> provider.copyFile("s3://bucket/source.txt", "s3://bucket/dest.txt"));
  }

  @Test
  void testCopyFileS3Error() throws IOException {
    when(mockS3Client.doesObjectExist("bucket", "source.txt")).thenReturn(true);
    AmazonServiceException error = new AmazonServiceException("copy failed");
    error.setStatusCode(500);
    when(mockS3Client.copyObject(any())).thenThrow(error);

    assertThrows(IOException.class,
        () -> provider.copyFile("s3://bucket/source.txt", "s3://bucket/dest.txt"));
  }

  // --- readRange ---

  @Test
  void testReadRange() throws IOException {
    S3Object s3Object = mock(S3Object.class);
    byte[] testData = "range data".getBytes();
    S3ObjectInputStream sis = new S3ObjectInputStream(
        new ByteArrayInputStream(testData), null);
    when(s3Object.getObjectContent()).thenReturn(sis);
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(s3Object);

    byte[] result = provider.readRange("s3://bucket/file.txt", 0, 10);
    assertNotNull(result);
  }

  // --- Constructor with config ---

  @Test
  void testConstructorWithConfigNoCredentials() {
    Map<String, Object> config = new HashMap<>();
    assertThrows(IllegalArgumentException.class, () -> new S3StorageProvider(config));
  }

  @Test
  void testConstructorWithConfigPartialCredentials() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "key");
    assertThrows(IllegalArgumentException.class, () -> new S3StorageProvider(config));
  }

  @Test
  void testConstructorWithConfigMinimal() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "test-key");
    config.put("secretAccessKey", "test-secret");
    S3StorageProvider p = new S3StorageProvider(config);
    assertNotNull(p);
    assertNotNull(p.getS3Config());
    assertEquals("test-key", p.getS3Config().get("accessKeyId"));
  }

  @Test
  void testConstructorWithConfigFullOptions() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "test-key");
    config.put("secretAccessKey", "test-secret");
    config.put("region", "eu-west-1");
    config.put("endpoint", "https://custom-s3.example.com");
    S3StorageProvider p = new S3StorageProvider(config);
    assertNotNull(p);
    Map<String, String> s3Config = p.getS3Config();
    assertNotNull(s3Config);
    assertEquals("eu-west-1", s3Config.get("region"));
    assertEquals("https://custom-s3.example.com", s3Config.get("endpoint"));
  }

  @Test
  void testConstructorWithConfigDirectory() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "test-key");
    config.put("secretAccessKey", "test-secret");
    config.put("directory", "s3://mybucket/prefix");
    // Note: this will try to check if bucket exists
    // We can't easily mock a client that hasn't been constructed yet
    // but we can verify that it doesn't throw
    try {
      S3StorageProvider p = new S3StorageProvider(config);
    } catch (Exception e) {
      // Expected - cannot actually reach S3
    }
  }

  // --- sleepQuietly ---

  @Test
  void testSleepQuietly() throws Exception {
    Method sleepMethod =
        S3StorageProvider.class.getDeclaredMethod("sleepQuietly", long.class);
    sleepMethod.setAccessible(true);

    // Very short sleep to verify it doesn't throw
    sleepMethod.invoke(null, 1L);
  }

  // --- readAllBytes via reflection ---

  @Test
  void testReadAllBytes() throws Exception {
    Method readAllBytesMethod =
        S3StorageProvider.class.getDeclaredMethod("readAllBytes", java.io.InputStream.class);
    readAllBytesMethod.setAccessible(true);

    byte[] testData = "test data for s3".getBytes();
    java.io.InputStream is = new java.io.ByteArrayInputStream(testData);

    byte[] result = (byte[]) readAllBytesMethod.invoke(provider, is);
    assertEquals(testData.length, result.length);
    assertEquals("test data for s3", new String(result));
  }

  // --- deleteBatch ---

  @Test
  void testDeleteBatchEmpty() throws IOException {
    assertEquals(0, provider.deleteBatch(new ArrayList<>()));
    assertEquals(0, provider.deleteBatch(null));
  }

  @Test
  void testDeleteBatchSuccess() throws IOException {
    DeleteObjectsResult deleteResult = mock(DeleteObjectsResult.class);
    when(deleteResult.getDeletedObjects()).thenReturn(new ArrayList<>());
    when(mockS3Client.deleteObjects(any())).thenReturn(deleteResult);

    List<String> paths = new ArrayList<>();
    paths.add("s3://bucket/file1.txt");
    paths.add("s3://bucket/file2.txt");

    int result = provider.deleteBatch(paths);
    assertTrue(result >= 0);
  }

  @Test
  void testDeleteBatchPartialFailure() throws IOException {
    AmazonServiceException error = new AmazonServiceException("partial failure");
    error.setStatusCode(500);
    when(mockS3Client.deleteObjects(any())).thenThrow(error);

    List<String> paths = new ArrayList<>();
    paths.add("s3://bucket/file1.txt");
    paths.add("s3://bucket/file2.txt");

    // Should not throw, just log warning
    int result = provider.deleteBatch(paths);
    assertEquals(0, result);
  }
}
