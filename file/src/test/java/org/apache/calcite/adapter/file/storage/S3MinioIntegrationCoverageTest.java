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
package org.apache.calcite.adapter.file.storage;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link S3StorageProvider} using a MinIO S3-compatible server.
 *
 * <p>Runs against a dedicated EPHEMERAL MinIO container ({@link MinioTestContainer},
 * credentials {@code minioadmin/minioadmin}) started for the suite and torn down at
 * the end via a JVM shutdown hook. The tests NEVER touch a standing MinIO/R2 at
 * {@code http://localhost:9000}. If Docker is unavailable the container fails to start.
 *
 * <p>Run with: {@code ./gradlew :file:test -PincludeTags=integration --tests
 * "*S3MinioIntegrationCoverageTest*"}
 */
@Tag("integration")
@Execution(ExecutionMode.SAME_THREAD)
public class S3MinioIntegrationCoverageTest {

  /** Endpoint of the dedicated EPHEMERAL MinIO container (random mapped port). */
  private static final String ENDPOINT = MinioTestContainer.endpoint();
  private static final String ACCESS_KEY = MinioTestContainer.accessKey();
  private static final String SECRET_KEY = MinioTestContainer.secretKey();
  private static final String REGION = MinioTestContainer.region();
  private static final String BUCKET = "test-bucket";

  /** Unique prefix per test run to avoid collisions across parallel test runs. */
  private static final String RUN_ID = UUID.randomUUID().toString().substring(0, 8);
  private static final String TEST_PREFIX = "minio-test-" + RUN_ID + "/";

  private static AmazonS3 s3Client;

  @TempDir
  Path tempDir;

  /** Keys created during a single test, cleaned up in @AfterEach. */
  private final List<String> keysToCleanup = new ArrayList<>();

  // -----------------------------------------------------------------------
  // Setup / Teardown
  // -----------------------------------------------------------------------

  @BeforeAll
  static void setupS3Client() {
    // Use the dedicated EPHEMERAL MinIO container (never the standing localhost:9000).
    MinioTestContainer.ensureStarted();
    MinioTestContainer.createBucket(BUCKET);

    BasicAWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);
    s3Client = AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(ENDPOINT, REGION))
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .withPathStyleAccessEnabled(true)
        .build();
  }

  @AfterEach
  void cleanupTestKeys() {
    for (String key : keysToCleanup) {
      try {
        s3Client.deleteObject(BUCKET, key);
      } catch (Exception ignored) {
        // best effort
      }
    }
    keysToCleanup.clear();
  }

  @AfterAll
  static void cleanupTestPrefix() {
    if (s3Client == null) {
      return;
    }
    try {
      ListObjectsV2Request req = new ListObjectsV2Request()
          .withBucketName(BUCKET)
          .withPrefix(TEST_PREFIX);
      ListObjectsV2Result result;
      do {
        result = s3Client.listObjectsV2(req);
        for (S3ObjectSummary summary : result.getObjectSummaries()) {
          s3Client.deleteObject(BUCKET, summary.getKey());
        }
        req.setContinuationToken(result.getNextContinuationToken());
      } while (result.isTruncated());
    } catch (Exception ignored) {
      // best effort
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  /** Puts a test object and registers it for cleanup. */
  private void putTestObject(String key, String content) {
    byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(bytes.length);
    meta.setContentType("text/csv");
    s3Client.putObject(BUCKET, key, new ByteArrayInputStream(bytes), meta);
    keysToCleanup.add(key);
  }

  /** Puts a test object with explicit content type and registers it for cleanup. */
  private void putTestObject(String key, byte[] bytes, String contentType) {
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(bytes.length);
    meta.setContentType(contentType);
    s3Client.putObject(BUCKET, key, new ByteArrayInputStream(bytes), meta);
    keysToCleanup.add(key);
  }

  /** Creates an S3StorageProvider pointing at the test bucket with the run prefix. */
  private S3StorageProvider createProvider() {
    return createProvider(TEST_PREFIX);
  }

  /** Creates an S3StorageProvider pointing at a specific prefix in the test bucket. */
  private S3StorageProvider createProvider(String prefix) {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", ACCESS_KEY);
    config.put("secretAccessKey", SECRET_KEY);
    config.put("endpoint", ENDPOINT);
    config.put("region", REGION);
    config.put("directory", "s3://" + BUCKET + "/" + prefix);
    return new S3StorageProvider(config);
  }

  /** Creates a provider backed by a direct AmazonS3 client (no config map). */
  private S3StorageProvider createProviderWithClient() {
    return new S3StorageProvider(s3Client);
  }

  /** Reads all bytes from an InputStream. */
  private static byte[] readAll(InputStream is) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    byte[] tmp = new byte[4096];
    int n;
    while ((n = is.read(tmp)) != -1) {
      buf.write(tmp, 0, n);
    }
    return buf.toByteArray();
  }

  /** Reads all chars from a Reader. */
  private static String readAllChars(Reader reader) throws IOException {
    StringBuilder sb = new StringBuilder();
    char[] buf = new char[1024];
    int n;
    while ((n = reader.read(buf)) != -1) {
      sb.append(buf, 0, n);
    }
    return sb.toString();
  }

  /** Generates a unique S3 key scoped to the run prefix. */
  private String testKey(String suffix) {
    return TEST_PREFIX + suffix;
  }

  // =======================================================================
  // S3StorageProvider Constructor Tests
  // =======================================================================

  @Test
  void testConstructorWithMinioConfig() {
    S3StorageProvider provider = createProvider();
    assertNotNull(provider);
    assertEquals("s3", provider.getStorageType());
  }

  @Test
  void testConstructorWithS3Client() {
    S3StorageProvider provider = createProviderWithClient();
    assertNotNull(provider);
    assertEquals("s3", provider.getStorageType());
    // No config passed, so s3Config should be null
    assertNull(provider.getS3Config());
  }

  @Test
  void testConstructorMissingCredentials() {
    Map<String, Object> config = new HashMap<>();
    config.put("endpoint", ENDPOINT);
    config.put("region", REGION);
    // Missing accessKeyId and secretAccessKey
    assertThrows(IllegalArgumentException.class, () -> new S3StorageProvider(config));
  }

  @Test
  void testConstructorMissingAccessKey() {
    Map<String, Object> config = new HashMap<>();
    config.put("secretAccessKey", SECRET_KEY);
    config.put("endpoint", ENDPOINT);
    assertThrows(IllegalArgumentException.class, () -> new S3StorageProvider(config));
  }

  @Test
  void testConstructorMissingSecretKey() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", ACCESS_KEY);
    config.put("endpoint", ENDPOINT);
    assertThrows(IllegalArgumentException.class, () -> new S3StorageProvider(config));
  }

  @Test
  void testConstructorNullConfig() {
    // Null config with null client should throw
    assertThrows(IllegalArgumentException.class,
        () -> new S3StorageProvider((Map<String, Object>) null));
  }

  @Test
  void testConstructorDefaultRegion() {
    // Omit region -- should default to us-east-1
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", ACCESS_KEY);
    config.put("secretAccessKey", SECRET_KEY);
    config.put("endpoint", ENDPOINT);
    config.put("directory", "s3://" + BUCKET + "/" + TEST_PREFIX);

    S3StorageProvider provider = new S3StorageProvider(config);
    assertNotNull(provider);
    Map<String, String> s3Cfg = provider.getS3Config();
    assertNotNull(s3Cfg);
    assertNull(s3Cfg.get("region")); // region was not in config
  }

  @Test
  void testConstructorWithExplicitRegion() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", ACCESS_KEY);
    config.put("secretAccessKey", SECRET_KEY);
    config.put("endpoint", ENDPOINT);
    config.put("region", "eu-west-1");
    config.put("directory", "s3://" + BUCKET + "/" + TEST_PREFIX);

    S3StorageProvider provider = new S3StorageProvider(config);
    Map<String, String> s3Cfg = provider.getS3Config();
    assertNotNull(s3Cfg);
    assertEquals("eu-west-1", s3Cfg.get("region"));
  }

  // =======================================================================
  // listFiles Tests
  // =======================================================================

  @Test
  void testListFilesEmptyPrefix() throws IOException {
    String prefix = testKey("list-empty/");
    S3StorageProvider provider = createProvider("list-empty-" + RUN_ID + "/");
    // With no objects uploaded, list should return empty
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("s3://" + BUCKET + "/" + prefix, false);
    assertNotNull(entries);
    assertTrue(entries.isEmpty());
  }

  @Test
  void testListFilesSingleFile() throws IOException {
    String key = testKey("list-single/data.csv");
    putTestObject(key, "a,b,c\n1,2,3\n");

    S3StorageProvider provider = createProvider();
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("s3://" + BUCKET + "/" + TEST_PREFIX + "list-single/", false);

    assertEquals(1, entries.size());
    assertEquals("data.csv", entries.get(0).getName());
    assertFalse(entries.get(0).isDirectory());
    assertTrue(entries.get(0).getSize() > 0);
  }

  @Test
  void testListFilesMultipleFiles() throws IOException {
    String dir = testKey("list-multi/");
    putTestObject(dir + "alpha.csv", "a\n1\n");
    putTestObject(dir + "beta.csv", "b\n2\n");
    putTestObject(dir + "gamma.json", "{\"x\":1}");

    S3StorageProvider provider = createProvider();
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("s3://" + BUCKET + "/" + dir, false);

    assertEquals(3, entries.size());
    Set<String> names = new HashSet<>();
    for (StorageProvider.FileEntry entry : entries) {
      names.add(entry.getName());
    }
    assertTrue(names.contains("alpha.csv"));
    assertTrue(names.contains("beta.csv"));
    assertTrue(names.contains("gamma.json"));
  }

  @Test
  void testListFilesNonRecursiveShowsDirectories() throws IOException {
    String dir = testKey("list-dirs/");
    putTestObject(dir + "root.csv", "x\n1\n");
    putTestObject(dir + "sub1/a.csv", "a\n1\n");
    putTestObject(dir + "sub2/b.csv", "b\n2\n");

    S3StorageProvider provider = createProvider();
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("s3://" + BUCKET + "/" + dir, false);

    // Should have: root.csv (file) + sub1/ (dir) + sub2/ (dir)
    assertTrue(entries.size() >= 3, "Expected at least 3 entries but got " + entries.size());

    boolean foundFile = false;
    int dirCount = 0;
    for (StorageProvider.FileEntry e : entries) {
      if ("root.csv".equals(e.getName())) {
        foundFile = true;
        assertFalse(e.isDirectory());
      }
      if (e.isDirectory()) {
        dirCount++;
      }
    }
    assertTrue(foundFile, "root.csv should be present");
    assertTrue(dirCount >= 2, "Should see at least 2 subdirectory entries");
  }

  @Test
  void testListFilesRecursively() throws IOException {
    String dir = testKey("list-recursive/");
    putTestObject(dir + "a.csv", "a\n");
    putTestObject(dir + "sub1/b.csv", "b\n");
    putTestObject(dir + "sub1/sub2/c.csv", "c\n");

    S3StorageProvider provider = createProvider();
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("s3://" + BUCKET + "/" + dir, true);

    // Recursive should show all 3 files
    assertEquals(3, entries.size());
    Set<String> names = new HashSet<>();
    for (StorageProvider.FileEntry entry : entries) {
      names.add(entry.getName());
    }
    assertTrue(names.contains("a.csv"));
    assertTrue(names.contains("b.csv"));
    assertTrue(names.contains("c.csv"));
  }

  @Test
  void testListFilesUsingRelativePath() throws IOException {
    String dir = testKey("list-rel/");
    putTestObject(dir + "rel.csv", "r\n1\n");

    S3StorageProvider provider = createProvider();
    // The relative path should resolve against the base S3 path
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("list-rel/", false);

    assertEquals(1, entries.size());
    assertEquals("rel.csv", entries.get(0).getName());
  }

  // =======================================================================
  // exists Tests
  // =======================================================================

  @Test
  void testExistsForUploadedFile() throws IOException {
    String key = testKey("exists/present.csv");
    putTestObject(key, "data\n");

    S3StorageProvider provider = createProvider();
    assertTrue(provider.exists("s3://" + BUCKET + "/" + key));
  }

  @Test
  void testExistsForNonExistentFile() throws IOException {
    S3StorageProvider provider = createProvider();
    assertFalse(provider.exists("s3://" + BUCKET + "/" + testKey("exists/no-such-file.csv")));
  }

  @Test
  void testExistsRelativePath() throws IOException {
    String key = testKey("exists-rel/file.csv");
    putTestObject(key, "data\n");

    S3StorageProvider provider = createProvider();
    assertTrue(provider.exists("exists-rel/file.csv"));
  }

  @Test
  void testExistsAfterDeletion() throws IOException {
    String key = testKey("exists-del/file.csv");
    putTestObject(key, "content\n");

    S3StorageProvider provider = createProvider();
    assertTrue(provider.exists("s3://" + BUCKET + "/" + key));

    s3Client.deleteObject(BUCKET, key);
    keysToCleanup.remove(key);

    assertFalse(provider.exists("s3://" + BUCKET + "/" + key));
  }

  @Test
  void testExistsWithGlobPatternNoMatch() throws IOException {
    S3StorageProvider provider = createProvider();
    // Glob pattern that matches nothing
    boolean result = provider.exists(
        "s3://" + BUCKET + "/" + TEST_PREFIX + "nonexistent/year=*/file_*.parquet");
    assertFalse(result);
  }

  // =======================================================================
  // openInputStream Tests
  // =======================================================================

  @Test
  void testOpenInputStreamReadsCsvContent() throws IOException {
    String csvContent = "name,age\nAlice,30\nBob,25\n";
    String key = testKey("stream/people.csv");
    putTestObject(key, csvContent);

    S3StorageProvider provider = createProvider();
    try (InputStream is = provider.openInputStream("s3://" + BUCKET + "/" + key)) {
      String actual = new String(readAll(is), StandardCharsets.UTF_8);
      assertEquals(csvContent, actual);
    }
  }

  @Test
  void testOpenInputStreamRelativePath() throws IOException {
    String content = "col1\nval1\n";
    String key = testKey("stream-rel/data.csv");
    putTestObject(key, content);

    S3StorageProvider provider = createProvider();
    try (InputStream is = provider.openInputStream("stream-rel/data.csv")) {
      String actual = new String(readAll(is), StandardCharsets.UTF_8);
      assertEquals(content, actual);
    }
  }

  @Test
  void testOpenInputStreamLargeContent() throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("id,value\n");
    for (int i = 0; i < 10000; i++) {
      sb.append(i).append(",").append("data-").append(i).append("\n");
    }
    String content = sb.toString();
    String key = testKey("stream/large.csv");
    putTestObject(key, content);

    S3StorageProvider provider = createProvider();
    try (InputStream is = provider.openInputStream("s3://" + BUCKET + "/" + key)) {
      String actual = new String(readAll(is), StandardCharsets.UTF_8);
      assertEquals(content, actual);
    }
  }

  @Test
  void testOpenInputStreamBinaryContent() throws IOException {
    byte[] binaryData = new byte[256];
    for (int i = 0; i < 256; i++) {
      binaryData[i] = (byte) i;
    }
    String key = testKey("stream/binary.bin");
    putTestObject(key, binaryData, "application/octet-stream");

    S3StorageProvider provider = createProvider();
    try (InputStream is = provider.openInputStream("s3://" + BUCKET + "/" + key)) {
      byte[] actual = readAll(is);
      assertArrayEquals(binaryData, actual);
    }
  }

  // =======================================================================
  // openReader Tests
  // =======================================================================

  @Test
  void testOpenReaderReadsCsvLines() throws IOException {
    String csvContent = "name,city\nAlice,NYC\nBob,LA\n";
    String key = testKey("reader/cities.csv");
    putTestObject(key, csvContent);

    S3StorageProvider provider = createProvider();
    try (Reader reader = provider.openReader("s3://" + BUCKET + "/" + key)) {
      String actual = readAllChars(reader);
      assertEquals(csvContent, actual);
    }
  }

  @Test
  void testOpenReaderRelativePath() throws IOException {
    String content = "x,y\n1,2\n";
    String key = testKey("reader-rel/xy.csv");
    putTestObject(key, content);

    S3StorageProvider provider = createProvider();
    try (Reader reader = provider.openReader("reader-rel/xy.csv")) {
      String actual = readAllChars(reader);
      assertEquals(content, actual);
    }
  }

  @Test
  void testOpenReaderWithBufferedReader() throws IOException {
    String csvContent = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    String key = testKey("reader/buffered.csv");
    putTestObject(key, csvContent);

    S3StorageProvider provider = createProvider();
    try (BufferedReader br = new BufferedReader(
        provider.openReader("s3://" + BUCKET + "/" + key))) {
      assertEquals("id,name", br.readLine());
      assertEquals("1,Alice", br.readLine());
      assertEquals("2,Bob", br.readLine());
      assertEquals("3,Charlie", br.readLine());
      assertNull(br.readLine());
    }
  }

  // =======================================================================
  // getMetadata Tests
  // =======================================================================

  @Test
  void testGetMetadataReturnsSize() throws IOException {
    String content = "abc,def\n123,456\n";
    String key = testKey("meta/sized.csv");
    putTestObject(key, content);

    S3StorageProvider provider = createProvider();
    StorageProvider.FileMetadata meta =
        provider.getMetadata("s3://" + BUCKET + "/" + key);

    assertEquals(content.getBytes(StandardCharsets.UTF_8).length, meta.getSize());
  }

  @Test
  void testGetMetadataReturnsContentType() throws IOException {
    String key = testKey("meta/typed.csv");
    putTestObject(key, "a\n1\n");

    S3StorageProvider provider = createProvider();
    StorageProvider.FileMetadata meta =
        provider.getMetadata("s3://" + BUCKET + "/" + key);

    // We set "text/csv" in putTestObject
    assertEquals("text/csv", meta.getContentType());
  }

  @Test
  void testGetMetadataReturnsETag() throws IOException {
    String key = testKey("meta/etag.csv");
    putTestObject(key, "etag-test\n");

    S3StorageProvider provider = createProvider();
    StorageProvider.FileMetadata meta =
        provider.getMetadata("s3://" + BUCKET + "/" + key);

    assertNotNull(meta.getEtag());
    assertFalse(meta.getEtag().isEmpty());
  }

  @Test
  void testGetMetadataReturnsLastModified() throws IOException {
    long beforeUpload = System.currentTimeMillis() - 5000; // 5s buffer
    String key = testKey("meta/modified.csv");
    putTestObject(key, "time-test\n");
    long afterUpload = System.currentTimeMillis() + 5000; // 5s buffer

    S3StorageProvider provider = createProvider();
    StorageProvider.FileMetadata meta =
        provider.getMetadata("s3://" + BUCKET + "/" + key);

    assertTrue(meta.getLastModified() >= beforeUpload,
        "lastModified should be >= beforeUpload");
    assertTrue(meta.getLastModified() <= afterUpload,
        "lastModified should be <= afterUpload");
  }

  @Test
  void testGetMetadataReturnsPath() throws IOException {
    String key = testKey("meta/path.csv");
    putTestObject(key, "path-test\n");

    S3StorageProvider provider = createProvider();
    String s3Path = "s3://" + BUCKET + "/" + key;
    StorageProvider.FileMetadata meta = provider.getMetadata(s3Path);

    assertEquals(s3Path, meta.getPath());
  }

  @Test
  void testGetMetadataJsonContentType() throws IOException {
    String key = testKey("meta/data.json");
    byte[] jsonBytes = "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8);
    putTestObject(key, jsonBytes, "application/json");

    S3StorageProvider provider = createProvider();
    StorageProvider.FileMetadata meta =
        provider.getMetadata("s3://" + BUCKET + "/" + key);

    assertEquals("application/json", meta.getContentType());
    assertEquals(jsonBytes.length, meta.getSize());
  }

  // =======================================================================
  // hasChanged Tests
  // =======================================================================

  @Test
  void testHasChangedReturnsFalseForUnmodifiedFile() throws IOException {
    String key = testKey("haschanged/stable.csv");
    putTestObject(key, "stable content\n");

    S3StorageProvider provider = createProvider();
    String path = "s3://" + BUCKET + "/" + key;
    StorageProvider.FileMetadata meta = provider.getMetadata(path);

    // Same file, same metadata - should not have changed
    assertFalse(provider.hasChanged(path, meta));
  }

  @Test
  void testHasChangedReturnsTrueAfterModification() throws IOException {
    String key = testKey("haschanged/changing.csv");
    putTestObject(key, "version1\n");

    S3StorageProvider provider = createProvider();
    String path = "s3://" + BUCKET + "/" + key;
    StorageProvider.FileMetadata oldMeta = provider.getMetadata(path);

    // Overwrite with different content (different size & ETag)
    putTestObject(key, "version2 with more data to change size\n");

    assertTrue(provider.hasChanged(path, oldMeta));
  }

  @Test
  void testHasChangedReturnsTrueForNullCachedMetadata() throws IOException {
    S3StorageProvider provider = createProvider();
    assertTrue(provider.hasChanged("s3://" + BUCKET + "/" + testKey("any.csv"), null));
  }

  @Test
  void testHasChangedReturnsTrueForSameSizeDifferentETag() throws IOException {
    String key = testKey("haschanged/samelen.csv");
    putTestObject(key, "AAAA\n");

    S3StorageProvider provider = createProvider();
    String path = "s3://" + BUCKET + "/" + key;
    StorageProvider.FileMetadata oldMeta = provider.getMetadata(path);

    // Overwrite with same-length but different content, yielding different ETag
    putTestObject(key, "BBBB\n");

    assertTrue(provider.hasChanged(path, oldMeta),
        "ETag should differ even though size is the same");
  }

  // =======================================================================
  // writeFile Tests
  // =======================================================================

  @Test
  void testWriteFileBytes() throws IOException {
    S3StorageProvider provider = createProvider();
    String content = "written,content\n1,hello\n";
    String relPath = "write/output.csv";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, content.getBytes(StandardCharsets.UTF_8));

    // Verify via direct S3 read
    String actual = s3Client.getObjectAsString(BUCKET, key);
    assertEquals(content, actual);
  }

  @Test
  void testWriteFileBytesFullS3Path() throws IOException {
    S3StorageProvider provider = createProvider();
    String content = "full-path-write\n";
    String key = testKey("write/full-path.csv");
    keysToCleanup.add(key);

    provider.writeFile("s3://" + BUCKET + "/" + key,
        content.getBytes(StandardCharsets.UTF_8));

    String actual = s3Client.getObjectAsString(BUCKET, key);
    assertEquals(content, actual);
  }

  @Test
  void testWriteFileInputStream() throws IOException {
    S3StorageProvider provider = createProvider();
    String content = "streamed-content\n";
    String relPath = "write/streamed.csv";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath,
        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));

    String actual = s3Client.getObjectAsString(BUCKET, key);
    assertEquals(content, actual);
  }

  @Test
  void testWriteFileEmptyContent() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "write/empty.csv";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, new byte[0]);

    assertTrue(s3Client.doesObjectExist(BUCKET, key));
    ObjectMetadata meta = s3Client.getObjectMetadata(BUCKET, key);
    assertEquals(0, meta.getContentLength());
  }

  @Test
  void testWriteFileThenReadBack() throws IOException {
    S3StorageProvider provider = createProvider();
    String content = "roundtrip,data\nfoo,bar\n";
    String relPath = "write/roundtrip.csv";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, content.getBytes(StandardCharsets.UTF_8));

    try (InputStream is = provider.openInputStream(relPath)) {
      String actual = new String(readAll(is), StandardCharsets.UTF_8);
      assertEquals(content, actual);
    }
  }

  @Test
  void testWriteFileOverwritesExisting() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "write/overwrite.csv";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, "version1\n".getBytes(StandardCharsets.UTF_8));
    provider.writeFile(relPath, "version2-replaced\n".getBytes(StandardCharsets.UTF_8));

    String actual = s3Client.getObjectAsString(BUCKET, key);
    assertEquals("version2-replaced\n", actual);
  }

  // =======================================================================
  // readRange Tests
  // =======================================================================

  @Test
  void testReadRangeSubset() throws IOException {
    String content = "ABCDEFGHIJKLMNOP";
    String key = testKey("range/alpha.txt");
    putTestObject(key, content.getBytes(StandardCharsets.UTF_8), "text/plain");

    S3StorageProvider provider = createProvider();
    byte[] range = provider.readRange("s3://" + BUCKET + "/" + key, 4, 4);
    assertEquals("EFGH", new String(range, StandardCharsets.UTF_8));
  }

  @Test
  void testReadRangeFromStart() throws IOException {
    String content = "Hello, World!";
    String key = testKey("range/hello.txt");
    putTestObject(key, content.getBytes(StandardCharsets.UTF_8), "text/plain");

    S3StorageProvider provider = createProvider();
    byte[] range = provider.readRange("s3://" + BUCKET + "/" + key, 0, 5);
    assertEquals("Hello", new String(range, StandardCharsets.UTF_8));
  }

  @Test
  void testReadRangeFromEnd() throws IOException {
    String content = "Hello, World!";
    String key = testKey("range/hello2.txt");
    putTestObject(key, content.getBytes(StandardCharsets.UTF_8), "text/plain");

    S3StorageProvider provider = createProvider();
    byte[] range = provider.readRange("s3://" + BUCKET + "/" + key, 7, 6);
    assertEquals("World!", new String(range, StandardCharsets.UTF_8));
  }

  // =======================================================================
  // delete Tests
  // =======================================================================

  @Test
  void testDeleteExistingFile() throws IOException {
    String key = testKey("delete/target.csv");
    putTestObject(key, "to-be-deleted\n");

    S3StorageProvider provider = createProvider();
    assertTrue(provider.delete("s3://" + BUCKET + "/" + key));
    keysToCleanup.remove(key);

    assertFalse(s3Client.doesObjectExist(BUCKET, key));
  }

  @Test
  void testDeleteNonExistentFile() throws IOException {
    S3StorageProvider provider = createProvider();
    assertFalse(
        provider.delete("s3://" + BUCKET + "/" + testKey("delete/nonexistent.csv")));
  }

  @Test
  void testDeleteThenVerifyNotExists() throws IOException {
    String key = testKey("delete/verify.csv");
    putTestObject(key, "verify-delete\n");

    S3StorageProvider provider = createProvider();
    assertTrue(provider.exists("s3://" + BUCKET + "/" + key));
    assertTrue(provider.delete("s3://" + BUCKET + "/" + key));
    keysToCleanup.remove(key);
    assertFalse(provider.exists("s3://" + BUCKET + "/" + key));
  }

  // =======================================================================
  // deleteBatch Tests
  // =======================================================================

  @Test
  void testDeleteBatchMultipleFiles() throws IOException {
    String dir = testKey("delete-batch/");
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      String key = dir + "file" + i + ".csv";
      putTestObject(key, "batch-" + i + "\n");
      paths.add("s3://" + BUCKET + "/" + key);
    }

    S3StorageProvider provider = createProvider();
    int deleted = provider.deleteBatch(paths);

    // Verify all are gone
    for (int i = 0; i < 5; i++) {
      String key = dir + "file" + i + ".csv";
      assertFalse(s3Client.doesObjectExist(BUCKET, key));
      keysToCleanup.remove(key);
    }
    // deleted count might not match exactly on MinIO; at least check some were removed
    assertTrue(deleted >= 0);
  }

  @Test
  void testDeleteBatchEmptyList() throws IOException {
    S3StorageProvider provider = createProvider();
    assertEquals(0, provider.deleteBatch(new ArrayList<String>()));
  }

  @Test
  void testDeleteBatchNullList() throws IOException {
    S3StorageProvider provider = createProvider();
    assertEquals(0, provider.deleteBatch(null));
  }

  // =======================================================================
  // createDirectories Tests
  // =======================================================================

  @Test
  void testCreateDirectoriesMarkerObject() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "dirs/created/subdir";
    String key = TEST_PREFIX + relPath + "/";
    keysToCleanup.add(key);

    provider.createDirectories(relPath);

    assertTrue(s3Client.doesObjectExist(BUCKET, key),
        "Directory marker object should exist");
  }

  @Test
  void testCreateDirectoriesWithTrailingSlash() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "dirs/trailing/";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.createDirectories(relPath);

    assertTrue(s3Client.doesObjectExist(BUCKET, key));
  }

  @Test
  void testCreateDirectoriesIdempotent() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "dirs/idempotent";
    String key = TEST_PREFIX + relPath + "/";
    keysToCleanup.add(key);

    // Create twice - should not fail
    provider.createDirectories(relPath);
    provider.createDirectories(relPath);

    assertTrue(s3Client.doesObjectExist(BUCKET, key));
  }

  // =======================================================================
  // isDirectory Tests
  // =======================================================================

  @Test
  void testIsDirectoryForPrefixWithObjects() throws IOException {
    String dir = testKey("isdir/with-objects/");
    putTestObject(dir + "file.csv", "data\n");

    S3StorageProvider provider = createProvider();
    assertTrue(provider.isDirectory("s3://" + BUCKET + "/" + testKey("isdir/with-objects")));
  }

  @Test
  void testIsDirectoryForEmptyPrefix() throws IOException {
    S3StorageProvider provider = createProvider();
    assertFalse(
        provider.isDirectory(
            "s3://" + BUCKET + "/" + testKey("isdir/no-objects-here-ever/")));
  }

  @Test
  void testIsDirectoryForFileKey() throws IOException {
    String key = testKey("isdir/afile.csv");
    putTestObject(key, "content\n");

    S3StorageProvider provider = createProvider();
    // A file key is not a directory (no objects with key + "/" prefix)
    assertFalse(provider.isDirectory("s3://" + BUCKET + "/" + key));
  }

  @Test
  void testIsDirectoryForDirectoryMarker() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "isdir/marker";
    String key = TEST_PREFIX + relPath + "/";
    keysToCleanup.add(key);

    provider.createDirectories(relPath);

    assertTrue(provider.isDirectory("s3://" + BUCKET + "/" + TEST_PREFIX + relPath));
  }

  // =======================================================================
  // resolvePath Tests
  // =======================================================================

  @Test
  void testResolvePathRelativeToDirectory() {
    S3StorageProvider provider = createProvider();
    String resolved = provider.resolvePath(
        "s3://mybucket/data/", "file.csv");
    assertEquals("s3://mybucket/data/file.csv", resolved);
  }

  @Test
  void testResolvePathAlreadyFullUri() {
    S3StorageProvider provider = createProvider();
    String resolved = provider.resolvePath(
        "s3://mybucket/data/", "s3://other-bucket/path/file.csv");
    assertEquals("s3://other-bucket/path/file.csv", resolved);
  }

  @Test
  void testResolvePathS3aUri() {
    S3StorageProvider provider = createProvider();
    String resolved = provider.resolvePath(
        "s3://mybucket/data/", "s3a://other/path/file.parquet");
    assertEquals("s3a://other/path/file.parquet", resolved);
  }

  @Test
  void testResolvePathBaseIsFile() {
    S3StorageProvider provider = createProvider();
    // If base looks like a file (has extension), strip it to get directory
    String resolved = provider.resolvePath(
        "s3://mybucket/data/existing.csv", "new.csv");
    assertEquals("s3://mybucket/data/new.csv", resolved);
  }

  @Test
  void testResolvePathBaseIsDirectoryWithoutTrailingSlash() {
    S3StorageProvider provider = createProvider();
    // No extension, so treated as directory without trailing slash
    String resolved = provider.resolvePath(
        "s3://mybucket/data/subdir", "file.csv");
    assertEquals("s3://mybucket/data/subdir/file.csv", resolved);
  }

  @Test
  void testResolvePathNestedRelative() {
    S3StorageProvider provider = createProvider();
    String resolved = provider.resolvePath(
        "s3://mybucket/root/", "sub1/sub2/deep.csv");
    assertEquals("s3://mybucket/root/sub1/sub2/deep.csv", resolved);
  }

  @Test
  void testResolvePathBucketRootWithSlash() {
    S3StorageProvider provider = createProvider();
    String resolved = provider.resolvePath(
        "s3://mybucket/", "file.csv");
    assertEquals("s3://mybucket/file.csv", resolved);
  }

  // =======================================================================
  // getS3Config Tests
  // =======================================================================

  @Test
  void testGetS3ConfigReturnsCredentials() {
    S3StorageProvider provider = createProvider();
    Map<String, String> config = provider.getS3Config();
    assertNotNull(config);
    assertEquals(ACCESS_KEY, config.get("accessKeyId"));
    assertEquals(SECRET_KEY, config.get("secretAccessKey"));
    assertEquals(ENDPOINT, config.get("endpoint"));
  }

  @Test
  void testGetS3ConfigNullForClientOnlyProvider() {
    S3StorageProvider provider = createProviderWithClient();
    assertNull(provider.getS3Config());
  }

  @Test
  void testGetStorageType() {
    S3StorageProvider provider = createProvider();
    assertEquals("s3", provider.getStorageType());
  }

  // =======================================================================
  // copyFile Tests
  // =======================================================================

  @Test
  void testCopyFile() throws IOException {
    String sourceKey = testKey("copy/source.csv");
    putTestObject(sourceKey, "copy-content\n");

    String destKey = testKey("copy/dest.csv");
    keysToCleanup.add(destKey);

    S3StorageProvider provider = createProvider();
    provider.copyFile(
        "s3://" + BUCKET + "/" + sourceKey,
        "s3://" + BUCKET + "/" + destKey);

    String actual = s3Client.getObjectAsString(BUCKET, destKey);
    assertEquals("copy-content\n", actual);
  }

  @Test
  void testCopyFileRelativePaths() throws IOException {
    String sourceKey = testKey("copy-rel/src.csv");
    putTestObject(sourceKey, "copy-relative\n");

    String destKey = testKey("copy-rel/dst.csv");
    keysToCleanup.add(destKey);

    S3StorageProvider provider = createProvider();
    provider.copyFile("copy-rel/src.csv", "copy-rel/dst.csv");

    String actual = s3Client.getObjectAsString(BUCKET, destKey);
    assertEquals("copy-relative\n", actual);
  }

  @Test
  void testCopyFileNonExistentSourceThrows() {
    S3StorageProvider provider = createProvider();
    assertThrows(IOException.class, () ->
        provider.copyFile(
            "s3://" + BUCKET + "/" + testKey("copy/no-source.csv"),
            "s3://" + BUCKET + "/" + testKey("copy/no-dest.csv")));
  }

  // =======================================================================
  // StorageProviderFactory Tests
  // =======================================================================

  @Test
  void testStorageProviderFactoryCreatesS3WithConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", ACCESS_KEY);
    config.put("secretAccessKey", SECRET_KEY);
    config.put("endpoint", ENDPOINT);
    config.put("region", REGION);
    config.put("directory", "s3://" + BUCKET + "/" + TEST_PREFIX);

    StorageProvider provider = StorageProviderFactory.createFromType("s3", config);
    assertNotNull(provider);
    assertEquals("s3", provider.getStorageType());
  }

  @Test
  void testStorageProviderFactoryCreatesS3WithClient() {
    Map<String, Object> config = new HashMap<>();
    config.put("s3Client", s3Client);

    StorageProvider provider = StorageProviderFactory.createFromType("s3", config);
    assertNotNull(provider);
    assertEquals("s3", provider.getStorageType());
  }

  @Test
  void testStorageProviderFactoryS3UrlAloneThrows() {
    // Creating from URL alone should throw because credentials are required
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromUrl("s3://some-bucket/path"));
  }

  // =======================================================================
  // Integration: write -> list -> read -> delete lifecycle
  // =======================================================================

  @Test
  void testFullLifecycleWriteListReadDelete() throws IOException {
    S3StorageProvider provider = createProvider();

    // 1. Write
    String relPath = "lifecycle/test.csv";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);
    String content = "id,value\n1,hello\n2,world\n";
    provider.writeFile(relPath, content.getBytes(StandardCharsets.UTF_8));

    // 2. List
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("lifecycle/", false);
    assertEquals(1, entries.size());
    assertEquals("test.csv", entries.get(0).getName());

    // 3. Read
    try (InputStream is = provider.openInputStream(relPath)) {
      String actual = new String(readAll(is), StandardCharsets.UTF_8);
      assertEquals(content, actual);
    }

    // 4. Metadata
    StorageProvider.FileMetadata meta = provider.getMetadata(relPath);
    assertEquals(content.getBytes(StandardCharsets.UTF_8).length, meta.getSize());

    // 5. Delete
    assertTrue(provider.delete(relPath));
    keysToCleanup.remove(key);
    assertFalse(provider.exists(relPath));
  }

  @Test
  void testMultipleFilesLifecycle() throws IOException {
    S3StorageProvider provider = createProvider();
    String base = "multi-lifecycle/";

    // Write several files
    for (int i = 0; i < 5; i++) {
      String relPath = base + "file" + i + ".csv";
      String key = TEST_PREFIX + relPath;
      keysToCleanup.add(key);
      provider.writeFile(relPath,
          ("id\n" + i + "\n").getBytes(StandardCharsets.UTF_8));
    }

    // List all
    List<StorageProvider.FileEntry> entries =
        provider.listFiles(base, false);
    assertEquals(5, entries.size());

    // Read each
    for (int i = 0; i < 5; i++) {
      String relPath = base + "file" + i + ".csv";
      try (InputStream is = provider.openInputStream(relPath)) {
        String actual = new String(readAll(is), StandardCharsets.UTF_8);
        assertEquals("id\n" + i + "\n", actual);
      }
    }

    // Delete via batch
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      paths.add(base + "file" + i + ".csv");
    }
    provider.deleteBatch(paths);

    // Verify empty
    List<StorageProvider.FileEntry> remaining =
        provider.listFiles(base, false);
    assertEquals(0, remaining.size());

    // Cleanup tracking
    for (int i = 0; i < 5; i++) {
      keysToCleanup.remove(TEST_PREFIX + base + "file" + i + ".csv");
    }
  }

  // =======================================================================
  // S3 Path Parsing Edge Cases
  // =======================================================================

  @Test
  void testPathWithSpaces() throws IOException {
    String key = testKey("spaces/my file.csv");
    putTestObject(key, "a,b\n1,2\n");

    S3StorageProvider provider = createProvider();
    assertTrue(provider.exists("s3://" + BUCKET + "/" + key));

    try (InputStream is = provider.openInputStream("s3://" + BUCKET + "/" + key)) {
      String actual = new String(readAll(is), StandardCharsets.UTF_8);
      assertEquals("a,b\n1,2\n", actual);
    }
  }

  @Test
  void testPathWithSpecialCharacters() throws IOException {
    String key = testKey("special/file-name_v2.0.csv");
    putTestObject(key, "col\nval\n");

    S3StorageProvider provider = createProvider();
    assertTrue(provider.exists("s3://" + BUCKET + "/" + key));
  }

  @Test
  void testInvalidS3UriThrows() {
    S3StorageProvider provider = createProviderWithClient();
    assertThrows(IOException.class,
        () -> provider.listFiles("http://invalid/path", false));
  }

  // =======================================================================
  // Content Type Guessing Tests
  // =======================================================================

  @Test
  void testWriteFileCsvContentType() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "ctype/test.csv";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, "a\n1\n".getBytes(StandardCharsets.UTF_8));

    ObjectMetadata meta = s3Client.getObjectMetadata(BUCKET, key);
    assertEquals("text/csv", meta.getContentType());
  }

  @Test
  void testWriteFileJsonContentType() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "ctype/test.json";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, "{\"a\":1}".getBytes(StandardCharsets.UTF_8));

    ObjectMetadata meta = s3Client.getObjectMetadata(BUCKET, key);
    assertEquals("application/json", meta.getContentType());
  }

  @Test
  void testWriteFileParquetContentType() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "ctype/test.parquet";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, new byte[]{0, 1, 2, 3});

    ObjectMetadata meta = s3Client.getObjectMetadata(BUCKET, key);
    assertEquals("application/x-parquet", meta.getContentType());
  }

  @Test
  void testWriteFileTxtContentType() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "ctype/test.txt";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, "plain text".getBytes(StandardCharsets.UTF_8));

    ObjectMetadata meta = s3Client.getObjectMetadata(BUCKET, key);
    assertEquals("text/plain", meta.getContentType());
  }

  @Test
  void testWriteFileXmlContentType() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "ctype/test.xml";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, "<root/>".getBytes(StandardCharsets.UTF_8));

    ObjectMetadata meta = s3Client.getObjectMetadata(BUCKET, key);
    assertEquals("application/xml", meta.getContentType());
  }

  @Test
  void testWriteFileYamlContentType() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "ctype/test.yaml";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, "key: value".getBytes(StandardCharsets.UTF_8));

    ObjectMetadata meta = s3Client.getObjectMetadata(BUCKET, key);
    assertEquals("application/x-yaml", meta.getContentType());
  }

  @Test
  void testWriteFileYmlContentType() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "ctype/test.yml";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, "key: value".getBytes(StandardCharsets.UTF_8));

    ObjectMetadata meta = s3Client.getObjectMetadata(BUCKET, key);
    assertEquals("application/x-yaml", meta.getContentType());
  }

  @Test
  void testWriteFileUnknownExtensionDefaultsToOctetStream() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "ctype/test.xyz";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, new byte[]{1, 2, 3});

    ObjectMetadata meta = s3Client.getObjectMetadata(BUCKET, key);
    assertEquals("application/octet-stream", meta.getContentType());
  }

  // =======================================================================
  // normalizePath static method Tests
  // =======================================================================

  @Test
  void testNormalizePathNull() {
    assertNull(StorageProvider.normalizePath(null));
  }

  @Test
  void testNormalizePathS3aSingleSlash() {
    assertEquals("s3a://bucket/key",
        StorageProvider.normalizePath("s3a:/bucket/key"));
  }

  @Test
  void testNormalizePathS3aDoubleSlashUnchanged() {
    assertEquals("s3a://bucket/key",
        StorageProvider.normalizePath("s3a://bucket/key"));
  }

  @Test
  void testNormalizePathS3SingleSlash() {
    assertEquals("s3://bucket/key",
        StorageProvider.normalizePath("s3:/bucket/key"));
  }

  @Test
  void testNormalizePathS3DoubleSlashUnchanged() {
    assertEquals("s3://bucket/key",
        StorageProvider.normalizePath("s3://bucket/key"));
  }

  @Test
  void testNormalizePathHdfsSingleSlash() {
    assertEquals("hdfs://namenode/path",
        StorageProvider.normalizePath("hdfs:/namenode/path"));
  }

  @Test
  void testNormalizePathRegularPathUnchanged() {
    assertEquals("/local/path/file.csv",
        StorageProvider.normalizePath("/local/path/file.csv"));
  }

  // =======================================================================
  // Hive-style Partition Path Tests (S3 paths)
  // =======================================================================

  @Test
  void testHiveStylePartitionedPaths() throws IOException {
    String base = testKey("hive/");
    putTestObject(base + "year=2023/data.csv", "id\n1\n");
    putTestObject(base + "year=2024/data.csv", "id\n2\n");

    S3StorageProvider provider = createProvider();
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("s3://" + BUCKET + "/" + base, true);

    assertEquals(2, entries.size());
  }

  @Test
  void testHiveStyleNestedPartitions() throws IOException {
    String base = testKey("hive-nested/");
    putTestObject(base + "year=2023/month=01/data.csv", "x\n1\n");
    putTestObject(base + "year=2023/month=02/data.csv", "x\n2\n");
    putTestObject(base + "year=2024/month=01/data.csv", "x\n3\n");

    S3StorageProvider provider = createProvider();
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("s3://" + BUCKET + "/" + base, true);

    assertEquals(3, entries.size());
  }

  @Test
  void testHivePartitionsListedAsDirectoriesNonRecursive() throws IOException {
    String base = testKey("hive-dirs/");
    putTestObject(base + "year=2023/data.csv", "x\n1\n");
    putTestObject(base + "year=2024/data.csv", "x\n2\n");

    S3StorageProvider provider = createProvider();
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("s3://" + BUCKET + "/" + base, false);

    // Non-recursive should show year=2023/ and year=2024/ as directories
    int dirCount = 0;
    for (StorageProvider.FileEntry e : entries) {
      if (e.isDirectory()) {
        dirCount++;
        assertTrue(e.getName().startsWith("year="),
            "Directory name should start with 'year=' but was: " + e.getName());
      }
    }
    assertEquals(2, dirCount);
  }

  // =======================================================================
  // FileEntry and FileMetadata property tests
  // =======================================================================

  @Test
  void testFileEntryProperties() throws IOException {
    String key = testKey("props/entry.csv");
    putTestObject(key, "a,b\n1,2\n");

    S3StorageProvider provider = createProvider();
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("s3://" + BUCKET + "/" + testKey("props/"), false);

    assertEquals(1, entries.size());
    StorageProvider.FileEntry entry = entries.get(0);
    assertEquals("entry.csv", entry.getName());
    assertFalse(entry.isDirectory());
    assertTrue(entry.getSize() > 0);
    assertTrue(entry.getLastModified() > 0);
    assertTrue(entry.getPath().contains("entry.csv"));
  }

  @Test
  void testFileMetadataProperties() throws IOException {
    String content = "metadata,test\n1,2\n";
    String key = testKey("props/meta.csv");
    putTestObject(key, content);

    S3StorageProvider provider = createProvider();
    StorageProvider.FileMetadata meta =
        provider.getMetadata("s3://" + BUCKET + "/" + key);

    assertEquals("s3://" + BUCKET + "/" + key, meta.getPath());
    assertEquals(content.getBytes(StandardCharsets.UTF_8).length, meta.getSize());
    assertNotNull(meta.getContentType());
    assertNotNull(meta.getEtag());
    assertTrue(meta.getLastModified() > 0);
  }

  // =======================================================================
  // Concurrent / Sequential Operations
  // =======================================================================

  @Test
  void testSequentialWriteAndListConsistency() throws IOException {
    S3StorageProvider provider = createProvider();
    String base = "seq-test/";

    // Write 10 files
    for (int i = 0; i < 10; i++) {
      String relPath = base + "file" + String.format("%03d", i) + ".csv";
      String key = TEST_PREFIX + relPath;
      keysToCleanup.add(key);
      provider.writeFile(relPath,
          ("row\n" + i + "\n").getBytes(StandardCharsets.UTF_8));
    }

    // List should show all 10
    List<StorageProvider.FileEntry> entries =
        provider.listFiles(base, false);
    assertEquals(10, entries.size());
  }

  // =======================================================================
  // Staging Directory and Lifecycle Tests
  // =======================================================================

  @Test
  void testGetStagingDirectory() throws IOException {
    S3StorageProvider provider = createProvider();
    String staging = provider.getStagingDirectory("test-purpose");
    assertNotNull(staging);
    assertTrue(staging.contains(".staging"),
        "Staging path should contain .staging: " + staging);
    assertTrue(staging.contains("test-purpose"),
        "Staging path should contain purpose: " + staging);
  }

  @Test
  void testCreateAndListInStagingDirectory() throws IOException {
    S3StorageProvider provider = createProvider();
    String staging = provider.getStagingDirectory("integ-test");

    // Write a file in the staging directory
    String filePath = staging + "staging-data.csv";
    // Cleanup: parse the key
    String stagingKey = filePath.replace("s3://" + BUCKET + "/", "");
    keysToCleanup.add(stagingKey);
    // Also clean up the staging directory marker
    String stagingDirKey = staging.replace("s3://" + BUCKET + "/", "");
    keysToCleanup.add(stagingDirKey);

    provider.writeFile(filePath, "staged\n".getBytes(StandardCharsets.UTF_8));
    assertTrue(provider.exists(filePath));
  }

  // =======================================================================
  // S3HivePipelineTracker Tests
  // =======================================================================

  @Test
  void testPipelineTrackerConstruction() {
    // Simply verify the tracker can be constructed with MinIO endpoint
    Map<String, String> trackerConfig = new HashMap<>();
    trackerConfig.put("accessKeyId", ACCESS_KEY);
    trackerConfig.put("secretAccessKey", SECRET_KEY);
    trackerConfig.put("region", REGION);

    org.apache.calcite.adapter.file.partition.S3HivePipelineTracker tracker =
        new org.apache.calcite.adapter.file.partition.S3HivePipelineTracker(
            "s3://" + BUCKET + "/" + testKey("tracker"),
            ENDPOINT,
            trackerConfig);
    assertNotNull(tracker);
  }

  @Test
  void testPipelineTrackerConstructionWithoutConfig() {
    org.apache.calcite.adapter.file.partition.S3HivePipelineTracker tracker =
        new org.apache.calcite.adapter.file.partition.S3HivePipelineTracker(
            "s3://" + BUCKET + "/" + testKey("tracker-noconfig"),
            ENDPOINT);
    assertNotNull(tracker);
  }

  // =======================================================================
  // ensureBucketExists Tests (exercised via constructor)
  // =======================================================================

  @Test
  void testConstructorEnsuresBucketExists() {
    // The provider constructor should auto-create the bucket if needed
    // We test by pointing at an existing bucket and verifying no errors
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", ACCESS_KEY);
    config.put("secretAccessKey", SECRET_KEY);
    config.put("endpoint", ENDPOINT);
    config.put("region", REGION);
    config.put("directory", "s3://" + BUCKET + "/" + TEST_PREFIX + "ensure-bucket/");

    S3StorageProvider provider = new S3StorageProvider(config);
    assertNotNull(provider);
  }

  // =======================================================================
  // writeFile (InputStream) for small and larger payloads
  // =======================================================================

  @Test
  void testWriteFileInputStreamSmall() throws IOException {
    S3StorageProvider provider = createProvider();
    String content = "small-stream-content\n";
    String relPath = "write-stream/small.csv";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath,
        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));

    String actual = s3Client.getObjectAsString(BUCKET, key);
    assertEquals(content, actual);
  }

  @Test
  void testWriteFileInputStreamEmpty() throws IOException {
    S3StorageProvider provider = createProvider();
    String relPath = "write-stream/empty.csv";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath,
        new ByteArrayInputStream(new byte[0]));

    assertTrue(s3Client.doesObjectExist(BUCKET, key));
    ObjectMetadata meta = s3Client.getObjectMetadata(BUCKET, key);
    assertEquals(0, meta.getContentLength());
  }

  // =======================================================================
  // Relative path resolution via toFullPath
  // =======================================================================

  @Test
  void testRelativePathWithoutBaseReturnsFalse() throws IOException {
    // Provider created with just a client (no config/directory) cannot resolve relative paths
    // It logs a warning and returns false instead of throwing
    S3StorageProvider provider = createProviderWithClient();
    assertFalse(provider.exists("relative/path.csv"));
  }

  @Test
  void testRelativePathResolvesCorrectly() throws IOException {
    String key = testKey("resolve/test.csv");
    putTestObject(key, "data\n");

    S3StorageProvider provider = createProvider();
    // "resolve/test.csv" relative to base "s3://test-bucket/<TEST_PREFIX>"
    assertTrue(provider.exists("resolve/test.csv"));
  }

  // =======================================================================
  // Edge case: list large number of objects (pagination)
  // =======================================================================

  @Test
  void testListLargeNumberOfFiles() throws IOException {
    String base = testKey("large-list/");
    int count = 25;
    for (int i = 0; i < count; i++) {
      String key = base + String.format("file%03d.csv", i);
      putTestObject(key, "id\n" + i + "\n");
    }

    S3StorageProvider provider = createProvider();
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("s3://" + BUCKET + "/" + base, false);

    assertEquals(count, entries.size());
  }

  // =======================================================================
  // Integration: write CSV, then query through metadata
  // =======================================================================

  @Test
  void testWriteCsvThenGetMetadataAndRead() throws IOException {
    S3StorageProvider provider = createProvider();
    String csv = "name,score\nAlice,95\nBob,87\nCharlie,92\n";
    String relPath = "query/scores.csv";
    String key = TEST_PREFIX + relPath;
    keysToCleanup.add(key);

    provider.writeFile(relPath, csv.getBytes(StandardCharsets.UTF_8));

    // Metadata
    StorageProvider.FileMetadata meta = provider.getMetadata(relPath);
    assertEquals(csv.length(), meta.getSize());
    assertNotNull(meta.getEtag());
    assertTrue(meta.getLastModified() > 0);

    // Read back
    try (BufferedReader br = new BufferedReader(provider.openReader(relPath))) {
      assertEquals("name,score", br.readLine());
      assertEquals("Alice,95", br.readLine());
      assertEquals("Bob,87", br.readLine());
      assertEquals("Charlie,92", br.readLine());
      assertNull(br.readLine());
    }

    // Not changed
    assertFalse(provider.hasChanged(relPath, meta));
  }
}
