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
package org.apache.calcite.adapter.file.cache;

import org.apache.calcite.adapter.file.format.parquet.ConcurrentParquetCache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ConcurrentParquetCache}.
 *
 * <p>Tests verify thread-safe lock acquisition, concurrent conversion operations,
 * and proper cleanup behavior. Uses CountDownLatch for precise concurrency control.
 */
@Tag("unit")
public class ConcurrentParquetCacheTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConcurrentParquetCacheTest.class);

  @TempDir
  File tempDir;

  private File cacheDir;
  private File sourceFile;

  @BeforeEach
  public void setUp() throws Exception {
    cacheDir = new File(tempDir, "cache");
    cacheDir.mkdirs();

    // Create a source file for conversion tests
    sourceFile = new File(tempDir, "test_source.csv");
    try (FileWriter writer = new FileWriter(sourceFile)) {
      writer.write("id,name,value\n");
      writer.write("1,alpha,100\n");
      writer.write("2,beta,200\n");
    }
  }

  @AfterEach
  public void tearDown() {
    // No-op: @TempDir handles cleanup
  }

  @Test public void testConvertWithLockingProducesOutputFile() throws Exception {
    final AtomicReference<File> convertedTarget = new AtomicReference<File>();

    File result = ConcurrentParquetCache.convertWithLocking(
        sourceFile, cacheDir, false, null, "TO_LOWER",
        new ConcurrentParquetCache.ConversionCallback() {
          @Override public void convert(File targetFile) throws Exception {
            // Simulate a conversion by writing a dummy file
            try (FileWriter writer = new FileWriter(targetFile)) {
              writer.write("converted_content");
            }
            convertedTarget.set(targetFile);
          }
        });

    assertNotNull(result, "Conversion result should not be null");
    assertTrue(result.exists(), "Result file should exist");
    LOGGER.debug("Converted file: {}", result.getAbsolutePath());
  }

  @Test public void testConcurrentConversionSerializesAccess() throws Exception {
    int threadCount = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    final AtomicInteger conversionCount = new AtomicInteger(0);
    List<Future<File>> futures = new ArrayList<Future<File>>();

    for (int i = 0; i < threadCount; i++) {
      final int threadIndex = i;
      futures.add(executor.submit(new java.util.concurrent.Callable<File>() {
        @Override public File call() throws Exception {
          startLatch.await(); // All threads start together
          try {
            return ConcurrentParquetCache.convertWithLocking(
                sourceFile, cacheDir, false, null, "TO_LOWER",
                new ConcurrentParquetCache.ConversionCallback() {
                  @Override public void convert(File targetFile) throws Exception {
                    conversionCount.incrementAndGet();
                    LOGGER.debug("Thread {} performing conversion", threadIndex);
                    // Simulate some work
                    try (FileWriter writer = new FileWriter(targetFile)) {
                      writer.write("thread_" + threadIndex);
                    }
                  }
                });
          } finally {
            doneLatch.countDown();
          }
        }
      }));
    }

    // Release all threads simultaneously
    startLatch.countDown();

    // Wait for all to complete
    boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
    assertTrue(completed, "All threads should complete within timeout");

    // All futures should succeed
    for (Future<File> future : futures) {
      File result = future.get(5, TimeUnit.SECONDS);
      assertNotNull(result, "Each thread should get a non-null result");
    }

    // Due to the cache check (needsConversion), only some threads may actually convert
    // but all should return a valid result file
    assertTrue(conversionCount.get() >= 1,
        "At least one thread should perform the actual conversion");
    LOGGER.debug("Conversion was performed {} times by {} threads",
        conversionCount.get(), threadCount);

    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test public void testConversionWithDifferentSchemaNames() throws Exception {
    // Schema name is used to create isolated lock keys, but the cache file path
    // is based only on the source file name. Different schema names use different
    // locks but can share the same cached conversion output.
    final AtomicInteger conversionCountSchema1 = new AtomicInteger(0);
    final AtomicInteger conversionCountSchema2 = new AtomicInteger(0);

    // Use different source files to test independent conversions
    File sourceFile2 = new File(tempDir, "test_source2.csv");
    try (FileWriter fw = new FileWriter(sourceFile2)) {
      fw.write("id,name,value\n");
      fw.write("3,gamma,300\n");
    }

    // Convert file1 with schema "schema1"
    File result1 = ConcurrentParquetCache.convertWithLocking(
        sourceFile, cacheDir, false, "schema1", "TO_LOWER",
        new ConcurrentParquetCache.ConversionCallback() {
          @Override public void convert(File targetFile) throws Exception {
            conversionCountSchema1.incrementAndGet();
            try (FileWriter fw = new FileWriter(targetFile)) {
              fw.write("schema1_data");
            }
          }
        });

    // Convert file2 with schema "schema2" - different source so gets a different cache file
    File result2 = ConcurrentParquetCache.convertWithLocking(
        sourceFile2, cacheDir, false, "schema2", "TO_LOWER",
        new ConcurrentParquetCache.ConversionCallback() {
          @Override public void convert(File targetFile) throws Exception {
            conversionCountSchema2.incrementAndGet();
            try (FileWriter fw = new FileWriter(targetFile)) {
              fw.write("schema2_data");
            }
          }
        });

    assertNotNull(result1, "Result for schema1 should not be null");
    assertNotNull(result2, "Result for schema2 should not be null");

    // Both should convert since they have different source files
    assertEquals(1, conversionCountSchema1.get(),
        "Schema1 conversion should have been performed");
    assertEquals(1, conversionCountSchema2.get(),
        "Schema2 conversion should have been performed");
  }

  @Test public void testConversionWithTypeInferenceFlag() throws Exception {
    // The typeInferenceEnabled flag is passed to getCachedParquetFile but currently
    // does not change the cached file path. Both calls resolve to the same cache file.
    // The first call converts; the second call finds the cached file and skips conversion.
    final AtomicInteger conversionCount = new AtomicInteger(0);

    // First conversion (without type inference)
    File result1 = ConcurrentParquetCache.convertWithLocking(
        sourceFile, cacheDir, false, null, "TO_LOWER",
        new ConcurrentParquetCache.ConversionCallback() {
          @Override public void convert(File targetFile) throws Exception {
            conversionCount.incrementAndGet();
            try (FileWriter fw = new FileWriter(targetFile)) {
              fw.write("converted_data");
            }
          }
        });

    // Second conversion (with type inference) - uses same cache file
    File result2 = ConcurrentParquetCache.convertWithLocking(
        sourceFile, cacheDir, true, null, "TO_LOWER",
        new ConcurrentParquetCache.ConversionCallback() {
          @Override public void convert(File targetFile) throws Exception {
            conversionCount.incrementAndGet();
            try (FileWriter fw = new FileWriter(targetFile)) {
              fw.write("converted_data_v2");
            }
          }
        });

    assertNotNull(result1, "Result without type inference should not be null");
    assertNotNull(result2, "Result with type inference should not be null");
    // Both should resolve to the same cached file
    assertEquals(result1.getAbsolutePath(), result2.getAbsolutePath(),
        "Both type inference modes should resolve to the same cache file");
    // Only one conversion should have been performed since cache is shared
    assertEquals(1, conversionCount.get(),
        "Only the first conversion should actually run; second uses cache");
  }

  @Test public void testConversionCallbackExceptionPropagates() throws Exception {
    assertThrows(IOException.class,
        () -> ConcurrentParquetCache.convertWithLocking(
            sourceFile, cacheDir, false, null, "TO_LOWER",
            new ConcurrentParquetCache.ConversionCallback() {
              @Override public void convert(File targetFile) throws Exception {
                throw new IOException("Simulated conversion failure");
              }
            }),
        "IOException from callback should propagate");
  }

  @Test public void testConcurrentReadWhileWriting() throws Exception {
    int threadCount = 3;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    final AtomicInteger successCount = new AtomicInteger(0);
    final AtomicReference<Exception> firstError = new AtomicReference<Exception>();

    // Launch multiple threads that all try to convert the same file
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(new Runnable() {
        @Override public void run() {
          try {
            startLatch.await();
            File result = ConcurrentParquetCache.convertWithLocking(
                sourceFile, cacheDir, false, "concurrent_test", "TO_LOWER",
                new ConcurrentParquetCache.ConversionCallback() {
                  @Override public void convert(File targetFile) throws Exception {
                    // Small delay to increase contention window
                    Thread.sleep(50);
                    try (FileWriter writer = new FileWriter(targetFile)) {
                      writer.write("thread_" + threadId + "_data");
                    }
                  }
                });
            if (result != null && result.exists()) {
              successCount.incrementAndGet();
            }
          } catch (Exception e) {
            firstError.compareAndSet(null, e);
          } finally {
            doneLatch.countDown();
          }
        }
      });
    }

    startLatch.countDown();
    boolean completed = doneLatch.await(60, TimeUnit.SECONDS);
    assertTrue(completed, "All threads should complete within timeout");

    if (firstError.get() != null) {
      LOGGER.debug("First error during concurrent test: {}", firstError.get().getMessage());
    }

    // All threads should succeed (one converts, others get cached result)
    assertEquals(threadCount, successCount.get(),
        "All threads should successfully get a result");

    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test public void testCacheDirCreatedIfNotExists() throws Exception {
    File newCacheDir = new File(tempDir, "new_cache_dir");
    // Do not create it - convertWithLocking should create it

    File result = ConcurrentParquetCache.convertWithLocking(
        sourceFile, newCacheDir, false, null, "TO_LOWER",
        new ConcurrentParquetCache.ConversionCallback() {
          @Override public void convert(File targetFile) throws Exception {
            try (FileWriter writer = new FileWriter(targetFile)) {
              writer.write("auto_created_dir");
            }
          }
        });

    assertNotNull(result, "Result should not be null");
    assertTrue(newCacheDir.exists(), "Cache directory should be auto-created");
  }
}
