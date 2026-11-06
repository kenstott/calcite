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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A server starts with every schema loader looking its tables up at once. The first lookup checks
 * the published cache; the others must wait for that verdict. If they answered "untrusted" at
 * once, each would read its tables live from the object store, which is the minutes-long start the
 * bundled cache exists to avoid.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
public class IcebergSchemaCacheConcurrentSyncTest {

  private static final String CACHE_DIR_PROPERTY = "iceberg.metadata.cache.directory";
  private static final String TABLE = "s3://warehouse/sec/filing_metadata";

  private String previousCacheDir;
  private java.util.function.Function<Map<String, String>, StorageProvider> previousFactory;

  @BeforeEach void pin(@TempDir Path tmp) {
    previousCacheDir = System.getProperty(CACHE_DIR_PROPERTY);
    System.setProperty(CACHE_DIR_PROPERTY, tmp.toFile().getAbsolutePath());
    previousFactory = IcebergSchemaCache.providerFactory;
    IcebergSchemaCache.invalidate();
  }

  @AfterEach void restore() {
    IcebergSchemaCache.providerFactory = previousFactory;
    IcebergSchemaCache.invalidate();
    if (previousCacheDir == null) {
      System.clearProperty(CACHE_DIR_PROPERTY);
    } else {
      System.setProperty(CACHE_DIR_PROPERTY, previousCacheDir);
    }
  }

  @Test void aLookupDuringTheSyncWaitsForItInsteadOfReadingLive() throws Exception {
    String doc = "{\"tables\":{\"" + TABLE + "\":{\"path\":\"" + TABLE
        + "\",\"schemaJson\":\"{}\"}}}";
    File cacheFile = IcebergSchemaCache.cacheFile();
    Files.write(cacheFile.toPath(), doc.getBytes(StandardCharsets.UTF_8));
    final String digest = IcebergSchemaCache.localFileDigest();
    final CountDownLatch headStarted = new CountDownLatch(1);
    final AtomicInteger heads = new AtomicInteger();
    IcebergSchemaCache.providerFactory = config -> (StorageProvider) Proxy.newProxyInstance(
        StorageProvider.class.getClassLoader(), new Class<?>[]{StorageProvider.class},
        (proxy, method, args) -> {
          if (!method.getName().equals("getMetadata")) {
            throw new UnsupportedOperationException(method.getName());
          }
          heads.incrementAndGet();
          headStarted.countDown();
          Thread.sleep(500); // the object-store round trip
          return new StorageProvider.FileMetadata((String) args[0], 1L, 0L, "application/json",
              "\"" + digest + "\"");
        });
    final Map<String, String> s3 = new HashMap<>();
    s3.put("accessKeyId", "k");
    s3.put("secretAccessKey", "s");

    final IcebergSchemaCache.TableSchema[] second = new IcebergSchemaCache.TableSchema[1];
    Thread first = new Thread(() -> IcebergSchemaCache.lookup(TABLE, s3));
    first.start();
    assertEquals(true, headStarted.await(5, java.util.concurrent.TimeUnit.SECONDS));
    Thread other = new Thread(() -> second[0] = IcebergSchemaCache.lookup(TABLE, s3));
    other.start();
    first.join();
    other.join();

    assertNotNull(second[0], "a lookup that arrived during the sync must get the trusted entry, "
        + "not a miss that sends it to the object store");
    assertEquals(1, heads.get(), "one HEAD per JVM");
  }
}
