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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A resume store exists only for a forced run of a source that has a raw cache. */
@Tag("unit")
class HttpSourceResumeStoreTest {

  @TempDir File tempDir;

  private HttpSource source(boolean rawCache, boolean forced) {
    HttpSourceConfig.Builder b = HttpSourceConfig.builder().url("http://localhost/x");
    if (rawCache) {
      b.rawCache(HttpSourceConfig.RawCacheConfig.enabled());
    }
    return new HttpSource(b.build(), (HooksConfig) null, new LocalFileStorageProvider(),
        rawCache ? new File(tempDir, "raw").getAbsolutePath() : null, tempDir.getAbsolutePath(),
        null, forced);
  }

  @Test void forcedRunWithRawCacheHasAStore() {
    assertNotNull(source(true, true).resumeStore());
  }

  @Test void ordinaryRunHasNoStore() {
    assertNull(source(true, false).resumeStore());
  }

  @Test void forcedRunWithoutRawCacheHasNoStore() {
    assertNull(source(false, true).resumeStore());
  }

  @Test void clearRemovesStoredResponsesAndIsANoOpWhenEmpty() throws Exception {
    HttpSource src = source(true, true);
    src.clearResumeStore();
    FetchResumeStore store = src.resumeStore();
    store.put("k", new byte[] {'x'});
    assertTrue(store.has("k"));
    src.clearResumeStore();
    assertFalse(store.has("k"));
  }
}
