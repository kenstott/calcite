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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a raw cache entry can be dropped, which is what lets a freshness gate see past it.
 *
 * <p>An entry is validated by existence alone — these are immutable and staleness is decided by the
 * tracker rather than a TTL — and nothing used to remove one short of a force-download or a full
 * reprocess. Two gates need it to:
 *
 * <ul>
 *   <li>A probe gate (etag / last_modified) reaches the network on its own, so it can tell the
 *       content changed. Without dropping the entry, the fetch that follows is then handed the very
 *       bytes the probe just called out of date.</li>
 *   <li>A hash gate has no probe at all — it is computed from the fetched body. Served a cached
 *       body it hashes what it already had, matches its own stored token, and reports "unchanged"
 *       every run while the source moves.</li>
 * </ul>
 *
 * <p>Either way the revision is written off as seen and no later run looks again, so the failure is
 * silent and permanent rather than loud.
 */
@Tag("unit")
public class RawCacheInvalidationTest {

  private static HttpSource sourceWithCache(Path cacheRoot) {
    Map<String, Object> rawCache = new LinkedHashMap<String, Object>();
    rawCache.put("enabled", true);
    Map<String, Object> source = new LinkedHashMap<String, Object>();
    source.put("url", "https://example.invalid/data.csv");
    source.put("rawCache", rawCache);
    HttpSourceConfig config = HttpSourceConfig.fromMap(source);
    return new HttpSource(config, (HooksConfig) null, new LocalFileStorageProvider(),
        cacheRoot.toString() + "/tbl", cacheRoot.toString());
  }

  private static Map<String, String> batch(String year) {
    Map<String, String> v = new LinkedHashMap<String, String>();
    v.put("year", year);
    return v;
  }

  private static String entryPath(Path cacheRoot, Map<String, String> variables) {
    return HttpSource.buildRawCachePath(cacheRoot.toString() + "/tbl", variables, null, 0, false);
  }

  @Test void dropsAnExistingEntry(@TempDir Path tmp) throws Exception {
    LocalFileStorageProvider sp = new LocalFileStorageProvider();
    Map<String, String> vars = batch("2024");
    String path = entryPath(tmp, vars);
    sp.createDirectories(path.substring(0, path.lastIndexOf('/')));
    sp.writeFile(path, new ByteArrayInputStream("stale".getBytes(StandardCharsets.UTF_8)));
    assertTrue(sp.exists(path), "entry seeded");

    assertTrue(sourceWithCache(tmp).invalidateRawCache(vars), "should report the removal");
    assertFalse(sp.exists(path), "the next fetch must reach the source, not this entry");
  }

  /** Nothing cached is not a failure — it is the ordinary first-sighting case. */
  @Test void reportsFalseWhenThereIsNothingToDrop(@TempDir Path tmp) {
    assertFalse(sourceWithCache(tmp).invalidateRawCache(batch("2024")));
  }

  /** Only the named batch is dropped; a sibling period keeps its entry. */
  @Test void leavesOtherBatchesAlone(@TempDir Path tmp) throws Exception {
    LocalFileStorageProvider sp = new LocalFileStorageProvider();
    Map<String, String> keep = batch("2023");
    Map<String, String> drop = batch("2024");
    for (Map<String, String> v : new Map[] {keep, drop}) {
      String p = entryPath(tmp, v);
      sp.createDirectories(p.substring(0, p.lastIndexOf('/')));
      sp.writeFile(p, new ByteArrayInputStream("x".getBytes(StandardCharsets.UTF_8)));
    }

    sourceWithCache(tmp).invalidateRawCache(drop);

    assertFalse(sp.exists(entryPath(tmp, drop)), "the reopened period's entry is gone");
    assertTrue(sp.exists(entryPath(tmp, keep)),
        "an unrelated period must keep its entry — dropping it would force a needless re-download");
  }

  /** A table with no raw cache has nothing to drop and must not fail trying. */
  @Test void isANoOpWhenTheTableDoesNotCache(@TempDir Path tmp) {
    Map<String, Object> rawCache = new LinkedHashMap<String, Object>();
    rawCache.put("enabled", false);
    Map<String, Object> source = new LinkedHashMap<String, Object>();
    source.put("url", "https://example.invalid/data.csv");
    source.put("rawCache", rawCache);
    HttpSource noCache = new HttpSource(HttpSourceConfig.fromMap(source), (HooksConfig) null,
        new LocalFileStorageProvider(), null, tmp.toString());

    assertFalse(noCache.invalidateRawCache(batch("2024")));
  }
}
