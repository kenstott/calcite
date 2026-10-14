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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * REQ-1992: the Iceberg schema cache has no default directory. Isolated: it changes a JVM-wide
 * property other test classes read.
 */
@Isolated
class IcebergSchemaCacheDirectoryTest {
  private static final String PROPERTY = "iceberg.metadata.cache.directory";

  @TempDir File directory;

  @Test void withoutThePropertyTheCacheRefusesNamingIt() {
    final String previous = System.getProperty(PROPERTY);
    System.clearProperty(PROPERTY);
    try {
      IllegalStateException e = assertThrows(IllegalStateException.class,
          IcebergSchemaCache::cacheDir);
      assertTrue(e.getMessage().contains(PROPERTY), e.getMessage());
    } finally {
      if (previous != null) {
        System.setProperty(PROPERTY, previous);
      }
    }
  }

  @Test void thePropertyNamesTheDirectory() {
    final String previous = System.getProperty(PROPERTY);
    System.setProperty(PROPERTY, directory.getAbsolutePath());
    try {
      assertEquals(directory.getAbsoluteFile(), IcebergSchemaCache.cacheDir().getAbsoluteFile());
    } finally {
      if (previous == null) {
        System.clearProperty(PROPERTY);
      } else {
        System.setProperty(PROPERTY, previous);
      }
    }
  }
}
