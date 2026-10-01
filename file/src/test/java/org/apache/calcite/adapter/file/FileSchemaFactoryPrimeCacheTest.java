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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link FileSchemaFactory#isPrimeCacheEnabled} keeps the background statistics primer off under
 * DuckDB, where it resolves every declared table against the object store for statistics DuckDB
 * never reads, unless the model asks for it.
 */
@Tag("unit")
class FileSchemaFactoryPrimeCacheTest {
  private static final Map<String, Object> NO_SETTING = Collections.<String, Object>emptyMap();

  @Test void duckDbDoesNotPrimeByDefault() {
    assertFalse(FileSchemaFactory.isPrimeCacheEnabled(NO_SETTING, true));
  }

  @Test void otherEnginesPrimeByDefault() {
    assertTrue(FileSchemaFactory.isPrimeCacheEnabled(NO_SETTING, false));
  }

  @Test void modelSettingWinsUnderEitherSpelling() {
    assertTrue(FileSchemaFactory.isPrimeCacheEnabled(operand("primeCache", true), true));
    assertTrue(FileSchemaFactory.isPrimeCacheEnabled(operand("prime_cache", true), true));
    assertFalse(FileSchemaFactory.isPrimeCacheEnabled(operand("primeCache", false), false));
    assertFalse(FileSchemaFactory.isPrimeCacheEnabled(operand("prime_cache", false), false));
  }

  private static Map<String, Object> operand(String key, boolean value) {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put(key, value);
    return operand;
  }
}
