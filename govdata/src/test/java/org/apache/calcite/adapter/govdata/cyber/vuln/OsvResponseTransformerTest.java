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
package org.apache.calcite.adapter.govdata.cyber.vuln;

import org.apache.calcite.adapter.file.etl.RequestContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link OsvResponseTransformer}.
 *
 * <p>These tests cover the context-reading contract only. The end-to-end
 * ecosystem fetch (downloading and flattening GCS {@code all.zip} archives)
 * hits the network and is covered by integration tests.
 */
@Tag("unit")
class OsvResponseTransformerTest {

  private OsvResponseTransformer transformer;

  @BeforeEach
  void setUp() {
    transformer = new OsvResponseTransformer();
  }

  @Test void testMissingEcosystemDimensionThrows() {
    // No dimension values at all -> must fail loudly (no silent fallback).
    RequestContext context = RequestContext.builder()
        .url("https://osv-vulnerabilities.storage.googleapis.com")
        .build();

    IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
        transformer.transform("ignored", context));

    assertTrue(ex.getMessage().contains("ecosystem"));
  }

  @Test void testBlankEcosystemDimensionThrows() {
    Map<String, String> dimensions = new HashMap<>();
    dimensions.put("ecosystem", "   ");

    RequestContext context = RequestContext.builder()
        .url("https://osv-vulnerabilities.storage.googleapis.com")
        .dimensionValues(dimensions)
        .build();

    IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
        transformer.transform("ignored", context));

    assertTrue(ex.getMessage().contains("ecosystem"));
  }
}
