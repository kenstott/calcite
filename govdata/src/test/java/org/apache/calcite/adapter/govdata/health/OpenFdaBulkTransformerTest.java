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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the bulk-ZIP path of {@link AbstractOpenFdaResponseTransformer} against a real
 * openFDA download-host partition file. Guards the streaming parser against the
 * {@code meta.results}-object-before-{@code results}-array ordering trap.
 */
@Tag("integration")
class OpenFdaBulkTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static RequestContext ctx(String url) {
    return RequestContext.builder()
        .url(url)
        .dimensionValues(Collections.<String, String>emptyMap())
        .build();
  }

  @Test void deviceRecallBulkZipProducesRows() throws Exception {
    String out = new FdaDeviceRecallsResponseTransformer().transform("",
        ctx("https://download.open.fda.gov/device/recall/device-recall-0001-of-0001.json.zip"));
    JsonNode arr = MAPPER.readTree(out);
    assertTrue(arr.isArray(), "output is a JSON array");
    assertTrue(arr.size() > 50000, "device.recall bulk should yield ~58k rows, got " + arr.size());
    JsonNode row0 = arr.get(0);
    assertTrue(row0.hasNonNull("cfres_id"), "row has cfres_id");
    assertEquals("fda_device_recalls", row0.path("type").asText());
  }

  @Test void faersPartitionBulkZipProducesRows() throws Exception {
    // One real FAERS quarter partition (~6k records) — proves the partition-file path too.
    String out = new FdaAdverseEventsResponseTransformer().transform("",
        ctx("https://download.open.fda.gov/drug/event/2024q1/drug-event-0001-of-0030.json.zip"));
    JsonNode arr = MAPPER.readTree(out);
    assertTrue(arr.isArray(), "output is a JSON array");
    assertTrue(arr.size() > 1000, "FAERS partition should yield thousands of rows, got " + arr.size());
    assertTrue(arr.get(0).hasNonNull("safety_report_id"), "row has safety_report_id");
  }
}
