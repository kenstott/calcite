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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link WorldBankResponseTransformer}. */
@Tag("unit")
class WorldBankResponseTransformerTest {

  private static String transform(String response) {
    RequestContext context = RequestContext.builder()
        .url("https://api.worldbank.org/v2/country/all/indicator/EN.ATM.CO2E.PC")
        .dimensionValues(Collections.singletonMap("indicator", "EN.ATM.CO2E.PC"))
        .build();
    return new WorldBankResponseTransformer().transform(response, context);
  }

  @Test void returnsTheDataArray() {
    assertEquals("[{\"date\":\"2024\",\"value\":4.5}]",
        transform("[{\"page\":1},[{\"date\":\"2024\",\"value\":4.5}]]"));
  }

  @Test void archivedIndicatorMessageFails() {
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> transform("[{\"message\":[{\"id\":\"175\",\"key\":\"Invalid format\","
            + "\"value\":\"The indicator was not found. It may have been deleted or "
            + "archived.\"}]}]"));
    assertTrue(e.getMessage().contains("deleted or archived"), e.getMessage());
    assertTrue(e.getMessage().contains("EN.ATM.CO2E.PC"), e.getMessage());
  }

  @Test void envelopeWithoutRowsFails() {
    assertThrows(IllegalStateException.class, () -> transform("[{\"total\":0},null]"));
    assertThrows(IllegalStateException.class, () -> transform("[{\"total\":0},[]]"));
  }

  @Test void nonJsonFails() {
    assertThrows(IllegalStateException.class, () -> transform("<html>error</html>"));
  }
}
