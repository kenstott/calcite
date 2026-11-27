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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link WorldBankCountryTransformer}. */
@Tag("unit")
class WorldBankCountryTransformerTest {

  private static Map<String, Object> row(String id, String name, String iso3) {
    Map<String, Object> country = new LinkedHashMap<String, Object>();
    country.put("id", id);
    country.put("value", name);
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("country", country);
    row.put("countryiso3code", iso3);
    return row;
  }

  @Test void aggregateWithoutIso3KeepsItsIdentifier() {
    List<Map<String, Object>> out =
        new WorldBankCountryTransformer().transform(row("XD", "High income", ""), null);
    assertEquals(1, out.size());
    assertEquals("XD", out.get(0).get("country_id"));
    assertEquals("High income", out.get(0).get("country_name"));
  }

  @Test void namibiaCodeStaysText() {
    List<Map<String, Object>> out =
        new WorldBankCountryTransformer().transform(row("NA", "Namibia", "NAM"), null);
    assertEquals("NA", out.get(0).get("country_id"));
  }

  @Test void rowWithoutCountryIdentifierFails() {
    assertThrows(IllegalStateException.class,
        () -> new WorldBankCountryTransformer().transform(row("", "High income", ""), null));
    assertThrows(IllegalStateException.class,
        () -> new WorldBankCountryTransformer()
            .transform(new LinkedHashMap<String, Object>(), null));
  }
}
