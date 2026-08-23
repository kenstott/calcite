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
package org.apache.calcite.config;

import org.apache.calcite.sql.validate.SqlConformanceEnum;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link CalciteConnectionProperty#CONFORMANCE_RAGGED_UNION_TO_VARYING}: layering
 * {@link org.apache.calcite.sql.validate.SqlConformance#shouldConvertRaggedUnionTypesToVarying()}
 * onto a base conformance without adopting that conformance's other behavior differences.
 */
class CalciteConnectionConfigConformanceTest {

  private static CalciteConnectionConfig config(String conformance, Boolean raggedOverride) {
    Properties properties = new Properties();
    if (conformance != null) {
      properties.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(), conformance);
    }
    if (raggedOverride != null) {
      properties.setProperty(
          CalciteConnectionProperty.CONFORMANCE_RAGGED_UNION_TO_VARYING.camelName(),
          raggedOverride.toString());
    }
    return new CalciteConnectionConfigImpl(properties);
  }

  @Test void defaultConformanceWithoutOverrideDoesNotConvert() {
    assertFalse(config("DEFAULT", null).conformance().shouldConvertRaggedUnionTypesToVarying());
  }

  @Test void overrideOnDefaultConvertsWithoutAdoptingAnotherConformance() {
    CalciteConnectionConfig cc = config("DEFAULT", true);
    assertTrue(cc.conformance().shouldConvertRaggedUnionTypesToVarying());
    // The point of the narrow property: every OTHER behavior stays DEFAULT's, not MYSQL_5's.
    assertEquals(SqlConformanceEnum.DEFAULT.isGroupByAlias(), cc.conformance().isGroupByAlias());
  }

  @Test void overrideIsANoOpWhenTheBaseConformanceAlreadyConverts() {
    // MYSQL_5 already returns true for this; the property must not double-wrap or change
    // identity-sensitive behavior in that case.
    CalciteConnectionConfig withOverride = config("MYSQL_5", true);
    CalciteConnectionConfig withoutOverride = config("MYSQL_5", null);
    assertTrue(withOverride.conformance().shouldConvertRaggedUnionTypesToVarying());
    assertEquals(withoutOverride.conformance().shouldConvertRaggedUnionTypesToVarying(),
        withOverride.conformance().shouldConvertRaggedUnionTypesToVarying());
  }

  @Test void overridePropertyDefaultsToFalse() {
    assertFalse(config("DEFAULT", null).conformance().shouldConvertRaggedUnionTypesToVarying());
  }
}
