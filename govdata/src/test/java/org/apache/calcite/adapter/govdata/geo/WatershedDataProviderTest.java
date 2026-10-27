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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.govdata.DuckDbExtensionInstaller;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The watershed loader hands an extension path to the {@code duckdb} command-line tool, and an
 * extension loads only into the release it was built for.
 */
@Tag("unit")
class WatershedDataProviderTest {

  @Test void versionPrintedByDuckDbIsReducedToTheReleaseNumber() {
    assertEquals("1.5.5", WatershedDataProvider.parseDuckDbVersion("v1.5.5"));
    assertEquals("1.5.6", WatershedDataProvider.parseDuckDbVersion(" 1.5.6 "));
  }

  @Test void anUnrecognisedVersionIsAnErrorNotADefault() {
    assertThrows(IllegalStateException.class, () -> WatershedDataProvider.parseDuckDbVersion(null));
    assertThrows(IllegalStateException.class,
        () -> WatershedDataProvider.parseDuckDbVersion("Variegata"));
  }

  @Test void extensionPathFollowsTheDuckDbReleaseThatLoadsIt() {
    String p155 = DuckDbExtensionInstaller.getLocalExtensionPath("spatial", "1.5.5");
    String p156 = DuckDbExtensionInstaller.getLocalExtensionPath("spatial", "1.5.6");
    assertTrue(p155.contains("/v1.5.5/") && p155.endsWith("spatial.duckdb_extension"), p155);
    assertTrue(p156.contains("/v1.5.6/"), p156);
    assertEquals(p156, DuckDbExtensionInstaller.getLocalExtensionPath("spatial"));
  }
}
