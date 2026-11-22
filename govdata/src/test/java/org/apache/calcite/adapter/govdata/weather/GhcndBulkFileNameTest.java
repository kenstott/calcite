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
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.PipelineClock;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the by_year cache file naming of {@link GhcndBulkDataProvider}. */
@Tag("unit")
class GhcndBulkFileNameTest {

  @Test void currentYearIsKeyedByRefreshMonth() {
    String year = String.valueOf(PipelineClock.currentYear());
    assertEquals("ghcnd_bulk_" + year + "_m11.csv.gz",
        GhcndBulkDataProvider.bulkFileName(year, "11"));
  }

  @Test void previousYearIsKeyedByRefreshMonth() {
    String year = String.valueOf(PipelineClock.currentYear() - 1);
    assertEquals("ghcnd_bulk_" + year + "_m12.csv.gz",
        GhcndBulkDataProvider.bulkFileName(year, "12"));
  }

  @Test void closedYearKeepsSingleName() {
    String year = String.valueOf(PipelineClock.currentYear() - 2);
    assertEquals("ghcnd_bulk_" + year + ".csv.gz",
        GhcndBulkDataProvider.bulkFileName(year, "11"));
  }

  @Test void openYearWithoutRefreshMonthFails() {
    String year = String.valueOf(PipelineClock.currentYear());
    assertThrows(IllegalStateException.class,
        () -> GhcndBulkDataProvider.bulkFileName(year, null));
  }
}
