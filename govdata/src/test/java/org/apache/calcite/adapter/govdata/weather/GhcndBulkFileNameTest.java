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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests the by_year cache file naming of {@link GhcndBulkDataProvider}. */
@Tag("unit")
class GhcndBulkFileNameTest {

  private static final LocalDate TODAY = LocalDate.of(2026, 11, 25);

  @Test void currentYearIsStampedWithIsoWeek() {
    assertEquals("ghcnd_bulk_2026_w2026-48.csv.gz",
        GhcndBulkDataProvider.bulkFileName("2026", TODAY));
  }

  @Test void previousYearIsStampedWithIsoWeek() {
    assertEquals("ghcnd_bulk_2025_w2026-48.csv.gz",
        GhcndBulkDataProvider.bulkFileName("2025", TODAY));
  }

  @Test void closedYearUsesFinalName() {
    assertEquals("ghcnd_bulk_2024_final.csv.gz",
        GhcndBulkDataProvider.bulkFileName("2024", TODAY));
  }

  @Test void newWeekChangesTheName() {
    assertEquals("ghcnd_bulk_2026_w2026-49.csv.gz",
        GhcndBulkDataProvider.bulkFileName("2026", TODAY.plusWeeks(1)));
  }

  @Test void januaryDaysOfThePreviousIsoYearKeepThatWeekYear() {
    assertEquals("ghcnd_bulk_2026_w2026-53.csv.gz",
        GhcndBulkDataProvider.bulkFileName("2026", LocalDate.of(2027, 1, 1)));
  }
}
