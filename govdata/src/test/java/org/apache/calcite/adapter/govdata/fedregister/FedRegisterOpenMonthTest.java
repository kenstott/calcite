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
package org.apache.calcite.adapter.govdata.fedregister;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Which Federal Register months bypass the raw ZIP cache. */
@Tag("unit")
class FedRegisterOpenMonthTest {
  private static final LocalDate TODAY = LocalDate.of(2026, 11, 22);

  @Test void currentAndTwoPriorMonthsAreOpen() {
    assertTrue(FedRegisterBulkXmlDataProvider.isOpenMonth(2026, 11, TODAY));
    assertTrue(FedRegisterBulkXmlDataProvider.isOpenMonth(2026, 10, TODAY));
    assertTrue(FedRegisterBulkXmlDataProvider.isOpenMonth(2026, 9, TODAY));
  }

  @Test void olderMonthsAreCached() {
    assertFalse(FedRegisterBulkXmlDataProvider.isOpenMonth(2026, 8, TODAY));
    assertFalse(FedRegisterBulkXmlDataProvider.isOpenMonth(2025, 11, TODAY));
  }

  @Test void windowSpansYearBoundary() {
    LocalDate jan = LocalDate.of(2027, 1, 5);
    assertTrue(FedRegisterBulkXmlDataProvider.isOpenMonth(2026, 11, jan));
    assertFalse(FedRegisterBulkXmlDataProvider.isOpenMonth(2026, 10, jan));
  }
}
