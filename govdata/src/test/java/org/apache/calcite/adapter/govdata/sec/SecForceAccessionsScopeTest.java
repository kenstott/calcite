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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager.ColumnDef;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression for kenstott/govdata-ops#917: force-reprocess passed the forced accession set to every
 * table, and the delete by {@code accession_number} failed for tables without that column
 * (stock_prices).
 */
@Tag("unit")
class SecForceAccessionsScopeTest {

  private static final Set<String> FORCED =
      new HashSet<String>(Arrays.asList("0000320193-23-000106", "0000789019-23-000070"));

  @Test void tableWithAccessionNumberKeepsForcedSet() {
    List<ColumnDef> columns = Arrays.asList(
        new ColumnDef("cik", "VARCHAR", null),
        new ColumnDef("accession_number", "VARCHAR", null));
    assertEquals(FORCED, SecSchemaFactory.forceAccessionsFor(columns, FORCED));
  }

  @Test void tableWithoutAccessionNumberGetsNoForcedSet() {
    List<ColumnDef> columns = Arrays.asList(
        new ColumnDef("ticker", "VARCHAR", null),
        new ColumnDef("trade_date", "DATE", null));
    assertTrue(SecSchemaFactory.forceAccessionsFor(columns, FORCED).isEmpty());
  }

  @Test void noColumnsGetsNoForcedSet() {
    assertTrue(SecSchemaFactory.forceAccessionsFor(
        Collections.<ColumnDef>emptyList(), FORCED).isEmpty());
  }
}
