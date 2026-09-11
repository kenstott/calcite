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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.RowContext;
import org.apache.calcite.adapter.file.etl.RowTransformer;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Drops {@code gleif_cik_mapping} rows whose {@code cik} is not purely numeric.
 *
 * <p>GLEIF's golden copy mixes two identifier shapes under registration authority RA000665
 * (SEC): real numeric CIKs for operating-company filers, and SEC fund-series identifiers
 * (e.g. {@code "S000005113"}) for fund series registered with SEC but not themselves 10-K/10-Q
 * filers. A series ID never matches a real CIK downstream (canonical_org_entity's sec_cik
 * bridge, entity_org_bridge, sec.filing_metadata joins), so it is dropped rather than diluting
 * the mapping. Confirmed live 2026-09-10: 22,932 of 27,929 rows were S-prefixed series IDs
 * against only 4,997 real numeric CIKs (kenstott/govdata-ops#79).
 */
public class GleifCikNumericFilterTransformer implements RowTransformer {

  /**
   * RowTransformer runs on the raw source row, keyed by the CSV's own header — the rename to
   * the declared column name ("cik", via the column's {@code source:} mapping) happens later,
   * during materialization. {@code row.get("cik")} would always be null here.
   */
  private static final String CIK_SOURCE_FIELD =
      "Entity.RegistrationAuthority.RegistrationAuthorityEntityID";

  @Override public List<Map<String, Object>> transform(Map<String, Object> row, RowContext context) {
    Object cik = row.get(CIK_SOURCE_FIELD);
    if (cik == null || !cik.toString().matches("[0-9]+")) {
      return Collections.emptyList();
    }
    return Collections.singletonList(row);
  }
}
