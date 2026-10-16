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
package org.apache.calcite.adapter.govdata.disasters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Maps FEMA {@code HousingAssistanceRenters} (OpenFEMA v2) records into
 * {@code ihp_housing_assistance_renters} rows. See {@link AbstractHousingAssistanceTransformer}
 * for the county FIPS resolution.
 */
public class FemaHousingAssistanceRentersTransformer extends AbstractHousingAssistanceTransformer {

  @Override protected String entityName() {
    return "HousingAssistanceRenters";
  }

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    mapLocation(rec, row);
    putInt(row, "valid_registrations", rec, "validRegistrations");
    putInt(row, "total_inspected", rec, "totalInspected");
    putInt(row, "inspected_no_damage", rec, "totalInspectedWithNoDamage");
    putInt(row, "moderate_damage", rec, "totalWithModerateDamage");
    putInt(row, "major_damage", rec, "totalWithMajorDamage");
    putInt(row, "substantial_damage", rec, "totalWithSubstantialDamage");
    mapAssistance(rec, row);
    putText(row, "id", rec, "id");
  }
}
