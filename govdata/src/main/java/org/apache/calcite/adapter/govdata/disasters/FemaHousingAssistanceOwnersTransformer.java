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
 * Maps FEMA {@code HousingAssistanceOwners} (OpenFEMA v2) records into
 * {@code ihp_housing_assistance_owners} rows. See {@link AbstractHousingAssistanceTransformer}
 * for the county FIPS resolution.
 */
public class FemaHousingAssistanceOwnersTransformer extends AbstractHousingAssistanceTransformer {

  @Override protected String entityName() {
    return "HousingAssistanceOwners";
  }

  @Override protected void mapRow(JsonNode rec, ObjectNode row) {
    mapLocation(rec, row);
    putInt(row, "valid_registrations", rec, "validRegistrations");
    putDouble(row, "average_inspected_damage", rec, "averageFemaInspectedDamage");
    putInt(row, "total_inspected", rec, "totalInspected");
    putDouble(row, "total_damage", rec, "totalDamage");
    putInt(row, "inspected_no_damage", rec, "noFemaInspectedDamage");
    putInt(row, "inspected_damage_1_to_10000", rec, "femaInspectedDamageBetween1And10000");
    putInt(row, "inspected_damage_10001_to_20000", rec, "femaInspectedDamageBetween10001And20000");
    putInt(row, "inspected_damage_20001_to_30000", rec, "femaInspectedDamageBetween20001And30000");
    putInt(row, "inspected_damage_over_30000", rec, "femaInspectedDamageGreaterThan30000");
    mapAssistance(rec, row);
    putText(row, "id", rec, "id");
  }
}
