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
package org.apache.calcite.adapter.govdata.health;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Flattens one page of openFDA FAERS adverse event results.
 *
 * <p>Key nesting: patient demographics under {@code patient.*}, drug info from
 * the first suspect drug in {@code patient.drug[]}, and reactions from
 * {@code patient.reaction[].reactionmeddrapt} joined with commas.
 */
public class FdaAdverseEventsResponseTransformer extends AbstractOpenFdaResponseTransformer {

  @Override
  protected void flattenRecord(JsonNode record, ObjectNode row) {
    put(row, "safety_report_id", text(record, "safetyreportid"));
    put(row, "receive_date", text(record, "receivedate"));

    String receiveDate = text(record, "receivedate");
    if (receiveDate != null && receiveDate.length() >= 4) {
      put(row, "receive_year", receiveDate.substring(0, 4));
    } else {
      row.putNull("receive_year");
    }

    put(row, "serious", text(record, "serious"));
    put(row, "serious_death", text(record, "seriousnessdeath"));

    JsonNode patient = record.path("patient");
    put(row, "patient_age", text(patient, "patientonsetage"));
    put(row, "patient_age_unit", text(patient, "patientonsetageunit"));
    put(row, "patient_sex", text(patient, "patientsex"));

    // Primary (first suspect) drug
    JsonNode drugs = patient.path("drug");
    JsonNode primaryDrug = MAPPER.createObjectNode();
    if (drugs.isArray()) {
      for (JsonNode drug : drugs) {
        // characterization: 1=suspect, 2=concomitant, 3=interacting
        String char_ = text(drug, "drugcharacterization");
        if ("1".equals(char_)) {
          primaryDrug = drug;
          break;
        }
      }
      // Fallback to first drug if no suspect found
      if (primaryDrug == MAPPER.createObjectNode() && drugs.size() > 0) {
        primaryDrug = drugs.get(0);
      }
    }
    put(row, "primary_drug", text(primaryDrug, "medicinalproduct"));
    put(row, "drug_indication", text(primaryDrug, "drugindication"));
    put(row, "drug_route", text(primaryDrug, "drugadministrationroute"));

    // Reactions: join all reactionmeddrapt values
    JsonNode reactions = patient.path("reaction");
    if (reactions.isArray() && reactions.size() > 0) {
      StringBuilder sb = new StringBuilder();
      for (JsonNode rx : reactions) {
        String term = text(rx, "reactionmeddrapt");
        if (term != null) {
          if (sb.length() > 0) {
            sb.append(", ");
          }
          sb.append(term);
        }
      }
      put(row, "reactions", sb.length() > 0 ? sb.toString() : null);
    } else {
      row.putNull("reactions");
    }

    put(row, "reporter_country", text(record, "occurcountry"));

    row.put("type", "fda_adverse_events");
  }
}
