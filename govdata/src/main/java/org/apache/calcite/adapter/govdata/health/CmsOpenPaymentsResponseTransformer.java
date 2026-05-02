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

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Transforms CMS Open Payments CSV data into the target schema.
 *
 * <p>Receives CSV batches (header + data rows). Maps 89 CMS columns to 15 target columns.
 * The payment_type dimension value (general/research/ownership) is injected by the framework
 * from the dimension variable.
 */
public class CmsOpenPaymentsResponseTransformer extends AbstractCsvResponseTransformer {

  @Override
  protected void mapRow(String[] headers, String[] values, ObjectNode row) {
    put(row, "program_year", col(headers, values, "program_year"));
    put(row, "change_type", col(headers, values, "change_type"));
    put(row, "physician_profile_id",
        col(headers, values, "covered_recipient_profile_id"));
    put(row, "physician_first_name",
        col(headers, values, "covered_recipient_first_name"));
    put(row, "physician_last_name",
        col(headers, values, "covered_recipient_last_name"));
    put(row, "physician_specialty",
        col(headers, values, "covered_recipient_primary_type_1"));
    put(row, "physician_state",
        col(headers, values, "recipient_state"));
    put(row, "physician_country",
        col(headers, values, "recipient_country"));
    put(row, "paying_entity_name",
        col(headers, values, "applicable_manufacturer_or_applicable_gpo_making_payment_name"));
    put(row, "total_amount",
        col(headers, values, "total_amount_of_payment_usdollars"));
    put(row, "payment_date",
        col(headers, values, "date_of_payment"));
    put(row, "number_of_payments",
        col(headers, values, "number_of_payments_included_in_total_amount"));
    put(row, "nature_of_payment",
        col(headers, values, "nature_of_payment_or_transfer_of_value"));
    put(row, "product_name",
        truncate(col(headers, values, "name_of_drug_or_biological_or_device_or_medical_supply_1"),
            500));
    put(row, "product_category",
        col(headers, values,
            "indicate_drug_or_biological_or_device_or_medical_supply_1"));
  }
}
