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
 * Transforms CMS Open Payments CSV bulk downloads.
 *
 * <p>Source: download.cms.gov direct CSV files (general/research/ownership).
 * Maps CMS CSV headers (Title_Case) to 15 target columns.
 * The payment_type dimension value is injected by the framework.
 */
public class CmsOpenPaymentsResponseTransformer extends AbstractCsvResponseTransformer {

  @Override
  protected void mapRow(String[] headers, String[] values, ObjectNode row) {
    put(row, "program_year", col(headers, values, "Program_Year"));
    put(row, "change_type", col(headers, values, "Change_Type"));
    put(row, "physician_profile_id", col(headers, values, "Covered_Recipient_Profile_ID"));
    put(row, "physician_first_name", col(headers, values, "Covered_Recipient_First_Name"));
    put(row, "physician_last_name", col(headers, values, "Covered_Recipient_Last_Name"));
    put(row, "physician_specialty", col(headers, values, "Covered_Recipient_Primary_Type_1"));
    put(row, "physician_state", col(headers, values, "Recipient_State"));
    put(row, "physician_country", col(headers, values, "Recipient_Country"));
    put(row, "paying_entity_name",
        col(headers, values, "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name"));
    put(row, "total_amount", col(headers, values, "Total_Amount_of_Payment_USDollars"));
    put(row, "payment_date", col(headers, values, "Date_of_Payment"));
    put(row, "number_of_payments",
        col(headers, values, "Number_of_Payments_Included_in_Total_Amount"));
    put(row, "nature_of_payment",
        col(headers, values, "Nature_of_Payment_or_Transfer_of_Value"));
    put(row, "product_name", truncate(
        col(headers, values, "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1"), 500));
    put(row, "product_category",
        col(headers, values, "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1"));
  }
}
