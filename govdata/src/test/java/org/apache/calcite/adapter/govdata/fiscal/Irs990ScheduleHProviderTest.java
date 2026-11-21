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
package org.apache.calcite.adapter.govdata.fiscal;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link Irs990ScheduleHProvider#parseScheduleH}. */
@Tag("unit")
class Irs990ScheduleHProviderTest {

  private static String header(String ein) {
    return "<ReturnHeader><ReturnTypeCd>990</ReturnTypeCd>"
        + "<TaxPeriodEndDt>2024-06-30</TaxPeriodEndDt><TaxYr>2023</TaxYr>"
        + "<PreparerFirmGrp><PreparerFirmName><BusinessNameLine1Txt>PREP LLC</BusinessNameLine1Txt>"
        + "</PreparerFirmName></PreparerFirmGrp>"
        + "<Filer>" + (ein == null ? "" : "<EIN>" + ein + "</EIN>")
        + "<BusinessName><BusinessNameLine1Txt>MERCY HOSPITAL</BusinessNameLine1Txt></BusinessName>"
        + "</Filer></ReturnHeader>";
  }

  private static final String SCHEDULE_H =
      "<IRS990ScheduleH documentId=\"RetDoc1\">"
      + "<FinancialAssistanceAtCostTyp><TotalCommunityBenefitExpnsAmt>1884145</TotalCommunityBenefitExpnsAmt>"
      + "<NetCommunityBenefitExpnsAmt>1884145</NetCommunityBenefitExpnsAmt></FinancialAssistanceAtCostTyp>"
      + "<UnreimbursedMedicaidGrp><TotalCommunityBenefitExpnsAmt>34803388</TotalCommunityBenefitExpnsAmt>"
      + "<DirectOffsettingRevenueAmt>28701754</DirectOffsettingRevenueAmt>"
      + "<NetCommunityBenefitExpnsAmt>6101634</NetCommunityBenefitExpnsAmt></UnreimbursedMedicaidGrp>"
      + "<TotalCommunityBenefitsGrp><TotalCommunityBenefitExpnsAmt>56238840</TotalCommunityBenefitExpnsAmt>"
      + "<NetCommunityBenefitExpnsAmt>16368121</NetCommunityBenefitExpnsAmt>"
      + "<TotalExpensePct>0.06810</TotalExpensePct></TotalCommunityBenefitsGrp>"
      + "<BadDebtExpenseAmt>8528715</BadDebtExpenseAmt>"
      + "<ReimbursedByMedicareAmt>65189398</ReimbursedByMedicareAmt>"
      + "<CostOfCareReimbursedByMedcrAmt>58952393</CostOfCareReimbursedByMedcrAmt>"
      + "<MedicareSurplusOrShortfallAmt>6237005</MedicareSurplusOrShortfallAmt>"
      + "<HospitalFacilitiesCnt>1</HospitalFacilitiesCnt>"
      + "<HospitalFacilitiesGrp><BusinessName><BusinessNameLine1Txt>FACILITY</BusinessNameLine1Txt>"
      + "</BusinessName></HospitalFacilitiesGrp></IRS990ScheduleH>";

  private static byte[] ret(String ein, String body) {
    return ("<Return><ReturnData>" + body + "</ReturnData>" + header(ein) + "</Return>")
        .getBytes(StandardCharsets.UTF_8);
  }

  @Test void extractsLine7ComponentsAndPartIII() {
    Map<String, Object> row = Irs990ScheduleHProvider.parseScheduleH(
        ret("12345678", "<IRS990>" + "<TotalRevenueAmt>1</TotalRevenueAmt></IRS990>" + SCHEDULE_H),
        "202400000000000001");
    assertNotNull(row);
    assertEquals("012345678", row.get("ein"));
    assertEquals("MERCY HOSPITAL", row.get("org_name"));
    assertEquals(Integer.valueOf(2023), row.get("tax_year"));
    assertEquals(Double.valueOf(1884145), row.get("financial_assistance_cost"));
    assertEquals(Double.valueOf(34803388), row.get("medicaid_cost"));
    assertEquals(Double.valueOf(28701754), row.get("medicaid_offsetting_revenue"));
    assertEquals(Double.valueOf(6101634), row.get("medicaid_net"));
    assertEquals(Double.valueOf(56238840), row.get("total_community_benefit_cost"));
    assertEquals(Double.valueOf(0.0681), row.get("total_community_benefit_pct_expenses"));
    assertEquals(Double.valueOf(8528715), row.get("bad_debt_expense"));
    assertEquals(Double.valueOf(6237005), row.get("medicare_shortfall"));
    assertEquals(Integer.valueOf(1), row.get("hospital_facilities_count"));
    // absent groups stay null, not zero
    assertNull(row.get("research_cost"));
    assertEquals(new java.util.HashSet<String>(Irs990ScheduleHProvider.COLUMNS), row.keySet());
  }

  @Test void returnWithoutScheduleHYieldsNoRow() {
    assertNull(Irs990ScheduleHProvider.parseScheduleH(
        ret("123456789", "<IRS990><TotalRevenueAmt>1</TotalRevenueAmt></IRS990>"), "1"));
  }

  @Test void scheduleHWithoutFilerEinThrows() {
    assertThrows(IllegalStateException.class,
        () -> Irs990ScheduleHProvider.parseScheduleH(ret(null, SCHEDULE_H), "2"));
  }
}
