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

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code exempt_org_990_schedule_h}: Form 990 Schedule H (Hospitals), one row per
 * filing that carries the schedule.
 *
 * <p>Reads the same IRS e-file index and TEOS zips as {@link Irs990Provider} (through its shared
 * {@link Irs990Provider#fetchReturns} walk, so the same completeness check applies) and extracts
 * the Part I community benefit table (lines 7a-7k), Part II community building, and Part III bad
 * debt and Medicare. A return without a Schedule H yields no row; a Schedule H part the return
 * omits leaves its columns null, nothing is zero-filled.
 */
public class Irs990ScheduleHProvider implements CachingDataProvider {

  private static final XMLInputFactory XML_FACTORY = newXmlFactory();

  private static final String SCHEDULE_H = "IRS990ScheduleH";

  /** Schedule H Part I / II benefit groups: column prefix -> element name of the group. */
  private static final Map<String, String> GROUPS = buildGroups();

  /** The three amounts each benefit group carries: column suffix -> element local name. */
  private static final String[][] GROUP_AMOUNTS = {
      {"_cost", "TotalCommunityBenefitExpnsAmt"},
      {"_offsetting_revenue", "DirectOffsettingRevenueAmt"},
      {"_net", "NetCommunityBenefitExpnsAmt"},
  };

  /** Direct children of Schedule H (Part III and facility count): column -> element name. */
  private static final Map<String, String> DIRECT_DOUBLES = buildDirectDoubles();

  /** Output column order, the order the schema declares. */
  static final List<String> COLUMNS = buildColumns();

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    return Irs990Provider.fetchReturns(variables, rawCache, new Irs990Provider.RowParser() {
      @Override public Map<String, Object> parse(byte[] xml, String objectId) {
        return parseScheduleH(xml, objectId);
      }
    });
  }

  /**
   * Parses one return. Returns null when it has no Schedule H element. A malformed entry or a
   * Schedule H return without a filer EIN throws: every indexed filing must be readable.
   */
  static Map<String, Object> parseScheduleH(byte[] xml, String objectId) {
    Map<String, String> header = new LinkedHashMap<String, String>();
    Map<String, String> leaves = new LinkedHashMap<String, String>();
    boolean sawSchedule = false;
    List<String> names = new ArrayList<String>();
    List<Boolean> hasChild = new ArrayList<Boolean>();
    StringBuilder text = new StringBuilder();
    XMLStreamReader r = null;
    try {
      r = XML_FACTORY.createXMLStreamReader(new ByteArrayInputStream(xml), "UTF-8");
      int scheduleDepth = -1;
      while (r.hasNext()) {
        int ev = r.next();
        if (ev == XMLStreamConstants.START_ELEMENT) {
          if (!hasChild.isEmpty()) {
            hasChild.set(hasChild.size() - 1, Boolean.TRUE);
          }
          names.add(r.getLocalName());
          hasChild.add(Boolean.FALSE);
          text.setLength(0);
          if (scheduleDepth < 0 && SCHEDULE_H.equals(r.getLocalName())) {
            scheduleDepth = names.size() - 1;
            sawSchedule = true;
          }
        } else if (ev == XMLStreamConstants.CHARACTERS || ev == XMLStreamConstants.CDATA) {
          text.append(r.getText());
        } else if (ev == XMLStreamConstants.END_ELEMENT) {
          int top = names.size() - 1;
          String local = names.get(top);
          if (!hasChild.get(top).booleanValue()) {
            String txt = FiscalHttp.str(text.toString());
            if (txt != null) {
              if (names.contains("ReturnHeader")) {
                recordHeader(header, names, local, txt);
              } else if (scheduleDepth >= 0 && top == scheduleDepth + 1) {
                leaves.putIfAbsent(local, txt);
              } else if (scheduleDepth >= 0 && top == scheduleDepth + 2) {
                leaves.putIfAbsent(names.get(top - 1) + "/" + local, txt);
              }
            }
          }
          if (top == scheduleDepth) {
            scheduleDepth = Integer.MAX_VALUE;  // the schedule is closed; ignore later ones
          }
          names.remove(top);
          hasChild.remove(top);
          text.setLength(0);
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException("exempt_org_990_schedule_h: return " + objectId
          + " could not be parsed: " + e.getMessage(), e);
    } finally {
      if (r != null) {
        try {
          r.close();
        } catch (Exception ignore) {
          // best effort
        }
      }
    }
    if (!sawSchedule) {
      return null;
    }
    if (header.get("ein") == null) {
      throw new IllegalStateException("exempt_org_990_schedule_h: return " + objectId
          + " carries no filer EIN");
    }
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("ein", FiscalHttp.pad(header.get("ein"), 9));
    row.put("org_name", header.get("org_name"));
    row.put("return_type", header.get("return_type"));
    row.put("tax_period_end", header.get("tax_period_end"));
    row.put("tax_year", FiscalHttp.toInt(header.get("tax_year")));
    row.put("object_id", objectId);
    for (Map.Entry<String, String> g : GROUPS.entrySet()) {
      for (String[] a : GROUP_AMOUNTS) {
        row.put(g.getKey() + a[0], FiscalHttp.toDouble(leaves.get(g.getValue() + "/" + a[1])));
      }
    }
    for (Map.Entry<String, String> d : DIRECT_DOUBLES.entrySet()) {
      row.put(d.getKey(), FiscalHttp.toDouble(leaves.get(d.getValue())));
    }
    row.put("hospital_facilities_count", FiscalHttp.toInt(leaves.get("HospitalFacilitiesCnt")));
    return row;
  }

  /** Filer identity from the return header; the first value wins and only the Filer block counts. */
  private static void recordHeader(Map<String, String> header, List<String> names, String local,
      String txt) {
    String field = null;
    if ("EIN".equals(local) && names.contains("Filer")) {
      field = "ein";
    } else if ("BusinessNameLine1Txt".equals(local) && names.contains("Filer")) {
      field = "org_name";
    } else if ("ReturnTypeCd".equals(local)) {
      field = "return_type";
    } else if ("TaxPeriodEndDt".equals(local)) {
      field = "tax_period_end";
    } else if ("TaxYr".equals(local)) {
      field = "tax_year";
    }
    if (field != null) {
      header.putIfAbsent(field, txt);
    }
  }

  private static Map<String, String> buildGroups() {
    Map<String, String> m = new LinkedHashMap<String, String>();
    m.put("financial_assistance", "FinancialAssistanceAtCostTyp");          // line 7a
    m.put("medicaid", "UnreimbursedMedicaidGrp");                           // line 7b
    m.put("other_means_tested", "UnreimbursedCostsGrp");                    // line 7c
    m.put("total_financial_assistance", "TotalFinancialAssistanceTyp");     // line 7d
    m.put("community_health_improvement", "CommunityHealthServicesGrp");    // line 7e
    m.put("health_professions_education", "HealthProfessionsEducationGrp"); // line 7f
    m.put("subsidized_health_services", "SubsidizedHealthServicesGrp");     // line 7g
    m.put("research", "ResearchGrp");                                       // line 7h
    m.put("cash_in_kind_contributions", "CashAndInKindContributionsGrp");   // line 7i
    m.put("total_other_benefits", "TotalOtherBenefitsGrp");                 // line 7j
    m.put("total_community_benefit", "TotalCommunityBenefitsGrp");          // line 7k
    m.put("community_building", "TotalCommuntityBuildingActyGrp");          // Part II total
    return Collections.unmodifiableMap(m);
  }

  private static Map<String, String> buildDirectDoubles() {
    Map<String, String> m = new LinkedHashMap<String, String>();
    m.put("total_community_benefit_pct_expenses", "TotalCommunityBenefitsGrp/TotalExpensePct");
    m.put("bad_debt_expense", "BadDebtExpenseAmt");
    m.put("bad_debt_attributable_to_financial_assistance", "BadDebtExpenseAttributableAmt");
    m.put("medicare_revenue", "ReimbursedByMedicareAmt");
    m.put("medicare_cost", "CostOfCareReimbursedByMedcrAmt");
    m.put("medicare_shortfall", "MedicareSurplusOrShortfallAmt");
    return Collections.unmodifiableMap(m);
  }

  private static List<String> buildColumns() {
    List<String> c = new ArrayList<String>();
    Collections.addAll(c, "ein", "org_name", "return_type", "tax_period_end", "tax_year");
    for (String prefix : GROUPS.keySet()) {
      for (String[] a : GROUP_AMOUNTS) {
        c.add(prefix + a[0]);
      }
    }
    c.addAll(DIRECT_DOUBLES.keySet());
    c.add("hospital_facilities_count");
    c.add("object_id");
    return Collections.unmodifiableList(c);
  }

  private static XMLInputFactory newXmlFactory() {
    XMLInputFactory f = XMLInputFactory.newInstance();
    f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
    f.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
    f.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);
    return f;
  }
}
