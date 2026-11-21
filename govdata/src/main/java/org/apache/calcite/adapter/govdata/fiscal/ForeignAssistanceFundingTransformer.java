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

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * U.S. foreign assistance by funding agency, funding account, recipient country, transaction type
 * and fiscal year, read from the ForeignAssistance.gov bulk file
 * {@code /static/us_foreign_aid_funding.csv}. The site sits behind a Cloudflare challenge that
 * answers HTTP 403 to non-browser clients, so the file is fetched through a headed browser session
 * ({@link ForeignAssistanceBrowserSession}). The header must match the expected one, every amount
 * and id must parse, and a repeated key fails the run.
 */
public class ForeignAssistanceFundingTransformer implements StreamingResponseTransformer {

  static final String FILE_URL = "/static/us_foreign_aid_funding.csv";
  static final List<String> HEADER = Arrays.asList("Funding Agency ID", "Funding Agency Acronym",
      "Funding Agency Name", "Funding Account ID", "Funding Account Name", "Country Code",
      "Country Name", "Transaction Type ID", "Transaction Type Name", "Fiscal Year",
      "current_amount", "constant_amount");

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        "foreign_assistance_by_country is a StreamingResponseTransformer; call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) {
    String csv;
    try {
      csv = ForeignAssistanceBrowserSession.get(context).fetchText(FILE_URL);
    } catch (IOException e) {
      throw new RuntimeException("foreign_assistance_by_country: cannot read " + FILE_URL, e);
    }
    if (csv == null) {
      throw new IllegalStateException("foreign_assistance_by_country: " + FILE_URL
          + " answered 404");
    }
    return parse(csv).iterator();
  }

  static List<Map<String, Object>> parse(String csv) {
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    Set<String> keys = new HashSet<String>();
    try {
      BufferedReader in = new BufferedReader(new StringReader(csv));
      String record = CsvRecordReader.readRecord(in);
      if (record == null || !HEADER.equals(CsvRecordReader.splitFields(record, ','))) {
        throw new IllegalStateException("foreign_assistance_by_country: header is '" + record
            + "', expected " + HEADER);
      }
      int lineNo = 1;
      while ((record = CsvRecordReader.readRecord(in)) != null) {
        lineNo++;
        if (record.trim().isEmpty()) {
          continue;
        }
        List<String> f = CsvRecordReader.splitFields(record, ',');
        if (f.size() != HEADER.size()) {
          throw new IllegalStateException("foreign_assistance_by_country: record " + lineNo
              + " has " + f.size() + " fields, expected " + HEADER.size());
        }
        Map<String, Object> r = new LinkedHashMap<String, Object>();
        r.put("fiscal_year", Integer.valueOf(intOf(f.get(9), lineNo)));
        r.put("funding_agency_id", Integer.valueOf(intOf(f.get(0), lineNo)));
        r.put("funding_agency_acronym", text(f.get(1), lineNo));
        r.put("funding_agency_name", text(f.get(2), lineNo));
        r.put("funding_account_id", text(f.get(3), lineNo));
        r.put("funding_account_name", text(f.get(4), lineNo));
        r.put("country_code", text(f.get(5), lineNo));
        r.put("country_name", text(f.get(6), lineNo));
        r.put("transaction_type_id", Integer.valueOf(intOf(f.get(7), lineNo)));
        r.put("transaction_type", text(f.get(8), lineNo));
        r.put("current_amount", Double.valueOf(amountOf(f.get(10), lineNo)));
        r.put("constant_amount", Double.valueOf(amountOf(f.get(11), lineNo)));
        String key = r.get("fiscal_year") + "|" + r.get("funding_agency_id") + "|"
            + r.get("funding_account_id") + "|" + r.get("country_code") + "|"
            + r.get("transaction_type_id");
        if (!keys.add(key)) {
          throw new IllegalStateException("foreign_assistance_by_country: record " + lineNo
              + " repeats key " + key);
        }
        out.add(r);
      }
    } catch (IOException e) {
      throw new RuntimeException("foreign_assistance_by_country: cannot parse the CSV", e);
    }
    if (out.isEmpty()) {
      throw new IllegalStateException("foreign_assistance_by_country: the file has no rows");
    }
    return out;
  }

  private static String text(String v, int lineNo) {
    String t = v.trim();
    if (t.isEmpty()) {
      throw new IllegalStateException("foreign_assistance_by_country: record " + lineNo
          + " has an empty required text field");
    }
    return t;
  }

  private static int intOf(String v, int lineNo) {
    try {
      return Integer.parseInt(v.trim());
    } catch (NumberFormatException e) {
      throw new IllegalStateException("foreign_assistance_by_country: record " + lineNo
          + " has non-integer '" + v + "'", e);
    }
  }

  private static double amountOf(String v, int lineNo) {
    try {
      return Double.parseDouble(v.trim());
    } catch (NumberFormatException e) {
      throw new IllegalStateException("foreign_assistance_by_country: record " + lineNo
          + " has non-numeric amount '" + v + "'", e);
    }
  }
}
