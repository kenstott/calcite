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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.energy.EiaBulkXlsxTransformer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.time.YearMonth;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transforms Federal Student Aid's PSLF Combined Report (pslf-combined-report.xlsx) into
 * {@code edu.fsa_pslf_discharge_summary} rows — one row per (section, program, metric) as of the
 * report's own covering date.
 *
 * <p>The workbook has a fixed layout (verified live 2026-09-27) across three data sheets:
 * "PSLF Application Status" (label/value pairs, forms + borrowers columns), "Cumulative PSLF
 * Portfolio" (label/value pairs, one value column), and "PSLF Discharges" (wide: one row per
 * metric, one column per program). A fourth sheet, "Report Definitions", is prose glossary text
 * and is not ingested. Two labels ("a) Government" / "b) Non-Profit - Section 501(c)(3) or
 * Other") repeat verbatim under two different parent sections in "PSLF Application Status"
 * (employment-certification counts, then a forgiveness-eligible subset of the same counts) —
 * disambiguated positionally via {@code employmentTypeBlockSeen} rather than by label text alone.
 */
public class FsaPslfDischargeSummaryTransformer extends EiaBulkXlsxTransformer {

  private static final Pattern AS_OF_PATTERN =
      Pattern.compile("([A-Za-z]{3})[a-z]*-end\\s+(\\d{4})", Pattern.CASE_INSENSITIVE);

  private static final Pattern USD_PATTERN =
      Pattern.compile("\\$\\s*([0-9,.]+)\\s*(billion|million|thousand)?", Pattern.CASE_INSENSITIVE);

  private static final String[] MONTH_ABBR = {
      "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"
  };

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception {
    String asOfDate = parseAsOfDate(findAsOfText(workbook)).atEndOfMonth().toString();
    ArrayNode result = MAPPER.createArrayNode();
    parseApplicationStatus(workbook.getSheet("PSLF Application Status"), asOfDate, result);
    parseCumulativePortfolio(workbook.getSheet("Cumulative PSLF Portfolio"), asOfDate, result);
    parseDischarges(workbook.getSheet("PSLF Discharges"), asOfDate, result);
    if (result.size() == 0) {
      throw new IllegalStateException(
          "FSA PSLF combined report: parsed zero rows — sheet layout may have changed");
    }
    LOGGER.debug("FSA PSLF discharge summary: emitted {} rows as of {}", result.size(), asOfDate);
    return result.toString();
  }

  private void parseApplicationStatus(Sheet sheet, String asOfDate, ArrayNode result) {
    if (sheet == null) {
      return;
    }
    boolean employmentTypeBlockSeen = false;
    for (Row row : sheet) {
      String label = normalizeLabel(cellString(row.getCell(0)));
      if (label == null) {
        continue;
      }
      Cell forms = row.getCell(1);
      switch (label) {
      case "Total Count of Submitted PSLF Applications":
        emit(result, asOfDate, "application_status", "PSLF",
            "total_submitted_applications_forms", cellDouble(forms), "forms");
        emit(result, asOfDate, "application_status", "PSLF",
            "total_submitted_applications_borrowers", cellDouble(row.getCell(2)), "borrowers");
        break;
      case "Count of Processed Applications":
        emit(result, asOfDate, "application_status", "PSLF",
            "processed_applications_forms", cellDouble(forms), "forms");
        emit(result, asOfDate, "application_status", "PSLF",
            "processed_applications_borrowers", cellDouble(row.getCell(2)), "borrowers");
        break;
      case "Count of Pending Applications":
        emit(result, asOfDate, "application_status", "PSLF",
            "pending_applications_forms", cellDouble(forms), "forms");
        emit(result, asOfDate, "application_status", "PSLF",
            "pending_applications_borrowers", cellDouble(row.getCell(2)), "borrowers");
        break;
      case "Count of Closed/Cancelled Applications":
        emit(result, asOfDate, "application_status", "PSLF",
            "closed_cancelled_applications_forms", cellDouble(forms), "forms");
        emit(result, asOfDate, "application_status", "PSLF",
            "closed_cancelled_applications_borrowers", cellDouble(row.getCell(2)), "borrowers");
        break;
      case "Count of forms that met employment certification requirements":
        emit(result, asOfDate, "application_status", "PSLF",
            "forms_meeting_employment_certification", cellDouble(forms), "forms");
        break;
      case "a) Government":
        emit(result, asOfDate, "application_status", "PSLF",
            employmentTypeBlockSeen ? "forms_meeting_forgiveness_government"
                : "forms_meeting_certification_government",
            cellDouble(forms), "forms");
        break;
      case "b) Non-Profit - Section 501(c)(3) or Other":
        emit(result, asOfDate, "application_status", "PSLF",
            employmentTypeBlockSeen ? "forms_meeting_forgiveness_nonprofit"
                : "forms_meeting_certification_nonprofit",
            cellDouble(forms), "forms");
        employmentTypeBlockSeen = true;
        break;
      case "Subset of forms that met employment certification requirements that also met "
          + "requirements for PSLF forgiveness":
        emit(result, asOfDate, "application_status", "PSLF",
            "forms_meeting_forgiveness_requirements", cellDouble(forms), "forms");
        break;
      case "Awaiting Signature Documentation":
        emit(result, asOfDate, "application_status", "PSLF",
            "pending_awaiting_signature", cellDouble(forms), "forms");
        break;
      case "Undergoing Employer Eligibility Assessment":
        emit(result, asOfDate, "application_status", "PSLF",
            "pending_employer_eligibility_assessment", cellDouble(forms), "forms");
        break;
      case "Assessing Qualifying Payment Counts":
        emit(result, asOfDate, "application_status", "PSLF",
            "pending_assessing_qp_counts", cellDouble(forms), "forms");
        break;
      case "Incomplete Application":
        emit(result, asOfDate, "application_status", "PSLF",
            "closed_incomplete_application", cellDouble(forms), "forms");
        break;
      case "Closed Per Borrower Request":
        emit(result, asOfDate, "application_status", "PSLF",
            "closed_per_borrower_request", cellDouble(forms), "forms");
        break;
      case "Signature Issues":
        emit(result, asOfDate, "application_status", "PSLF",
            "closed_signature_issues", cellDouble(forms), "forms");
        break;
      case "Employer Eligibility Issues":
        emit(result, asOfDate, "application_status", "PSLF",
            "closed_employer_eligibility_issues", cellDouble(forms), "forms");
        break;
      case "No Open Loans":
        emit(result, asOfDate, "application_status", "PSLF",
            "closed_no_open_loans", cellDouble(forms), "forms");
        break;
      case "Other":
        emit(result, asOfDate, "application_status", "PSLF",
            "closed_other", cellDouble(forms), "forms");
        break;
      default:
        // Section header, sub-header, or footnote row — not a data row.
        break;
      }
    }
  }

  private void parseCumulativePortfolio(Sheet sheet, String asOfDate, ArrayNode result) {
    if (sheet == null) {
      return;
    }
    for (Row row : sheet) {
      String label = normalizeLabel(cellString(row.getCell(0)));
      if (label == null) {
        continue;
      }
      Cell value = row.getCell(1);
      switch (label) {
      case "Cumulative PSLF borrowers with Eligible Employment":
        emit(result, asOfDate, "cumulative_portfolio", "PSLF",
            "cumulative_borrowers_eligible_employment", cellDouble(value), "borrowers");
        break;
      case "Total outstanding balance for borrowers with eligible employment":
        emit(result, asOfDate, "cumulative_portfolio", "PSLF",
            "total_outstanding_balance_eligible_employment", parseUsd(value), "usd");
        break;
      case "Average outstanding balance for borrowers with eligible employment":
        emit(result, asOfDate, "cumulative_portfolio", "PSLF",
            "average_outstanding_balance_eligible_employment", parseUsd(value), "usd");
        break;
      case "a) 0":
        emit(result, asOfDate, "cumulative_portfolio", "PSLF",
            "qp_count_0", cellDouble(value), "borrowers");
        break;
      case "b) 1-24":
        emit(result, asOfDate, "cumulative_portfolio", "PSLF",
            "qp_count_1_24", cellDouble(value), "borrowers");
        break;
      case "c) 25-48":
        emit(result, asOfDate, "cumulative_portfolio", "PSLF",
            "qp_count_25_48", cellDouble(value), "borrowers");
        break;
      case "d) 49-72":
        emit(result, asOfDate, "cumulative_portfolio", "PSLF",
            "qp_count_49_72", cellDouble(value), "borrowers");
        break;
      case "e) 73-96":
        emit(result, asOfDate, "cumulative_portfolio", "PSLF",
            "qp_count_73_96", cellDouble(value), "borrowers");
        break;
      case "f) 97-119":
        emit(result, asOfDate, "cumulative_portfolio", "PSLF",
            "qp_count_97_119", cellDouble(value), "borrowers");
        break;
      case "g) Over 119":
        emit(result, asOfDate, "cumulative_portfolio", "PSLF",
            "qp_count_over_119", cellDouble(value), "borrowers");
        break;
      default:
        break;
      }
    }
  }

  private void parseDischarges(Sheet sheet, String asOfDate, ArrayNode result) {
    if (sheet == null) {
      return;
    }
    Row header = findDischargeHeaderRow(sheet);
    if (header == null) {
      return;
    }
    Map<Integer, String> columnPrograms = new LinkedHashMap<>();
    for (Cell cell : header) {
      String text = cellString(cell);
      if (text == null) {
        continue;
      }
      text = text.trim();
      if (text.endsWith("Discharges")) {
        String program = text.replace("Combined PSLF Discharges", "Combined")
            .replace(" Discharges", "");
        columnPrograms.put(cell.getColumnIndex(), program);
      }
    }
    for (Row row : sheet) {
      if (row.getRowNum() == header.getRowNum()) {
        continue;
      }
      String label = normalizeLabel(cellString(row.getCell(0)));
      if (label == null) {
        continue;
      }
      String metricName;
      String unit;
      boolean isUsd;
      if ("Unique Borrowers Processed".equals(label)) {
        metricName = "unique_borrowers_processed";
        unit = "borrowers";
        isUsd = false;
      } else if ("Total Balance Discharged".equals(label)) {
        metricName = "total_balance_discharged";
        unit = "usd";
        isUsd = true;
      } else if ("Average Balance Discharged".equals(label)) {
        metricName = "average_balance_discharged";
        unit = "usd";
        isUsd = true;
      } else {
        continue;
      }
      for (Map.Entry<Integer, String> entry : columnPrograms.entrySet()) {
        Cell cell = row.getCell(entry.getKey());
        Double value = isUsd ? parseUsd(cell) : cellDouble(cell);
        emit(result, asOfDate, "discharges", entry.getValue(), metricName, value, unit);
      }
    }
  }

  private Row findDischargeHeaderRow(Sheet sheet) {
    for (Row row : sheet) {
      for (Cell cell : row) {
        String text = cellString(cell);
        if (text != null && text.trim().equals("Combined PSLF Discharges")) {
          return row;
        }
      }
    }
    return null;
  }

  private String findAsOfText(XSSFWorkbook workbook) {
    for (String sheetName : new String[] {"PSLF Discharges", "Cumulative PSLF Portfolio"}) {
      Sheet sheet = workbook.getSheet(sheetName);
      if (sheet == null) {
        continue;
      }
      for (Row row : sheet) {
        String text = cellString(row.getCell(0));
        if (text != null && text.toLowerCase(Locale.ROOT).contains("as of")) {
          return text;
        }
      }
    }
    throw new IllegalStateException(
        "FSA PSLF combined report: could not locate an 'as of' date label");
  }

  private YearMonth parseAsOfDate(String text) {
    Matcher matcher = AS_OF_PATTERN.matcher(text);
    if (!matcher.find()) {
      throw new IllegalStateException("FSA PSLF combined report: could not parse as-of date from '"
          + text + "'");
    }
    String monthAbbr = matcher.group(1).toLowerCase(Locale.ROOT);
    int month = -1;
    for (int i = 0; i < MONTH_ABBR.length; i++) {
      if (MONTH_ABBR[i].equals(monthAbbr)) {
        month = i + 1;
        break;
      }
    }
    if (month < 0) {
      throw new IllegalStateException(
          "FSA PSLF combined report: unrecognized month abbreviation '" + monthAbbr + "'");
    }
    int year = Integer.parseInt(matcher.group(2));
    return YearMonth.of(year, month);
  }

  private Double parseUsd(Cell cell) {
    if (cell == null) {
      return null;
    }
    if (cell.getCellType() != CellType.STRING) {
      return cellDouble(cell);
    }
    Matcher matcher = USD_PATTERN.matcher(cell.getStringCellValue().trim());
    if (!matcher.matches()) {
      return null;
    }
    double amount = Double.parseDouble(matcher.group(1).replace(",", ""));
    String unit = matcher.group(2);
    double multiplier = 1.0;
    if (unit != null) {
      String lower = unit.toLowerCase(Locale.ROOT);
      if ("billion".equals(lower)) {
        multiplier = 1_000_000_000.0;
      } else if ("million".equals(lower)) {
        multiplier = 1_000_000.0;
      } else if ("thousand".equals(lower)) {
        multiplier = 1_000.0;
      }
    }
    return amount * multiplier;
  }

  private String normalizeLabel(String raw) {
    if (raw == null) {
      return null;
    }
    String normalized = raw.replaceAll("\\s+", " ").trim();
    return normalized.isEmpty() ? null : normalized;
  }

  private void emit(ArrayNode result, String asOfDate, String section, String program,
      String metricName, Double value, String unit) {
    ObjectNode out = MAPPER.createObjectNode();
    out.put("report_as_of_date", asOfDate);
    out.put("section", section);
    out.put("program", program);
    out.put("metric_name", metricName);
    if (value != null) {
      out.put("metric_value", value);
    } else {
      out.putNull("metric_value");
    }
    out.put("metric_unit", unit);
    result.add(out);
  }
}
