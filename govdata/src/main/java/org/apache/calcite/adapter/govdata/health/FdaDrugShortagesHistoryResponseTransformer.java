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

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Locale;

/**
 * Parses one Wayback Machine capture of FDA's own drug-shortage CSV export
 * ({@code accessdata.fda.gov/scripts/drugshortages/Drugshortages.cfm}) into rows for
 * {@code fda_drug_shortages_history}.
 *
 * <p>FDA purges resolved shortages from its live feed six months after resolution, so
 * {@code fda_drug_shortages} (the live-feed table) carries no real multi-year history. The
 * Wayback Machine holds ~99 point-in-time captures of the source's own CSV export back to
 * Oct 2019 (confirmed live via the CDX index), each served as {@code text/csv}. This
 * transformer runs once per captured snapshot (the {@code snapshot_ts} dimension is the
 * literal, fixed list of capture timestamps — see the table's dimension comment).
 *
 * <p>Two normalizations are needed because the export's own header line drifts across
 * snapshots (leading/trailing spaces, occasional casing changes) even though the 22-column
 * shape itself is stable end to end (verified against an Oct 2019 and a Nov 2025 capture):
 * header lookup is done via a whitespace/case-insensitive key, and {@code Status} values are
 * canonicalized (FDA's own export mixes {@code "To be Discontinued"} and
 * {@code "To Be Discontinued"} across snapshots) so the DQ domain check and downstream
 * queries see one consistent value set.
 *
 * <p>The CSV itself opens with a blank line before the header — blank records are skipped
 * while scanning for the header row. Quoted fields (including ones with embedded commas)
 * are handled via {@link CsvRecordReader}, the same RFC4180 reader the pipeline's own
 * streaming CSV path uses.
 */
public class FdaDrugShortagesHistoryResponseTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** normalized (lowercase, non-alphanumeric stripped) source header -> output column. */
  private static final String[][] COLUMN_MAP = {
      {"genericname", "generic_name"},
      {"companyname", "company_name"},
      {"contactinfo", "contact_info"},
      {"presentation", "presentation"},
      {"typeofupdate", "update_type"},
      {"dateofupdate", "update_date"},
      {"availabilityinformation", "availability"},
      {"relatedinformation", "related_info"},
      {"resolvednote", "resolved_note"},
      {"reasonforshortage", "reason_for_shortage"},
      {"therapeuticcategory", "therapeutic_category"},
      {"status", "status"},
      {"changedate", "change_date"},
      {"datediscontinued", "discontinued_date"},
      {"initialpostingdate", "initial_posting_date"},
      {"genericnamenote", "generic_name_note"},
      {"genericnamelink", "generic_name_link"},
      {"companyinfolink", "company_info_link"},
      {"availabilitylink", "availability_link"},
      {"relatedinfolink", "related_info_link"},
      {"resolvednotelink", "resolved_note_link"},
      {"discontinuednotelink", "discontinued_note_link"},
  };

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.trim().isEmpty()) {
      return "[]";
    }
    try {
      String snapshotTs = context.getDimensionValues().get("snapshot_ts");
      String snapshotDate = toIsoDate(snapshotTs);

      BufferedReader reader = new BufferedReader(new StringReader(response));
      String headerRecord = null;
      String record;
      while ((record = CsvRecordReader.readRecord(reader)) != null) {
        if (!record.trim().isEmpty()) {
          headerRecord = record;
          break;
        }
      }
      ArrayNode out = MAPPER.createArrayNode();
      if (headerRecord == null) {
        return "[]";
      }
      List<String> rawHeaders = CsvRecordReader.splitFields(headerRecord, ',');
      // index -> output column name (null when a header doesn't map to a known column)
      String[] indexToColumn = new String[rawHeaders.size()];
      for (int i = 0; i < rawHeaders.size(); i++) {
        String key = normalizeKey(rawHeaders.get(i));
        indexToColumn[i] = lookupColumn(key);
      }

      int therapeuticCategoryIndex = indexOfColumn(indexToColumn, "therapeutic_category");

      while ((record = CsvRecordReader.readRecord(reader)) != null) {
        if (record.trim().isEmpty()) {
          continue;
        }
        List<String> fields = CsvRecordReader.splitFields(record, ',');
        // A handful of source rows (multi-category shortages, e.g. "Gastroenterology;Other;
        // Pediatric") carry one unescaped extra field where FDA's own export should have
        // joined the categories with ';' — this desyncs every column after
        // therapeutic_category. Detected live (6 of ~10,700 sampled rows): re-collapse the
        // overflow field(s) at that position, joined with ';', to restore alignment.
        if (fields.size() > indexToColumn.length && therapeuticCategoryIndex >= 0) {
          fields = collapseOverflow(fields, therapeuticCategoryIndex,
              fields.size() - indexToColumn.length);
        }
        ObjectNode row = MAPPER.createObjectNode();
        for (int i = 0; i < indexToColumn.length && i < fields.size(); i++) {
          if (indexToColumn[i] == null) {
            continue;
          }
          String value = trimToNull(fields.get(i));
          if ("status".equals(indexToColumn[i])) {
            value = canonicalizeStatus(value);
          }
          if (value == null) {
            row.putNull(indexToColumn[i]);
          } else {
            row.put(indexToColumn[i], value);
          }
        }
        row.put("snapshot_date", snapshotDate);
        row.put("type", "fda_drug_shortages_history");
        out.add(row);
      }
      return out.toString();
    } catch (IOException e) {
      throw new RuntimeException("Failed to transform FDA drug shortages history CSV", e);
    }
  }

  private static int indexOfColumn(String[] indexToColumn, String column) {
    for (int i = 0; i < indexToColumn.length; i++) {
      if (column.equals(indexToColumn[i])) {
        return i;
      }
    }
    return -1;
  }

  /** Merges {@code overflowCount + 1} fields starting at {@code startIndex} into one field
   * (joined with {@code ";"}), restoring one-field-per-header alignment for a row that
   * carried extra unescaped delimiters at that position. */
  private static List<String> collapseOverflow(List<String> fields, int startIndex,
      int overflowCount) {
    java.util.List<String> merged = new java.util.ArrayList<String>(fields.subList(0, startIndex));
    StringBuilder joined = new StringBuilder();
    for (int i = startIndex; i <= startIndex + overflowCount && i < fields.size(); i++) {
      String part = trimToNull(fields.get(i));
      if (part == null) {
        continue;
      }
      if (joined.length() > 0) {
        joined.append(';');
      }
      joined.append(part);
    }
    merged.add(joined.toString());
    merged.addAll(fields.subList(Math.min(startIndex + overflowCount + 1, fields.size()),
        fields.size()));
    return merged;
  }

  private static String lookupColumn(String normalizedKey) {
    for (String[] mapping : COLUMN_MAP) {
      if (mapping[0].equals(normalizedKey)) {
        return mapping[1];
      }
    }
    return null;
  }

  private static String normalizeKey(String key) {
    return key == null ? "" : key.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
  }

  private static String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  /** Collapses the export's own casing drift ("To be Discontinued" vs "To Be Discontinued")
   * into one canonical value per status, matched case-insensitively. */
  private static String canonicalizeStatus(String value) {
    if (value == null) {
      return null;
    }
    if ("current".equalsIgnoreCase(value)) {
      return "Current";
    }
    if ("resolved".equalsIgnoreCase(value)) {
      return "Resolved";
    }
    if ("to be discontinued".equalsIgnoreCase(value)) {
      return "To Be Discontinued";
    }
    return value;
  }

  /** Converts a Wayback capture timestamp ({@code yyyyMMddHHmmss}) to {@code yyyy-MM-dd}. */
  private static String toIsoDate(String snapshotTs) {
    if (snapshotTs == null || snapshotTs.length() < 8) {
      return null;
    }
    return snapshotTs.substring(0, 4) + "-" + snapshotTs.substring(4, 6) + "-"
        + snapshotTs.substring(6, 8);
  }
}
