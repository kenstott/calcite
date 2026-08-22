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
package org.apache.calcite.adapter.govdata.banking;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Base for the FDIC BankFind Suite (api.fdic.gov/banks/*) response transformers.
 *
 * <p>The engine pages every FDIC endpoint with the OFFSET paginator ({@code limit}/{@code offset})
 * and hands this transformer the raw per-page envelope
 * {@code {"meta": {...}, "data": [ {"data": {...FIELDS...}, "score": N}, ... ]}}. Unlike OpenFEMA,
 * FDIC never varies its wrapper key by endpoint, but each array element wraps the actual record
 * fields one level deeper under its own {@code "data"} key (the sibling {@code "score"} is FDIC's
 * full-text relevance score and is not surfaced).
 */
abstract class AbstractFdicTransformer implements ResponseTransformer {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  // M/d/yyyy (not MM/dd/yyyy): FDIC does not zero-pad every endpoint — failures.FAILDATE
  // arrives as e.g. "1/30/2026", confirmed live, while institutions.ESTYMD is "03/31/2026".
  // The single-letter month/day pattern accepts both 1- and 2-digit forms.
  private static final DateTimeFormatter FDIC_SLASH_DATE_FORMAT = DateTimeFormatter.ofPattern("M/d/yyyy");
  private static final DateTimeFormatter FDIC_COMPACT_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
  private static final DateTimeFormatter FDIC_ISO_DATETIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
  private static final DateTimeFormatter ISO_DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Maps one raw FDIC record (already unwrapped from its element-level {@code data} key) into the schema row. */
  protected abstract void mapRow(JsonNode record, ObjectNode row);

  @Override public final String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode records = root.isArray() ? root : root.path("data");
      if (!records.isArray()) {
        JsonNode errors = root.path("errors");
        if (errors.isArray() && errors.size() > 0) {
          throw new RuntimeException(getClass().getSimpleName() + ": FDIC API error: " + errors);
        }
        logger.warn("{}: no 'data' array in response (first 200 chars: {})",
            getClass().getSimpleName(),
            response.substring(0, Math.min(200, response.length())));
        return "[]";
      }
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode element : records) {
        if (!element.isObject()) {
          continue;
        }
        JsonNode record = element.has("data") ? element.path("data") : element;
        if (!record.isObject()) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        mapRow(record, row);
        out.add(row);
      }
      logger.debug("{}: transformed {} records", getClass().getSimpleName(), out.size());
      return MAPPER.writeValueAsString(out);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(getClass().getSimpleName() + " transform failed: " + e.getMessage(), e);
    }
  }

  // --- shared field helpers -------------------------------------------------

  protected static void putText(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
    } else {
      row.put(col, v.asText());
    }
  }

  protected static void putInt(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull() || (v.isTextual() && v.asText().isEmpty())) {
      row.putNull(col);
    } else {
      row.put(col, v.asInt());
    }
  }

  protected static void putLong(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull() || (v.isTextual() && v.asText().isEmpty())) {
      row.putNull(col);
    } else {
      row.put(col, v.asLong());
    }
  }

  protected static void putDouble(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull() || (v.isTextual() && v.asText().isEmpty())) {
      row.putNull(col);
    } else {
      row.put(col, v.asDouble());
    }
  }

  /**
   * FDIC encodes booleans as the numeric flag {@code 1}/{@code 0} (e.g. {@code ACTIVE},
   * {@code MAINOFF}), not JSON {@code true}/{@code false}, so this checks numeric and textual
   * forms rather than delegating to {@link JsonNode#asBoolean()}.
   */
  protected static void putBool(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
      return;
    }
    if (v.isBoolean()) {
      row.put(col, v.booleanValue());
    } else if (v.isNumber()) {
      row.put(col, v.asInt() != 0);
    } else {
      String s = v.asText().trim();
      if (s.isEmpty()) {
        row.putNull(col);
      } else {
        row.put(col, "1".equals(s) || "true".equalsIgnoreCase(s) || "Y".equalsIgnoreCase(s));
      }
    }
  }

  /**
   * Parses an FDIC date string into ISO {@code yyyy-MM-dd}. FDIC is not consistent — confirmed
   * live, not documented — across endpoints or even within one: most {@code institutions}/
   * {@code locations}/{@code failures}/{@code sod} fields emit {@code M/D/YYYY}, not always
   * zero-padded (e.g. {@code "03/31/2026"} but also {@code "1/30/2026"}); {@code financials}'
   * REPDTE (the {@code risview} index) emits compact {@code YYYYMMDD} (e.g. {@code "20250331"});
   * and {@code history}'s EFFDATE for a future/open item emits ISO-8601 with a zero time
   * component (e.g. {@code "2026-02-01T00:00:00"}). Try each in turn. The sentinel
   * {@code 12/31/9999} (meaning "still open/active", e.g. an institution
   * with no {@code ENDEFYMD}) parses to a real, very-large date rather than being nulled out — the
   * engine carries it through as-is since it is meaningful source data, not a missing value.
   */
  protected static void putFdicDate(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
      return;
    }
    String s = v.asText().trim();
    if (s.isEmpty()) {
      row.putNull(col);
      return;
    }
    for (DateTimeFormatter fmt : new DateTimeFormatter[] {
        FDIC_SLASH_DATE_FORMAT, FDIC_COMPACT_DATE_FORMAT, FDIC_ISO_DATETIME_FORMAT}) {
      try {
        LocalDate d = fmt == FDIC_ISO_DATETIME_FORMAT
            ? java.time.LocalDateTime.parse(s, fmt).toLocalDate()
            : LocalDate.parse(s, fmt);
        row.put(col, d.format(ISO_DATE_FORMAT));
        return;
      } catch (DateTimeParseException ignored) {
        // Try the next format.
      }
    }
    throw new RuntimeException("Unparseable FDIC date '" + s + "' in field '" + field + "'");
  }

  protected static String text(JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    return v.isMissingNode() || v.isNull() ? null : v.asText();
  }
}
