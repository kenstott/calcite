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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Melts BTS's "Monthly TEU Data" (Socrata {@code rd72-aq8r}) from one wide row per month (one
 * column per port) into one row per (port, month). The source's date column is itself named
 * {@code port} (a quirk of the upstream export, not a typo here) and holds {@code M/D/YYYY}
 * strings, converted to ISO dates.
 *
 * <p>Port column names are the fixed Top-10 U.S. container ports this Port Performance program
 * has tracked since the series began in Jan 2019 (verified live: identical column set across all
 * 46 rows, Jan 2019-Oct 2022 — this static dataset has not been updated since, see the table
 * comment).
 */
public class BtsPortTeuTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter SOURCE_FORMAT = DateTimeFormatter.ofPattern("M/d/yyyy");

  private static final Map<String, String> PORT_NAMES = new HashMap<>();
  static {
    PORT_NAMES.put("charleston_sc", "Charleston, SC");
    PORT_NAMES.put("houston_tx", "Houston, TX");
    PORT_NAMES.put("long_beach_ca", "Long Beach, CA");
    PORT_NAMES.put("los_angeles_ca", "Los Angeles, CA");
    PORT_NAMES.put("nwsa_seattle_tacoma_wa", "NWSA Seattle-Tacoma, WA");
    PORT_NAMES.put("oakland_ca", "Oakland, CA");
    PORT_NAMES.put("port_of_ny_nj", "Port of NY/NJ");
    PORT_NAMES.put("port_of_virginia_va", "Port of Virginia, VA");
    PORT_NAMES.put("savannah_ga", "Savannah, GA");
  }

  @Override
  public String transform(String response, RequestContext context) {
    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode record : root) {
        String rawDate = record.path("port").asText(null);
        if (rawDate == null) {
          continue;
        }
        String isoDate = LocalDate.parse(rawDate, SOURCE_FORMAT).toString();
        Iterator<Map.Entry<String, JsonNode>> fields = record.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> field = fields.next();
          String portCode = field.getKey();
          if ("port".equals(portCode)) {
            continue;
          }
          String rawTeu = field.getValue().asText(null);
          ObjectNode row = MAPPER.createObjectNode();
          row.put("report_date", isoDate);
          row.put("port_code", portCode);
          row.put("port_name", PORT_NAMES.getOrDefault(portCode, portCode));
          if (rawTeu == null || rawTeu.isEmpty()) {
            row.putNull("teu");
          } else {
            row.put("teu", Double.parseDouble(rawTeu));
          }
          row.put("type", "bts_port_teu");
          out.add(row);
        }
      }
      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      throw new RuntimeException("BTS port TEU: failed to parse", e);
    }
  }
}
