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
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parsers for the NOAA CPC ENSO index text files. Each nested class handles one file and fails
 * on any line that does not match the documented layout, so a format change is loud.
 */
public final class EnsoTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Three-month season codes in calendar order; index + 1 is the centre month. */
  private static final List<String> SEASONS = Arrays.asList(
      "DJF", "JFM", "FMA", "MAM", "AMJ", "MJJ", "JJA", "JAS", "ASO", "SON", "OND", "NDJ");

  private static final List<String> MONTHS = Arrays.asList(
      "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC");

  private EnsoTransformer() {
  }

  private static int seasonCentreMonth(String season, String file) {
    int idx = SEASONS.indexOf(season);
    if (idx < 0) {
      throw new RuntimeException(file + ": unknown season code '" + season + "'");
    }
    return idx + 1;
  }

  private static String serialise(ArrayNode out, String file) {
    if (out.size() == 0) {
      throw new RuntimeException(file + ": no data rows parsed");
    }
    try {
      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      throw new RuntimeException(file + " transform failed: " + e.getMessage(), e);
    }
  }

  /** {@code oni.ascii.txt}: {@code SEAS YR TOTAL ANOM}. */
  public static class Oni implements ResponseTransformer {
    private static final Pattern ROW =
        Pattern.compile("^\\s*([A-Z]{3})\\s+(\\d{4})\\s+(-?\\d+\\.\\d+)\\s+(-?\\d+\\.\\d+)\\s*$");

    @Override public String transform(String response, RequestContext context) {
      ArrayNode out = MAPPER.createArrayNode();
      for (String line : response.split("\r?\n")) {
        if (line.trim().isEmpty() || line.trim().startsWith("SEAS")) {
          continue;
        }
        Matcher m = ROW.matcher(line);
        if (!m.matches()) {
          throw new RuntimeException("CPC ONI: unexpected line '" + line + "'");
        }
        ObjectNode row = MAPPER.createObjectNode();
        row.put("year", Integer.parseInt(m.group(2)));
        row.put("season", m.group(1));
        row.put("center_month", seasonCentreMonth(m.group(1), "CPC ONI"));
        row.put("nino34_sst_c", Double.parseDouble(m.group(3)));
        row.put("oni_c", Double.parseDouble(m.group(4)));
        out.add(row);
      }
      return serialise(out, "CPC ONI");
    }
  }

  /** {@code RONI.ascii.txt}: {@code SEAS YR ANOM}. */
  public static class Roni implements ResponseTransformer {
    private static final Pattern ROW =
        Pattern.compile("^\\s*([A-Z]{3})\\s+(\\d{4})\\s+(-?\\d+\\.\\d+)\\s*$");

    @Override public String transform(String response, RequestContext context) {
      ArrayNode out = MAPPER.createArrayNode();
      for (String line : response.split("\r?\n")) {
        if (line.trim().isEmpty() || line.trim().startsWith("SEAS")) {
          continue;
        }
        Matcher m = ROW.matcher(line);
        if (!m.matches()) {
          throw new RuntimeException("CPC RONI: unexpected line '" + line + "'");
        }
        ObjectNode row = MAPPER.createObjectNode();
        row.put("year", Integer.parseInt(m.group(2)));
        row.put("season", m.group(1));
        row.put("center_month", seasonCentreMonth(m.group(1), "CPC RONI"));
        row.put("roni_c", Double.parseDouble(m.group(3)));
        out.add(row);
      }
      return serialise(out, "CPC RONI");
    }
  }

  /**
   * {@code sstoi.indices}: {@code YR MON} then SST and anomaly for Nino 1+2, 3, 4 and 3.4
   * (monthly OISST v2.1 means, 1982-).
   */
  public static class NinoMonthly implements ResponseTransformer {
    private static final Pattern ROW = Pattern.compile(
        "^\\s*(\\d{4})\\s+(\\d{1,2})(?:\\s+(-?\\d+\\.\\d+)){8}\\s*$");
    private static final Pattern NUM = Pattern.compile("-?\\d+\\.\\d+");

    @Override public String transform(String response, RequestContext context) {
      ArrayNode out = MAPPER.createArrayNode();
      for (String line : response.split("\r?\n")) {
        if (line.trim().isEmpty() || line.trim().startsWith("YR")) {
          continue;
        }
        Matcher m = ROW.matcher(line);
        if (!m.matches()) {
          throw new RuntimeException("CPC Nino monthly: unexpected line '" + line + "'");
        }
        String[] head = line.trim().split("\\s+", 3);
        Matcher n = NUM.matcher(head[2]);
        double[] v = new double[8];
        for (int i = 0; i < 8; i++) {
          if (!n.find()) {
            throw new RuntimeException("CPC Nino monthly: short line '" + line + "'");
          }
          v[i] = Double.parseDouble(n.group());
        }
        ObjectNode row = MAPPER.createObjectNode();
        row.put("year", Integer.parseInt(head[0]));
        row.put("month", Integer.parseInt(head[1]));
        row.put("nino12_sst_c", v[0]);
        row.put("nino12_anomaly_c", v[1]);
        row.put("nino3_sst_c", v[2]);
        row.put("nino3_anomaly_c", v[3]);
        row.put("nino4_sst_c", v[4]);
        row.put("nino4_anomaly_c", v[5]);
        row.put("nino34_sst_c", v[6]);
        row.put("nino34_anomaly_c", v[7]);
        out.add(row);
      }
      return serialise(out, "CPC Nino monthly");
    }
  }

  /**
   * {@code wksst9120.for}: weekly OISST means centred on a Wednesday, {@code 02SEP1981}, then
   * SST and anomaly for Nino 1+2, 3, 3.4 and 4. SST and anomaly are run together with no
   * separator when the anomaly is negative ({@code 20.6-0.1}).
   */
  public static class NinoWeekly implements ResponseTransformer {
    private static final Pattern ROW = Pattern.compile(
        "^\\s*(\\d{2})([A-Z]{3})(\\d{4})((?:\\s+\\d+\\.\\d\\s*-?\\d+\\.\\d){4})\\s*$");
    private static final Pattern PAIR = Pattern.compile("(\\d+\\.\\d)\\s*(-?\\d+\\.\\d)");

    @Override public String transform(String response, RequestContext context) {
      ArrayNode out = MAPPER.createArrayNode();
      boolean sawHeader = false;
      for (String line : response.split("\r?\n")) {
        if (line.trim().isEmpty()) {
          continue;
        }
        if (!sawHeader) {
          if (line.trim().startsWith("Week ")) {
            sawHeader = true;
          }
          continue;
        }
        Matcher m = ROW.matcher(line);
        if (!m.matches()) {
          throw new RuntimeException("CPC Nino weekly: unexpected line '" + line + "'");
        }
        int month = MONTHS.indexOf(m.group(2)) + 1;
        if (month == 0) {
          throw new RuntimeException("CPC Nino weekly: unknown month in '" + line + "'");
        }
        Matcher p = PAIR.matcher(m.group(4));
        double[] v = new double[8];
        for (int i = 0; i < 4; i++) {
          if (!p.find()) {
            throw new RuntimeException("CPC Nino weekly: short line '" + line + "'");
          }
          v[2 * i] = Double.parseDouble(p.group(1));
          v[2 * i + 1] = Double.parseDouble(p.group(2));
        }
        ObjectNode row = MAPPER.createObjectNode();
        row.put("week_center_date", String.format("%s-%02d-%s", m.group(3), month, m.group(1)));
        row.put("year", Integer.parseInt(m.group(3)));
        row.put("nino12_sst_c", v[0]);
        row.put("nino12_anomaly_c", v[1]);
        row.put("nino3_sst_c", v[2]);
        row.put("nino3_anomaly_c", v[3]);
        row.put("nino34_sst_c", v[4]);
        row.put("nino34_anomaly_c", v[5]);
        row.put("nino4_sst_c", v[6]);
        row.put("nino4_anomaly_c", v[7]);
        out.add(row);
      }
      if (!sawHeader) {
        throw new RuntimeException("CPC Nino weekly: no 'Week' header in response");
      }
      return serialise(out, "CPC Nino weekly");
    }
  }
}
