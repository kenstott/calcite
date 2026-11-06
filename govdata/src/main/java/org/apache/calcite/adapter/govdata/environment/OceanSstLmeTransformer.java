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
package org.apache.calcite.adapter.govdata.environment;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Aggregates a NOAA Coral Reef Watch CoralTemp monthly SST grid (CoastWatch ERDDAP dataset
 * {@code NOAA_DHW_monthly_Lon0360}, CSV subset) into one row per (US large marine ecosystem,
 * month) for the {@code ocean_sst_monthly} table.
 *
 * <p>Cell-to-region rule: each grid cell center is assigned to the first bundled LME polygon
 * (see {@code us-large-marine-ecosystems.json}, from the NOAA LME boundary set) that contains it
 * by ray-casting point-in-polygon; cells outside every polygon, over land, or with NaN SST are
 * dropped. The cell longitude arrives in 0-360 form and is normalized to -180..180 before the
 * test. Each row carries the unweighted mean of the cell SST and SST anomaly values and the
 * number of valid cells; no latitude weighting is applied (cells are 0.25 degrees after the
 * server-side stride of 5 over the 0.05 degree grid).
 */
public class OceanSstLmeTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OceanSstLmeTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String LME_RESOURCE = "/environment/us-large-marine-ecosystems.json";

  /** Coastal states whose adjacent waters fall in each LME, keyed by LME number. */
  private static final String[][] LME_STATES = {
      {"1", "AK"}, {"2", "AK"}, {"54", "AK"}, {"55", "AK"}, {"65", "AK"},
      {"3", "WA,OR,CA"},
      {"5", "TX,LA,MS,AL,FL"},
      {"6", "FL,GA,SC,NC"},
      {"7", "VA,MD,DE,NJ,NY,CT,RI,MA,NH,ME"},
      {"10", "HI"},
      {"12", "PR,VI"},
  };

  private static final List<Lme> LMES = loadLmes();

  /** One LME: identity, bounding box and polygon rings (outer ring first per polygon). */
  private static final class Lme {
    final int number;
    final String name;
    final String states;
    final List<double[][][]> polygons = new ArrayList<>();
    double minLon = Double.MAX_VALUE;
    double maxLon = -Double.MAX_VALUE;
    double minLat = Double.MAX_VALUE;
    double maxLat = -Double.MAX_VALUE;

    Lme(int number, String name, String states) {
      this.number = number;
      this.name = name;
      this.states = states;
    }

    boolean contains(double lon, double lat) {
      if (lon < minLon || lon > maxLon || lat < minLat || lat > maxLat) {
        return false;
      }
      for (double[][][] rings : polygons) {
        if (inRing(rings[0], lon, lat)) {
          boolean inHole = false;
          for (int h = 1; h < rings.length && !inHole; h++) {
            inHole = inRing(rings[h], lon, lat);
          }
          if (!inHole) {
            return true;
          }
        }
      }
      return false;
    }
  }

  /** Per-LME running sums. */
  private static final class Acc {
    double sstSum;
    double anomSum;
    int sstCount;
    int anomCount;
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    String year = context.getDimensionValues().get("year");
    String month = context.getDimensionValues().get("month");
    Acc[] accs = new Acc[LMES.size()];
    for (int i = 0; i < accs.length; i++) {
      accs[i] = new Acc();
    }
    try (BufferedReader reader = new BufferedReader(new StringReader(response))) {
      String header = reader.readLine();
      if (header == null || !header.startsWith("time,latitude,longitude,sea_surface_temperature")) {
        throw new IllegalStateException("unexpected ERDDAP CSV header: " + header);
      }
      reader.readLine(); // units row
      String line;
      while ((line = reader.readLine()) != null) {
        String[] f = line.split(",", -1);
        if (f.length < 5) {
          continue;
        }
        double sst = parse(f[3]);
        if (Double.isNaN(sst)) {
          continue;
        }
        double lat = Double.parseDouble(f[1]);
        double lon = Double.parseDouble(f[2]);
        if (lon > 180.0) {
          lon -= 360.0;
        }
        double anom = parse(f[4]);
        for (int i = 0; i < accs.length; i++) {
          if (LMES.get(i).contains(lon, lat)) {
            accs[i].sstSum += sst;
            accs[i].sstCount++;
            if (!Double.isNaN(anom)) {
              accs[i].anomSum += anom;
              accs[i].anomCount++;
            }
            break;
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("ocean_sst_monthly transform failed: " + e.getMessage(), e);
    }

    ArrayNode out = MAPPER.createArrayNode();
    for (int i = 0; i < accs.length; i++) {
      Acc a = accs[i];
      if (a.sstCount == 0) {
        continue;
      }
      Lme lme = LMES.get(i);
      ObjectNode row = MAPPER.createObjectNode();
      row.put("lme_number", lme.number);
      row.put("lme_name", lme.name);
      row.put("state_abbrs", lme.states);
      row.put("year", Integer.parseInt(year));
      row.put("month", month);
      row.put("mean_sst_c", round(a.sstSum / a.sstCount));
      if (a.anomCount > 0) {
        row.put("mean_sst_anomaly_c", round(a.anomSum / a.anomCount));
      } else {
        row.putNull("mean_sst_anomaly_c");
      }
      row.put("cell_count", a.sstCount);
      out.add(row);
    }
    LOGGER.debug("ocean_sst_monthly {}-{}: {} LME rows", year, month, out.size());
    try {
      return MAPPER.writeValueAsString(out);
    } catch (IOException e) {
      throw new RuntimeException("ocean_sst_monthly serialization failed: " + e.getMessage(), e);
    }
  }

  private static double round(double v) {
    return Math.round(v * 1000.0) / 1000.0;
  }

  private static double parse(String s) {
    return "NaN".equals(s) || s.isEmpty() ? Double.NaN : Double.parseDouble(s);
  }

  private static boolean inRing(double[][] ring, double x, double y) {
    boolean inside = false;
    for (int i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      double xi = ring[i][0];
      double yi = ring[i][1];
      double xj = ring[j][0];
      double yj = ring[j][1];
      if ((yi > y) != (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi) {
        inside = !inside;
      }
    }
    return inside;
  }

  private static List<Lme> loadLmes() {
    try (InputStream in = OceanSstLmeTransformer.class.getResourceAsStream(LME_RESOURCE)) {
      if (in == null) {
        throw new IllegalStateException("missing resource " + LME_RESOURCE);
      }
      JsonNode root = MAPPER.readTree(in);
      List<Lme> lmes = new ArrayList<>();
      for (JsonNode n : root) {
        int number = n.get("lmeNumber").asInt();
        String states = null;
        for (String[] e : LME_STATES) {
          if (Integer.parseInt(e[0]) == number) {
            states = e[1];
          }
        }
        if (states == null) {
          throw new IllegalStateException("no adjacent-state mapping for LME " + number);
        }
        Lme lme = new Lme(number, n.get("lmeName").asText(), states);
        for (JsonNode poly : n.get("polygons")) {
          double[][][] rings = new double[poly.size()][][];
          for (int r = 0; r < poly.size(); r++) {
            JsonNode ring = poly.get(r);
            rings[r] = new double[ring.size()][];
            for (int p = 0; p < ring.size(); p++) {
              double x = ring.get(p).get(0).asDouble();
              double y = ring.get(p).get(1).asDouble();
              rings[r][p] = new double[] {x, y};
              if (r == 0) {
                lme.minLon = Math.min(lme.minLon, x);
                lme.maxLon = Math.max(lme.maxLon, x);
                lme.minLat = Math.min(lme.minLat, y);
                lme.maxLat = Math.max(lme.maxLat, y);
              }
            }
          }
          lme.polygons.add(rings);
        }
        lmes.add(lme);
      }
      return lmes;
    } catch (IOException e) {
      throw new RuntimeException("cannot load " + LME_RESOURCE + ": " + e.getMessage(), e);
    }
  }
}
