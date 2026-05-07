/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.govdata.geo.TigerShapefileParser;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * DataProvider for NOAA Hazard Mapping System (HMS) daily smoke data.
 *
 * <p>HMS publishes daily shapefiles of satellite-detected smoke plume polygons
 * (light/medium/heavy density) at
 * https://www.ospo.noaa.gov/Products/land/hms.html. This provider downloads
 * those daily ZIPs, parses the smoke polygon shapefiles, downloads TIGER/Line
 * county boundary shapefiles, and performs an areal intersection to produce
 * county-level smoke coverage percentages.
 *
 * <p>Configured via hooks in weather-schema.yaml:
 * <pre>
 * hooks:
 *   dataProvider: "org.apache.calcite.adapter.govdata.weather.HmsSmokeSpatialJoinProvider"
 * </pre>
 *
 * <p>Dimensions expected: {@code year} and {@code month}.
 * Each batch processes all days in the given month and returns rows for every
 * county that had any smoke coverage on any day of that month.
 *
 * <p>Output columns: county_fips, state_fips, date, year,
 * smoke_coverage_pct, heavy_smoke_pct, medium_smoke_pct, light_smoke_pct.
 */
public class HmsSmokeSpatialJoinProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(HmsSmokeSpatialJoinProvider.class);

  private static final String HMS_BASE_URL =
      "https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/Shapefile";
  private static final String TIGER_COUNTY_URL_PATTERN =
      "https://www2.census.gov/geo/tiger/TIGER{year}/COUNTY/tl_{year}_us_county.zip";

  /** Max TIGER year for which county boundaries are published. */
  private static final int MAX_TIGER_YEAR = 2023;

  /** Table name for county-level coverage output. */
  static final String TABLE_DAILY   = "hms_smoke_daily";
  /** Table name for raw smoke polygon geometry output. */
  static final String TABLE_POLYGONS = "hms_smoke_polygons";

  /** JVM-lifetime cache of county geometries keyed by TIGER year (string). */
  private static final Map<String, List<CountyGeometry>> COUNTY_CACHE =
      new ConcurrentHashMap<String, List<CountyGeometry>>();

  /**
   * Cross-batch polygon cache so hms_smoke_polygons can reuse the smoke data already
   * parsed by hms_smoke_daily for the same year+month without re-downloading.
   * Key: "{year}-{month}", value: polygon rows ready to emit.
   */
  private static final Map<String, List<Map<String, Object>>> POLYGON_CACHE =
      new ConcurrentHashMap<String, List<Map<String, Object>>>();

  @Override public Iterator<Map<String, Object>> fetch(
      EtlPipelineConfig config, Map<String, String> variables) throws IOException {

    String year = variables.get("year");
    String month = variables.get("month");
    if (year == null || month == null) {
      throw new IOException("HmsSmokeSpatialJoinProvider requires 'year' and 'month' dimensions");
    }

    String tableName = config.getName();
    if (TABLE_POLYGONS.equals(tableName)) {
      return fetchPolygons(year, month);
    }
    return fetchCountyCoverage(year, month);
  }

  // ---------------------------------------------------------------------------
  // Mode: hms_smoke_daily — spatial join producing county coverage rows
  // ---------------------------------------------------------------------------

  private Iterator<Map<String, Object>> fetchCountyCoverage(String year, String month)
      throws IOException {

    int yearInt  = Integer.parseInt(year);
    int monthInt = Integer.parseInt(month);
    String tigerYear = String.valueOf(Math.max(yearInt, 2010));

    List<CountyGeometry> counties = getCountyGeometries(tigerYear);
    if (counties.isEmpty()) {
      LOGGER.warn("No county geometries for year={} — returning empty", tigerYear);
      return new ArrayList<Map<String, Object>>().iterator();
    }

    List<Envelope> countyEnvelopes = new ArrayList<Envelope>(counties.size());
    for (CountyGeometry county : counties) {
      countyEnvelopes.add(county.geometry.getEnvelopeInternal());
    }

    List<SmokePolygon> allPolygons = downloadMonthPolygons(year, month, yearInt, monthInt);

    // Cache polygon rows for hms_smoke_polygons (which runs after this table)
    String cacheKey = year + "-" + month;
    POLYGON_CACHE.put(cacheKey, toPolygonRows(allPolygons, year, month));

    // Spatial join: group polygons by date, join each day's set against counties
    Map<String, List<SmokePolygon>> byDate = new java.util.LinkedHashMap<String, List<SmokePolygon>>();
    for (SmokePolygon p : allPolygons) {
      List<SmokePolygon> list = byDate.get(p.date);
      if (list == null) {
        list = new ArrayList<SmokePolygon>();
        byDate.put(p.date, list);
      }
      list.add(p);
    }

    List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    for (Map.Entry<String, List<SmokePolygon>> entry : byDate.entrySet()) {
      List<Map<String, Object>> dailyRows =
          spatialJoin(entry.getValue(), counties, countyEnvelopes, entry.getKey(), yearInt);
      results.addAll(dailyRows);
      LOGGER.info("HMS smoke {}: {} county rows", entry.getKey(), dailyRows.size());
    }

    LOGGER.info("HMS smoke {}-{}: {} total county rows", year, month, results.size());
    return results.iterator();
  }

  // ---------------------------------------------------------------------------
  // Mode: hms_smoke_polygons — raw geometry rows
  // ---------------------------------------------------------------------------

  private Iterator<Map<String, Object>> fetchPolygons(String year, String month)
      throws IOException {

    String cacheKey = year + "-" + month;
    List<Map<String, Object>> cached = POLYGON_CACHE.remove(cacheKey);
    if (cached != null) {
      LOGGER.info("HMS polygon cache hit for {}-{}: {} rows", year, month, cached.size());
      return cached.iterator();
    }

    // Cache miss: hms_smoke_daily didn't run first (e.g. resuming a partial run).
    // Re-download and parse the smoke ZIPs for this month.
    LOGGER.info("HMS polygon cache miss for {}-{} — re-fetching", year, month);
    int yearInt  = Integer.parseInt(year);
    int monthInt = Integer.parseInt(month);
    List<SmokePolygon> polygons = downloadMonthPolygons(year, month, yearInt, monthInt);
    List<Map<String, Object>> rows = toPolygonRows(polygons, year, month);
    LOGGER.info("HMS smoke {}-{}: {} polygon rows", year, month, rows.size());
    return rows.iterator();
  }

  // ---------------------------------------------------------------------------
  // Shared: download all daily smoke ZIPs for one year+month
  // ---------------------------------------------------------------------------

  private List<SmokePolygon> downloadMonthPolygons(
      String year, String month, int yearInt, int monthInt) {

    Calendar cal = Calendar.getInstance();
    cal.set(yearInt, monthInt - 1, 1);
    int daysInMonth = cal.getActualMaximum(Calendar.DAY_OF_MONTH);

    List<SmokePolygon> allPolygons = new ArrayList<SmokePolygon>();

    for (int day = 1; day <= daysInMonth; day++) {
      String dayPad      = String.format("%02d", day);
      String dateStr     = year + "-" + month + "-" + dayPad;
      String dateCompact = year + month + dayPad;
      // Archive structure: /Shapefile/{year}/{month}/{filename}
      // e.g. /Shapefile/2025/01/hms_smoke20250101.zip
      String zipUrl = HMS_BASE_URL + "/" + year + "/" + month
          + "/hms_smoke" + dateCompact + ".zip";

      Path tempDir = null;
      try {
        tempDir = Files.createTempDirectory("hms_smoke_");
        File zipFile = new File(tempDir.toFile(), "hms_smoke.zip");

        if (!downloadFile(zipUrl, zipFile)) {
          LOGGER.debug("HMS smoke ZIP not available for {}", dateStr);
          continue;
        }

        extractZip(zipFile, tempDir.toFile());
        List<SmokePolygon> daily = parseSmokeShapefile(tempDir.toFile(), dateStr);
        LOGGER.info("HMS smoke {}: {} polygons", dateStr, daily.size());
        allPolygons.addAll(daily);

      } catch (IOException e) {
        LOGGER.warn("Failed to fetch HMS smoke for {}: {}", dateStr, e.getMessage());
      } finally {
        deleteTempDir(tempDir);
      }
    }
    return allPolygons;
  }

  /** Converts a list of SmokePolygon objects to serializable polygon rows (WKT geometry). */
  private List<Map<String, Object>> toPolygonRows(
      List<SmokePolygon> polygons, String year, String month) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>(polygons.size());
    for (SmokePolygon p : polygons) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("date",     p.date);
      row.put("year",     Integer.parseInt(year));
      row.put("month",    month);
      row.put("density",  p.density.name().charAt(0)
          + p.density.name().substring(1).toLowerCase()); // "Light" / "Medium" / "Heavy"
      row.put("geometry", p.geometry.toText()); // WKT
      rows.add(row);
    }
    return rows;
  }

  // ---------------------------------------------------------------------------
  // County geometry loading — reads from materialized geo.counties Iceberg table
  // ---------------------------------------------------------------------------

  private List<CountyGeometry> getCountyGeometries(String year) throws IOException {
    List<CountyGeometry> cached = COUNTY_CACHE.get(year);
    if (cached != null) {
      return cached;
    }

    List<CountyGeometry> counties;
    try {
      counties = loadCountiesFromIceberg(year);
    } catch (Exception e) {
      LOGGER.warn("Could not read counties from Iceberg ({}), falling back to TIGER download", e.getMessage());
      counties = loadCountiesFromTiger(year);
    }

    LOGGER.info("Loaded {} county geometries for year={}", counties.size(), year);
    COUNTY_CACHE.put(year, counties);
    return counties;
  }

  /**
   * Reads county boundaries from the already-materialized geo.counties Iceberg table via DuckDB.
   * Uses the most recent TIGER year ≤ the requested year.
   */
  private List<CountyGeometry> loadCountiesFromIceberg(String year) throws Exception {
    String parquetDir = System.getenv("GOVDATA_PARQUET_DIR");
    if (parquetDir == null || parquetDir.isEmpty()) {
      throw new IOException("GOVDATA_PARQUET_DIR not set");
    }
    String warehousePath = parquetDir;

    // Cap to the range of TIGER years we have materialized
    int tigerYear = Math.min(Integer.parseInt(year), MAX_TIGER_YEAR);

    String accessKey  = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey  = System.getenv("AWS_SECRET_ACCESS_KEY");
    String endpoint   = System.getenv("AWS_ENDPOINT_OVERRIDE");
    String region     = System.getenv("AWS_REGION");

    Class.forName("org.duckdb.DuckDBDriver");
    List<CountyGeometry> counties = new ArrayList<CountyGeometry>();
    WKTReader wktReader = new WKTReader();

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = conn.createStatement()) {

      st.execute("INSTALL httpfs; LOAD httpfs");
      st.execute("INSTALL iceberg; LOAD iceberg");

      if (accessKey != null && secretKey != null) {
        StringBuilder secret = new StringBuilder(
            "CREATE OR REPLACE SECRET hms_s3 (TYPE s3, PROVIDER config");
        secret.append(", KEY_ID '").append(accessKey).append("'");
        secret.append(", SECRET '").append(secretKey).append("'");
        if (region != null)   secret.append(", REGION '").append(region).append("'");
        if (endpoint != null) {
          String host = endpoint.replaceFirst("https?://", "");
          boolean ssl = endpoint.startsWith("https://");
          secret.append(", ENDPOINT '").append(host).append("'");
          secret.append(", URL_STYLE 'path'");
          secret.append(", USE_SSL ").append(ssl);
        }
        secret.append(")");
        st.execute(secret.toString());
      }

      String sql = "SELECT county_fips, state_fips, geometry"
          + " FROM iceberg_scan('" + warehousePath + "/counties', allow_moved_paths=true)"
          + " WHERE year = " + tigerYear
          + " AND geometry IS NOT NULL";

      LOGGER.info("Loading counties from Iceberg: year={}", tigerYear);
      try (ResultSet rs = st.executeQuery(sql)) {
        while (rs.next()) {
          String fips    = rs.getString("county_fips");
          String sfips   = rs.getString("state_fips");
          String wkt     = rs.getString("geometry");
          if (fips == null || wkt == null) continue;
          try {
            Geometry geom = wktReader.read(wkt);
            if (geom != null && !geom.isEmpty()) {
              counties.add(new CountyGeometry(fips, sfips, geom));
            }
          } catch (ParseException pe) {
            LOGGER.debug("Skipping county {} — invalid WKT: {}", fips, pe.getMessage());
          }
        }
      }
    }

    if (counties.isEmpty()) {
      throw new IOException("No county geometries found in Iceberg for year=" + tigerYear);
    }
    return counties;
  }

  /** Fallback: downloads and parses the TIGER county shapefile directly. */
  private List<CountyGeometry> loadCountiesFromTiger(String year) throws IOException {
    int tigerYear = Math.min(Integer.parseInt(year), MAX_TIGER_YEAR);
    String url = TIGER_COUNTY_URL_PATTERN.replace("{year}", String.valueOf(tigerYear));
    LOGGER.info("Downloading TIGER county shapefile for year={}: {}", tigerYear, url);

    Path tempDir = null;
    try {
      tempDir = Files.createTempDirectory("tiger_counties_");
      File zipFile = new File(tempDir.toFile(), "counties.zip");

      if (!downloadFile(url, zipFile)) {
        throw new IOException("Failed to download TIGER county shapefile from: " + url);
      }
      extractZip(zipFile, tempDir.toFile());

      String prefix = findShapefilePrefix(tempDir.toFile());
      if (prefix == null) {
        throw new IOException("No shapefile found in TIGER county ZIP for year=" + tigerYear);
      }

      List<Object[]> records = TigerShapefileParser.parseShapefile(
          tempDir.toFile(), prefix, new CountyAttributeMapper());

      List<CountyGeometry> counties = new ArrayList<CountyGeometry>(records.size());
      for (Object[] rec : records) {
        String geoid  = (String) rec[0];
        String sfips  = (String) rec[1];
        Geometry geom = (Geometry) rec[2];
        if (geoid != null && !geoid.isEmpty() && geom != null && !geom.isEmpty()) {
          counties.add(new CountyGeometry(geoid, sfips, geom));
        }
      }
      return counties;
    } finally {
      deleteTempDir(tempDir);
    }
  }

  // ---------------------------------------------------------------------------
  // HMS smoke shapefile parsing
  // ---------------------------------------------------------------------------

  private List<SmokePolygon> parseSmokeShapefile(File dir, String dateStr) throws IOException {
    String prefix = findShapefilePrefix(dir);
    if (prefix == null) {
      LOGGER.warn("No shapefile found in HMS smoke ZIP for date={}", dateStr);
      return new ArrayList<SmokePolygon>();
    }

    List<Object[]> records = TigerShapefileParser.parseShapefile(
        dir, prefix, new SmokeAttributeMapper());

    List<SmokePolygon> polygons = new ArrayList<SmokePolygon>(records.size());
    for (Object[] rec : records) {
      String densityRaw = rec[0] != null ? rec[0].toString().trim() : "";
      Geometry geom      = (Geometry) rec[1];

      if (geom == null || geom.isEmpty()) continue;

      SmokeDensity density = parseDensity(densityRaw);
      polygons.add(new SmokePolygon(density, geom, dateStr));
    }
    return polygons;
  }

  private SmokeDensity parseDensity(String raw) {
    if (raw == null || raw.isEmpty()) return SmokeDensity.LIGHT;

    // Newer files use string labels
    String lower = raw.toLowerCase();
    if (lower.startsWith("heavy") || lower.equals("3")) return SmokeDensity.HEAVY;
    if (lower.startsWith("medium") || lower.equals("2")) return SmokeDensity.MEDIUM;
    return SmokeDensity.LIGHT;
  }

  // ---------------------------------------------------------------------------
  // Spatial join
  // ---------------------------------------------------------------------------

  private List<Map<String, Object>> spatialJoin(
      List<SmokePolygon> smokePolygons,
      List<CountyGeometry> counties,
      List<Envelope> countyEnvelopes,
      String dateStr,
      int year) {

    // coverage[i] = [total, heavy, medium, light] as ratios [0..1+]
    double[][] coverage = new double[counties.size()][4];

    for (SmokePolygon smoke : smokePolygons) {
      Envelope smokeEnv = smoke.geometry.getEnvelopeInternal();

      for (int ci = 0; ci < counties.size(); ci++) {
        if (!smokeEnv.intersects(countyEnvelopes.get(ci))) continue;

        Geometry countyGeom = counties.get(ci).geometry;
        if (!smoke.geometry.intersects(countyGeom)) continue;

        try {
          Geometry intersection = smoke.geometry.intersection(countyGeom);
          double intersectionArea = intersection.getArea();
          if (intersectionArea <= 0) continue;

          double countyArea = countyGeom.getArea();
          if (countyArea <= 0) continue;

          double ratio = intersectionArea / countyArea;
          coverage[ci][0] += ratio;
          switch (smoke.density) {
          case HEAVY:  coverage[ci][1] += ratio; break;
          case MEDIUM: coverage[ci][2] += ratio; break;
          default:     coverage[ci][3] += ratio; break;
          }
        } catch (Exception e) {
          LOGGER.debug("Geometry intersection failed for county {}: {}",
              counties.get(ci).fips, e.getMessage());
        }
      }
    }

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int ci = 0; ci < counties.size(); ci++) {
      if (coverage[ci][0] <= 0) continue;

      CountyGeometry county = counties.get(ci);
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("county_fips",        county.fips);
      row.put("state_fips",         county.stateFips);
      row.put("date",               dateStr);
      row.put("year",               year);
      row.put("month",              dateStr.substring(5, 7));
      row.put("smoke_coverage_pct", clamp(coverage[ci][0] * 100));
      row.put("heavy_smoke_pct",    clamp(coverage[ci][1] * 100));
      row.put("medium_smoke_pct",   clamp(coverage[ci][2] * 100));
      row.put("light_smoke_pct",    clamp(coverage[ci][3] * 100));
      rows.add(row);
    }
    return rows;
  }

  private double clamp(double pct) {
    return Math.min(pct, 100.0);
  }

  // ---------------------------------------------------------------------------
  // File utilities
  // ---------------------------------------------------------------------------

  /**
   * Downloads a URL to a file. Returns false (without throwing) for 404/missing files
   * since HMS smoke files are absent on many days (nights, cloudy periods).
   */
  private boolean downloadFile(String urlStr, File dest) throws IOException {
    URL url = URI.create(urlStr).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);
    conn.setRequestProperty("User-Agent", "CalciteGovDataAdapter/1.0");

    int responseCode = conn.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_NOT_FOUND
        || responseCode == HttpURLConnection.HTTP_GONE) {
      conn.disconnect();
      return false;
    }
    if (responseCode != HttpURLConnection.HTTP_OK) {
      conn.disconnect();
      throw new IOException("HTTP " + responseCode + " downloading " + urlStr);
    }

    try (BufferedInputStream in = new BufferedInputStream(conn.getInputStream());
         FileOutputStream out = new FileOutputStream(dest)) {
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) != -1) {
        out.write(buf, 0, n);
      }
    } finally {
      conn.disconnect();
    }
    return true;
  }

  private void extractZip(File zipFile, File destDir) throws IOException {
    try (ZipInputStream zis = new ZipInputStream(
        new BufferedInputStream(Files.newInputStream(zipFile.toPath())))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        if (entry.isDirectory()) {
          zis.closeEntry();
          continue;
        }
        String name = new File(entry.getName()).getName(); // strip path components
        File outFile = new File(destDir, name);
        try (FileOutputStream fos = new FileOutputStream(outFile)) {
          byte[] buf = new byte[8192];
          int n;
          while ((n = zis.read(buf)) != -1) {
            fos.write(buf, 0, n);
          }
        }
        zis.closeEntry();
      }
    }
  }

  private String findShapefilePrefix(File dir) {
    File[] shpFiles = dir.listFiles((d, name) ->
        name.toLowerCase().endsWith(".shp"));
    if (shpFiles == null || shpFiles.length == 0) return null;
    String name = shpFiles[0].getName();
    return name.substring(0, name.length() - 4); // strip ".shp"
  }

  private void deleteTempDir(Path tempDir) {
    if (tempDir == null) return;
    try {
      Files.walk(tempDir)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (IOException e) {
      LOGGER.debug("Failed to clean up temp dir {}: {}", tempDir, e.getMessage());
    }
  }

  // ---------------------------------------------------------------------------
  // Attribute mappers
  // ---------------------------------------------------------------------------

  /** Extracts GEOID, STATEFP, and geometry from a TIGER county feature. */
  private static class CountyAttributeMapper implements TigerShapefileParser.AttributeMapper {
    @Override public Object[] mapAttributes(TigerShapefileParser.ShapefileFeature feature) {
      String geoid    = TigerShapefileParser.getStringAttribute(feature, "GEOID");
      String stateFp  = TigerShapefileParser.getStringAttribute(feature, "STATEFP");
      Object geomRaw  = feature.getAttribute("_GEOMETRY_");
      Geometry geom   = (geomRaw instanceof Geometry) ? (Geometry) geomRaw : null;
      return new Object[] { geoid, stateFp, geom };
    }
  }

  /** Extracts Density and geometry from an HMS smoke feature. */
  private static class SmokeAttributeMapper implements TigerShapefileParser.AttributeMapper {
    @Override public Object[] mapAttributes(TigerShapefileParser.ShapefileFeature feature) {
      // HMS shapefiles use "Density" (newer) or "density" (older) field
      Object density = feature.getAttribute("Density");
      if (density == null) density = feature.getAttribute("density");
      if (density == null) density = feature.getAttribute("DENSITY");
      Object geomRaw  = feature.getAttribute("_GEOMETRY_");
      Geometry geom   = (geomRaw instanceof Geometry) ? (Geometry) geomRaw : null;
      return new Object[] { density, geom };
    }
  }

  // ---------------------------------------------------------------------------
  // Inner types
  // ---------------------------------------------------------------------------

  private enum SmokeDensity { LIGHT, MEDIUM, HEAVY }

  private static final class SmokePolygon {
    final SmokeDensity density;
    final Geometry geometry;
    final String date;

    SmokePolygon(SmokeDensity density, Geometry geometry, String date) {
      this.density = density;
      this.geometry = geometry;
      this.date = date;
    }
  }

  private static final class CountyGeometry {
    final String fips;      // 5-digit GEOID
    final String stateFips; // 2-digit STATEFP
    final Geometry geometry;

    CountyGeometry(String fips, String stateFips, Geometry geometry) {
      this.fips = fips;
      this.stateFips = stateFips;
      this.geometry = geometry;
    }
  }
}
