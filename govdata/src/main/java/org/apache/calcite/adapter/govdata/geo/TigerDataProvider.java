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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.locationtech.jts.geom.Geometry;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Custom DataProvider for TIGER shapefile data.
 *
 * <p>Downloads TIGER shapefile ZIPs from Census Bureau, extracts and parses
 * shapefiles, and returns records as Map&lt;String, Object&gt;.
 *
 * <p>Configured via hooks in geo-schema.yaml:
 * <pre>
 * hooks:
 *   dataProvider: "org.apache.calcite.adapter.govdata.geo.TigerDataProvider"
 * </pre>
 */
public class TigerDataProvider implements DataProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerDataProvider.class);
  private static final String TIGER_BASE_URL = "https://www2.census.gov/geo/tiger";

  @Override
  public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config, Map<String, String> variables)
      throws IOException {

    String tableName = config.getName();
    String year = variables.get("year");
    String stateFips = variables.get("state_fips");

    LOGGER.info("TigerDataProvider: Fetching {} for year={}, state={}",
        tableName, year, stateFips);

    // Build download URL based on table type
    String url = buildDownloadUrl(tableName, year, stateFips);
    if (url == null) {
      LOGGER.warn("Could not build download URL for table {} year={}", tableName, year);
      return new ArrayList<Map<String, Object>>().iterator();
    }

    // Download and extract to temp directory
    Path tempDir = null;
    try {
      // Log memory status before processing (helps diagnose OOM crashes)
      Runtime runtime = Runtime.getRuntime();
      long usedMb = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
      long maxMb = runtime.maxMemory() / (1024 * 1024);
      LOGGER.info("Memory before fetch: {}MB used / {}MB max", usedMb, maxMb);

      tempDir = Files.createTempDirectory("tiger_" + tableName + "_");
      File zipFile = new File(tempDir.toFile(), "download.zip");

      LOGGER.info("Downloading TIGER shapefile from: {}", url);
      downloadFile(url, zipFile);

      LOGGER.info("Extracting shapefile to: {}", tempDir);
      extractZip(zipFile, tempDir.toFile());

      // Find shapefile prefix
      String prefix = findShapefilePrefix(tempDir.toFile());
      if (prefix == null) {
        LOGGER.error("No shapefile found in extracted ZIP for table {}", tableName);
        return new ArrayList<Map<String, Object>>().iterator();
      }

      // Parse shapefile
      int yearInt = Integer.parseInt(year);
      TigerShapefileParser.AttributeMapper mapper = getMapperForTable(tableName, yearInt);
      List<Object[]> records = TigerShapefileParser.parseShapefile(
          tempDir.toFile(), prefix, mapper);

      // Convert to Map records
      List<Map<String, Object>> result = new ArrayList<>();
      String[] columnNames = getColumnNamesForTable(tableName);

      for (Object[] record : records) {
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columnNames.length && i < record.length; i++) {
          row.put(columnNames[i], record[i]);
        }
        // Add partition columns
        row.put("type", "boundary");
        row.put("year", Integer.parseInt(year));
        result.add(row);
      }

      LOGGER.info("Parsed {} records from TIGER shapefile for table {}",
          result.size(), tableName);

      // Cleanup temp directory
      final Path tempDirToClean = tempDir;
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          Files.walk(tempDirToClean)
              .sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
        } catch (IOException e) {
          LOGGER.debug("Failed to cleanup temp directory: {}", e.getMessage());
        }
      }));

      return result.iterator();

    } catch (IOException e) {
      if (tempDir != null) {
        try {
          Files.walk(tempDir)
              .sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
        } catch (IOException cleanupError) {
          LOGGER.debug("Failed to cleanup temp directory: {}", cleanupError.getMessage());
        }
      }
      throw e;
    } catch (OutOfMemoryError oom) {
      // Critical: Log OOM before JVM crashes, flush immediately
      Runtime runtime = Runtime.getRuntime();
      long usedMb = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
      LOGGER.error("FATAL OutOfMemoryError in TigerDataProvider for table={}, year={}, state={}. "
          + "Memory at failure: {}MB. Forcing GC and rethrowing.",
          tableName, year, stateFips, usedMb);
      System.err.println("FATAL OOM: TigerDataProvider table=" + tableName + " state=" + stateFips);
      System.err.flush();
      throw oom;
    } catch (Error e) {
      // Catch any other JVM errors (StackOverflow, etc.) and log before crash
      LOGGER.error("FATAL Error in TigerDataProvider for table={}, year={}, state={}: {}",
          tableName, year, stateFips, e.getClass().getName() + ": " + e.getMessage());
      System.err.println("FATAL ERROR: " + e.getClass().getName() + " in TigerDataProvider: " + e.getMessage());
      System.err.flush();
      throw e;
    }
  }

  private String buildDownloadUrl(String tableName, String year, String stateFips) {
    int yearInt = Integer.parseInt(year);
    String tigerPath = "TIGER" + year;

    switch (tableName) {
    case "states":
      String stateSuffix = (yearInt == 2010) ? "state10" : "state";
      return String.format("%s/%s/STATE/tl_%s_us_%s.zip",
          TIGER_BASE_URL, tigerPath, year, stateSuffix);

    case "counties":
      String countySuffix = (yearInt == 2010) ? "county10" : "county";
      return String.format("%s/%s/COUNTY/tl_%s_us_%s.zip",
          TIGER_BASE_URL, tigerPath, year, countySuffix);

    case "places":
      if (stateFips == null) {
        return null;
      }
      String placeSuffix = (yearInt == 2010) ? "place10" : "place";
      return String.format("%s/%s/PLACE/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, year, stateFips, placeSuffix);

    case "zctas":
      String zctaType = (yearInt == 2010) ? "zcta510" : "zcta520";
      String zctaDir = (yearInt == 2010) ? "ZCTA5" : "ZCTA520";
      return String.format("%s/%s/%s/tl_%s_us_%s.zip",
          TIGER_BASE_URL, tigerPath, zctaDir, year, zctaType);

    case "census_tracts":
      if (stateFips == null) {
        return null;
      }
      String tractSuffix = (yearInt == 2010) ? "tract10" : "tract";
      return String.format("%s/%s/TRACT/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, year, stateFips, tractSuffix);

    case "block_groups":
      if (stateFips == null) {
        return null;
      }
      String bgSuffix = (yearInt == 2010) ? "bg10" : "bg";
      return String.format("%s/%s/BG/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, year, stateFips, bgSuffix);

    case "cbsa":
      String cbsaSuffix = (yearInt == 2010) ? "cbsa10" : "cbsa";
      return String.format("%s/%s/CBSA/tl_%s_us_%s.zip",
          TIGER_BASE_URL, tigerPath, year, cbsaSuffix);

    case "congressional_districts":
      if (stateFips == null) {
        return null;
      }
      // Congressional district files are per-state
      // 118th Congress (2023-2024), 119th Congress (2025-2026)
      int congressNum = ((yearInt - 1789) / 2) + 1;
      return String.format("%s/%s/CD/tl_%s_%s_cd%d.zip",
          TIGER_BASE_URL, tigerPath, year, stateFips, congressNum);

    case "school_districts":
      if (stateFips == null) {
        return null;
      }
      String sdSuffix = (yearInt == 2010) ? "unsd10" : "unsd";
      return String.format("%s/%s/UNSD/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, year, stateFips, sdSuffix);

    case "state_legislative_lower":
      if (stateFips == null) {
        return null;
      }
      String sldlSuffix = (yearInt == 2010) ? "sldl10" : "sldl";
      return String.format("%s/%s/SLDL/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, year, stateFips, sldlSuffix);

    case "state_legislative_upper":
      if (stateFips == null) {
        return null;
      }
      String slduSuffix = (yearInt == 2010) ? "sldu10" : "sldu";
      return String.format("%s/%s/SLDU/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, year, stateFips, slduSuffix);

    case "county_subdivisions":
      if (stateFips == null) {
        return null;
      }
      String cousubSuffix = (yearInt == 2010) ? "cousub10" : "cousub";
      return String.format("%s/%s/COUSUB/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, year, stateFips, cousubSuffix);

    case "tribal_areas":
      String aiannhSuffix = (yearInt == 2010) ? "aiannh10" : "aiannh";
      return String.format("%s/%s/AIANNH/tl_%s_us_%s.zip",
          TIGER_BASE_URL, tigerPath, year, aiannhSuffix);

    case "urban_areas":
      // UAC directory and suffix both have vintage indicator (UAC10 for 2010, UAC20 for 2020+)
      String uacSuffix = (yearInt == 2010) ? "uac10" : "uac20";
      String uacDir = (yearInt == 2010) ? "UAC10" : "UAC20";
      return String.format("%s/%s/%s/tl_%s_us_%s.zip",
          TIGER_BASE_URL, tigerPath, uacDir, year, uacSuffix);

    case "pumas":
      if (stateFips == null) {
        return null;
      }
      // PUMA directory and suffix both have vintage indicator (PUMA10 for 2010, PUMA20 for 2020+)
      String pumaSuffix = (yearInt == 2010) ? "puma10" : "puma20";
      String pumaDir = (yearInt == 2010) ? "PUMA10" : "PUMA20";
      return String.format("%s/%s/%s/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, pumaDir, year, stateFips, pumaSuffix);

    case "voting_districts":
      if (stateFips == null) {
        return null;
      }
      // VTD files have different URL patterns based on census vintage:
      // - 2012: TIGER2012/VTD/tl_2012_{state}_vtd10.zip (2010 census boundaries)
      // - 2020+: TIGER2020PL/LAYER/VTD/2020/tl_2020_{state}_vtd20.zip (2020 census boundaries)
      if (yearInt >= 2020) {
        // 2020 census vintage - different path structure
        return String.format("%s/TIGER2020PL/LAYER/VTD/2020/tl_2020_%s_vtd20.zip",
            TIGER_BASE_URL, stateFips);
      } else {
        // 2010 census vintage (available in TIGER2012)
        return String.format("%s/%s/VTD/tl_%s_%s_vtd10.zip",
            TIGER_BASE_URL, tigerPath, year, stateFips);
      }

    default:
      return null;
    }
  }

  private void downloadFile(String urlString, File outputFile) throws IOException {
    URI uri = URI.create(urlString);
    URL url = uri.toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000); // 5 minutes for large shapefiles (block_groups can be 10+ MB)
    // Required headers - Census Bureau uses Cloudflare which may throttle/reject bare requests
    conn.setRequestProperty("User-Agent", "Apache-Calcite-GovData-Adapter/1.0");
    conn.setRequestProperty("Accept", "application/zip, application/octet-stream, */*");

    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new IOException("HTTP " + responseCode + " for URL: " + urlString);
    }

    long expectedLength = conn.getContentLengthLong();
    long totalBytesRead = 0;

    try (BufferedInputStream in = new BufferedInputStream(conn.getInputStream());
         FileOutputStream out = new FileOutputStream(outputFile)) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
        totalBytesRead += bytesRead;
      }
    }

    // Verify download completeness if Content-Length was provided
    if (expectedLength > 0 && totalBytesRead != expectedLength) {
      outputFile.delete();
      throw new IOException("Incomplete download: expected " + expectedLength
          + " bytes but got " + totalBytesRead + " for URL: " + urlString);
    }
  }

  private void extractZip(File zipFile, File destDir) throws IOException {
    try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile.toPath()))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        if (entry.isDirectory()) {
          continue;
        }
        File destFile = new File(destDir, entry.getName());
        destFile.getParentFile().mkdirs();
        try (FileOutputStream fos = new FileOutputStream(destFile)) {
          byte[] buffer = new byte[8192];
          int len;
          while ((len = zis.read(buffer)) > 0) {
            fos.write(buffer, 0, len);
          }
        }
      }
    }
  }

  private String findShapefilePrefix(File dir) {
    File[] files = dir.listFiles((d, name) -> name.endsWith(".shp"));
    if (files != null && files.length > 0) {
      String name = files[0].getName();
      return name.substring(0, name.length() - 4);
    }
    return null;
  }

  private TigerShapefileParser.AttributeMapper getMapperForTable(String tableName, int year) {
    switch (tableName) {
    case "states":
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            getAttrString(feature, "STATEFP"),
            getAttrString(feature, "GEOID"),
            getAttrString(feature, "NAME"),
            getAttrString(feature, "STUSPS"),
            getAttrDouble(feature, "ALAND"),
            getAttrDouble(feature, "AWATER"),
            geom != null ? geom.toText() : null
        };
      };

    case "counties":
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            getAttrString(feature, "GEOID"),
            getAttrString(feature, "STATEFP"),
            getAttrString(feature, "NAME"),
            getAttrString(feature, "NAMELSAD"),
            getAttrDouble(feature, "ALAND"),
            getAttrDouble(feature, "AWATER"),
            geom != null ? geom.toText() : null
        };
      };

    case "places":
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            getAttrString(feature, "GEOID"),
            getAttrString(feature, "STATEFP"),
            getAttrString(feature, "NAME"),
            getAttrString(feature, "NAMELSAD"),
            geom != null ? geom.toText() : null
        };
      };

    case "zctas":
      // Uses TigerFieldNormalizer for vintage-aware field resolution
      final TigerFieldNormalizer zctaNormalizer = TigerFieldNormalizer.forTable("zctas", year);
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            zctaNormalizer.getStringField(feature, "zcta"),
            zctaNormalizer.getDoubleField(feature, "land_area"),
            zctaNormalizer.getDoubleField(feature, "water_area"),
            geom != null ? geom.toText() : null
        };
      };

    case "census_tracts":
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            getAttrString(feature, "GEOID"),
            getAttrString(feature, "STATEFP"),
            getAttrString(feature, "COUNTYFP"),
            getAttrString(feature, "NAME"),
            getAttrDouble(feature, "ALAND"),
            getAttrDouble(feature, "AWATER"),
            geom != null ? geom.toText() : null
        };
      };

    case "block_groups":
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            getAttrString(feature, "GEOID"),
            getAttrString(feature, "STATEFP"),
            getAttrString(feature, "COUNTYFP"),
            getAttrString(feature, "TRACTCE"),
            getAttrDouble(feature, "ALAND"),
            getAttrDouble(feature, "AWATER"),
            geom != null ? geom.toText() : null
        };
      };

    case "cbsa":
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        String cbsaCode = getAttrString(feature, "CBSAFP");
        if (cbsaCode == null) {
          cbsaCode = getAttrString(feature, "GEOID");
        }
        return new Object[]{
            cbsaCode,
            getAttrString(feature, "NAME"),
            getAttrString(feature, "LSAD"),
            getAttrDouble(feature, "ALAND"),
            getAttrDouble(feature, "AWATER"),
            geom != null ? geom.toText() : null
        };
      };

    case "congressional_districts":
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            getAttrString(feature, "GEOID"),
            getAttrString(feature, "STATEFP"),
            getAttrString(feature, "NAMELSAD"),
            getAttrDouble(feature, "ALAND"),
            getAttrDouble(feature, "AWATER"),
            geom != null ? geom.toText() : null
        };
      };

    case "school_districts":
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            getAttrString(feature, "GEOID"),
            getAttrString(feature, "STATEFP"),
            getAttrString(feature, "NAME"),
            getAttrString(feature, "LOGRADE"),
            getAttrDouble(feature, "ALAND"),
            getAttrDouble(feature, "AWATER"),
            geom != null ? geom.toText() : null
        };
      };

    case "state_legislative_lower":
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            getAttrString(feature, "GEOID"),
            getAttrString(feature, "STATEFP"),
            getAttrString(feature, "NAMELSAD"),
            getAttrDouble(feature, "ALAND"),
            getAttrDouble(feature, "AWATER"),
            geom != null ? geom.toText() : null
        };
      };

    case "state_legislative_upper":
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            getAttrString(feature, "GEOID"),
            getAttrString(feature, "STATEFP"),
            getAttrString(feature, "NAMELSAD"),
            getAttrDouble(feature, "ALAND"),
            getAttrDouble(feature, "AWATER"),
            geom != null ? geom.toText() : null
        };
      };

    case "county_subdivisions":
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            getAttrString(feature, "GEOID"),
            getAttrString(feature, "STATEFP"),
            getAttrString(feature, "COUNTYFP"),
            getAttrString(feature, "NAME"),
            getAttrString(feature, "NAMELSAD"),
            getAttrDouble(feature, "ALAND"),
            getAttrDouble(feature, "AWATER"),
            geom != null ? geom.toText() : null
        };
      };

    case "tribal_areas":
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            getAttrString(feature, "GEOID"),
            getAttrString(feature, "NAME"),
            getAttrString(feature, "NAMELSAD"),
            getAttrDouble(feature, "ALAND"),
            getAttrDouble(feature, "AWATER"),
            geom != null ? geom.toText() : null
        };
      };

    case "urban_areas":
      // Uses TigerFieldNormalizer for vintage-aware field resolution
      final TigerFieldNormalizer urbanNormalizer = TigerFieldNormalizer.forTable("urban_areas", year);
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            urbanNormalizer.getStringField(feature, "uace"),
            urbanNormalizer.getStringField(feature, "name"),
            urbanNormalizer.getStringField(feature, "urban_type"),
            urbanNormalizer.getDoubleField(feature, "land_area"),
            urbanNormalizer.getDoubleField(feature, "water_area"),
            geom != null ? geom.toText() : null
        };
      };

    case "pumas":
      // Uses TigerFieldNormalizer for vintage-aware field resolution
      final TigerFieldNormalizer pumaNormalizer = TigerFieldNormalizer.forTable("pumas", year);
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            pumaNormalizer.getStringField(feature, "puma_code"),
            pumaNormalizer.getStringField(feature, "state_fips"),
            pumaNormalizer.getStringField(feature, "puma_name"),
            pumaNormalizer.getDoubleField(feature, "land_area"),
            pumaNormalizer.getDoubleField(feature, "water_area"),
            geom != null ? geom.toText() : null
        };
      };

    case "voting_districts":
      // Uses TigerFieldNormalizer for vintage-aware field resolution
      // Note: VTD data has different URL patterns between vintages (handled in buildDownloadUrl)
      final TigerFieldNormalizer vtdNormalizer = TigerFieldNormalizer.forTable("voting_districts", year);
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            vtdNormalizer.getStringField(feature, "vtd_code"),
            vtdNormalizer.getStringField(feature, "state_fips"),
            vtdNormalizer.getStringField(feature, "county_fips"),
            vtdNormalizer.getStringField(feature, "vtd_name"),
            vtdNormalizer.getDoubleField(feature, "land_area"),
            vtdNormalizer.getDoubleField(feature, "water_area"),
            geom != null ? geom.toText() : null
        };
      };

    default:
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{geom != null ? geom.toText() : null};
      };
    }
  }

  private String[] getColumnNamesForTable(String tableName) {
    switch (tableName) {
    case "states":
      return new String[]{"state_fips", "state_code", "state_name", "state_abbr",
          "land_area", "water_area", "geometry"};
    case "counties":
      return new String[]{"county_fips", "state_fips", "county_name", "county_code",
          "land_area", "water_area", "geometry"};
    case "places":
      return new String[]{"place_fips", "state_fips", "place_name", "place_type", "geometry"};
    case "zctas":
      return new String[]{"zcta", "land_area", "water_area", "geometry"};
    case "census_tracts":
      return new String[]{"tract_fips", "state_fips", "county_fips", "tract_name",
          "land_area", "water_area", "geometry"};
    case "block_groups":
      return new String[]{"block_group_fips", "state_fips", "county_fips", "tract_fips",
          "land_area", "water_area", "geometry"};
    case "cbsa":
      return new String[]{"cbsa_fips", "cbsa_name", "metro_micro",
          "land_area", "water_area", "geometry"};
    case "congressional_districts":
      return new String[]{"cd_fips", "state_fips", "cd_name",
          "land_area", "water_area", "geometry"};
    case "school_districts":
      return new String[]{"sd_lea", "state_fips", "sd_name", "sd_type",
          "land_area", "water_area", "geometry"};
    case "state_legislative_lower":
      return new String[]{"sldl_fips", "state_fips", "sldl_name",
          "land_area", "water_area", "geometry"};
    case "state_legislative_upper":
      return new String[]{"sldu_fips", "state_fips", "sldu_name",
          "land_area", "water_area", "geometry"};
    case "county_subdivisions":
      return new String[]{"cousub_fips", "state_fips", "county_fips", "cousub_name", "cousub_type",
          "land_area", "water_area", "geometry"};
    case "tribal_areas":
      return new String[]{"aiannh_fips", "aiannh_name", "aiannh_type",
          "land_area", "water_area", "geometry"};
    case "urban_areas":
      return new String[]{"ua_code", "ua_name", "ua_type",
          "land_area", "water_area", "geometry"};
    case "pumas":
      return new String[]{"puma_fips", "state_fips", "puma_name",
          "land_area", "water_area", "geometry"};
    case "voting_districts":
      return new String[]{"vtd_fips", "state_fips", "county_fips", "vtd_name",
          "land_area", "water_area", "geometry"};
    default:
      return new String[]{"geometry"};
    }
  }

  private String getAttrString(TigerShapefileParser.ShapefileFeature feature, String key) {
    Object val = feature.getAttribute(key);
    return val != null ? val.toString().trim() : null;
  }

  private Double getAttrDouble(TigerShapefileParser.ShapefileFeature feature, String key) {
    Object val = feature.getAttribute(key);
    if (val instanceof Number) {
      return ((Number) val).doubleValue();
    }
    if (val instanceof String) {
      try {
        return Double.parseDouble((String) val);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
