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
      TigerShapefileParser.AttributeMapper mapper = getMapperForTable(tableName);
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
      int congressNum = ((yearInt - 1789) / 2) + 1;
      return String.format("%s/%s/CD/tl_%s_us_cd%d.zip",
          TIGER_BASE_URL, tigerPath, year, congressNum);

    case "school_districts":
      if (stateFips == null) {
        return null;
      }
      String sdSuffix = (yearInt == 2010) ? "unsd10" : "unsd";
      return String.format("%s/%s/UNSD/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, year, stateFips, sdSuffix);

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

  private TigerShapefileParser.AttributeMapper getMapperForTable(String tableName) {
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
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        String zcta = getAttrString(feature, "ZCTA5CE20");
        if (zcta == null) {
          zcta = getAttrString(feature, "ZCTA5CE10");
        }
        if (zcta == null) {
          zcta = getAttrString(feature, "GEOID20");
        }
        if (zcta == null) {
          zcta = getAttrString(feature, "GEOID");
        }
        Double landArea = getAttrDouble(feature, "ALAND20");
        if (landArea == null) {
          landArea = getAttrDouble(feature, "ALAND");
        }
        Double waterArea = getAttrDouble(feature, "AWATER20");
        if (waterArea == null) {
          waterArea = getAttrDouble(feature, "AWATER");
        }
        return new Object[]{
            zcta,
            landArea,
            waterArea,
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
