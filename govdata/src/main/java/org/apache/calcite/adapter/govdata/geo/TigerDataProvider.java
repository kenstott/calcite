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
import org.apache.calcite.adapter.file.etl.StorageAwareDataProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
public class TigerDataProvider implements StorageAwareDataProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerDataProvider.class);
  private static final String TIGER_BASE_URL = "https://www2.census.gov/geo/tiger";

  private StorageProvider storageProvider;
  private String cacheBaseDir;

  @Override public void setStorageProvider(StorageProvider storageProvider, String cacheDirectory) {
    this.storageProvider = storageProvider;
    this.cacheBaseDir = cacheDirectory;
  }

  private StorageProvider storageProvider() {
    if (storageProvider == null) {
      storageProvider = StorageProviderFactory.createForGovDataCache();
      cacheBaseDir = StorageProviderFactory.getGovDataCacheDir();
    }
    return storageProvider;
  }

  private String cachePath(String tableName, String year, String stateFips) {
    StorageProvider sp = storageProvider();
    String path = sp.resolvePath(cacheBaseDir, "tiger");
    path = sp.resolvePath(path, "year=" + year);
    path = sp.resolvePath(path, tableName);
    if (stateFips != null) {
      path = sp.resolvePath(path, stateFips);
    }
    return path;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config, Map<String, String> variables)
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

    File tempDir = null;
    try {
      Runtime runtime = Runtime.getRuntime();
      long usedMb = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
      long maxMb = runtime.maxMemory() / (1024 * 1024);
      LOGGER.info("Memory before fetch: {}MB used / {}MB max", usedMb, maxMb);

      // Check cache before downloading
      String cachePath = cachePath(tableName, year, stateFips);
      tempDir = restoreFromCache(cachePath, tableName, year, stateFips);
      if (tempDir == null) {
        LOGGER.info("Downloading TIGER shapefile from: {}", url);
        tempDir = ZipDownloadUtils.downloadZipToTempDir(url, null, "tiger-" + tableName);
        writeToCache(tempDir, cachePath);
      }

      // Find shapefile prefix
      String prefix = findShapefilePrefix(tempDir);
      if (prefix == null) {
        LOGGER.error("No shapefile found in extracted ZIP for table {}", tableName);
        return new ArrayList<Map<String, Object>>().iterator();
      }

      // Parse shapefile
      int yearInt = Integer.parseInt(year);
      TigerShapefileParser.AttributeMapper mapper = getMapperForTable(tableName, yearInt);
      List<Object[]> records =
          TigerShapefileParser.parseShapefile(tempDir, prefix, mapper);

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
      ZipDownloadUtils.deleteDirectory(tempDir);
      tempDir = null;

      return result.iterator();

    } catch (IOException e) {
      // 404 means no data exists for this partition (e.g. voting_districts only has
      // census vintages 2012 and 2020 — all other years are legitimately absent).
      ZipDownloadUtils.deleteDirectory(tempDir);
      if (e.getMessage() != null && e.getMessage().startsWith("HTTP 404")) {
        LOGGER.info("No data for table {} year={} state={} (HTTP 404 — skipping partition)",
            tableName, year, stateFips);
        return new ArrayList<Map<String, Object>>().iterator();
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

  /**
   * Restore shapefiles from cache to a temp dir. Returns null if not cached.
   * TIGER files are immutable by year, so no TTL check needed.
   */
  private File restoreFromCache(String cachePath, String tableName, String year, String stateFips) {
    try {
      java.util.List<StorageProvider.FileEntry> files = storageProvider().listFiles(cachePath, true);
      if (files.isEmpty()) {
        return null;
      }
      boolean hasShapefile = files.stream()
          .anyMatch(f -> !f.isDirectory() && f.getPath().endsWith(".shp"));
      if (!hasShapefile) {
        return null;
      }
      LOGGER.info("Cache hit for {} year={} state={} — restoring from {}", tableName, year, stateFips, cachePath);
      File tempDir = Files.createTempDirectory("tiger-" + tableName + "-cached-").toFile();
      for (StorageProvider.FileEntry entry : files) {
        if (entry.isDirectory()) continue;
        String relative = entry.getPath().substring(cachePath.length());
        if (relative.startsWith("/")) relative = relative.substring(1);
        File dest = new File(tempDir, relative);
        dest.getParentFile().mkdirs();
        try (InputStream in = storageProvider().openInputStream(entry.getPath());
             FileOutputStream out = new FileOutputStream(dest)) {
          byte[] buf = new byte[65536];
          int len;
          while ((len = in.read(buf)) != -1) out.write(buf, 0, len);
        }
      }
      return tempDir;
    } catch (Exception e) {
      LOGGER.debug("Cache restore failed for {}: {}", cachePath, e.getMessage());
      return null;
    }
  }

  /** Write extracted shapefile temp dir to cache via storageProvider. */
  private void writeToCache(File tempDir, String cachePath) {
    try {
      File[] files = tempDir.listFiles();
      if (files == null) return;
      for (File file : files) {
        if (file.isDirectory()) continue;
        if (file.getName().endsWith(".zip")) continue;
        String destPath = storageProvider().resolvePath(cachePath, file.getName());
        try (InputStream in = new java.io.FileInputStream(file)) {
          storageProvider().writeFile(destPath, in);
        }
      }
      LOGGER.info("Cached shapefile to {}", cachePath);
    } catch (Exception e) {
      LOGGER.warn("Failed to write shapefile to cache {}: {}", cachePath, e.getMessage());
      // Non-fatal — data was already parsed successfully
    }
  }

  private String buildDownloadUrl(String tableName, String year, String stateFips) {
    int yearInt = Integer.parseInt(year);

    // TIGER 2000-2001 files don't exist on Census Bureau servers
    // Data availability starts from TIGER2002
    if (yearInt >= 2000 && yearInt <= 2001) {
      LOGGER.debug("TIGER {} data not available for year {} - Census Bureau only has 2002+",
          tableName, year);
      return null;
    }

    String tigerPath = "TIGER" + year;
    // TIGER2010 has an extra /2010/ subdirectory between entity folder and files
    // e.g., TIGER2010/STATE/2010/tl_2010_us_state10.zip (vs TIGER2024/STATE/tl_2024_us_state.zip)
    String subdir2010 = (yearInt == 2010) ? "/2010" : "";

    switch (tableName) {
    case "states":
      String stateSuffix = (yearInt == 2010) ? "state10" : "state";
      return String.format("%s/%s/STATE%s/tl_%s_us_%s.zip",
          TIGER_BASE_URL, tigerPath, subdir2010, year, stateSuffix);

    case "counties":
      String countySuffix = (yearInt == 2010) ? "county10" : "county";
      return String.format("%s/%s/COUNTY%s/tl_%s_us_%s.zip",
          TIGER_BASE_URL, tigerPath, subdir2010, year, countySuffix);

    case "places":
      if (stateFips == null) {
        return null;
      }
      String placeSuffix = (yearInt == 2010) ? "place10" : "place";
      return String.format("%s/%s/PLACE%s/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, subdir2010, year, stateFips, placeSuffix);

    case "zctas":
      String zctaType = (yearInt == 2010) ? "zcta510" : "zcta520";
      String zctaDir = (yearInt == 2010) ? "ZCTA5" : "ZCTA520";
      return String.format("%s/%s/%s%s/tl_%s_us_%s.zip",
          TIGER_BASE_URL, tigerPath, zctaDir, subdir2010, year, zctaType);

    case "census_tracts":
      if (stateFips == null) {
        return null;
      }
      String tractSuffix = (yearInt == 2010) ? "tract10" : "tract";
      return String.format("%s/%s/TRACT%s/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, subdir2010, year, stateFips, tractSuffix);

    case "block_groups":
      if (stateFips == null) {
        return null;
      }
      String bgSuffix = (yearInt == 2010) ? "bg10" : "bg";
      return String.format("%s/%s/BG%s/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, subdir2010, year, stateFips, bgSuffix);

    case "cbsa":
      String cbsaSuffix = (yearInt == 2010) ? "cbsa10" : "cbsa";
      return String.format("%s/%s/CBSA%s/tl_%s_us_%s.zip",
          TIGER_BASE_URL, tigerPath, subdir2010, year, cbsaSuffix);

    case "congressional_districts":
      if (stateFips == null) {
        return null;
      }
      // Congressional district files are per-state
      // 118th Congress (2023-2024), 119th Congress (2025-2026)
      int congressNum = ((yearInt - 1789) / 2) + 1;
      return String.format("%s/%s/CD%s/tl_%s_%s_cd%d.zip",
          TIGER_BASE_URL, tigerPath, subdir2010, year, stateFips, congressNum);

    case "school_districts":
      if (stateFips == null) {
        return null;
      }
      String sdSuffix = (yearInt == 2010) ? "unsd10" : "unsd";
      return String.format("%s/%s/UNSD%s/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, subdir2010, year, stateFips, sdSuffix);

    case "state_legislative_lower":
      if (stateFips == null) {
        return null;
      }
      String sldlSuffix = (yearInt == 2010) ? "sldl10" : "sldl";
      return String.format("%s/%s/SLDL%s/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, subdir2010, year, stateFips, sldlSuffix);

    case "state_legislative_upper":
      if (stateFips == null) {
        return null;
      }
      String slduSuffix = (yearInt == 2010) ? "sldu10" : "sldu";
      return String.format("%s/%s/SLDU%s/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, subdir2010, year, stateFips, slduSuffix);

    case "county_subdivisions":
      if (stateFips == null) {
        return null;
      }
      String cousubSuffix = (yearInt == 2010) ? "cousub10" : "cousub";
      return String.format("%s/%s/COUSUB%s/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, subdir2010, year, stateFips, cousubSuffix);

    case "tribal_areas":
      String aiannhSuffix = (yearInt == 2010) ? "aiannh10" : "aiannh";
      return String.format("%s/%s/AIANNH%s/tl_%s_us_%s.zip",
          TIGER_BASE_URL, tigerPath, subdir2010, year, aiannhSuffix);

    case "urban_areas":
      // UAC directory and suffix both have vintage indicator (UAC10 for 2010, UAC20 for 2020+)
      String uacSuffix = (yearInt == 2010) ? "uac10" : "uac20";
      String uacDir = (yearInt == 2010) ? "UAC10" : "UAC20";
      return String.format("%s/%s/%s%s/tl_%s_us_%s.zip",
          TIGER_BASE_URL, tigerPath, uacDir, subdir2010, year, uacSuffix);

    case "pumas":
      if (stateFips == null) {
        return null;
      }
      // PUMA URL pattern depends on census vintage:
      //   2010:      TIGER2010/PUMA10/2010/tl_2010_{fips}_puma10.zip  (2010 census, special path)
      //   2012-2021: TIGER{year}/PUMA/tl_{year}_{fips}_puma10.zip     (2010-census vintage)
      //   2022-2023: TIGER{year}/PUMA/tl_{year}_{fips}_puma20.zip     (2020-census vintage, same dir)
      //   2024+:     TIGER{year}/PUMA20/tl_{year}_{fips}_puma20.zip   (2020-census vintage, new dir)
      String pumaSuffix;
      String pumaDir;
      if (yearInt == 2010) {
        pumaSuffix = "puma10";
        pumaDir = "PUMA10";
      } else if (yearInt <= 2021) {
        pumaSuffix = "puma10";
        pumaDir = "PUMA";
      } else if (yearInt <= 2023) {
        pumaSuffix = "puma20";
        pumaDir = "PUMA";
      } else {
        pumaSuffix = "puma20";
        pumaDir = "PUMA20";
      }
      return String.format("%s/%s/%s%s/tl_%s_%s_%s.zip",
          TIGER_BASE_URL, tigerPath, pumaDir, subdir2010, year, stateFips, pumaSuffix);

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
      // Uses TigerFieldNormalizer: AIANNHCE is the correct field; GEOID in AIANNH files is the same
      // value but may be empty. All years use consistent field names (no vintage suffix).
      final TigerFieldNormalizer tribalNormalizer = TigerFieldNormalizer.forTable("tribal_areas", year);
      return feature -> {
        Geometry geom = (Geometry) feature.getAttribute("_GEOMETRY_");
        return new Object[]{
            tribalNormalizer.getStringField(feature, "aiannhce"),
            tribalNormalizer.getStringField(feature, "name"),
            getAttrString(feature, "NAMELSAD"),
            tribalNormalizer.getDoubleField(feature, "land_area"),
            tribalNormalizer.getDoubleField(feature, "water_area"),
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
      return new String[]{"sldl_fips", "state_fips", "district_name",
          "land_area", "water_area", "geometry"};
    case "state_legislative_upper":
      return new String[]{"sldu_fips", "state_fips", "district_name",
          "land_area", "water_area", "geometry"};
    case "county_subdivisions":
      return new String[]{"cousub_fips", "state_fips", "county_fips", "cousub_name", "cousub_type",
          "land_area", "water_area", "geometry"};
    case "tribal_areas":
      return new String[]{"aiannhce", "name", "namelsad",
          "land_area", "water_area", "geometry"};
    case "urban_areas":
      return new String[]{"uace", "name", "urban_type",
          "land_area", "water_area", "geometry"};
    case "pumas":
      return new String[]{"puma_code", "state_fips", "puma_name",
          "land_area", "water_area", "geometry"};
    case "voting_districts":
      return new String[]{"vtd_code", "state_fips", "county_fips", "vtd_name",
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
