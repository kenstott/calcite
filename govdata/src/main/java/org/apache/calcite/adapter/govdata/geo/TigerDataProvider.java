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
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

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
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
      // Fallback when setStorageProvider() wasn't called with the schema-scoped operand:
      // getGovDataCacheDir() is the unscoped raw-bucket root, so scope it to "geo" here —
      // otherwise cachePath() writes tiger data at <raw>/tiger instead of <raw>/geo/tiger.
      cacheBaseDir =
          storageProvider.resolvePath(StorageProviderFactory.getGovDataCacheDir(), "geo");
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
    // Download, gate, and parse by the data VINTAGE, not the publish year. The framework injects
    // effective_year = year - dataLag for every YEAR_RANGE dimension (DimensionIterator line 187) and
    // partitions by it (materialize.partition.valueSource.year = effective_year). TIGER directories
    // are named by vintage (TIGER2025/tl_2025_us_state.zip), so using the publish year (e.g. 2026)
    // requests an unpublished/wrong-vintage directory — the gate then skips and 0 rows are written.
    // effective_year is always present for these YEAR_RANGE-backed tables; fall back to year only if
    // a table somehow has no YEAR_RANGE companion (then year already is the effective vintage).
    String publishYear = variables.get("year");
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = publishYear;
    }
    String stateFips = variables.get("state_fips");

    LOGGER.info("TigerDataProvider: Fetching {} for vintage={} (publishYear={}), state={}",
        tableName, year, publishYear, stateFips);

    // Catalog gate FIRST: never request a TIGER vintage Census has not published yet. An unpublished
    // vintage directory (e.g. TIGER2026 before its autumn release) does not 404 cleanly — the host
    // accepts the connection and then stalls, so each doomed GET burns the full read-timeout × retry
    // budget (observed as multi-hour geo workers). The directory listing IS the catalog. This must
    // run BEFORE buildDownloadUrl: that method may itself probe a per-vintage catalog directory (e.g.
    // congressional districts read TIGER<year>/CD/ to discover the cd<N> suffix), which 404s and fails
    // the batch for an unpublished vintage if the gate ran afterward.
    int requestedVintage = Integer.parseInt(year);
    int latestVintage = TigerDataDownloader.latestPublishedVintage();
    if (requestedVintage > latestVintage) {
      LOGGER.info("TIGER catalog gate: skipping {} year={} — exceeds latest published vintage {} "
          + "(an unpublished vintage directory stalls rather than 404s)",
          tableName, year, latestVintage);
      return new ArrayList<Map<String, Object>>().iterator();
    }

    // Lower bound, same rationale as the upper gate and compared against the same VINTAGE value:
    // TIGER2009 and earlier publish under state-named subdirectories (TIGER2009/29_MISSOURI/...)
    // rather than the flat per-entity layout these URLs are built for, so the request can never
    // succeed. Because TIGER lags publication by ~a year (dataLag: 1), a range whose publish year
    // starts at 2010 asks for vintage 2009 and hits this. Without the gate each such request
    // stalled instead of 404ing — 37 hard-failed batches and 222 skipped in one geo run.
    if (requestedVintage < TigerDataDownloader.EARLIEST_FLAT_LAYOUT_VINTAGE) {
      LOGGER.info("TIGER catalog gate: skipping {} year={} — precedes earliest flat-layout vintage "
          + "{} (older vintages nest under state-named subdirectories, so the flat URL cannot "
          + "resolve)", tableName, year, TigerDataDownloader.EARLIEST_FLAT_LAYOUT_VINTAGE);
      return new ArrayList<Map<String, Object>>().iterator();
    }

    // Build download URLs based on table type. Usually one file; congressional districts fan out
    // over one file per state on 2022-and-later vintages (see buildDownloadUrls).
    List<String> urls = buildDownloadUrls(tableName, year, stateFips);
    if (urls.isEmpty()) {
      LOGGER.warn("Could not build download URL for table {} year={}", tableName, year);
      return new ArrayList<Map<String, Object>>().iterator();
    }

    File tempDir = null;
    try {
      Runtime runtime = Runtime.getRuntime();
      long usedMb = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
      long maxMb = runtime.maxMemory() / (1024 * 1024);
      LOGGER.info("Memory before fetch: {}MB used / {}MB max", usedMb, maxMb);

      int yearInt = Integer.parseInt(year);
      TigerShapefileParser.AttributeMapper mapper = getMapperForTable(tableName, yearInt);
      List<Object[]> records = new ArrayList<Object[]>();

      for (String url : urls) {
        // One cache directory per downloaded file. A multi-file vintage would otherwise write
        // every state's shapefile into the same directory, where the restore path — which keys off
        // "some .shp is present" — would hand back one arbitrary state as if it were the year.
        String cachePath = cachePath(tableName, year, stateFips);
        if (urls.size() > 1) {
          cachePath = storageProvider().resolvePath(cachePath, fileStem(url));
        }
        tempDir = restoreFromCache(cachePath, tableName, year, stateFips);
        if (tempDir == null) {
          LOGGER.info("Downloading TIGER shapefile from: {}", url);
          tempDir = ZipDownloadUtils.downloadZipToTempDir(url, null, "tiger-" + tableName);
          writeToCache(tempDir, cachePath);
        }

        // Find shapefile prefix
        String prefix = findShapefilePrefix(tempDir);
        if (prefix == null) {
          LOGGER.error("No shapefile found in extracted ZIP for table {} ({})", tableName, url);
          ZipDownloadUtils.deleteDirectory(tempDir);
          tempDir = null;
          return new ArrayList<Map<String, Object>>().iterator();
        }

        // Parse shapefile
        records.addAll(TigerShapefileParser.parseShapefile(tempDir, prefix, mapper));
        ZipDownloadUtils.deleteDirectory(tempDir);
        tempDir = null;
      }

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

      LOGGER.info("Parsed {} records from {} TIGER shapefile(s) for table {}",
          result.size(), urls.size(), tableName);

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
    // fallback-guard: allow optional cache-read fast path; failure falls through to the real extraction/processing path rather than fabricating a result
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

  /** Per-CD-directory memo of the congressional-district ZIP file names published for the newest
   *  Congress in that directory — one national file, or one per state, depending on vintage. */
  private static final ConcurrentMap<String, List<String>> CD_FILES_BY_DIR =
      new ConcurrentHashMap<String, List<String>>();

  /**
   * Discovers the congressional-district ZIP files published under {@code cdDirUrl} by reading the
   * directory listing (the listing IS the catalog — there is no JSON API). Two things vary by
   * vintage and neither is derivable from the year:
   *
   * <ul>
   *   <li>Census labels each vintage's CD files by the Congress number it actually ships, which is
   *       not the in-session Congress for that calendar year — TIGER2024 ships {@code cd119}, not
   *       {@code cd118}.</li>
   *   <li>The file layout changed at the 2022 vintage. TIGER2021 and earlier publish ONE national
   *       file ({@code tl_2021_us_cd116.zip}). TIGER2023 and later publish one file PER STATE
   *       ({@code tl_2023_01_cd118.zip} …) and no national file at all. TIGER2022 carries both, for
   *       different Congresses: 56 per-state {@code cd118} files alongside a national
   *       {@code cd116}.</li>
   * </ul>
   *
   * <p>So take the highest Congress number present, then return every file that ships it — one
   * national file under the old layout, 56 state files under the new one. Anchoring on the highest
   * Congress rather than on the presence of a national file is what keeps 2022 correct: its
   * national file is a two-Congress-old leftover, not the vintage's real content.
   *
   * <p>Memoized per directory. Throws if the listing cannot be read or ships no {@code cd<N>.zip}.
   */
  private static List<String> discoverCdFiles(String cdDirUrl) throws IOException {
    List<String> cached = CD_FILES_BY_DIR.get(cdDirUrl);
    if (cached != null) {
      return cached;
    }
    HttpURLConnection conn = (HttpURLConnection) URI.create(cdDirUrl).toURL().openConnection();
    conn.setConnectTimeout(15000);
    conn.setReadTimeout(15000);
    conn.setRequestProperty("User-Agent", "Apache-Calcite-GovData/1.0");
    int status = conn.getResponseCode();
    if (status != HttpURLConnection.HTTP_OK) {
      conn.disconnect();
      throw new IOException("HTTP " + status + " reading CD catalog listing " + cdDirUrl);
    }
    // Congress number -> the file names shipping it. A listing names each file more than once
    // (href plus link text), so the per-Congress collection dedupes and orders by name.
    TreeMap<Integer, Set<String>> byCongress = new TreeMap<Integer, Set<String>>();
    java.util.regex.Pattern pattern =
        java.util.regex.Pattern.compile("(tl_\\d{4}_[0-9a-z]+_cd(\\d+)\\.zip)");
    try (InputStream in = conn.getInputStream()) {
      java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
      byte[] buf = new byte[65536];
      int len;
      while ((len = in.read(buf)) != -1) {
        bos.write(buf, 0, len);
      }
      java.util.regex.Matcher matcher = pattern.matcher(bos.toString("UTF-8"));
      while (matcher.find()) {
        Integer congress = Integer.valueOf(matcher.group(2));
        Set<String> names = byCongress.get(congress);
        if (names == null) {
          names = new TreeSet<String>();
          byCongress.put(congress, names);
        }
        names.add(matcher.group(1));
      }
    } finally {
      conn.disconnect();
    }
    if (byCongress.isEmpty()) {
      throw new IOException("No cd<N>.zip entries found in CD catalog listing " + cdDirUrl);
    }
    Integer newest = byCongress.lastKey();
    List<String> files =
        Collections.unmodifiableList(new ArrayList<String>(byCongress.get(newest)));
    CD_FILES_BY_DIR.put(cdDirUrl, files);
    LOGGER.info("TIGER CD catalog: {} publishes cd{} as {} file(s)", cdDirUrl, newest, files.size());
    return files;
  }

  /**
   * URLs to download for one fetch unit. Every table resolves to a single file except congressional
   * districts, whose 2022-and-later vintages are published per state (see {@link #discoverCdFiles}).
   * Those fan out here rather than through a {@code state_fips} dimension so the per-vintage layout
   * difference stays inside this provider and the table keeps one partition per year.
   */
  private List<String> buildDownloadUrls(String tableName, String year, String stateFips)
      throws IOException {
    if ("congressional_districts".equals(tableName)) {
      int yearInt = Integer.parseInt(year);
      String subdir2010 = (yearInt == 2010) ? "/2010" : "";
      String cdDirUrl = String.format("%s/TIGER%s/CD%s/", TIGER_BASE_URL, year, subdir2010);
      List<String> urls = new ArrayList<String>();
      for (String file : discoverCdFiles(cdDirUrl)) {
        urls.add(cdDirUrl + file);
      }
      return urls;
    }
    String url = buildDownloadUrl(tableName, year, stateFips);
    return url == null ? Collections.<String>emptyList() : Collections.singletonList(url);
  }

  /** Last path segment of a download URL without its {@code .zip} suffix, used to give each file
   *  of a multi-file vintage its own cache directory. */
  private static String fileStem(String url) {
    String name = url.substring(url.lastIndexOf('/') + 1);
    return name.endsWith(".zip") ? name.substring(0, name.length() - 4) : name;
  }

  private String buildDownloadUrl(String tableName, String year, String stateFips)
      throws IOException {
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
      // Handled by buildDownloadUrls: the vintage decides both the Congress number and
      // whether CD ships as one national file or one per state, and only a directory
      // listing can tell us which. Nothing here can name that file from the year alone.
      return null;

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
      // fallback-guard: allow nullable numeric attribute parser for an optional shapefile feature attribute
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
