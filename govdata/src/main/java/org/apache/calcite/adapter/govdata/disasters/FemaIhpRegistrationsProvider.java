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
package org.apache.calcite.adapter.govdata.disasters;

import org.apache.calcite.adapter.file.StagingDirectory;
import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.govdata.housing.HmdaLoanLevelAggregateProvider.ResumableHttpStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code disasters.ihp_valid_registrations_by_county}: the OpenFEMA
 * {@code IndividualsAndHouseholdsProgramValidRegistrations} v2 dataset (26.2 million
 * registration-level rows) aggregated to one row per disaster x county.
 *
 * <p>The API cannot aggregate and pages at about 10 seconds per 1,000 rows at deep offsets (about
 * three days for a full read). OpenFEMA publishes the same dataset as a single Parquet file
 * (about 760 MB; the CSV of the same data is over 10 GB), so this provider takes that file
 * through the pipeline's {@link RawCache} (downloaded once, resumable on a dropped connection),
 * copies it to a staging file and aggregates it with an embedded DuckDB query that streams the
 * row groups off disk. The 26 million rows are never held in the JVM; only the aggregate (about
 * 11,000 rows) is.
 *
 * <p>County. The registration's {@code fips} is the county of the damaged dwelling and is the
 * key. About 3 percent of the registrations (whole disaster-county groups of some disasters)
 * carry no {@code fips}; for those the county code comes from the declaration of the same disaster
 * number, state and area name ({@link FemaDeclaredCountyFips}, the rule the housing-assistance
 * tables use), and a row the declarations do not resolve to exactly one county (a tribal or
 * statewide area, an unlisted name) keeps a null county and is grouped by its area name. Nothing
 * is guessed; {@code fips_from_declaration_registrations} counts the registrations whose code was
 * taken from the declaration.
 *
 * <p>A fetch returns the whole snapshot. {@code freshness: hash} makes the pipeline fetch twice per
 * batch (to hash and to write); the second fetch is served from the raw cache.
 */
public class FemaIhpRegistrationsProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(FemaIhpRegistrationsProvider.class);

  static final String URL = "https://www.fema.gov/about/reports-and-data/openfema/v2/"
      + "IndividualsAndHouseholdsProgramValidRegistrationsV2.parquet";
  private static final String LABEL = "OpenFEMA IHP valid registrations";
  /**
   * null = the JDK default agent. The Akamai edge in front of the OpenFEMA bulk files answers 403
   * to a Mozilla or custom agent string and serves curl, Java and Apache-HttpClient (checked
   * 2026-10-17), so no agent is set.
   */
  private static final String JDK_USER_AGENT = null;
  private static final int MAX_ATTEMPTS = 3;
  private static final long RESUME_BACKOFF_MS = 5_000L;

  /** Resolves a registration area to the declaration's state-plus-county code. */
  interface CountyCodeLookup {
    /** The 5-digit code (county part 000 for tribal/statewide areas), or null when unknown. */
    String lookup(int disasterNumber, String state, String area) throws IOException;
  }

  /** Registrations whose own {@code fips} is empty; their areas go to the declaration lookup. */
  private static final String UNCODED_AREAS_SQL =
      "SELECT DISTINCT disasterNumber, damagedStateAbbreviation, county "
      + "FROM read_parquet(?) WHERE fips IS NULL";

  /**
   * One row per disaster x county. A registration's county is its own {@code fips}, else the code
   * the declaration lookup resolved to a county; registrations with neither are grouped by state
   * and area name so that no row mixes two areas. Booleans that are null count as false.
   */
  private static final String AGGREGATE_SQL =
      "WITH reg AS ("
      + "  SELECT r.*, "
      + "    CASE WHEN r.fips IS NOT NULL THEN r.fips "
      + "         WHEN d.code IS NOT NULL AND d.code NOT LIKE '%000' THEN d.code END AS cfips, "
      + "    LEFT(COALESCE(r.fips, d.code), 2) AS sfips "
      + "  FROM read_parquet(?) r LEFT JOIN declared d "
      + "    ON r.fips IS NULL AND d.disaster_number = r.disasterNumber "
      + "   AND d.state_abbr = r.damagedStateAbbreviation AND d.county = r.county) "
      + "SELECT "
      + "  CAST(disasterNumber AS INTEGER) AS disaster_number, "
      + "  MODE(damagedStateAbbreviation) AS state_abbr, "
      + "  MODE(sfips) AS state_fips, "
      + "  cfips AS county_fips, "
      + "  MODE(county) AS county_name, "
      + "  COUNT(*) AS registrations, "
      + "  COUNT(*) FILTER (WHERE fips IS NULL AND cfips IS NOT NULL) "
      + "    AS fips_from_declaration_registrations, "
      + "  COUNT(*) FILTER (WHERE ownRent = 'O') AS owner_registrations, "
      + "  COUNT(*) FILTER (WHERE ownRent = 'R') AS renter_registrations, "
      + "  COUNT(*) FILTER (WHERE ownRent IS NULL) AS tenure_unknown_registrations, "
      + "  COUNT(*) FILTER (WHERE primaryResidence) AS primary_residence_registrations, "
      + "  COUNT(*) FILTER (WHERE homeDamage) AS home_damage_registrations, "
      + "  COUNT(*) FILTER (WHERE destroyed) AS destroyed_registrations, "
      + "  COUNT(*) FILTER (WHERE floodDamage) AS flood_damage_registrations, "
      + "  COUNT(*) FILTER (WHERE inspnIssued) AS inspections_issued, "
      + "  COUNT(*) FILTER (WHERE inspnReturned) AS inspections_returned, "
      + "  COUNT(*) FILTER (WHERE ihpEligible) AS ihp_eligible_registrations, "
      + "  COUNT(*) FILTER (WHERE ihpAmount > 0) AS ihp_awarded_registrations, "
      + "  COUNT(*) FILTER (WHERE haEligible) AS ha_eligible_registrations, "
      + "  COUNT(*) FILTER (WHERE onaEligible) AS ona_eligible_registrations, "
      + "  COUNT(*) FILTER (WHERE sbaApproved) AS sba_approved_registrations, "
      + "  COUNT(*) FILTER (WHERE tsaEligible) AS tsa_eligible_registrations, "
      + "  COUNT(*) FILTER (WHERE tsaCheckedIn) AS tsa_checked_in_registrations, "
      + "  COUNT(*) FILTER (WHERE homeOwnersInsurance) AS homeowners_insurance_registrations, "
      + "  COUNT(*) FILTER (WHERE floodInsurance) AS flood_insurance_registrations, "
      + "  COUNT(*) FILTER (WHERE grossIncome = '0') AS income_zero_registrations, "
      + "  COUNT(*) FILTER (WHERE grossIncome = '<$15,000') AS income_under_15000_registrations, "
      + "  COUNT(*) FILTER (WHERE grossIncome = '$15,000-$30,000') "
      + "    AS income_15000_to_30000_registrations, "
      + "  COUNT(*) FILTER (WHERE grossIncome = '$30,001-$60,000') "
      + "    AS income_30001_to_60000_registrations, "
      + "  COUNT(*) FILTER (WHERE grossIncome = '$60,001-$120,000') "
      + "    AS income_60001_to_120000_registrations, "
      + "  COUNT(*) FILTER (WHERE grossIncome = '$120,001-$175,000') "
      + "    AS income_120001_to_175000_registrations, "
      + "  COUNT(*) FILTER (WHERE grossIncome = '>$175,000') AS income_over_175000_registrations, "
      + "  COUNT(*) FILTER (WHERE grossIncome IS NULL) AS income_unreported_registrations, "
      + "  COUNT(*) FILTER (WHERE householdComposition = '1') AS household_1_person_registrations, "
      + "  COUNT(*) FILTER (WHERE householdComposition = '2') AS household_2_person_registrations, "
      + "  COUNT(*) FILTER (WHERE householdComposition = '3') AS household_3_person_registrations, "
      + "  COUNT(*) FILTER (WHERE householdComposition = '4') AS household_4_person_registrations, "
      + "  COUNT(*) FILTER (WHERE householdComposition = '5') AS household_5_person_registrations, "
      + "  COUNT(*) FILTER (WHERE householdComposition = '>5') "
      + "    AS household_over_5_person_registrations, "
      + "  CAST(SUM(ihpAmount) AS DOUBLE) AS ihp_amount, "
      + "  CAST(SUM(haAmount) AS DOUBLE) AS ha_amount, "
      + "  CAST(SUM(onaAmount) AS DOUBLE) AS ona_amount, "
      + "  CAST(SUM(repairAmount) AS DOUBLE) AS repair_amount, "
      + "  CAST(SUM(replacementAmount) AS DOUBLE) AS replacement_amount, "
      + "  CAST(SUM(rentalAssistanceAmount) AS DOUBLE) AS rental_assistance_amount, "
      + "  CAST(SUM(personalPropertyAmount) AS DOUBLE) AS personal_property_amount, "
      + "  CAST(SUM(transientAccommodAmount) AS DOUBLE) AS transient_accommodation_amount, "
      + "  CAST(SUM(fipAmount) AS DOUBLE) AS flood_insurance_premium_amount, "
      + "  CAST(SUM(rpfvl) AS DOUBLE) AS real_property_verified_loss, "
      + "  CAST(SUM(ppfvl) AS DOUBLE) AS personal_property_verified_loss, "
      + "  CAST(SUM(unmetNeedRp) AS DOUBLE) AS unmet_need_real_property, "
      + "  CAST(SUM(unmetNeedPp) AS DOUBLE) AS unmet_need_personal_property, "
      + "  CAST(SUM(floodDamageAmount) AS DOUBLE) AS flood_damage_amount "
      + "FROM reg "
      + "GROUP BY disasterNumber, cfips, "
      + "  CASE WHEN cfips IS NULL THEN damagedStateAbbreviation END, "
      + "  CASE WHEN cfips IS NULL THEN county END";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    File parquet = StagingDirectory.newTempFile("ihp-registrations-", ".parquet");
    try {
      LOGGER.info("{}: fetching {} (through raw cache)", LABEL, URL);
      copyThroughCache(rawCache, URL, parquet);
      LOGGER.info("{}: {} MB, aggregating to disaster x county", LABEL,
          Long.valueOf(parquet.length() / (1024 * 1024)));
      List<Map<String, Object>> rows = aggregate(parquet, FemaDeclaredCountyFips::lookup);
      LOGGER.info("{}: {} disaster x county rows", LABEL, Integer.valueOf(rows.size()));
      return rows.iterator();
    } catch (SQLException e) {
      throw new IOException(LABEL + ": DuckDB aggregation failed", e);
    } finally {
      if (!parquet.delete()) {
        LOGGER.warn("{}: failed to delete temp file {}", LABEL, parquet);
      }
    }
  }

  /**
   * Reads {@code url} through {@code rawCache} (a hit is served from storage, a miss is downloaded
   * once and stored) and copies the bytes to {@code dest}. The download resumes from the byte
   * where a dropped connection stopped; the whole call is retried if it still fails.
   */
  private static void copyThroughCache(RawCache rawCache, String url, File dest)
      throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try (InputStream in = rawCache.openStream(url,
              () -> ResumableHttpStream.open(LABEL, JDK_USER_AGENT, url, RESUME_BACKOFF_MS));
           // local staging file for DuckDB (read_parquet needs a real path), deleted after use
           FileOutputStream out = new FileOutputStream(dest)) {
        byte[] buf = new byte[1 << 20];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
        return;
      } catch (IOException e) {
        last = e;
        LOGGER.warn("{}: attempt {}/{} failed for {}: {}", LABEL, Integer.valueOf(attempt),
            Integer.valueOf(MAX_ATTEMPTS), url, e.getMessage());
      }
    }
    throw last;
  }

  /** Aggregates the registrations file to one row per disaster x county. */
  List<Map<String, Object>> aggregate(File parquet, CountyCodeLookup lookup)
      throws SQLException, IOException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement init = conn.createStatement()) {
      init.execute("PRAGMA memory_limit='4GB'");
      init.execute("CREATE TEMP TABLE declared (disaster_number INTEGER, state_abbr VARCHAR, "
          + "county VARCHAR, code VARCHAR)");
      loadDeclaredCodes(conn, parquet, lookup);
      try (PreparedStatement ps = conn.prepareStatement(AGGREGATE_SQL)) {
        ps.setString(1, parquet.getAbsolutePath());
        try (ResultSet rs = ps.executeQuery()) {
          ResultSetMetaData meta = rs.getMetaData();
          int cols = meta.getColumnCount();
          while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            for (int i = 1; i <= cols; i++) {
              row.put(meta.getColumnLabel(i), rs.getObject(i));
            }
            rows.add(row);
          }
        }
      }
    }
    return rows;
  }

  /** Resolves each area that has registrations without a {@code fips} and stores the answers. */
  private static void loadDeclaredCodes(Connection conn, File parquet, CountyCodeLookup lookup)
      throws SQLException, IOException {
    int areas = 0;
    int resolved = 0;
    try (PreparedStatement select = conn.prepareStatement(UNCODED_AREAS_SQL);
         PreparedStatement insert =
             conn.prepareStatement("INSERT INTO declared VALUES (?, ?, ?, ?)")) {
      select.setString(1, parquet.getAbsolutePath());
      try (ResultSet rs = select.executeQuery()) {
        while (rs.next()) {
          areas++;
          int disaster = rs.getInt(1);
          String state = rs.getString(2);
          String area = rs.getString(3);
          String code = lookup.lookup(disaster, state, area);
          if (code == null) {
            continue;
          }
          resolved++;
          insert.setInt(1, disaster);
          insert.setString(2, state);
          insert.setString(3, area);
          insert.setString(4, code);
          insert.addBatch();
        }
      }
      insert.executeBatch();
    }
    LOGGER.info("{}: {} disaster/state/area groups lack a fips, {} resolved from the "
        + "declarations", LABEL, Integer.valueOf(areas), Integer.valueOf(resolved));
  }
}
