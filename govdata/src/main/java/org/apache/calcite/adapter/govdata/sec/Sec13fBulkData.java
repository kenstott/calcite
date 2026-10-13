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
package org.apache.calcite.adapter.govdata.sec;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A local copy of SEC's Form 13F data sets, used instead of fetching each filing's information
 * table from sec.gov.
 *
 * <p>Fetching a 13F filing costs about three requests (cover document, {@code index.json},
 * information table) against the host-wide 8 requests per second budget every SEC worker shares.
 * SEC publishes the same information-table rows in one file per quarter, so a filing found here
 * needs only its cover document. A filing that is not here (filed after the last release) is
 * fetched from sec.gov exactly as before.
 *
 * <p>The directory named by the {@code sec.bulk13fDir} operand holds {@code infotable_*.parquet}
 * files with the data set's INFOTABLE columns as text, one file per release, written sorted by
 * {@code ACCESSION_NUMBER} so a lookup reads only the row groups that can contain it.
 * {@code govdata/scripts/fetch-13f-bulk.sh} produces them and deletes the downloaded zip. They are
 * transient: delete the directory when the run is over.
 */
final class Sec13fBulkData implements AutoCloseable {

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(Sec13fBulkData.class);

  /** One INFOTABLE row, every value as the data set carries it (null when empty). */
  static final class Row {
    final String issuer;
    final String titleOfClass;
    final String cusip;
    final String value;
    final String sharesOrPrincipal;
    final String sharesOrPrincipalType;
    final String putCall;
    final String investmentDiscretion;
    final String otherManager;
    final String votingSole;
    final String votingShared;
    final String votingNone;

    Row(String issuer, String titleOfClass, String cusip, String value,
        String sharesOrPrincipal, String sharesOrPrincipalType, String putCall,
        String investmentDiscretion, String otherManager, String votingSole,
        String votingShared, String votingNone) {
      this.issuer = issuer;
      this.titleOfClass = titleOfClass;
      this.cusip = cusip;
      this.value = value;
      this.sharesOrPrincipal = sharesOrPrincipal;
      this.sharesOrPrincipalType = sharesOrPrincipalType;
      this.putCall = putCall;
      this.investmentDiscretion = investmentDiscretion;
      this.otherManager = otherManager;
      this.votingSole = votingSole;
      this.votingShared = votingShared;
      this.votingNone = votingNone;
    }
  }

  private static final String SELECT_BY_ACCESSION =
      "SELECT NAMEOFISSUER, TITLEOFCLASS, CUSIP, VALUE, SSHPRNAMT, SSHPRNAMTTYPE, PUTCALL, "
      + "INVESTMENTDISCRETION, OTHERMANAGER, VOTING_AUTH_SOLE, VOTING_AUTH_SHARED, "
      + "VOTING_AUTH_NONE FROM infotable WHERE ACCESSION_NUMBER = ?";

  private final Connection conn;
  private final PreparedStatement byAccession;

  private Sec13fBulkData(Connection conn, PreparedStatement byAccession) {
    this.conn = conn;
    this.byAccession = byAccession;
  }

  /**
   * The data set in {@code dir} (the {@code bulk13fDir} operand of the SEC schema), or null when
   * {@code dir} is blank. A configured directory that is missing or holds no
   * {@code infotable_*.parquet} is an error, not a reason to quietly fetch every filing from
   * sec.gov.
   *
   * <p>The caller passes the value from its own operand map. {@code ModelOperand} cannot serve
   * here: the SEC ETL runs inside schema creation, before the operand is captured, so every
   * {@code ModelOperand} read made during the run comes back null.
   */
  static Sec13fBulkData forDirectory(String dir) {
    if (dir == null || dir.trim().isEmpty()) {
      LOGGER.info("13F bulk data set: not configured (bulk13fDir empty): every 13F filing is "
          + "fetched from sec.gov");
      return null;
    }
    try {
      Sec13fBulkData data = open(new File(dir.trim())); // justification: local scratch, see class doc
      LOGGER.info("13F bulk data set: using {}", dir.trim());
      return data;
    } catch (SQLException e) {
      throw new IllegalStateException("bulk13fDir " + dir + " is configured but could not be opened", e);
    }
  }

  /** Opens the {@code infotable_*.parquet} files of {@code dir}. */
  static Sec13fBulkData open(File dir) throws SQLException {
    // justification: same local scratch directory; never written to the lake, deleted after the run
    File[] files = dir.listFiles();
    List<String> parquet = new ArrayList<String>();
    if (files != null) {
      Arrays.sort(files);
      for (File f : files) {
        String n = f.getName();
        if (n.startsWith("infotable_") && n.endsWith(".parquet")) {
          parquet.add("'" + f.getAbsolutePath().replace("'", "''") + "'");
        }
      }
    }
    if (parquet.isEmpty()) {
      throw new IllegalStateException(
          "no infotable_*.parquet in " + dir.getAbsolutePath()
              + " (produce them with govdata/scripts/fetch-13f-bulk.sh)");
    }
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      try (java.sql.Statement st = conn.createStatement()) {
        st.execute("CREATE VIEW infotable AS SELECT * FROM read_parquet(["
            + String.join(", ", parquet) + "])");
      }
      return new Sec13fBulkData(conn, conn.prepareStatement(SELECT_BY_ACCESSION));
    } catch (SQLException e) {
      conn.close();
      throw e;
    }
  }

  /**
   * The information-table rows of {@code accession}, or null when the data set does not contain
   * it (so the caller fetches it from sec.gov).
   */
  synchronized List<Row> rows(String accession) {
    List<Row> out = new ArrayList<Row>();
    try {
      byAccession.setString(1, accession);
      try (ResultSet rs = byAccession.executeQuery()) {
        while (rs.next()) {
          out.add(new Row(
              text(rs, 1), text(rs, 2), text(rs, 3), text(rs, 4), text(rs, 5), text(rs, 6),
              text(rs, 7), text(rs, 8), text(rs, 9), text(rs, 10), text(rs, 11), text(rs, 12)));
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("bulk 13F lookup failed for " + accession, e);
    }
    return out.isEmpty() ? null : out;
  }

  /** Trimmed text, null when the value is absent or blank: the data set pads fixed-width fields. */
  private static String text(ResultSet rs, int col) throws SQLException {
    String s = rs.getString(col);
    if (s == null) {
      return null;
    }
    s = s.trim();
    return s.isEmpty() ? null : s;
  }

  @Override public void close() throws SQLException {
    byAccession.close();
    conn.close();
  }
}
