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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Unified cache for SEC filing processing state.
 *
 * <p>Single source of truth for:
 * <ul>
 *   <li>Whether a filing has been processed</li>
 *   <li>Which output files exist</li>
 *   <li>Vectorization state</li>
 *   <li>Error tracking</li>
 * </ul>
 *
 * <p>Implements self-healing: on cache miss, checks S3 for existing files
 * and heals the cache if files are found.
 */
public class SecFilingCache implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecFilingCache.class);

  private static final int MAX_ERROR_RETRIES = 3;

  private final Connection connection;
  private final StorageProvider storageProvider;
  private final String parquetBaseDir;

  /**
   * Create a new filing cache.
   *
   * @param cacheDir Directory for DuckDB cache file
   * @param storageProvider Storage provider for S3 file checks
   * @param parquetBaseDir Base directory for parquet files (e.g., s3://bucket/govdata-parquet)
   */
  public SecFilingCache(String cacheDir, StorageProvider storageProvider, String parquetBaseDir) {
    this.storageProvider = storageProvider;
    this.parquetBaseDir = parquetBaseDir;

    String dbPath = cacheDir + "/sec_filing_cache.duckdb";
    try {
      Class.forName("org.duckdb.DuckDBDriver");
      this.connection = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
      initializeSchema();
      LOGGER.info("Initialized SEC filing cache at {}", dbPath);
    } catch (ClassNotFoundException | SQLException e) {
      throw new RuntimeException("Failed to initialize SEC filing cache", e);
    }
  }

  private void initializeSchema() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS sec_filing_cache (\n"
          + "  cik VARCHAR(10) NOT NULL,\n"
          + "  accession VARCHAR(25) NOT NULL,\n"
          + "  form_type VARCHAR(20) NOT NULL,\n"
          + "  filing_date VARCHAR(10) NOT NULL,\n"
          + "  fiscal_year INTEGER NOT NULL,\n"
          + "  status VARCHAR(20) NOT NULL DEFAULT 'pending',\n"
          + "  vectorization_enabled BOOLEAN NOT NULL DEFAULT FALSE,\n"
          + "  has_metadata BOOLEAN NOT NULL DEFAULT FALSE,\n"
          + "  has_facts BOOLEAN NOT NULL DEFAULT FALSE,\n"
          + "  has_contexts BOOLEAN NOT NULL DEFAULT FALSE,\n"
          + "  has_relationships BOOLEAN NOT NULL DEFAULT FALSE,\n"
          + "  has_mda BOOLEAN NOT NULL DEFAULT FALSE,\n"
          + "  has_insider BOOLEAN NOT NULL DEFAULT FALSE,\n"
          + "  has_earnings BOOLEAN NOT NULL DEFAULT FALSE,\n"
          + "  has_chunks BOOLEAN NOT NULL DEFAULT FALSE,\n"
          + "  processed_at BIGINT,\n"
          + "  error_message VARCHAR,\n"
          + "  error_count INTEGER NOT NULL DEFAULT 0,\n"
          + "  PRIMARY KEY (cik, accession)\n"
          + ")");

      stmt.execute(
          "CREATE INDEX IF NOT EXISTS idx_sec_filing_cache_status "
          + "ON sec_filing_cache(status)");
      stmt.execute(
          "CREATE INDEX IF NOT EXISTS idx_sec_filing_cache_cik_year "
          + "ON sec_filing_cache(cik, fiscal_year)");
    }
  }

  /**
   * Check if a filing needs processing.
   *
   * <p>Implements self-healing: if files exist in S3 but cache entry is missing,
   * creates the cache entry and returns SKIP.
   */
  public ProcessingDecision checkFiling(String cik, String accession, String formType,
      String filingDate, boolean vectorizationEnabled) {

    FilingCacheEntry entry = getEntry(cik, accession);
    FormType form = FormType.fromString(formType);
    int fiscalYear = extractYear(filingDate);

    if (entry == null) {
      // Not in cache - check if files exist (self-healing)
      FileInventory inventory = checkS3Files(cik, accession);
      if (inventory.isComplete(form, vectorizationEnabled)) {
        // Files exist, heal cache
        insertEntry(cik, accession, formType, filingDate, fiscalYear,
            "complete", vectorizationEnabled, inventory);
        LOGGER.info("Self-healed cache for {}:{} - files exist in S3", cik, accession);
        return ProcessingDecision.skip("Self-healed: files exist");
      }
      if (inventory.hasAnyFiles()) {
        // Check if only chunks are missing (vectorization upgrade)
        // Complete without vectorization means all non-chunk files exist
        if (vectorizationEnabled && inventory.isComplete(form, false) && !inventory.hasChunks()) {
          // All base files exist, only chunks missing - vectorization upgrade
          insertEntry(cik, accession, formType, filingDate, fiscalYear,
              "complete", false, inventory);  // Mark as complete without vectorization
          LOGGER.info("Self-healed cache for {}:{} - needs vectorization upgrade", cik, accession);
          return ProcessingDecision.processChunksOnly("Self-healed: vectorization upgrade needed");
        }
        // Partial files exist - need to complete
        insertEntry(cik, accession, formType, filingDate, fiscalYear,
            "partial", vectorizationEnabled, inventory);
        LOGGER.debug("Partial files for {}:{} - needs completion", cik, accession);
        return ProcessingDecision.process("Partial files, need completion");
      }
      // No files, needs full processing
      return ProcessingDecision.process("New filing");
    }

    // Entry exists - check status
    switch (entry.status) {
    case "complete":
      // Check if vectorization upgrade needed
      if (vectorizationEnabled && !entry.vectorizationEnabled && !entry.hasChunks) {
        return ProcessingDecision.processChunksOnly("Vectorization upgrade needed");
      }
      return ProcessingDecision.skip("Already complete");

    case "partial":
      // Check if now complete via self-healing
      FileInventory currentInventory = checkS3Files(cik, accession);
      if (currentInventory.isComplete(form, vectorizationEnabled)) {
        updateStatus(cik, accession, "complete", currentInventory);
        return ProcessingDecision.skip("Self-healed: now complete");
      }
      return ProcessingDecision.process("Completing partial processing");

    case "no_xbrl":
      return ProcessingDecision.skip("No XBRL data available");

    case "failed":
      if (entry.errorCount >= MAX_ERROR_RETRIES) {
        return ProcessingDecision.skip("Max retries exceeded");
      }
      return ProcessingDecision.process("Retrying failed filing");

    default:
      return ProcessingDecision.process("Unknown status: " + entry.status);
    }
  }

  /**
   * Check which output files exist in S3.
   */
  public FileInventory checkS3Files(String cik, String accession) {
    String secDir = storageProvider.resolvePath(parquetBaseDir, "source=sec");

    FileInventory.Builder builder = FileInventory.builder();

    builder.hasMetadata(fileExists(secDir, cik, accession, "metadata"));
    builder.hasFacts(fileExists(secDir, cik, accession, "facts"));
    builder.hasContexts(fileExists(secDir, cik, accession, "contexts"));
    builder.hasRelationships(fileExists(secDir, cik, accession, "relationships"));
    builder.hasMda(fileExists(secDir, cik, accession, "mda"));
    builder.hasInsider(fileExists(secDir, cik, accession, "insider"));
    builder.hasEarnings(fileExists(secDir, cik, accession, "earnings"));
    builder.hasChunks(fileExists(secDir, cik, accession, "chunks"));

    return builder.build();
  }

  private boolean fileExists(String secDir, String cik, String accession, String suffix) {
    String fileName = cik + "_" + accession + "_" + suffix + ".parquet";
    String pattern = storageProvider.resolvePath(secDir, "year=*/" + fileName);
    try {
      return storageProvider.exists(pattern);
    } catch (IOException e) {
      LOGGER.debug("Error checking file existence: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Mark filing as successfully processed.
   */
  public void markComplete(String cik, String accession, String formType, String filingDate,
      boolean vectorizationEnabled, FileInventory inventory) {
    int fiscalYear = extractYear(filingDate);
    upsertEntry(cik, accession, formType, filingDate, fiscalYear,
        "complete", vectorizationEnabled, inventory, null, 0);
    LOGGER.debug("Marked complete: {}:{}", cik, accession);
  }

  /**
   * Mark filing as having no XBRL data.
   */
  public void markNoXbrl(String cik, String accession, String formType, String filingDate) {
    int fiscalYear = extractYear(filingDate);
    upsertEntry(cik, accession, formType, filingDate, fiscalYear,
        "no_xbrl", false, FileInventory.empty(), null, 0);
    LOGGER.debug("Marked no_xbrl: {}:{}", cik, accession);
  }

  /**
   * Mark filing as failed.
   */
  public void markFailed(String cik, String accession, String formType, String filingDate,
      String errorMessage) {
    FilingCacheEntry existing = getEntry(cik, accession);
    int errorCount = (existing != null) ? existing.errorCount + 1 : 1;
    int fiscalYear = extractYear(filingDate);

    FileInventory inventory = (existing != null)
        ? existing.toInventory()
        : FileInventory.empty();

    upsertEntry(cik, accession, formType, filingDate, fiscalYear,
        "failed", false, inventory, errorMessage, errorCount);
    LOGGER.debug("Marked failed: {}:{} (attempt {})", cik, accession, errorCount);
  }

  /**
   * Update status and inventory for existing entry.
   */
  public void updateStatus(String cik, String accession, String status, FileInventory inventory) {
    String sql = "UPDATE sec_filing_cache SET status = ?, "
        + "has_metadata = ?, has_facts = ?, has_contexts = ?, has_relationships = ?, "
        + "has_mda = ?, has_insider = ?, has_earnings = ?, has_chunks = ?, "
        + "processed_at = ? WHERE cik = ? AND accession = ?";

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, status);
      stmt.setBoolean(2, inventory.hasMetadata());
      stmt.setBoolean(3, inventory.hasFacts());
      stmt.setBoolean(4, inventory.hasContexts());
      stmt.setBoolean(5, inventory.hasRelationships());
      stmt.setBoolean(6, inventory.hasMda());
      stmt.setBoolean(7, inventory.hasInsider());
      stmt.setBoolean(8, inventory.hasEarnings());
      stmt.setBoolean(9, inventory.hasChunks());
      stmt.setLong(10, System.currentTimeMillis());
      stmt.setString(11, cik);
      stmt.setString(12, accession);
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Failed to update status for {}:{}: {}", cik, accession, e.getMessage());
    }
  }

  /**
   * Find all filings that need vectorization upgrade.
   */
  public List<FilingCacheEntry> findNeedingVectorizationUpgrade() {
    String sql = "SELECT * FROM sec_filing_cache "
        + "WHERE status = 'complete' AND has_chunks = FALSE "
        + "ORDER BY cik, accession";

    List<FilingCacheEntry> entries = new ArrayList<>();
    try (PreparedStatement stmt = connection.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      while (rs.next()) {
        entries.add(fromResultSet(rs));
      }
    } catch (SQLException e) {
      LOGGER.error("Failed to find filings needing vectorization: {}", e.getMessage());
    }
    return entries;
  }

  /**
   * Get statistics about cache contents.
   */
  public CacheStats getStats() {
    CacheStats stats = new CacheStats();
    String sql = "SELECT status, COUNT(*) as cnt, "
        + "SUM(CASE WHEN has_chunks THEN 1 ELSE 0 END) as with_chunks "
        + "FROM sec_filing_cache GROUP BY status";

    try (PreparedStatement stmt = connection.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      while (rs.next()) {
        String status = rs.getString("status");
        int count = rs.getInt("cnt");
        int withChunks = rs.getInt("with_chunks");

        switch (status) {
        case "complete":
          stats.complete = count;
          stats.withChunks = withChunks;
          break;
        case "partial":
          stats.partial = count;
          break;
        case "no_xbrl":
          stats.noXbrl = count;
          break;
        case "failed":
          stats.failed = count;
          break;
        default:
          stats.pending += count;
        }
      }
    } catch (SQLException e) {
      LOGGER.error("Failed to get cache stats: {}", e.getMessage());
    }
    return stats;
  }

  /**
   * Invalidate all entries for a CIK.
   */
  public int invalidateCik(String cik) {
    String sql = "DELETE FROM sec_filing_cache WHERE cik = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, cik);
      int deleted = stmt.executeUpdate();
      LOGGER.info("Invalidated {} cache entries for CIK {}", deleted, cik);
      return deleted;
    } catch (SQLException e) {
      LOGGER.error("Failed to invalidate CIK {}: {}", cik, e.getMessage());
      return 0;
    }
  }

  /**
   * Clear all cache entries.
   */
  public int clearAll() {
    String sql = "DELETE FROM sec_filing_cache";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      int deleted = stmt.executeUpdate();
      LOGGER.info("Cleared {} cache entries", deleted);
      return deleted;
    } catch (SQLException e) {
      LOGGER.error("Failed to clear cache: {}", e.getMessage());
      return 0;
    }
  }

  private FilingCacheEntry getEntry(String cik, String accession) {
    String sql = "SELECT * FROM sec_filing_cache WHERE cik = ? AND accession = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, cik);
      stmt.setString(2, accession);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return fromResultSet(rs);
        }
      }
    } catch (SQLException e) {
      LOGGER.error("Failed to get entry for {}:{}: {}", cik, accession, e.getMessage());
    }
    return null;
  }

  private void insertEntry(String cik, String accession, String formType, String filingDate,
      int fiscalYear, String status, boolean vectorizationEnabled, FileInventory inventory) {
    upsertEntry(cik, accession, formType, filingDate, fiscalYear, status,
        vectorizationEnabled, inventory, null, 0);
  }

  private void upsertEntry(String cik, String accession, String formType, String filingDate,
      int fiscalYear, String status, boolean vectorizationEnabled, FileInventory inventory,
      String errorMessage, int errorCount) {

    String sql = "INSERT INTO sec_filing_cache "
        + "(cik, accession, form_type, filing_date, fiscal_year, status, vectorization_enabled, "
        + "has_metadata, has_facts, has_contexts, has_relationships, has_mda, has_insider, "
        + "has_earnings, has_chunks, processed_at, error_message, error_count) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        + "ON CONFLICT (cik, accession) DO UPDATE SET "
        + "form_type = EXCLUDED.form_type, filing_date = EXCLUDED.filing_date, "
        + "fiscal_year = EXCLUDED.fiscal_year, status = EXCLUDED.status, "
        + "vectorization_enabled = EXCLUDED.vectorization_enabled, "
        + "has_metadata = EXCLUDED.has_metadata, has_facts = EXCLUDED.has_facts, "
        + "has_contexts = EXCLUDED.has_contexts, has_relationships = EXCLUDED.has_relationships, "
        + "has_mda = EXCLUDED.has_mda, has_insider = EXCLUDED.has_insider, "
        + "has_earnings = EXCLUDED.has_earnings, has_chunks = EXCLUDED.has_chunks, "
        + "processed_at = EXCLUDED.processed_at, error_message = EXCLUDED.error_message, "
        + "error_count = EXCLUDED.error_count";

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, cik);
      stmt.setString(2, accession);
      stmt.setString(3, formType);
      stmt.setString(4, filingDate);
      stmt.setInt(5, fiscalYear);
      stmt.setString(6, status);
      stmt.setBoolean(7, vectorizationEnabled);
      stmt.setBoolean(8, inventory.hasMetadata());
      stmt.setBoolean(9, inventory.hasFacts());
      stmt.setBoolean(10, inventory.hasContexts());
      stmt.setBoolean(11, inventory.hasRelationships());
      stmt.setBoolean(12, inventory.hasMda());
      stmt.setBoolean(13, inventory.hasInsider());
      stmt.setBoolean(14, inventory.hasEarnings());
      stmt.setBoolean(15, inventory.hasChunks());
      stmt.setLong(16, System.currentTimeMillis());
      stmt.setString(17, errorMessage);
      stmt.setInt(18, errorCount);
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Failed to upsert entry for {}:{}: {}", cik, accession, e.getMessage());
    }
  }

  private FilingCacheEntry fromResultSet(ResultSet rs) throws SQLException {
    FilingCacheEntry entry = new FilingCacheEntry();
    entry.cik = rs.getString("cik");
    entry.accession = rs.getString("accession");
    entry.formType = rs.getString("form_type");
    entry.filingDate = rs.getString("filing_date");
    entry.fiscalYear = rs.getInt("fiscal_year");
    entry.status = rs.getString("status");
    entry.vectorizationEnabled = rs.getBoolean("vectorization_enabled");
    entry.hasMetadata = rs.getBoolean("has_metadata");
    entry.hasFacts = rs.getBoolean("has_facts");
    entry.hasContexts = rs.getBoolean("has_contexts");
    entry.hasRelationships = rs.getBoolean("has_relationships");
    entry.hasMda = rs.getBoolean("has_mda");
    entry.hasInsider = rs.getBoolean("has_insider");
    entry.hasEarnings = rs.getBoolean("has_earnings");
    entry.hasChunks = rs.getBoolean("has_chunks");
    entry.processedAt = rs.getLong("processed_at");
    entry.errorMessage = rs.getString("error_message");
    entry.errorCount = rs.getInt("error_count");
    return entry;
  }

  private int extractYear(String filingDate) {
    if (filingDate == null || filingDate.length() < 4) {
      return 0;
    }
    try {
      return Integer.parseInt(filingDate.substring(0, 4));
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Override
  public void close() {
    try {
      if (connection != null && !connection.isClosed()) {
        connection.close();
      }
    } catch (SQLException e) {
      LOGGER.error("Error closing cache connection: {}", e.getMessage());
    }
  }

  /**
   * Cache entry data.
   */
  public static class FilingCacheEntry {
    public String cik;
    public String accession;
    public String formType;
    public String filingDate;
    public int fiscalYear;
    public String status;
    public boolean vectorizationEnabled;
    public boolean hasMetadata;
    public boolean hasFacts;
    public boolean hasContexts;
    public boolean hasRelationships;
    public boolean hasMda;
    public boolean hasInsider;
    public boolean hasEarnings;
    public boolean hasChunks;
    public long processedAt;
    public String errorMessage;
    public int errorCount;

    public FileInventory toInventory() {
      return FileInventory.builder()
          .hasMetadata(hasMetadata)
          .hasFacts(hasFacts)
          .hasContexts(hasContexts)
          .hasRelationships(hasRelationships)
          .hasMda(hasMda)
          .hasInsider(hasInsider)
          .hasEarnings(hasEarnings)
          .hasChunks(hasChunks)
          .build();
    }
  }

  /**
   * Cache statistics.
   */
  public static class CacheStats {
    public int complete;
    public int partial;
    public int noXbrl;
    public int failed;
    public int pending;
    public int withChunks;

    public int total() {
      return complete + partial + noXbrl + failed + pending;
    }

    @Override
    public String toString() {
      return String.format(
          "CacheStats{complete=%d (withChunks=%d), partial=%d, noXbrl=%d, failed=%d, pending=%d}",
          complete, withChunks, partial, noXbrl, failed, pending);
    }
  }
}
