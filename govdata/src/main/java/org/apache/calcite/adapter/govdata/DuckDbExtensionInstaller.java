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
package org.apache.calcite.adapter.govdata;
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages extraction of bundled DuckDB extensions from JAR to local filesystem.
 *
 * <p>DuckDB extensions are bundled as resources in the shadow JAR for air-gapped operation.
 * At runtime, they are extracted to {@code ~/.duckdb/extensions/v{version}/{platform}/} on first use.
 * Once extracted, absolute-path {@code LOAD} statements read the local files directly, avoiding
 * any network calls to extensions.duckdb.org.
 *
 * <p>Usage:
 * <pre>
 *   // For JDBC — call once per connection
 *   DuckDbExtensionInstaller.ensureInstalled(conn);
 *   // Then use: stmt.execute("LOAD '/path/spatial.duckdb_extension'")
 *
 *   // For CLI — compute path before subprocess
 *   String path = DuckDbExtensionInstaller.getLocalExtensionPath("spatial");
 *   // Then use: duckdb -c "LOAD '/path/spatial.duckdb_extension'; ..."
 * </pre>
 */
public class DuckDbExtensionInstaller {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDbExtensionInstaller.class);

  private static final String[] BUNDLED_EXTENSIONS = {
      "spatial", "httpfs", "iceberg", "h3", "excel", "fts", "zipfs", "parquet"
  };

  /**
   * Extracts bundled extensions to local DuckDB directory on first use (idempotent).
   *
   * @param conn DuckDB JDBC connection — used to detect version
   * @throws SQLException if version detection fails
   */
  public static void ensureInstalled(Connection conn) throws SQLException {
    String version = detectDuckDBVersion(conn);
    String platform = detectPlatform();

    int extracted = 0;
    int alreadyPresent = 0;

    for (String ext : BUNDLED_EXTENSIONS) {
      Path target = getInstalledPath(version, platform, ext);

      // Create parent directories
      try {
        Files.createDirectories(target.getParent());
      } catch (IOException e) {
        LOGGER.warn("Failed to create DuckDB extension directory {}", target.getParent(), e);
        continue;
      }

      // Skip if already present (idempotent)
      if (Files.exists(target)) {
        alreadyPresent++;
        continue;
      }

      // Extract from JAR resource
      String resourcePath = "/duckdb/extensions/" + platform + "/" + ext + ".duckdb_extension";
      try (InputStream is = DuckDbExtensionInstaller.class.getResourceAsStream(resourcePath)) {
        if (is == null) {
          LOGGER.warn("Extension resource not found: {}", resourcePath);
          continue;
        }
        Files.copy(is, target);
        extracted++;
        LOGGER.debug("Extracted DuckDB extension: {} to {}", ext, target);
      } catch (IOException e) {
        LOGGER.warn("Failed to extract DuckDB extension {} to {}", ext, target, e);
      }
    }

    if (extracted > 0 || alreadyPresent > 0) {
      LOGGER.info("DuckDB extensions: {} extracted, {} already present", extracted, alreadyPresent);
    }
  }

  /**
   * Returns the local filesystem path for an installed extension.
   *
   * <p>This method is safe to call before any DuckDB connection exists (e.g., in ProcessBuilder
   * invocations). It computes the path based on the system's detected platform and detects
   * the DuckDB version from the system environment.
   *
   * @param extensionName Name of the extension (e.g., "spatial", "httpfs")
   * @return Absolute path to the extension file (may not exist yet)
   */
  public static String getLocalExtensionPath(String extensionName) {
    // Default to the version we have bundled
    // In air-gapped mode this should match the bundled extensions version
    String version = "1.4.4";

    String platform = detectPlatform();
    return getInstalledPath(version, platform, extensionName).toString();
  }

  /**
   * Returns the installed path for a specific extension, given version and platform.
   *
   * @param version DuckDB version (e.g., "1.4.3")
   * @param platform Platform string (e.g., "linux_amd64", "osx_arm64")
   * @param extensionName Extension name (e.g., "spatial")
   * @return Path to the extension file
   */
  private static Path getInstalledPath(String version, String platform, String extensionName) {
    String home = System.getProperty("user.home");
    return Paths.get(home, ".duckdb", "extensions", "v" + version, platform,
        extensionName + ".duckdb_extension");
  }

  /**
   * Detects the DuckDB version from the JDBC connection.
   *
   * @param conn DuckDB connection
   * @return Version string (e.g., "1.4.3")
   * @throws SQLException if query fails
   */
  private static String detectDuckDBVersion(Connection conn) throws SQLException {
    try (ResultSet rs = conn.createStatement().executeQuery("SELECT library_version FROM pragma_version()")) {
      if (rs.next()) {
        String fullVersion = rs.getString(1);
        // Extract major.minor.patch (e.g., "1.4.3" from "v1.4.3")
        if (fullVersion.startsWith("v")) {
          fullVersion = fullVersion.substring(1);
        }
        return fullVersion;
      }
    }
    // Fallback to metadata
    return conn.getMetaData().getDatabaseProductVersion();
  }

  /**
   * Detects the current platform (OS + architecture).
   *
   * @return Platform string (e.g., "osx_arm64", "linux_amd64", "windows_amd64")
   */
  private static String detectPlatform() {
    String osName = System.getProperty("os.name").toLowerCase();
    String osArch = System.getProperty("os.arch").toLowerCase();

    String os;
    if (osName.contains("mac") || osName.contains("darwin")) {
      os = "osx";
    } else if (osName.contains("linux")) {
      os = "linux";
    } else if (osName.contains("windows")) {
      os = "windows";
    } else {
      os = "unknown";
    }

    String arch;
    if (osArch.equals("aarch64") || osArch.equals("arm64")) {
      arch = "arm64";
    } else if (osArch.contains("amd64") || osArch.contains("x86_64")) {
      arch = "amd64";
    } else {
      arch = "unknown";
    }

    return os + "_" + arch;
  }
}
