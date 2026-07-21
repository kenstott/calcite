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
package org.apache.calcite.adapter.askamerica;

import org.apache.calcite.adapter.driver.BaseDriverWrapper;
import org.apache.calcite.adapter.govdata.GovDataDriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

/**
 * JDBC driver for the AskAmerica engine.
 *
 * <p>Accepts {@code jdbc:askamerica:} URLs and delegates to {@link GovDataDriver}.
 * {@code DatabaseMetaData.getURL()} returns the original {@code jdbc:askamerica:} URL,
 * {@code getDatabaseProductName()} returns {@code "AskAmerica"}, and
 * {@code getDriverName()} returns {@code "AskAmerica JDBC Driver"}.
 *
 * <p>Example URL: {@code jdbc:askamerica:source=geo,sec}
 *
 * <p>Registered via {@code META-INF/services/java.sql.Driver} for automatic
 * discovery by {@code java.sql.DriverManager}.
 */
public class AskAmericaDriver extends BaseDriverWrapper {

    private static final GovDataDriver INNER = new GovDataDriver();

    static {
        registerDriver(new AskAmericaDriver());
    }

    @Override protected String urlPrefix()   { return "jdbc:askamerica:"; }
    @Override protected String innerPrefix() { return "jdbc:govdata:"; }
    @Override protected String productName() { return "AskAmerica"; }
    @Override protected String driverName()  { return "AskAmerica JDBC Driver"; }
    @Override protected Driver innerDriver() { return INNER; }

    @Override public Connection connect(String url, Properties info) throws SQLException {
        // ASKAMERICA_DATA_DIR: env var takes priority, then system property (set by McpServer),
        // then fall back to ~/.askamerica. Always wins over any previously pinned value so that
        // a user-supplied env var is never silently ignored.
        String dataDir = System.getenv("ASKAMERICA_DATA_DIR");
        if (dataDir == null || dataDir.isEmpty()) {
            dataDir = System.getProperty("ASKAMERICA_DATA_DIR");
        }
        if (dataDir == null || dataDir.isEmpty()) {
            String home = System.getProperty("user.home");
            dataDir = (home != null && !home.isEmpty()) ? home + "/.askamerica" : null;
        }
        if (dataDir != null && !dataDir.isEmpty()) {
            System.setProperty("govdata.operating.dir.base", dataDir);
            if (System.getProperty("duckdb.cache_httpfs.directory") == null) {
                System.setProperty("duckdb.cache_httpfs.directory",
                    dataDir + "/.duckdb_httpfs_cache");
            }
            if (System.getProperty("duckdb.catalog.path") == null) {
                java.io.File duckdbDir = new java.io.File(dataDir, ".duckdb");
                if (!duckdbDir.exists()) {
                    duckdbDir.mkdirs();
                }
                System.setProperty("duckdb.catalog.path",
                    new java.io.File(duckdbDir, "catalog.duckdb").getAbsolutePath());
            }
        }
        // Meter at the Calcite/JDBC layer — the one point every client-compute path
        // (raw JDBC, Python, MCP) funnels through — so all are metered uniformly.
        return UsageMetering.wrap(super.connect(url, info), UsageMetering.resolveApiKey(info));
    }

}
