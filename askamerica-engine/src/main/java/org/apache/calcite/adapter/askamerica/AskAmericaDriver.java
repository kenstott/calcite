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
        // Checked first, before any side effect: DriverManager tries every registered driver's
        // connect() against every URL, including the plain jdbc:duckdb: connections opened deep
        // inside schema creation for an unrelated jdbc:govdata: connection. Pinning the operating
        // dir unconditionally here meant those unrelated connections silently reset
        // govdata.operating.dir.base mid-flight, after a govdata connection had already baked the
        // old value into its generated model's database_filename — the model kept using the old
        // catalog path while every property read afterward (including this server's own startup
        // log line) reported the new one.
        if (!acceptsURL(url)) {
            return null;
        }
        pinOperatingDir(resolveDataDir());
        // Meter at the Calcite/JDBC layer — the one point every client-compute path
        // (raw JDBC, Python, MCP) funnels through — so all are metered uniformly.
        return UsageMetering.wrap(super.connect(url, info), UsageMetering.resolveApiKey(info));
    }

    /**
     * ASKAMERICA_DATA_DIR: env var takes priority, then system property (set by McpServer),
     * then fall back to ~/.askamerica.
     */
    private static String resolveDataDir() {
        String dataDir = System.getenv("ASKAMERICA_DATA_DIR");
        if (dataDir == null || dataDir.isEmpty()) {
            dataDir = System.getProperty("ASKAMERICA_DATA_DIR");
        }
        if (dataDir == null || dataDir.isEmpty()) {
            String home = System.getProperty("user.home");
            dataDir = (home != null && !home.isEmpty()) ? home + "/.askamerica" : null;
        }
        return dataDir;
    }

    /**
     * Pins the govdata operating directory and its DuckDB httpfs cache to {@code dataDir}.
     * Always wins over any previously pinned value so a user-supplied data dir is never silently
     * ignored.
     *
     * <p>Package-private and called directly by {@link McpServer} too: the MCP server connects
     * govdata straight through {@code GovDataDriver}, never through {@code jdbc:askamerica:}, so
     * it cannot rely on {@link #connect} above to run this for it.
     */
    static void pinOperatingDir(String dataDir) {
        if (dataDir == null || dataDir.isEmpty()) {
            return;
        }
        System.setProperty("govdata.operating.dir.base", dataDir);
        if (System.getProperty("duckdb.cache_httpfs.directory") == null) {
            System.setProperty("duckdb.cache_httpfs.directory", dataDir + "/.duckdb_httpfs_cache");
        }
    }

}
