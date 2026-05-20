/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without experimental written
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
        // AskAmerica JDBC connections use ~/.askamerica for both operating state and cache.
        // Set before delegating so GovDataDriver's own checks find them already set.
        if (System.getProperty("govdata.operating.dir.base") == null) {
            String home = System.getProperty("user.home");
            if (home != null && !home.isEmpty()) {
                System.setProperty("govdata.operating.dir.base", home + "/.askamerica");
            }
        }
        if (System.getProperty("duckdb.cache_httpfs.directory") == null) {
            String home = System.getProperty("user.home");
            if (home != null && !home.isEmpty()) {
                System.setProperty("duckdb.cache_httpfs.directory",
                    home + "/.askamerica/.duckdb_httpfs_cache");
            }
        }
        return super.connect(url, info);
    }
}
