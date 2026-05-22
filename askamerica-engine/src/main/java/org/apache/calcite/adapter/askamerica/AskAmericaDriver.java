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
        // Resolve operating dir: ASKAMERICA_DATA_DIR env var, or ~/.askamerica.
        // Set system properties before delegating so GovDataDriver finds them already set.
        if (System.getProperty("govdata.operating.dir.base") == null) {
            String base = resolveAskAmericaDataDir();
            if (base != null) {
                System.setProperty("govdata.operating.dir.base", base);
                if (System.getProperty("duckdb.cache_httpfs.directory") == null) {
                    System.setProperty("duckdb.cache_httpfs.directory",
                        base + "/.duckdb_httpfs_cache");
                }
            }
        }
        return super.connect(url, info);
    }

    private static String resolveAskAmericaDataDir() {
        String envDir = System.getenv("ASKAMERICA_DATA_DIR");
        if (envDir != null && !envDir.isEmpty()) {
            return envDir;
        }
        String home = System.getProperty("user.home");
        if (home != null && !home.isEmpty()) {
            return home + "/.askamerica";
        }
        return null;
    }
}
