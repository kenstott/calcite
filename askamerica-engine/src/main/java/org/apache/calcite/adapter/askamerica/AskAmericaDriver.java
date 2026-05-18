/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import org.apache.calcite.adapter.govdata.GovDataDriver;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver for the AskAmerica engine.
 *
 * <p>Accepts {@code jdbc:askamerica:} URLs and delegates to {@link GovDataDriver},
 * translating the URL prefix transparently. {@code DatabaseMetaData.getURL()}
 * returns the original {@code jdbc:askamerica:} URL.
 *
 * <p>Example URL: {@code jdbc:askamerica:source=geo,sec}
 *
 * <p>Registered via {@code META-INF/services/java.sql.Driver} for automatic
 * discovery by {@code java.sql.DriverManager}.
 */
public class AskAmericaDriver implements Driver {

    static final String AA_PREFIX  = "jdbc:askamerica:";
    static final String GOV_PREFIX = "jdbc:govdata:";

    private static final GovDataDriver DELEGATE = new GovDataDriver();

    static {
        try {
            java.sql.DriverManager.registerDriver(new AskAmericaDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        String govUrl = GOV_PREFIX + url.substring(AA_PREFIX.length());
        Connection inner = DELEGATE.connect(govUrl, info);
        if (inner == null) {
            throw new SQLException("GovDataDriver returned null for url: " + govUrl);
        }
        return wrapConnection(inner, url);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(AA_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return DELEGATE.getPropertyInfo(
            GOV_PREFIX + url.substring(AA_PREFIX.length()), info);
    }

    @Override
    public int getMajorVersion() {
        return DELEGATE.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return DELEGATE.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return DELEGATE.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return DELEGATE.getParentLogger();
    }

    // ── Connection proxy ──────────────────────────────────────────────────────

    private static Connection wrapConnection(final Connection inner, final String originalUrl) {
        return (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class[]{Connection.class},
            new InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    if ("getMetaData".equals(method.getName()) && (args == null || args.length == 0)) {
                        DatabaseMetaData innerMeta = inner.getMetaData();
                        return wrapMetaData(innerMeta, originalUrl);
                    }
                    try {
                        return method.invoke(inner, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
            }
        );
    }

    // ── DatabaseMetaData proxy ────────────────────────────────────────────────

    private static DatabaseMetaData wrapMetaData(
            final DatabaseMetaData inner, final String originalUrl) {
        return (DatabaseMetaData) Proxy.newProxyInstance(
            DatabaseMetaData.class.getClassLoader(),
            new Class[]{DatabaseMetaData.class},
            new InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    if ("getURL".equals(method.getName()) && (args == null || args.length == 0)) {
                        return originalUrl;
                    }
                    if ("getDatabaseProductName".equals(method.getName())
                            && (args == null || args.length == 0)) {
                        return "AskAmerica";
                    }
                    if ("getDriverName".equals(method.getName())
                            && (args == null || args.length == 0)) {
                        return "AskAmerica JDBC Driver";
                    }
                    try {
                        return method.invoke(inner, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
            }
        );
    }
}
