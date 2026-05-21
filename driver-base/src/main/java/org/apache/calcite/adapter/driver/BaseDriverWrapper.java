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
package org.apache.calcite.adapter.driver;

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
 * Base class for all custom Calcite JDBC driver wrappers.
 *
 * <p>Subclasses declare:
 * <ul>
 *   <li>{@link #urlPrefix()} — the {@code jdbc:xxx:} prefix this driver accepts</li>
 *   <li>{@link #innerPrefix()} — the inner driver's prefix to translate to</li>
 *   <li>{@link #productName()} — value returned by {@code getDatabaseProductName()}</li>
 *   <li>{@link #driverName()} — value returned by {@code getDriverName()}</li>
 *   <li>{@link #innerDriver()} — the delegate {@link Driver} instance</li>
 * </ul>
 *
 * <p>The wrapper transparently:
 * <ul>
 *   <li>Translates {@code jdbc:xxx:} → inner prefix before connecting</li>
 *   <li>Patches {@code DatabaseMetaData.getURL()} to return the original URL</li>
 *   <li>Patches {@code getDatabaseProductName()} and {@code getDriverName()}</li>
 *   <li>Ensures all schema/table/column metadata names are lowercase (PG-like)</li>
 * </ul>
 *
 * <p>Example subclass:
 * <pre>{@code
 * public class MyDriver extends BaseDriverWrapper {
 *   private static final MyInnerDriver INNER = new MyInnerDriver();
 *   static { registerDriver(new MyDriver()); }
 *
 *   protected String urlPrefix()    { return "jdbc:myproduct:"; }
 *   protected String innerPrefix()  { return "jdbc:calcite:"; }
 *   protected String productName()  { return "MyProduct"; }
 *   protected String driverName()   { return "MyProduct JDBC Driver"; }
 *   protected Driver innerDriver()  { return INNER; }
 * }
 * }</pre>
 */
public abstract class BaseDriverWrapper implements Driver {

    /** The {@code jdbc:xxx:} URL prefix this driver accepts. */
    protected abstract String urlPrefix();

    /** The inner driver's URL prefix to translate to. */
    protected abstract String innerPrefix();

    /** Value for {@code DatabaseMetaData.getDatabaseProductName()}. */
    protected abstract String productName();

    /** Value for {@code DatabaseMetaData.getDriverName()}. */
    protected abstract String driverName();

    /** The delegate driver that does the actual connection work. */
    protected abstract Driver innerDriver();

    // ── Driver interface ──────────────────────────────────────────────────────

    @Override public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        String innerUrl = innerPrefix() + url.substring(urlPrefix().length());
        Connection inner = innerDriver().connect(innerUrl, info);
        if (inner == null) {
            throw new SQLException(
                innerDriver().getClass().getSimpleName()
                + " returned null for url: " + innerUrl);
        }
        return wrapConnection(inner, url);
    }

    @Override public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(urlPrefix());
    }

    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return innerDriver().getPropertyInfo(
            innerPrefix() + url.substring(urlPrefix().length()), info);
    }

    @Override public int getMajorVersion() {
        return innerDriver().getMajorVersion();
    }

    @Override public int getMinorVersion() {
        return innerDriver().getMinorVersion();
    }

    @Override public boolean jdbcCompliant() {
        return innerDriver().jdbcCompliant();
    }

    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return innerDriver().getParentLogger();
    }

    // ── Registration helper ───────────────────────────────────────────────────

    /**
     * Registers this driver with {@link java.sql.DriverManager}.
     * Call from a {@code static} initializer in each concrete subclass.
     */
    protected static void registerDriver(Driver driver) {
        try {
            java.sql.DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ── Connection proxy ──────────────────────────────────────────────────────

    private Connection wrapConnection(final Connection inner, final String originalUrl) {
        return (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class[]{Connection.class},
            new InvocationHandler() {
                @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    if ("getMetaData".equals(method.getName())
                            && (args == null || args.length == 0)) {
                        return wrapMetaData(inner.getMetaData(), originalUrl);
                    }
                    try {
                        return method.invoke(inner, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
            });
    }

    // ── DatabaseMetaData proxy ────────────────────────────────────────────────

    private DatabaseMetaData wrapMetaData(
            final DatabaseMetaData inner, final String originalUrl) {
        final String product = productName();
        final String driver  = driverName();
        return (DatabaseMetaData) Proxy.newProxyInstance(
            DatabaseMetaData.class.getClassLoader(),
            new Class[]{DatabaseMetaData.class},
            new InvocationHandler() {
                @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    String m = method.getName();
                    boolean noArgs = (args == null || args.length == 0);
                    if (noArgs && "getURL".equals(m))                  return originalUrl;
                    if (noArgs && "getDatabaseProductName".equals(m))  return product;
                    if (noArgs && "getDriverName".equals(m))           return driver;
                    try {
                        return method.invoke(inner, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
            });
    }
}
