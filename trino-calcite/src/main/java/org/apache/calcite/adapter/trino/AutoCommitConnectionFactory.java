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
package org.apache.calcite.adapter.trino;

import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.spi.connector.ConnectorSession;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Wraps a {@link ConnectionFactory} to present a non-transactional connection, which Trino's JDBC
 * write path requires but Calcite does not provide.
 *
 * <p>Calcite's JDBC connection reports {@code autoCommit=false} and throws
 * {@link UnsupportedOperationException} on {@code commit()}/{@code rollback()}. Trino, however,
 * verifies {@code autoCommit} is true before an INSERT and calls {@code commit()} when the writer
 * finishes. Since the backing adapters (e.g. SharePoint) apply each row immediately and have no
 * transactions, this proxy reports {@code autoCommit=true} and turns {@code setAutoCommit},
 * {@code commit} and {@code rollback} into no-ops. Reads are unaffected.
 */
public class AutoCommitConnectionFactory
        implements ConnectionFactory
{
    private final ConnectionFactory delegate;

    public AutoCommitConnectionFactory(ConnectionFactory delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Connection connection = delegate.openConnection(session);
        return (Connection) Proxy.newProxyInstance(
                AutoCommitConnectionFactory.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                new NonTransactionalHandler(connection));
    }

    @Override
    public void close()
            throws SQLException
    {
        delegate.close();
    }

    private static final class NonTransactionalHandler
            implements InvocationHandler
    {
        private final Connection connection;

        NonTransactionalHandler(Connection connection)
        {
            this.connection = connection;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable
        {
            switch (method.getName()) {
            case "getAutoCommit":
                return Boolean.TRUE;
            case "setAutoCommit":
            case "commit":
            case "rollback":
                return null;
            default:
                try {
                    return method.invoke(connection, args);
                }
                catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        }
    }
}
