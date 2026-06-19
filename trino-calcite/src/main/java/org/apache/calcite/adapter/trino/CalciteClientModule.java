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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.credential.CredentialProvider;

/**
 * Guice module wiring the generic Calcite JDBC connector: it binds {@link CalciteClient} as the
 * {@link JdbcClient} and provides a {@link ConnectionFactory} backed by the Calcite JDBC driver.
 *
 * <p>The {@code connection-url} (a {@code jdbc:calcite:...} URL, typically
 * {@code jdbc:calcite:model=/path/model.json}) is read from {@link BaseJdbcConfig}.
 *
 * <p>Calcite's Avatica metadata reports {@code storesUpperCaseIdentifiers()==true}, but Calcite
 * matches identifiers case-sensitively exactly as declared in the model. Set
 * {@code case-insensitive-name-matching=true} on the catalog so Trino resolves names against the
 * actual remote names instead of upper-casing them.
 */
public class CalciteClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // BaseJdbcConfig (and thus connection-url) is bound by the framework's JdbcModule;
        // we only need to bind the client implementation here. (Case-insensitive name matching is
        // a catalog property - see the class javadoc - because the framework decides whether to
        // install the caching identifier mapping by reading that config at module-setup time.)
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class)
                .to(CalciteClient.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory connectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            OpenTelemetry openTelemetry)
    {
        return DriverConnectionFactory.builder(
                        new org.apache.calcite.jdbc.Driver(),
                        config.getConnectionUrl(),
                        credentialProvider)
                .setOpenTelemetry(openTelemetry)
                .build();
    }
}
