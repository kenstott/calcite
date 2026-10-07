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
package org.apache.calcite.adapter.salesforce.trino;

import io.trino.plugin.jdbc.JdbcPlugin;

/**
 * Trino plugin exposing a Salesforce org as a SQL catalog. Wraps the generic Calcite connector with
 * a Salesforce-specific connection factory and friendly catalog properties.
 *
 * <p>The connector is named {@code salesforce}; configure a catalog with
 * {@code connector.name=salesforce} plus {@code login-url} and credentials (see
 * {@link SalesforceConfig}).
 */
public class SalesforcePlugin
        extends JdbcPlugin
{
    public SalesforcePlugin()
    {
        super("salesforce", SalesforceClientModule::new);
    }
}
