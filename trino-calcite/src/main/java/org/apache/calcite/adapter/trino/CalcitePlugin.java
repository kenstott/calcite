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

import io.trino.plugin.jdbc.JdbcPlugin;

/**
 * Trino plugin that exposes any Apache Calcite model (and therefore any Calcite adapter -
 * File, GovData, SharePoint, ...) to Trino through the generic Calcite JDBC driver
 * ({@code jdbc:calcite:model=...}).
 *
 * <p>The connector is named {@code calcite}; configure a catalog with
 * {@code connector.name=calcite} and {@code connection-url=jdbc:calcite:model=/path/model.json}.
 */
public class CalcitePlugin
        extends JdbcPlugin
{
    public CalcitePlugin()
    {
        super("calcite", CalciteClientModule::new);
    }
}
