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

import io.trino.plugin.base.mapping.MappingConfig;

/**
 * Shared configuration guards for the Calcite-backed Trino connectors.
 */
public final class CalciteConnectorConfig
{
    private CalciteConnectorConfig() {}

    /**
     * Fail catalog creation unless {@code case-insensitive-name-matching=true} is set.
     *
     * <p>Calcite matches identifiers case-sensitively (as declared in the model, typically
     * lower-case) while its Avatica metadata reports {@code storesUpperCaseIdentifiers()==true}.
     * Without case-insensitive matching, Trino upper-cases requested names and resolves none of an
     * adapter's tables: {@code listTables()} still enumerates them, but {@code getTableHandle} fails
     * with {@code TABLE_NOT_FOUND} and {@code information_schema.columns} returns no user columns.
     *
     * <p>The flag cannot be defaulted in code because the JDBC framework reads it at module-setup
     * time (in {@code IdentifierMappingModule}) to decide whether to install the caching identifier
     * mapping. Adapter-backed connectors (SharePoint, CloudOps, File, Splunk) generate their own
     * lower-case names, so the flag is effectively mandatory and we fail fast with an actionable
     * message. The generic {@code calcite} connector lets the model author control casing, so it
     * documents the flag instead of requiring it.
     *
     * @param enabled value of {@code MappingConfig.isCaseInsensitiveNameMatching()} for the catalog
     * @param connector human-readable connector name for the error message (e.g. {@code "SharePoint"})
     */
    public static void requireCaseInsensitiveNameMatching(boolean enabled, String connector)
    {
        if (!enabled) {
            throw new IllegalStateException(
                    "The " + connector + " connector requires '"
                            + MappingConfig.CASE_INSENSITIVE_NAME_MATCHING + "=true' in the catalog "
                            + "properties. Calcite matches identifiers case-sensitively while reporting "
                            + "upper-case storage, so without it tables are listed but cannot be queried "
                            + "(DESCRIBE/SELECT fail with TABLE_NOT_FOUND and information_schema.columns "
                            + "is empty). Add '" + MappingConfig.CASE_INSENSITIVE_NAME_MATCHING
                            + "=true' to etc/catalog/<catalog>.properties and restart Trino.");
        }
    }
}
