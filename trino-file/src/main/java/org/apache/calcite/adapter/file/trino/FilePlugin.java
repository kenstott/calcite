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
package org.apache.calcite.adapter.file.trino;

import io.trino.plugin.jdbc.JdbcPlugin;

/**
 * Trino plugin exposing files (CSV/JSON/Parquet/...) as a SQL catalog via the Calcite file adapter.
 * Wraps the generic Calcite connector with a file-specific connection factory and friendly catalog
 * properties.
 *
 * <p>The connector is named {@code file}; configure a catalog with {@code connector.name=file}
 * plus a {@code glob} (a local directory/path or an {@code s3://bucket/prefix} glob). S3
 * credentials default to the standard AWS environment variables and can be overridden with the
 * {@code aws.*} catalog properties (see {@link FileConfig}).
 */
public class FilePlugin
        extends JdbcPlugin
{
    public FilePlugin()
    {
        super("file", FileClientModule::new);
    }
}
