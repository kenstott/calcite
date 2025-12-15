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

/**
 * ETL capabilities for the File adapter.
 *
 * <p>This package provides components for Extract-Transform-Load operations:
 *
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.etl.HiveParquetWriter} - Materializes data
 *       to hive-partitioned Parquet files using DuckDB</li>
 *   <li>{@link org.apache.calcite.adapter.file.etl.MaterializeConfig} - Configuration for
 *       materialization operations</li>
 *   <li>{@link org.apache.calcite.adapter.file.etl.DataSource} - Interface for data sources
 *       that can be materialized</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * StorageProvider storageProvider = StorageProviderFactory.createFromUrl("s3://bucket/");
 * HiveParquetWriter writer = new HiveParquetWriter(storageProvider, "/data/output");
 *
 * MaterializeConfig config = MaterializeConfig.builder()
 *     .output(MaterializeOutputConfig.builder()
 *         .location("/data/output")
 *         .format("parquet")
 *         .compression("snappy")
 *         .build())
 *     .partition(MaterializePartitionConfig.builder()
 *         .columns(Arrays.asList("year", "region"))
 *         .batchBy(Arrays.asList("year"))
 *         .build())
 *     .build();
 *
 * writer.materialize(config, dataSource);
 * }</pre>
 *
 * @see org.apache.calcite.adapter.file.partition.ParquetReorganizer
 */
package org.apache.calcite.adapter.file.etl;
