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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests that a table whose metadata location differs from its load path is refused. */
@Tag("unit")
class S3FileIOTablesLocationGuardTest {
  private static TableMetadata metadataAt(String location) {
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(), location,
        Collections.<String, String>emptyMap());
  }

  @Test void sameLocationAcrossS3Schemes() {
    assertDoesNotThrow(() -> S3FileIOTables.requireLocationUnder(
        "s3://dq-bucket/patents/t/", metadataAt("s3a://dq-bucket/patents/t")));
  }

  @Test void otherBucketIsRefused() {
    assertThrows(IllegalStateException.class, () -> S3FileIOTables.requireLocationUnder(
        "s3://dq-bucket/patents/t", metadataAt("s3a://prod-bucket/patents/t")));
  }
}
