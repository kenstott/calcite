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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A commit writes {@code v{N+1}.metadata.json} and only then moves the version hint, so an
 * interrupted commit leaves a complete next-version file with the hint still at N. Every later
 * commit then fails writing that same file. {@code continuesFrom} decides whether such a file is a
 * commit that landed (adopt it) or something else (leave it alone and fail).
 */
@Tag("unit")
public class S3FileIOTableOperationsLeftoverVersionTest {

  @TempDir Path tmp;

  private Table create(String name) {
    return new HadoopTables(new Configuration()).create(
        new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())),
        PartitionSpec.unpartitioned(), tmp.resolve(name).toString());
  }

  private static void append(Table table, String file) {
    table.newAppend().appendFile(DataFiles.builder(table.spec())
        .withPath(table.location() + "/data/" + file).withFileSizeInBytes(10)
        .withRecordCount(1).withFormat(FileFormat.PARQUET).build()).commit();
  }

  private static TableMetadata metadata(Table table) {
    return ((HasTableOperations) table).operations().refresh();
  }

  @Test void aCommitThatAppendedOneSnapshotContinuesItsBase() {
    Table t = create("t");
    append(t, "a.parquet");
    TableMetadata base = metadata(t);
    append(t, "b.parquet");
    TableMetadata leftover = metadata(t);

    assertTrue(S3FileIOTableOperations.continuesFrom(base, leftover));
  }

  @Test void aCommitThatChangedOnlyPropertiesContinuesItsBase() {
    Table t = create("t");
    append(t, "a.parquet");
    TableMetadata base = metadata(t);
    t.updateProperties().set("k", "v").commit();
    TableMetadata leftover = metadata(t);

    assertTrue(S3FileIOTableOperations.continuesFrom(base, leftover));
  }

  @Test void aLeftoverTwoSnapshotsAheadIsNotAContinuation() {
    Table t = create("t");
    append(t, "a.parquet");
    TableMetadata base = metadata(t);
    append(t, "b.parquet");
    append(t, "c.parquet");
    TableMetadata leftover = metadata(t);

    assertFalse(S3FileIOTableOperations.continuesFrom(base, leftover));
  }

  @Test void anOlderLeftoverIsNotAContinuation() {
    Table t = create("t");
    append(t, "a.parquet");
    TableMetadata older = metadata(t);
    append(t, "b.parquet");
    TableMetadata base = metadata(t);

    assertFalse(S3FileIOTableOperations.continuesFrom(base, older));
  }

  @Test void anotherTablesMetadataIsNotAContinuation() {
    Table one = create("one");
    append(one, "a.parquet");
    Table other = create("other");
    append(other, "a.parquet");

    assertFalse(S3FileIOTableOperations.continuesFrom(metadata(one), metadata(other)));
  }
}
