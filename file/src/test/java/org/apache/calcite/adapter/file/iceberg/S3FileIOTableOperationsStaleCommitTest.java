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
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A commit built on stale metadata must fail, not silently win.
 *
 * <p>This store has no compare-and-swap: {@code commit} writes {@code v{version+1}.metadata.json}
 * and then moves {@code version-hint.text} onto it. Without a stale-base check, a committer
 * holding metadata read before someone else's commit overwrites that commit's metadata and
 * reverts the table to the older snapshot. When the commit it reverted past was a compaction
 * whose superseded data files snapshot expiry has since reclaimed, the current snapshot ends up
 * referencing data files that no longer exist — a table that reads as corrupt.
 *
 * <p>Serializing commits does not prevent this: a lock makes the stale committer wait its turn
 * and then perform exactly the same overwrite. Only rejecting the stale base does, which is why
 * Iceberg's own {@code HadoopTableOperations} opens with the same check.
 */
@Tag("unit")
public class S3FileIOTableOperationsStaleCommitTest {

  private static final String LOCATION = "s3://bucket/schema/table";

  private static TableMetadata metadata() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(), LOCATION,
        Collections.<String, String>emptyMap());
  }

  /**
   * Installs table state without touching S3, so the guard can be exercised in isolation. The
   * guard runs before any I/O, which is what makes this checkable at unit speed.
   */
  private static S3FileIOTableOperations opsAt(TableMetadata current, int version)
      throws Exception {
    S3FileIOTableOperations ops = new S3FileIOTableOperations(LOCATION, new S3FileIO());
    for (String name : new String[] {"current", "version", "refreshed"}) {
      Field f = S3FileIOTableOperations.class.getDeclaredField(name);
      f.setAccessible(true);
      if ("current".equals(name)) {
        f.set(ops, current);
      } else if ("version".equals(name)) {
        f.setInt(ops, version);
      } else {
        f.setBoolean(ops, true);
      }
    }
    return ops;
  }

  /** The core case: committer holds metadata that is no longer the table's current metadata. */
  @Test void aCommitBuiltOnStaleMetadataIsRejected() throws Exception {
    TableMetadata live = metadata();
    TableMetadata stale = metadata();
    S3FileIOTableOperations ops = opsAt(live, 7);

    CommitFailedException e = assertThrows(CommitFailedException.class,
        () -> ops.commit(stale, metadata()));
    assertTrue(e.getMessage().toLowerCase(java.util.Locale.ROOT).contains("stale"),
        "the failure must name staleness so a retry is recognisable, got: " + e.getMessage());
  }

  /**
   * A null base means "creating this table". Against a table that already exists that is stale
   * by definition — honouring it would restart the version series at v0 and overwrite history.
   */
  @Test void aCreateAgainstAnExistingTableIsRejected() throws Exception {
    S3FileIOTableOperations ops = opsAt(metadata(), 7);
    assertThrows(CommitFailedException.class, () -> ops.commit(null, metadata()));
  }

  /**
   * The guard must not reject a legitimate commit. A matching base passes the stale check and
   * proceeds to the on-disk pointer check, which does reach S3 — so failing here is expected,
   * but it must not be the staleness rejection.
   */
  @Test void aCommitOnCurrentMetadataPassesTheStaleCheck() throws Exception {
    TableMetadata live = metadata();
    S3FileIOTableOperations ops = opsAt(live, 7);

    RuntimeException e =
        assertThrows(RuntimeException.class, () -> ops.commit(live, metadata()));
    assertTrue(!e.getMessage().toLowerCase(java.util.Locale.ROOT).contains("stale table metadata"),
        "a commit on current metadata must clear the stale-base guard, got: " + e.getMessage());
  }
}
