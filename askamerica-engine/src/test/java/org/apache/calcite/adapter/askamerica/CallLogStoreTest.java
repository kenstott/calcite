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
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The persisted tool-call log survives a restart, ages out, and a failing write never throws. */
@Tag("unit")
class CallLogStoreTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final long NOW = Instant.parse("2026-10-15T12:00:00Z").toEpochMilli();
  private static final long TEN_MINUTES = 10 * 60 * 1000L;

  private static ObjectNode call(String tool, int seq, long agoMillis) {
    ObjectNode e = MAPPER.createObjectNode();
    e.put("tool", tool);
    e.put("seq", seq);
    e.put("ts", Instant.ofEpochMilli(NOW - agoMillis).toString());
    return e;
  }

  private static CallLogStore store(Path file, int max) {
    return new CallLogStore(file, MAPPER, max, TEN_MINUTES);
  }

  @Test void aRestartReloadsWhatTheEarlierProcessRecorded(@TempDir Path dir) {
    Path file = dir.resolve("log.jsonl");
    CallLogStore before = store(file, 100);
    before.append(call("search_catalog", 1, 5 * 60_000L));
    before.append(call("query", 2, 4 * 60_000L));
    before.append(call("query", 3, 3 * 60_000L));
    List<ObjectNode> after = store(file, 100).load(NOW);
    assertEquals(3, after.size());
    assertEquals("search_catalog", after.get(0).get("tool").asText());
    assertEquals(3, after.get(2).get("seq").asInt());
  }

  @Test void entriesOlderThanTheIdleGapAreDroppedAndTheFileShrinks(@TempDir Path dir) throws IOException {
    Path file = dir.resolve("log.jsonl");
    CallLogStore s = store(file, 100);
    s.append(call("query", 1, 60 * 60_000L));
    s.append(call("query", 2, 30 * 60_000L));
    s.append(call("query", 3, 2 * 60_000L));
    List<ObjectNode> kept = store(file, 100).load(NOW);
    assertEquals(1, kept.size());
    assertEquals(3, kept.get(0).get("seq").asInt());
    assertEquals(1, Files.readAllLines(file, StandardCharsets.UTF_8).size());
  }

  @Test void onlyTheNewestMaxEntriesAreKept(@TempDir Path dir) {
    Path file = dir.resolve("log.jsonl");
    CallLogStore s = store(file, 2);
    for (int i = 1; i <= 5; i++) {
      s.append(call("query", i, (6 - i) * 60_000L));
    }
    List<ObjectNode> kept = store(file, 2).load(NOW);
    assertEquals(2, kept.size());
    assertEquals(4, kept.get(0).get("seq").asInt());
    assertEquals(5, kept.get(1).get("seq").asInt());
  }

  @Test void anUnreadableLineIsCountedAndTheRestKept(@TempDir Path dir) throws IOException {
    Path file = dir.resolve("log.jsonl");
    CallLogStore s = store(file, 100);
    s.append(call("query", 1, 60_000L));
    Files.write(file, "{not json\n".getBytes(StandardCharsets.UTF_8),
        java.nio.file.StandardOpenOption.APPEND);
    s.append(call("query", 2, 30_000L));
    CallLogStore reload = store(file, 100);
    List<ObjectNode> kept = reload.load(NOW);
    assertEquals(2, kept.size());
    assertEquals(1, reload.unreadableLines());
  }

  @Test void aMissingFileLoadsEmpty(@TempDir Path dir) {
    assertTrue(store(dir.resolve("none.jsonl"), 10).load(NOW).isEmpty());
  }

  @Test void aWriteThatCannotHappenIsReportedAndNeverThrows(@TempDir Path dir) throws IOException {
    Path notADirectory = Files.write(dir.resolve("file"), new byte[] {1});
    CallLogStore s = store(notADirectory.resolve("log.jsonl"), 10);
    s.append(call("query", 1, 1000L));
    s.append(call("query", 2, 1000L));
    assertEquals(2, s.failures());
  }

  @Test void oldFilesAreRemovedAndRecentOnesKept(@TempDir Path dir) throws IOException {
    Path old = Files.write(dir.resolve("call-log-111.jsonl"), new byte[] {1});
    Path recent = Files.write(dir.resolve("call-log-222.jsonl"), new byte[] {1});
    Path other = Files.write(dir.resolve("unrelated.txt"), new byte[] {1});
    Files.setLastModifiedTime(old, FileTime.fromMillis(NOW - 30L * 24 * 3600_000));
    Files.setLastModifiedTime(other, FileTime.fromMillis(NOW - 30L * 24 * 3600_000));
    Files.setLastModifiedTime(recent, FileTime.fromMillis(NOW - 3600_000));
    assertEquals(1, CallLogStore.deleteStale(dir, NOW, 7L * 24 * 3600_000));
    assertFalse(Files.exists(old));
    assertTrue(Files.exists(recent));
    assertTrue(Files.exists(other));
  }

  @Test void theFileIsNamedAfterTheParentProcess(@TempDir Path dir) {
    java.util.Optional<Path> f = CallLogStore.fileForParent(dir);
    if (f.isPresent()) {
      assertTrue(f.get().getFileName().toString().matches("call-log-\\d+\\.jsonl"), f.get().toString());
    }
  }
}
