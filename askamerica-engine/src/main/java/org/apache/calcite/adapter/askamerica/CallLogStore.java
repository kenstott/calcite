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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The engine's tool-call log on disk, so a restart of the engine process does not erase it.
 *
 * <p>The report checks read the calls a conversation made (which queries returned rows, which
 * method tools were run) from an in-memory list. A restart or reconnect of the process emptied
 * it, and a report was then refused with "no query call anywhere this session" although the
 * conversation had run them. Each call is appended here as one JSON line; a new process reloads
 * the lines young enough to matter.
 *
 * <p>One file per parent process (Claude Desktop's pid), so a restarted server under the same app
 * finds its predecessor's history and a server of another app, or a later launch of the app, does
 * not. When the parent cannot be identified nothing is persisted. History older than the idle gap
 * the checks already treat as "a different conversation" is dropped on load.
 *
 * <p>Writing is best effort and never fails a tool call, but a failure is reported on stderr
 * (once per distinct cause), not swallowed.
 */
final class CallLogStore {

  private static final String PREFIX = "call-log-";
  private static final String SUFFIX = ".jsonl";

  private final Path file;
  private final ObjectMapper mapper;
  private final int maxEntries;
  private final long maxAgeMillis;
  private String lastFailure;
  private int failures;
  private int unreadable;

  CallLogStore(Path file, ObjectMapper mapper, int maxEntries, long maxAgeMillis) {
    this.file = file;
    this.mapper = mapper;
    this.maxEntries = maxEntries;
    this.maxAgeMillis = maxAgeMillis;
  }

  /** The log file for this process's parent under {@code dir}, or empty when the parent is unknown. */
  static Optional<Path> fileForParent(Path dir) {
    return ProcessHandle.current().parent()
        .map(parent -> dir.resolve(PREFIX + parent.pid() + SUFFIX));
  }

  /**
   * The entries to start from: those not older than the maximum age, newest {@code maxEntries},
   * oldest first. When that drops anything the file is rewritten without it, so it cannot grow
   * without bound across restarts.
   */
  synchronized List<ObjectNode> load(long nowMillis) {
    unreadable = 0;
    List<String> lines;
    try {
      if (!Files.exists(file)) {
        return new ArrayList<ObjectNode>();
      }
      lines = Files.readAllLines(file, StandardCharsets.UTF_8);
    } catch (IOException e) {
      fail("could not read " + file + ": " + e.getMessage());
      return new ArrayList<ObjectNode>();
    }
    List<ObjectNode> kept = new ArrayList<ObjectNode>();
    for (String line : lines) {
      if (line.trim().isEmpty()) {
        continue;
      }
      ObjectNode entry = parse(line);
      if (entry == null) {
        unreadable++;
        continue;
      }
      Long ts = millis(entry.path("ts"));
      if (ts == null || nowMillis - ts > maxAgeMillis) {
        continue;
      }
      kept.add(entry);
    }
    if (kept.size() > maxEntries) {
      kept = new ArrayList<ObjectNode>(kept.subList(kept.size() - maxEntries, kept.size()));
    }
    if (unreadable > 0) {
      System.err.println("[askamerica-mcp] call log " + file + ": ignored " + unreadable
          + " unreadable line(s)");
    }
    if (kept.size() < lines.size()) {
      rewrite(kept);
    }
    return kept;
  }

  /** Appends one call; never throws. */
  synchronized void append(ObjectNode entry) {
    try {
      Path parent = file.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      Files.write(file, (mapper.writeValueAsString(entry) + "\n").getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (IOException e) {
      fail("could not append to " + file + ": " + e.getMessage());
    }
  }

  /** Deletes the {@code call-log-*.jsonl} files under {@code dir} not modified for the maximum age. */
  static int deleteStale(Path dir, long nowMillis, long maxAgeMillis) {
    int deleted = 0;
    if (!Files.isDirectory(dir)) {
      return 0;
    }
    try (java.util.stream.Stream<Path> files = Files.list(dir)) {
      for (Path p : (Iterable<Path>) files::iterator) {
        String name = p.getFileName().toString();
        if (!name.startsWith(PREFIX) || !name.endsWith(SUFFIX)) {
          continue;
        }
        try {
          if (nowMillis - Files.getLastModifiedTime(p).toMillis() > maxAgeMillis) {
            Files.delete(p);
            deleted++;
          }
        } catch (IOException e) {
          System.err.println("[askamerica-mcp] could not remove old call log " + p + ": " + e.getMessage());
        }
      }
    } catch (IOException e) {
      System.err.println("[askamerica-mcp] could not list " + dir + ": " + e.getMessage());
    }
    return deleted;
  }

  /** How many writes or reads have failed, for tests and diagnostics. */
  synchronized int failures() {
    return failures;
  }

  /** How many lines of the last {@link #load} could not be parsed. */
  synchronized int unreadableLines() {
    return unreadable;
  }

  private void rewrite(List<ObjectNode> entries) {
    Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
    try {
      StringBuilder sb = new StringBuilder();
      for (ObjectNode e : entries) {
        sb.append(mapper.writeValueAsString(e)).append('\n');
      }
      Files.write(tmp, sb.toString().getBytes(StandardCharsets.UTF_8));
      Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      fail("could not rewrite " + file + ": " + e.getMessage());
    }
  }

  private ObjectNode parse(String line) {
    try {
      JsonNode n = mapper.readTree(line);
      return n instanceof ObjectNode ? (ObjectNode) n : null;
    } catch (IOException e) {
      return null;
    }
  }

  private static Long millis(JsonNode ts) {
    if (!ts.isTextual()) {
      return null;
    }
    try {
      return Instant.parse(ts.asText()).toEpochMilli();
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  private void fail(String message) {
    failures++;
    if (!message.equals(lastFailure)) {
      lastFailure = message;
      System.err.println("[askamerica-mcp] call history is not being persisted: " + message);
    }
  }
}
