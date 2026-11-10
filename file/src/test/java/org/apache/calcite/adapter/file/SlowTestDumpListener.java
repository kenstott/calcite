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
package org.apache.calcite.adapter.file;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Diagnostic only: writes a full thread dump to a file under {@code file.test.dumpDir}
 * (flushed and closed at once, so it survives a hung or killed job) and to stderr, once, for every test that has been
 * running longer than {@code file.test.slowDumpMinutes} (default 5). It never interrupts or
 * fails a test. A CI job that later dies silently (issue #1112) then has the stacks of what
 * was running in its log.
 */
public class SlowTestDumpListener implements TestExecutionListener {
  private static final long LIMIT_MS =
      Long.getLong("file.test.slowDumpMinutes", 5L) * 60_000L;
  private static final long POLL_MS = Math.max(1_000L, Math.min(30_000L, LIMIT_MS / 2));

  private static final String DUMP_DIR = System.getProperty("file.test.dumpDir");

  private final Map<String, Long> running = new ConcurrentHashMap<>();
  private final Map<String, Boolean> dumped = new ConcurrentHashMap<>();
  private volatile Thread watchdog;

  @Override public synchronized void testPlanExecutionStarted(
      org.junit.platform.launcher.TestPlan testPlan) {
    if (watchdog != null) {
      return;
    }
    Thread t = new Thread(this::watch, "slow-test-dump-watchdog");
    t.setDaemon(true);
    watchdog = t;
    t.start();
  }

  @Override public void executionStarted(TestIdentifier id) {
    if (id.isTest()) {
      running.put(id.getDisplayName() + " [" + id.getUniqueId() + "]", System.currentTimeMillis());
    }
  }

  @Override public void executionFinished(TestIdentifier id, TestExecutionResult result) {
    if (id.isTest()) {
      String key = id.getDisplayName() + " [" + id.getUniqueId() + "]";
      running.remove(key);
      dumped.remove(key);
    }
  }

  private void watch() {
    try {
      while (true) {
        Thread.sleep(POLL_MS);
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> e : running.entrySet()) {
          if (now - e.getValue() > LIMIT_MS && dumped.put(e.getKey(), Boolean.TRUE) == null) {
            dump(e.getKey(), now - e.getValue());
          }
        }
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  private static void dump(String test, long ms) {
    StringBuilder sb = new StringBuilder();
    sb.append("=== SLOW TEST (").append(ms / 1000).append("s): ").append(test)
        .append("\n    heap used/max MB: ")
        .append((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20)
        .append('/').append(Runtime.getRuntime().maxMemory() >> 20).append('\n');
    for (ThreadInfo ti : ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)) {
      sb.append(ti);
    }
    System.err.println(sb);
    System.err.flush();
    if (DUMP_DIR != null) {
      writeFile(sb.toString());
    }
  }

  /** Writes the dump to its own file; the name carries a timestamp so dumps never overwrite. */
  private static void writeFile(String text) {
    try {
      Path dir = Paths.get(DUMP_DIR);
      Files.createDirectories(dir);
      Path f = dir.resolve("slow-test-" + System.currentTimeMillis() + ".txt");
      Files.write(f, text.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW);
    } catch (IOException e) {
      System.err.println("SlowTestDumpListener could not write dump file: " + e);
      System.err.flush();
    }
  }
}
