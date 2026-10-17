/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a data call is told while the shared pgwire-govdata server is being started: a start
 * that failed is reported at once with the server's own output, a start still running is
 * waited on for a bounded time and never begun twice, and a failure is not remembered against
 * the next call.
 */
@Tag("unit")
class PgwireGovDataConnectorStartTest {

  /** This test's own starter: the connector's is shared by the whole process, and another
   *  test's real start in progress would be joined by these calls. */
  private final PgwireGovDataConnector.Starter starter = new PgwireGovDataConnector.Starter();

  private static void ignore(Connection c) {
  }

  private static Connection connection() {
    return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
          throw new UnsupportedOperationException(method.getName());
        });
  }

  @Test void aFailedStartIsReportedAtOnceAndNotRememberedAgainstTheNextCall() throws Exception {
    AtomicInteger starts = new AtomicInteger();
    long began = System.currentTimeMillis();
    IllegalStateException failure =
        assertThrows(IllegalStateException.class, () ->
            starter.await(() -> {
              starts.incrementAndGet();
              throw new IllegalStateException("the server exited with status 1");
            }, 60_000, PgwireGovDataConnectorStartTest::ignore));
    assertEquals("the server exited with status 1", failure.getMessage());
    assertTrue(System.currentTimeMillis() - began < 10_000,
        "a failed start is reported without waiting out the call's bound");

    Connection up = connection();
    assertSame(up,
        starter.await(() -> {
          starts.incrementAndGet();
          return up;
        }, 60_000, PgwireGovDataConnectorStartTest::ignore));
    assertEquals(2, starts.get(), "the next call started the server again");
  }

  @Test void aStartStillRunningIsWaitedOnForABoundedTimeAndNeverBegunTwice() throws Exception {
    AtomicInteger starts = new AtomicInteger();
    CountDownLatch begun = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Connection up = connection();
    java.util.concurrent.Callable<Connection> slow = () -> {
      starts.incrementAndGet();
      begun.countDown();
      release.await();
      return up;
    };
    try {
      IllegalStateException first =
          assertThrows(IllegalStateException.class,
              () -> starter.await(slow, 200, PgwireGovDataConnectorStartTest::ignore));
      assertTrue(first.getMessage().contains("has not answered"), first.getMessage());
      IllegalStateException second =
          assertThrows(IllegalStateException.class,
              () -> starter.await(slow, 200, PgwireGovDataConnectorStartTest::ignore));
      assertTrue(second.getMessage().contains("has not answered"), second.getMessage());
      // The start runs on a thread of its own, which a busy machine may not have scheduled
      // within the two calls' waits: count once it has begun.
      assertTrue(begun.await(60, java.util.concurrent.TimeUnit.SECONDS), "the start never began");
      assertEquals(1, starts.get(), "the second call waited on the first call's start");
    } finally {
      release.countDown();
    }
    // Either this call joins the start just released, or that one has finished and this
    // begins one of its own; both return a connection.
    assertFalse(starter.await(() -> up, 60_000, PgwireGovDataConnectorStartTest::ignore) == null);
  }

  @Test void failedStartErrorCarriesOnlyWhatThisSpawnPrinted(@TempDir Path dir) throws Exception {
    File log = dir.resolve("spawn.log").toFile();
    Files.write(log.toPath(), "an earlier spawn's output\n".getBytes(StandardCharsets.UTF_8));
    long start = log.length();
    Files.write(log.toPath(),
        "The system cannot find the path specified.\n".getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND);

    String tail = PgwireGovDataConnector.spawnLogTail(log, start);
    assertEquals("The system cannot find the path specified.", tail);

    String message = PgwireGovDataConnector.failedStartMessage(4242, 1, log, tail);
    assertTrue(message.contains("pgwire-govdata"), message);
    assertTrue(message.contains("pid 4242"), message);
    assertTrue(message.contains("status 1"), message);
    assertTrue(message.contains("The system cannot find the path specified."), message);
    assertTrue(message.contains(log.toString()), message);
  }

  @Test void failedStartErrorKeepsOnlyTheEndOfALongOutput(@TempDir Path dir) throws Exception {
    File log = dir.resolve("spawn.log").toFile();
    StringBuilder long1 = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      long1.append("line ").append(i).append('\n');
    }
    Files.write(log.toPath(), long1.toString().getBytes(StandardCharsets.UTF_8));

    String tail = PgwireGovDataConnector.spawnLogTail(log, 0);
    assertTrue(tail.length() <= 2000, "at most the last 2000 bytes");
    assertTrue(tail.endsWith("line 999"), tail);
  }

  @Test void aSpawnThatPrintedNothingSaysSo(@TempDir Path dir) throws Exception {
    File log = dir.resolve("spawn.log").toFile();
    Files.write(log.toPath(), new byte[0]);
    String message = PgwireGovDataConnector.failedStartMessage(7, 9009, log,
        PgwireGovDataConnector.spawnLogTail(log, 0));
    assertTrue(message.contains("It printed nothing."), message);
  }

  private static void writeBytes(File file, long size, char fill) throws IOException {
    byte[] bytes = new byte[(int) size];
    java.util.Arrays.fill(bytes, (byte) fill);
    Files.write(file.toPath(), bytes);
  }

  @Test void aSpawnLogWithinTheCapIsLeftAsItIs(@TempDir Path dir) throws Exception {
    File log = dir.resolve("spawn.log").toFile();
    writeBytes(log, PgwireGovDataConnector.SPAWN_LOG_MAX_BYTES, 'a');
    PgwireGovDataConnector.rotateSpawnLog(log);
    assertEquals(PgwireGovDataConnector.SPAWN_LOG_MAX_BYTES, log.length());
    assertFalse(new File(log.getPath() + ".1").exists());
  }

  @Test void aSpawnLogPastTheCapMovesToTheFirstBackupAndTheOldestIsDropped(@TempDir Path dir)
      throws Exception {
    File log = dir.resolve("spawn.log").toFile();
    writeBytes(log, PgwireGovDataConnector.SPAWN_LOG_MAX_BYTES + 1, 'n');
    for (int i = 1; i <= PgwireGovDataConnector.SPAWN_LOG_BACKUPS; i++) {
      Files.write(dir.resolve("spawn.log." + i), ("backup " + i).getBytes(StandardCharsets.UTF_8));
    }

    PgwireGovDataConnector.rotateSpawnLog(log);

    assertFalse(log.exists(), "the next spawn starts a new spawn.log");
    assertEquals(PgwireGovDataConnector.SPAWN_LOG_MAX_BYTES + 1,
        dir.resolve("spawn.log.1").toFile().length());
    assertEquals("backup 1", Files.readString(dir.resolve("spawn.log.2")));
    assertEquals("backup 2", Files.readString(dir.resolve("spawn.log.3")));
    assertFalse(dir.resolve("spawn.log.4").toFile().exists(), "backup 3 was the oldest");
  }
}
