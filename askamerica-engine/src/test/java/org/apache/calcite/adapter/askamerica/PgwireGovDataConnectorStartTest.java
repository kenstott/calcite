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
            PgwireGovDataConnector.awaitStart(() -> {
              starts.incrementAndGet();
              throw new IllegalStateException("the server exited with status 1");
            }, 60_000));
    assertEquals("the server exited with status 1", failure.getMessage());
    assertTrue(System.currentTimeMillis() - began < 10_000,
        "a failed start is reported without waiting out the call's bound");

    Connection up = connection();
    assertSame(up,
        PgwireGovDataConnector.awaitStart(() -> {
          starts.incrementAndGet();
          return up;
        }, 60_000));
    assertEquals(2, starts.get(), "the next call started the server again");
  }

  @Test void aStartStillRunningIsWaitedOnForABoundedTimeAndNeverBegunTwice() throws Exception {
    AtomicInteger starts = new AtomicInteger();
    CountDownLatch release = new CountDownLatch(1);
    Connection up = connection();
    java.util.concurrent.Callable<Connection> slow = () -> {
      starts.incrementAndGet();
      release.await();
      return up;
    };
    try {
      IllegalStateException first =
          assertThrows(IllegalStateException.class,
              () -> PgwireGovDataConnector.awaitStart(slow, 200));
      assertTrue(first.getMessage().contains("still starting"), first.getMessage());
      IllegalStateException second =
          assertThrows(IllegalStateException.class,
              () -> PgwireGovDataConnector.awaitStart(slow, 200));
      assertTrue(second.getMessage().contains("still starting"), second.getMessage());
      assertEquals(1, starts.get(), "the second call waited on the first call's start");
    } finally {
      release.countDown();
    }
    // Either this call joins the start just released, or that one has finished and this
    // begins one of its own; both return a connection.
    assertFalse(PgwireGovDataConnector.awaitStart(() -> up, 60_000) == null);
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
}
