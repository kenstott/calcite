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

import org.apache.calcite.adapter.askamerica.PgwireGovDataConnector.OccupantState;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * How the connector treats whatever holds the pgwire port when pgwire-govdata isn't answering:
 * a still-mounting server is waited for, a long-unresponsive one is killed, and a foreign
 * process is never touched. The holder is found by who LISTENs on the port, so an orphan the
 * pid file doesn't record is still identified.
 */
@Tag("unit")
class PgwireGovDataConnectorOccupantTest {
  private static final long BUDGET = 600_000;
  private static final Instant NOW = Instant.parse("2026-09-30T12:00:00Z");

  @Test void pgwireGovDataInsideItsBudgetIsStarting() {
    assertEquals(OccupantState.STARTING, PgwireGovDataConnector.occupantState(
        true, Optional.of(NOW.minusSeconds(240)), NOW, BUDGET));
  }

  @Test void pgwireGovDataPastItsBudgetIsWedged() {
    assertEquals(OccupantState.WEDGED, PgwireGovDataConnector.occupantState(
        true, Optional.of(NOW.minusSeconds(601)), NOW, BUDGET));
  }

  @Test void unknownStartTimeGetsAFullBudget() {
    assertEquals(OccupantState.STARTING,
        PgwireGovDataConnector.occupantState(true, Optional.empty(), NOW, BUDGET));
  }

  @Test void anythingElseIsForeignRegardlessOfAge() {
    assertEquals(OccupantState.FOREIGN, PgwireGovDataConnector.occupantState(
        false, Optional.of(NOW.minusSeconds(86_400)), NOW, BUDGET));
  }

  @DisabledOnOs(OS.WINDOWS)
  @Test void listenerPidIsThisProcessWhileItHoldsThePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      assertEquals(ProcessHandle.current().pid(),
          PgwireGovDataConnector.portListenerPid(s.getLocalPort()));
    }
  }

  @DisabledOnOs(OS.WINDOWS)
  @Test void freePortHasNoListener() throws Exception {
    int port;
    try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      port = s.getLocalPort();
    }
    assertNull(PgwireGovDataConnector.portListenerPid(port));
  }
}
