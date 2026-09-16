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

import java.io.File;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Client side of the pgwire-govdata shared-server design (kenstott/calcite#364).
 *
 * <p>Every {@code askamerica-engine} process normally embeds its own DuckDB connection to the
 * shared govdata catalog file. DuckDB allows only one process to hold that file read-write at a
 * time, so concurrent processes — which happens on every single Claude Desktop launch when both
 * a standalone and an MSIX/Store install are present, not just when a user opens a second
 * conversation — either fail outright or fall back to disk-hungry numbered catalog copies (see
 * {@code DuckDBJdbcSchemaFactory}).
 *
 * <p>This connector makes that duplication avoidable: instead of embedding DuckDB, a process can
 * connect as a thin PostgreSQL-wire client to one shared {@code pgwire-govdata} server, spawning
 * it on demand if nothing is listening yet. All 26 govdata schemas are mounted under one shared
 * catalog on that server (see {@code pgwire-govdata/model.json}), so a single shared connection
 * serves every schema this engine's tools ask for — there is no per-schema connection to manage
 * on this side at all.
 *
 * <p><b>Opt-in, off by default.</b> Swapping the embedded engine for a thin pgwire client is a
 * fundamental deployment-shape change, and #364's own "must verify before writing code" list
 * (UDF/settings parity with the embedded connection, whether {@code updateSchema()}/
 * {@code setMemoryLimit()}'s {@code unwrap(CalciteConnection.class)} calls have any pgwire
 * equivalent, statistical-tool UDF reachability, behavior when the shared server crashes
 * mid-session) has not been validated yet. Set {@code ASKAMERICA_PGWIRE_MODE=1} to opt in;
 * unset, every process keeps embedding DuckDB exactly as before.
 */
final class PgwireGovDataConnector {
  // Read dynamically, not cached: McpServer.log is null until McpServer.main() assigns it,
  // which runs after the class-init order a static final field capture would rely on.
  private static PrintStream log() {
    return McpServer.log != null ? McpServer.log : System.err;
  }

  private static final String DEFAULT_HOST = "127.0.0.1";
  private static final int DEFAULT_PORT = 5433;
  /** How long to wait for a direct connect attempt before assuming nothing is listening. */
  private static final int CONNECT_TIMEOUT_MILLIS = 2000;
  /** How long to wait for a freshly spawned server to start accepting connections. */
  private static final int SPAWN_TIMEOUT_MILLIS = 60_000;
  private static final int SPAWN_POLL_INTERVAL_MILLIS = 500;
  /** Idle grace period passed to the spawned server (see pgwire-calcite's idle-shutdown watcher). */
  private static final String IDLE_SHUTDOWN_SECONDS = "120";

  private static final Object LOCK = new Object();
  private static volatile Connection sharedConnection;

  private PgwireGovDataConnector() {}

  static boolean isEnabled() {
    return truthy(System.getenv("ASKAMERICA_PGWIRE_MODE"))
        || truthy(System.getProperty("ASKAMERICA_PGWIRE_MODE"));
  }

  private static boolean truthy(String v) {
    return v != null && (v.equals("1") || v.equalsIgnoreCase("true"));
  }

  private static String host() {
    String h = System.getenv("ASKAMERICA_PGWIRE_HOST");
    return (h == null || h.isEmpty()) ? DEFAULT_HOST : h;
  }

  private static int port() {
    String p = System.getenv("ASKAMERICA_PGWIRE_PORT");
    if (p == null || p.isEmpty()) {
      return DEFAULT_PORT;
    }
    try {
      return Integer.parseInt(p.trim());
    } catch (NumberFormatException e) {
      return DEFAULT_PORT;
    }
  }

  /**
   * Returns the shared pgwire connection, spawning the server if nothing is listening yet.
   * Cached across calls (all 26 schemas share it); re-validated and reconnected if it has died.
   */
  static Connection getSharedConnection() throws Exception {
    Connection existing = sharedConnection;
    if (existing != null && !existing.isClosed() && existing.isValid(5)) {
      return existing;
    }
    synchronized (LOCK) {
      existing = sharedConnection;
      if (existing != null && !existing.isClosed() && existing.isValid(5)) {
        return existing;
      }
      Connection fresh = connect();
      sharedConnection = fresh;
      return fresh;
    }
  }

  private static Connection connect() throws Exception {
    Connection c = tryDirectConnect();
    if (c != null) {
      return c;
    }
    log().println("[askamerica-mcp] No pgwire-govdata server listening on " + host() + ":" + port()
        + " — attempting to spawn one.");
    spawnIfPossible();
    long deadline = System.currentTimeMillis() + SPAWN_TIMEOUT_MILLIS;
    while (System.currentTimeMillis() < deadline) {
      c = tryDirectConnect();
      if (c != null) {
        log().println("[askamerica-mcp] Connected to pgwire-govdata after spawn.");
        return c;
      }
      Thread.sleep(SPAWN_POLL_INTERVAL_MILLIS);
    }
    throw new IllegalStateException(
        "pgwire-govdata did not start accepting connections on " + host() + ":" + port()
        + " within " + (SPAWN_TIMEOUT_MILLIS / 1000) + "s");
  }

  private static Connection tryDirectConnect() {
    // A raw socket probe first: DriverManager.getConnection's own timeout handling for a
    // straight ECONNREFUSED varies by platform/driver version, and this needs to fail fast
    // and uniformly to know whether to spawn.
    try (Socket probe = new Socket()) {
      probe.connect(new InetSocketAddress(host(), port()), CONNECT_TIMEOUT_MILLIS);
    } catch (Exception e) {
      return null;
    }
    try {
      Properties props = new Properties();
      props.setProperty("user", "askamerica");
      props.setProperty("connectTimeout", String.valueOf(CONNECT_TIMEOUT_MILLIS / 1000));
      return DriverManager.getConnection(
          "jdbc:postgresql://" + host() + ":" + port() + "/govdata", props);
    } catch (SQLException e) {
      log().println("[askamerica-mcp] pgwire-govdata port is open but JDBC connect failed: "
          + e.getMessage());
      return null;
    }
  }

  /**
   * Spawns the bundled pgwire-govdata launcher as a detached background process, if one can be
   * located. Race-safe by construction: if two processes spawn simultaneously, only one can
   * actually bind the port — the loser's spawned server exits immediately on bind failure, and
   * both processes' poll loops converge on whichever one won. Silently does nothing (leaving the
   * caller's poll loop to time out and the whole connector call to throw) if no bundled launcher
   * can be found — that failure is expected to be caught by {@code McpServer} and treated as
   * "fall back to the embedded engine," not a hard error, since not every install has the
   * pgwire-govdata bundle co-installed yet.
   */
  private static void spawnIfPossible() {
    File launcher = resolveLauncher();
    if (launcher == null) {
      log().println("[askamerica-mcp] No bundled pgwire-govdata launcher found; "
          + "will fall back to the embedded engine.");
      return;
    }
    try {
      ProcessBuilder pb = new ProcessBuilder(launcher.getAbsolutePath(),
          "--host", host(), "--port", String.valueOf(port()));
      pb.environment().put("PGWIRE_CALCITE_IDLE_SHUTDOWN_SECONDS", IDLE_SHUTDOWN_SECONDS);
      // Reuse the exact catalog file this engine's own embedded mode already seeds/maintains
      // (~/.mcp_askamerica/.duckdb/govdata.duckdb by default — see McpServer's ASKAMERICA_DATA_DIR
      // resolution) rather than letting the spawned server default to its own relative path and
      // pay for a redundant seed extraction.
      String dataDir = System.getProperty("ASKAMERICA_DATA_DIR");
      if (dataDir != null && !dataDir.isEmpty()) {
        pb.environment().put("GOVDATA_DUCKDB_CATALOG",
            new File(new File(dataDir, ".duckdb"), "govdata.duckdb").getAbsolutePath());
      }
      pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
      pb.redirectError(ProcessBuilder.Redirect.DISCARD);
      pb.redirectInput(ProcessBuilder.Redirect.DISCARD);
      Process p = pb.start();
      log().println("[askamerica-mcp] Spawned pgwire-govdata (pid " + p.pid() + "): "
          + launcher.getAbsolutePath());
    } catch (Exception e) {
      log().println("[askamerica-mcp] Failed to spawn pgwire-govdata: "
          + e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }

  private static File resolveLauncher() {
    String override = System.getenv("ASKAMERICA_PGWIRE_LAUNCHER");
    if (override != null && !override.isEmpty()) {
      File f = new File(override);
      return f.isFile() ? f : null;
    }
    String home = System.getProperty("user.home");
    if (home == null || home.isEmpty()) {
      return null;
    }
    boolean windows = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT)
        .contains("win");
    String binName = windows ? "pgwire-govdata.exe" : "pgwire-govdata";
    File f = new File(new File(new File(home, ".askamerica"), "pgwire-govdata/bin"), binName);
    return f.isFile() ? f : null;
  }
}
