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
 * <p><b>On by default.</b> See {@link #isEnabled()}'s own doc for why, and for the deliberate
 * absence of a silent fallback to the embedded path on a pgwire failure — set {@code
 * ASKAMERICA_PGWIRE_MODE=0} to opt back into embedded-only as an explicit operator choice.
 */
final class PgwireGovDataConnector {
  // Read dynamically, not cached: McpServer.log is null until McpServer.main() assigns it,
  // which runs after the class-init order a static final field capture would rely on.
  private static PrintStream log() {
    return McpServer.log != null ? McpServer.log : System.err;
  }

  private static final String DEFAULT_HOST = "127.0.0.1";
  // NOT 5433 — that's pgwire-calcite's own shared default, reused by every pgwire-* adapter
  // (file, splunk, sharepoint, cloudops, govdata alike). A machine that also runs one of the
  // others locally (e.g. pgwire-file for DataGrip) would have this connector silently attach
  // to the wrong catalog on 5433 instead of ever spawning pgwire-govdata itself — connect()
  // only checks that *some* valid PG connection answers, not which adapter it is. A distinct
  // default sidesteps the collision; verifyIsGovData() below is the backstop for the case
  // where an operator points ASKAMERICA_PGWIRE_PORT at a shared port anyway.
  private static final int DEFAULT_PORT = 45433;
  /** How long to wait for a direct connect attempt before assuming nothing is listening. */
  private static final int CONNECT_TIMEOUT_MILLIS = 2000;
  /**
   * How long to wait for a freshly spawned server to start accepting connections. Matches the
   * embedded path's own 600s latch wait (getSchemaConnection) rather than something shorter:
   * a spawn that can't reuse an already-seeded catalog (GOVDATA_DUCKDB_CATALOG unset, or a
   * genuinely first-ever install) cold-mounts all 26 schemas from S3/Iceberg metadata, which
   * takes minutes, not seconds — confirmed live (a 60s timeout here failed a real spawn that
   * was still actively mounting schemas, not stuck).
   */
  private static final int SPAWN_TIMEOUT_MILLIS = 600_000;
  private static final int SPAWN_POLL_INTERVAL_MILLIS = 500;
  /** Idle grace period passed to the spawned server (see pgwire-calcite's idle-shutdown watcher). */
  private static final String IDLE_SHUTDOWN_SECONDS = "120";
  /** Per-query timeout passed to the spawned server's --statement-timeout-ms (see spawnIfPossible). */
  private static final String STATEMENT_TIMEOUT_MS = "30000";

  private static final Object LOCK = new Object();
  private static volatile Connection sharedConnection;

  private PgwireGovDataConnector() {}

  /**
   * On by default (kenstott/calcite#364). The embedded per-process DuckDB engine this replaces
   * has no working default on a machine with more than one Claude Desktop install (standalone +
   * MSIX/Store) — every launch spawns two McpServer processes racing for the same catalog
   * write-lock, permanently duplicating disk/memory and paying a reseed cost on every single
   * start, not as a rare edge case. Set ASKAMERICA_PGWIRE_MODE=0 (or false) to opt back into the
   * embedded-only path, as a deliberate operator choice — getSchemaConnection() does NOT fall
   * back to it automatically on a pgwire failure. Two data-access paths that could each be
   * seeded from a different build is exactly the multiple-divergent-instance problem this
   * design exists to eliminate; a pgwire failure surfaces as a loud, clear error instead.
   */
  static boolean isEnabled() {
    return !falsy(System.getenv("ASKAMERICA_PGWIRE_MODE"))
        && !falsy(System.getProperty("ASKAMERICA_PGWIRE_MODE"));
  }

  private static boolean falsy(String v) {
    return v != null && (v.equals("0") || v.equalsIgnoreCase("false"));
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
    // Substitution itself is safe, and logged below (not silent): an operator-set port
    // typo should be noticed, not silently substituted.
    // fallback-guard: allow -- see log line below, this is the noticed case
    } catch (NumberFormatException e) {
      log().println("[askamerica-mcp] ASKAMERICA_PGWIRE_PORT=\"" + p
          + "\" is not a valid port number — using the default (" + DEFAULT_PORT + ") instead.");
      return DEFAULT_PORT;
    }
  }

  /** Human-readable target for diagnostics — never includes credentials. */
  static String describeTarget() {
    return host() + ":" + port();
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
    // null means "nothing listening," never confused with a real connection; the caller's
    // `if (c != null)` check is exactly this distinction.
    // fallback-guard: allow -- null is the documented "nothing listening" sentinel
    } catch (Exception e) {
      return null;
    }
    try {
      Properties props = new Properties();
      props.setProperty("user", "askamerica");
      props.setProperty("connectTimeout", String.valueOf(CONNECT_TIMEOUT_MILLIS / 1000));
      // Bounds EVERY query on this connection, not just the identity check below — a shared
      // server that wedges mid-query (seen live: information_schema.schemata queries hang
      // indefinitely against pgwire-calcite, no error, no response) must not silently freeze
      // whatever MCP tool call is waiting on it forever.
      props.setProperty("socketTimeout", "30");
      Connection c = DriverManager.getConnection(
          "jdbc:postgresql://" + host() + ":" + port() + "/govdata", props);
      if (!verifyIsGovData(c)) {
        log().println("[askamerica-mcp] Something is listening on " + host() + ":" + port()
            + " but it isn't pgwire-govdata (sec.filing_metadata not queryable) — treating the "
            + "port as unavailable rather than using the wrong catalog. Point "
            + "ASKAMERICA_PGWIRE_PORT at a free port for this connector, or free this one.");
        closeQuietly(c);
        return null;
      }
      return c;
    // Logged below, and null means "not connected," never confused with a real connection.
    // fallback-guard: allow -- see log line below
    } catch (SQLException e) {
      log().println("[askamerica-mcp] pgwire-govdata port is open but JDBC connect failed: "
          + e.getMessage());
      return null;
    }
  }

  /**
   * Backstop against ASKAMERICA_PGWIRE_PORT (or the default, if an operator ever changes it to
   * match pgwire-calcite's own shared 5433) pointing at a DIFFERENT pgwire-* adapter (file,
   * splunk, sharepoint, cloudops) that happens to answer on this port — the postgresql driver's
   * "/govdata" database name in the connect URL is only a label these lightweight servers
   * report back, not something they actually validate against the connecting client. Queries a
   * real table every govdata deployment always has, NOT information_schema (see the
   * socketTimeout comment above — schemata lookups hang against this server, so this is not
   * just a style choice).
   */
  private static boolean verifyIsGovData(Connection c) {
    try (java.sql.Statement st = c.createStatement();
         java.sql.ResultSet rs = st.executeQuery(
             "SELECT 1 FROM sec.filing_metadata LIMIT 1")) {
      return rs.next();
    // false ("not verified as govdata") is the safe direction to fail toward: the caller
    // discards this connection and spawns its own rather than trusting an unverified one.
    // Logged by the caller when this returns false.
    // fallback-guard: allow -- safe-direction sentinel, see comment above
    } catch (SQLException e) {
      return false;
    }
  }

  private static void closeQuietly(Connection c) {
    try {
      c.close();
    } catch (SQLException ignored) {
      // best-effort — the connection is being discarded either way
    }
  }

  /**
   * Spawns the bundled pgwire-govdata launcher as a detached background process, if one can be
   * located. Race-safe by construction: if two processes spawn simultaneously, only one can
   * actually bind the port — the loser's spawned server exits immediately on bind failure, and
   * both processes' poll loops converge on whichever one won. Silently does nothing (leaving the
   * caller's poll loop to time out) if no launcher can be found at all — {@code
   * getSchemaConnection()} then throws a clear error naming the failure. Deliberately NOT a
   * fallback to the embedded engine: see getSchemaConnection()'s doc for why a silent
   * second data-access path is exactly the problem this design exists to eliminate.
   */
  private static void spawnIfPossible() {
    File launcher = resolveLauncher();
    if (launcher == null) {
      // Not bundled with the installer for an older build, or a local dev run — lazily
      // download+extract the airgapped bundle on this first use of pgwire mode instead.
      // Returns null ONLY when there's genuinely nothing published for this OS; any real
      // failure (bad download, bad sha256, bad extraction) throws InstallFailedException
      // deliberately uncaught here, so it propagates straight through connect() instead of
      // being swallowed into a meaningless downstream timeout — confirmed live that a swallowed
      // sha256 mismatch cost hours to trace precisely because it looked like a generic hang.
      launcher = PgwireGovDataInstaller.ensureLauncher();
    }
    if (launcher == null) {
      log().println("[askamerica-mcp] No pgwire-govdata launcher available "
          + "(not bundled, and no matching release asset exists for this OS) — the "
          + "connection attempt will time out and fail.");
      return;
    }
    try {
      java.util.List<String> command = new java.util.ArrayList<>();
      // ProcessBuilder does not reliably run a .bat directly on every JDK/Windows
      // combination (CreateProcess needs an actual executable; whether a bare .bat path is
      // transparently resolved through cmd.exe is not something to depend on unverified) —
      // invoke it explicitly through cmd.exe /c, which is documented, unambiguous behavior.
      if (isWindows() && launcher.getName().toLowerCase(java.util.Locale.ROOT).endsWith(".bat")) {
        command.add("cmd.exe");
        command.add("/c");
      }
      command.add(launcher.getAbsolutePath());
      ProcessBuilder pb = new ProcessBuilder(command);
      pb.command().addAll(java.util.Arrays.asList(
          "--host", host(), "--port", String.valueOf(port()),
          // Server-wide default (state.py: statement_timeout_ms=0, i.e. unlimited, unless a
          // launcher flag sets it). Without this, one hung query — from any of the many
          // clients sharing this server, or from the server's own internal catalog
          // introspection — holds CalciteBackend's single execution lock forever and wedges
          // every other connected client too. Confirmed live: an information_schema query
          // that never returned left even an unrelated psql session hanging on a
          // previously-instant, unrelated query against the same server.
          "--statement-timeout-ms", STATEMENT_TIMEOUT_MS,
          // Must match the connProps set below in getSchemaConnection's embedded-mode init
          // thread, or query behavior silently differs between the two modes (#364's own
          // "must verify" list, confirmed live). --fun alone does NOT fix this: a bare
          // reserved word like YEAR used as a column name (SELECT ... year FROM crime.cde_reta)
          // fails to parse under Calcite's default core grammar regardless of --lex/--fun —
          // only swapping in babel's parser (SqlBabelParserImpl, confirmed present in
          // pgwire-govdata's bundled jars) actually accepts it, same reason the embedded engine
          // sets it. --jdbc-prop is pgwire-calcite's generic passthrough to the JDBC Properties
          // object (launcher.py: "--jdbc-prop", forwarded verbatim by calcite_backend.py).
          "--fun", "standard,postgresql,spatial,mssql,bigquery",
          "--jdbc-prop",
          "parserFactory=org.apache.calcite.sql.parser.babel.SqlBabelParserImpl#FACTORY"));
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
      // NOT Redirect.DISCARD: a spawn that fails immediately (bad credentials, a missing env
      // var, a packaging defect) would otherwise leave zero diagnostic trail whatsoever — the
      // caller just sees a generic "did not start accepting connections" timeout minutes
      // later. Confirmed live: with DISCARD, a spawn crashing instantly on a missing
      // GOVDATA_PARQUET_DIR looked identical to one that was still slowly mounting 26 schemas
      // — completely undiagnosable without re-running the launcher by hand. A rotating-by-size
      // convention isn't needed: PGWIRE_CALCITE_IDLE_SHUTDOWN_SECONDS means this process's
      // lifetime, and therefore its log, is naturally bounded.
      File logFile = new File(cacheDirLogPath());
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
      pb.redirectError(ProcessBuilder.Redirect.appendTo(logFile));
      // NOT Redirect.DISCARD here — that's a WRITE-only redirect (valid for output/error,
      // for a process's own writes to /dev/null); applying it to stdin threw
      // "IllegalArgumentException: Redirect invalid for reading: WRITE" and made every spawn
      // attempt fail, confirmed live. The spawned server never reads stdin at all, so simply
      // not touching redirectInput (default PIPE, left unread and unwritten) is correct.
      Process p = pb.start();
      log().println("[askamerica-mcp] Spawned pgwire-govdata (pid " + p.pid() + "): "
          + launcher.getAbsolutePath() + " — log: " + logFile);
    } catch (Exception e) {
      log().println("[askamerica-mcp] Failed to spawn pgwire-govdata: "
          + e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }

  /** Where a spawned server's stdout/stderr goes — beside the launcher itself, not under the
   * per-process data dir, since that path may not be resolved yet if a caller reaches
   * spawnIfPossible before McpServer's own ASKAMERICA_DATA_DIR init has run. */
  private static String cacheDirLogPath() {
    String home = System.getProperty("user.home", "");
    return new File(new File(home, ".askamerica"), "pgwire-govdata/spawn.log").getAbsolutePath();
  }

  private static File resolveLauncher() {
    String override = System.getenv("ASKAMERICA_PGWIRE_LAUNCHER");
    if (override != null && !override.isEmpty()) {
      File f = new File(override);
      return f.isFile() ? f : null;
    }
    // Bundled directly with the installer (McpServerLauncher sets this to
    // <installdir>/app/pgwire-govdata) — the common case once a release actually stages
    // one; checked before the lazy-download cache so an installer that already carries the
    // bundle never re-downloads it. Absent (property unset, or the directory not present —
    // a local dev build, or an older installer built before this existed) falls through.
    String bundled = System.getProperty("askamerica.bundled.pgwire.dir");
    if (bundled != null && !bundled.isEmpty()) {
      File f = launcherPathUnder(new File(bundled));
      if (f.isFile()) {
        return f;
      }
    }
    String home = System.getProperty("user.home");
    if (home == null || home.isEmpty()) {
      return null;
    }
    File f = launcherPathUnder(new File(new File(home, ".askamerica"), "pgwire-govdata"));
    return f.isFile() ? f : null;
  }

  /** {@code base/bin/pgwire-govdata} (or {@code .bat} on Windows — no compiled Windows
   * executable exists, see the CI-side "Stage adapter launcher (Windows)" step) under a
   * pgwire-govdata bundle root, whether that root is the bundled-with-installer copy or
   * the lazy-download cache — same internal layout either way. */
  private static File launcherPathUnder(File base) {
    String binName = isWindows() ? "pgwire-govdata.bat" : "pgwire-govdata";
    return new File(new File(base, "bin"), binName);
  }

  private static boolean isWindows() {
    return System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).contains("win");
  }
}
