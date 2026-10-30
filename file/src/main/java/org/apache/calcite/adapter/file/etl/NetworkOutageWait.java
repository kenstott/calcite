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
package org.apache.calcite.adapter.file.etl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.UnresolvedAddressException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Waits out a short loss of the host's network instead of failing the caller in seconds.
 *
 * <p>Only a request that never reached a server is an outage: a name-resolution failure, a
 * refused or unroutable connection. An HTTP status (404, 429, 5xx) is the server's answer and is
 * never waited on here. While the network is down the action is retried with a 5 s backoff that
 * doubles up to 60 s, for at most {@link #maxWaitMs()} in total, then fails with the elapsed
 * time. The start and the end of a wait are each logged once.
 *
 * <p>The limit is one JVM-wide setting, taken from the model's {@code networkOutageWaitMinutes}
 * operand ({@link #configureMinutes}); 0 disables waiting.
 */
public final class NetworkOutageWait {

  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkOutageWait.class);

  static final long DEFAULT_MAX_WAIT_MS = 25L * 60_000L;
  static final long FIRST_BACKOFF_MS = 5_000L;
  static final long MAX_BACKOFF_MS = 60_000L;

  private static final AtomicLong MAX_WAIT_MS = new AtomicLong(DEFAULT_MAX_WAIT_MS);
  private static volatile long firstBackoffMs = FIRST_BACKOFF_MS;
  private static volatile long maxBackoffMs = MAX_BACKOFF_MS;

  private NetworkOutageWait() {
  }

  /** An I/O action that may be retried after a network outage. */
  public interface IoAction<T> {
    T run() throws IOException;
  }

  /** Sets the total wait limit in minutes; 0 disables waiting. */
  public static void configureMinutes(long minutes) {
    if (minutes < 0) {
      throw new IllegalArgumentException("networkOutageWaitMinutes must be >= 0: " + minutes);
    }
    MAX_WAIT_MS.set(minutes * 60_000L);
  }

  /** Visible for testing: sets the limit and backoff steps in milliseconds. */
  static void configureMillis(long maxWaitMs, long firstBackoff, long maxBackoff) {
    MAX_WAIT_MS.set(maxWaitMs);
    firstBackoffMs = firstBackoff;
    maxBackoffMs = maxBackoff;
  }

  /** Visible for testing: restores the defaults. */
  static void reset() {
    configureMillis(DEFAULT_MAX_WAIT_MS, FIRST_BACKOFF_MS, MAX_BACKOFF_MS);
  }

  public static long maxWaitMs() {
    return MAX_WAIT_MS.get();
  }

  /**
   * True when {@code e}, or a cause, shows the request never reached a server: the host name did
   * not resolve, or the connection was refused or had no route.
   */
  public static boolean isOutage(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof UnknownHostException || t instanceof UnresolvedAddressException
          || t instanceof NoRouteToHostException) {
        return true;
      }
      if (t instanceof ConnectException) {
        return true;
      }
      if (t instanceof SocketException && t.getMessage() != null
          && t.getMessage().contains("Network is unreachable")) {
        return true;
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return false;
  }

  /**
   * Runs {@code action}; on an outage-type failure keeps retrying until it succeeds, fails for
   * another reason, or the total wait passes the limit.
   *
   * @param what a short description of the request, for the log and the failure message
   */
  public static <T> T call(String what, IoAction<T> action) throws IOException {
    long limitMs = MAX_WAIT_MS.get();
    long startNs = 0L;
    long backoffMs = firstBackoffMs;
    int attempts = 0;
    while (true) {
      try {
        T result = action.run();
        if (attempts > 0) {
          LOGGER.warn("Network is back after {} s ({} retries): {}", elapsedSec(startNs), attempts,
              what);
        }
        return result;
      } catch (IOException e) {
        if (limitMs <= 0 || !isOutage(e)) {
          throw e;
        }
        if (attempts == 0) {
          startNs = System.nanoTime();
          LOGGER.warn("Network unavailable, waiting up to {} s for it to return: {}: {}",
              limitMs / 1000, what, e.toString());
        }
        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;
        if (elapsedMs + backoffMs > limitMs) {
          IOException failure = new IOException("Network unavailable for " + elapsedMs / 1000
              + " s (limit " + limitMs / 1000 + " s), giving up: " + what + ": " + e, e);
          LOGGER.warn("Network still unavailable after {} s, failing: {}", elapsedMs / 1000, what);
          throw failure;
        }
        attempts++;
        sleep(backoffMs);
        backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
      }
    }
  }

  private static long elapsedSec(long startNs) {
    return (System.nanoTime() - startNs) / 1_000_000_000L;
  }

  private static void sleep(long ms) throws IOException {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for the network", ie);
    }
  }
}
