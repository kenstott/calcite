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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.etl.CrossProcessRateLimiter;
import org.apache.calcite.adapter.file.etl.ModelOperand;

/**
 * EDGAR-specific paging over the generic {@link CrossProcessRateLimiter}.
 *
 * <p>SEC enforces ~10 requests/second <em>per IP</em>, across all sec.gov
 * endpoints (www.sec.gov/Archives, data.sec.gov/submissions, full-index, ...).
 * All SEC fetchers — the govdata downloaders here and the file-layer
 * {@code DocumentSource} submissions fetch — pace through the single
 * {@code "sec.gov"} budget so parallel worker JVMs on one host share it.
 *
 * <p>The "all sec.gov is one per-IP budget" policy and the EDGAR rate value
 * ({@code sec.edgarRateLimitMs}, default 100ms) live here in the SEC adapter;
 * the file layer only provides the generic cross-process mechanism. The
 * {@code DocumentSource} side reaches the same {@code "sec.gov"} budget by
 * keying on the request's registrable domain, which collapses every sec.gov
 * host to {@code "sec.gov"}.
 */
public final class EdgarRateLimiter {

  /** Shared budget key for all sec.gov traffic (matches DocumentSource's registrable-domain key). */
  public static final String SEC_KEY = "sec.gov";
  // 125ms = 8 req/s: headroom below SEC's 10 req/s per-IP ceiling (100ms left no margin for
  // slot-claim-to-send jitter and tripped SEC's escalating IP block). Matches sec-schema.yaml's
  // edgarRateLimitMs default and the file-layer DocumentSource default so all SEC paths agree.
  private static final long DEFAULT_INTERVAL_MS = 125L;

  private EdgarRateLimiter() {
  }

  /** Blocks until the next slot in the host-wide sec.gov request budget. */
  public static void acquire() {
    long interval = ModelOperand.getLong("sec.edgarRateLimitMs", DEFAULT_INTERVAL_MS);
    CrossProcessRateLimiter.acquire(SEC_KEY, interval > 0 ? interval : DEFAULT_INTERVAL_MS);
  }
}
