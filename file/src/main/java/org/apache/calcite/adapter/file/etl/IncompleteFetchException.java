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
package org.apache.calcite.adapter.file.etl;

/**
 * Signals that a required source fetch could not be completed (e.g. an upstream
 * rate-limit exhausted all retries), so the document must be treated as a
 * <em>failure</em> rather than a partial success.
 *
 * <p>The motivating case: a 13F filing whose holdings information table is
 * dropped after exhausting 429 retries. Previously such a drop produced an empty
 * holdings table that was still committed and marked processed — a silent,
 * permanent data gap. Throwing this instead ensures the document is counted as
 * failed, surfaced in the run summary, and left unprocessed so it is re-fetched
 * on the next run.
 *
 * <p>It is unchecked so it propagates through existing {@code catch (Exception)}
 * blocks only where they explicitly re-raise it, and through code that catches
 * only {@link java.io.IOException}, reaching the document-loop handler in
 * {@link DocumentETLProcessor} without forcing a {@code throws} cascade across
 * every downloader call site. Producers live in other modules (e.g. the SEC
 * adapter); this type lives in the file ETL layer so the processor that counts
 * and surfaces it can reference it directly.
 */
public class IncompleteFetchException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public IncompleteFetchException(String message) {
    super(message);
  }

  public IncompleteFetchException(String message, Throwable cause) {
    super(message, cause);
  }
}
