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

/**
 * Thrown when one or more EDGAR quarterly full-index files could not be loaded
 * (HTTP 429/5xx/network) after retries.
 *
 * <p>Signals a <em>transient</em> failure: an incomplete index must never be
 * treated as "no filings". Callers fail the run non-zero so the pool re-runs the
 * year rather than ingesting nothing and marking it complete (the "0m" false
 * success). A distinct type lets callers separate this from a genuine init error
 * (fail the run vs. fall back to the per-CIK API path).
 */
public class EdgarIndexUnavailableException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public EdgarIndexUnavailableException(String message) {
    super(message);
  }
}
