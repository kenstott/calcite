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

import java.util.List;
import java.util.Set;

/**
 * Provides filing index lookup to determine whether a CIK needs an EDGAR API call.
 *
 * <p>Implementations typically load quarterly full-index files from EDGAR,
 * which list all filings for a quarter. This allows skipping per-CIK API calls
 * when all filings are already tracked.
 */
public interface FilingIndexProvider {

  /** Decision returned by {@link #checkCik}. */
  enum CacheDecision {
    /** No filings found or all filings already tracked — skip EDGAR API call. */
    SKIP,
    /** Has untracked filings — call EDGAR API to get primaryDocument details. */
    PROCESS,
    /** Current quarter in range or unable to determine — fall through to EDGAR API. */
    FALLBACK
  }

  /**
   * Check whether a CIK needs an EDGAR API call for the given year.
   *
   * @param cik           The normalized CIK (10-digit, zero-padded)
   * @param year          The filing year
   * @param filingTypes   Filing types to filter (e.g. "10-K", "4"), or null for all
   * @param tracker       Tracker to check which filings are already processed
   * @return SKIP if no API call needed, PROCESS if untracked filings exist, FALLBACK otherwise
   */
  CacheDecision checkCik(String cik, int year, List<String> filingTypes,
      ProcessedDocumentTracker tracker);

  /**
   * Get CIKs that have filings matching the given year and types.
   *
   * @param year        The filing year
   * @param filingTypes Filing types to filter (e.g. "10-K", "4"), or null for all
   * @param tracker     Tracker to exclude CIKs whose filings are all processed, or null
   * @return set of normalized CIKs with unprocessed matching filings
   */
  Set<String> getActiveCiks(int year, List<String> filingTypes,
      ProcessedDocumentTracker tracker);
}
