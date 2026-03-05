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

import java.util.Collections;
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
