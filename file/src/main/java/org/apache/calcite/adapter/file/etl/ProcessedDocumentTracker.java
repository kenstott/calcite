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

/**
 * Interface for tracking which documents have been processed.
 * Implementations should persist this information to avoid S3 checks on restart.
 */
public interface ProcessedDocumentTracker {

  /**
   * Check if a document has already been processed with all required outputs.
   *
   * @param cik The CIK identifier
   * @param accession The accession number
   * @param formType The filing form type (e.g. "10-K", "8-K", "4")
   * @return true if already processed, false otherwise
   */
  boolean isProcessed(String cik, String accession, String formType);

  /**
   * Mark a document as processed after successful conversion.
   *
   * @param cik The CIK identifier
   * @param accession The accession number
   * @param formType The filing form type (e.g. "10-K", "8-K", "4")
   * @param outputFiles List of output file paths created by conversion
   */
  void markProcessed(String cik, String accession, String formType, List<String> outputFiles);

  /**
   * Bulk-check whether all given accessions are fully processed.
   *
   * <p>Default loops through {@link #isProcessed}. Implementations backed by
   * a bulk-capable tracker should override for a single round-trip.
   *
   * @param cik         The CIK identifier
   * @param accessions  Accession numbers to check
   * @param formTypes   Corresponding form types (parallel to accessions)
   * @return true if every accession is already processed
   */
  default boolean areAllProcessed(String cik, List<String> accessions, List<String> formTypes) {
    for (int i = 0; i < accessions.size(); i++) {
      if (!isProcessed(cik, accessions.get(i), formTypes.get(i))) {
        return false;
      }
    }
    return true;
  }
}
