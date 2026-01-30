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
 * Interface for tracking which documents have been processed.
 * Implementations should persist this information to avoid S3 checks on restart.
 */
public interface ProcessedDocumentTracker {

  /**
   * Check if a document has already been processed.
   *
   * @param cik The CIK identifier
   * @param accession The accession number
   * @return true if already processed, false otherwise
   */
  boolean isProcessed(String cik, String accession);

  /**
   * Mark a document as processed after successful conversion.
   *
   * @param cik The CIK identifier
   * @param accession The accession number
   * @param outputFileCount Number of output files created
   */
  void markProcessed(String cik, String accession, int outputFileCount);
}
