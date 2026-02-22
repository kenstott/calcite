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
}
