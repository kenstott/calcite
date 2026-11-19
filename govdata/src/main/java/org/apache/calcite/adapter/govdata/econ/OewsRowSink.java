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
package org.apache.calcite.adapter.govdata.econ;

import java.util.List;

/**
 * Receives the rows of an OEWS workbook's data sheet, one at a time, from the shared xlsx/xls
 * readers in {@link OewsStateWorkbookTransformer}. The first row is the header.
 */
interface OewsRowSink {
  /** Receives one sheet row; cells are indexed by column, null where empty. */
  void accept(List<String> cells);
}
