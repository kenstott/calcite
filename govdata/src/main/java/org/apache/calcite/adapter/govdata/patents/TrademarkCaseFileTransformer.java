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
package org.apache.calcite.adapter.govdata.patents;

/** Lands the TRCFECO2 {@code case_file.csv} verbatim as {@code trademark_case_file} (one row per application). */
public class TrademarkCaseFileTransformer extends AbstractTrademarkFileTransformer {
  @Override protected String fileName() {
    return "case_file";
  }
}
