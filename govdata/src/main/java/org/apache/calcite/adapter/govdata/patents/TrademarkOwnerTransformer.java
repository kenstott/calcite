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

/** Lands the TRCFECO2 {@code owner.csv} verbatim as {@code trademark_owner} (one row per serial_no + own_seq). */
public class TrademarkOwnerTransformer extends AbstractTrademarkFileTransformer {
  @Override protected String fileName() {
    return "owner";
  }
}
