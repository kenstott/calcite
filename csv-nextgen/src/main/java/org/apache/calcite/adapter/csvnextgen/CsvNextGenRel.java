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
package org.apache.calcite.adapter.csvnextgen;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * Relational expression that uses CSV NextGen calling convention.
 */
public interface CsvNextGenRel extends RelNode {
  /** Calling convention for CSV NextGen operations. */
  Convention CONVENTION = new Convention.Impl("CSV_NEXTGEN", CsvNextGenRel.class);
}
