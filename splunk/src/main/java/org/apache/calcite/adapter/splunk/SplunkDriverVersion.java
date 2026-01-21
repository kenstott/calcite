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
package org.apache.calcite.adapter.splunk;

import org.apache.calcite.avatica.DriverVersion;

/**
 * Version information for Calcite JDBC Driver for Splunk.
 */
class SplunkDriverVersion extends DriverVersion {
  /** Creates a SplunkDriverVersion. */
  SplunkDriverVersion() {
    super(
        "Calcite JDBC Driver for Splunk",
        "0.2",
        "Calcite-Splunk",
        "0.2",
        true,
        0,
        1,
        0,
        1);
  }
}
