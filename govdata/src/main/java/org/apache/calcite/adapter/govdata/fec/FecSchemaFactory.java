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
package org.apache.calcite.adapter.govdata.fec;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Factory for the FEC (Federal Election Commission) campaign finance schema.
 *
 * <p>This schema provides comprehensive campaign finance data from FEC bulk
 * downloads including:
 * <ul>
 *   <li>Candidates — federal candidate registrations</li>
 *   <li>Committees — PAC, party, and campaign committee registrations</li>
 *   <li>Individual contributions — itemized donations &gt;$200</li>
 *   <li>Committee-to-candidate contributions</li>
 *   <li>Inter-committee transactions</li>
 *   <li>Operating expenditures</li>
 *   <li>Independent expenditures — Super PAC spending</li>
 *   <li>Electioneering communications</li>
 *   <li>Communication costs</li>
 *   <li>Candidate and committee financial summaries</li>
 * </ul>
 *
 * <p>This is a standalone schema with no dependencies. FEC bulk files use
 * pipe-delimited format with no header rows (headers defined in YAML).
 */
public class FecSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(FecSchemaFactory.class);

  @Override public String getSchemaResourceName() {
    return "/fec/fec-schema.yaml";
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for FEC schema");
    // FEC uses standard CSV/pipe-delimited bulk downloads — no special hooks needed
  }
}
