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
package org.apache.calcite.adapter.govdata.banking;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Factory for the banking schema.
 *
 * <p>Sources FDIC BankFind Suite data (api.fdic.gov/banks/*) — institutions, locations, branch
 * history, failures, Summary of Deposits, call-report financials, and industry summaries — plus
 * the CFPB Consumer Complaint Database. All endpoints are free and unauthenticated, so no table
 * is key-gated.
 */
public class BankingSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(BankingSchemaFactory.class);

  @Override public String getSchemaResourceName() {
    return "/banking/banking-schema.yaml";
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for BANKING schema");
    // FDIC and CFPB sources are free and unauthenticated — no auth-gating hooks needed.
  }
}
