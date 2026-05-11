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
package org.apache.calcite.adapter.govdata.sec;

import java.util.EnumSet;
import java.util.Set;

/**
 * SEC form types and their expected output files.
 */
public enum FormType {
  FORM_10K("10-K", true, true, true, true, true, false, false, true),
  FORM_10K_A("10-K/A", true, true, true, true, true, false, false, true),
  FORM_10Q("10-Q", true, true, true, true, true, false, false, true),
  FORM_10Q_A("10-Q/A", true, true, true, true, true, false, false, true),
  FORM_8K("8-K", true, false, false, false, false, false, true, true),
  FORM_8K_A("8-K/A", true, false, false, false, false, false, true, true),
  FORM_DEF14A("DEF 14A", true, true, true, false, false, false, false, false),
  FORM_3("3", true, false, false, false, false, true, false, false),
  FORM_4("4", true, false, false, false, false, true, false, false),
  FORM_5("5", true, false, false, false, false, true, false, false),
  FORM_OTHER("OTHER", true, false, false, false, false, false, false, false);

  private final String formName;
  private final boolean expectsMetadata;
  private final boolean expectsFacts;
  private final boolean expectsContexts;
  private final boolean expectsRelationships;
  private final boolean expectsMda;
  private final boolean expectsInsider;
  private final boolean expectsEarnings;
  private final boolean expectsChunks;

  FormType(String formName, boolean expectsMetadata, boolean expectsFacts,
      boolean expectsContexts, boolean expectsRelationships, boolean expectsMda,
      boolean expectsInsider, boolean expectsEarnings, boolean expectsChunks) {
    this.formName = formName;
    this.expectsMetadata = expectsMetadata;
    this.expectsFacts = expectsFacts;
    this.expectsContexts = expectsContexts;
    this.expectsRelationships = expectsRelationships;
    this.expectsMda = expectsMda;
    this.expectsInsider = expectsInsider;
    this.expectsEarnings = expectsEarnings;
    this.expectsChunks = expectsChunks;
  }

  public String getFormName() {
    return formName;
  }

  public boolean expectsMetadata() {
    return expectsMetadata;
  }

  public boolean expectsFacts() {
    return expectsFacts;
  }

  public boolean expectsContexts() {
    return expectsContexts;
  }

  public boolean expectsRelationships() {
    return expectsRelationships;
  }

  public boolean expectsMda() {
    return expectsMda;
  }

  public boolean expectsInsider() {
    return expectsInsider;
  }

  public boolean expectsEarnings() {
    return expectsEarnings;
  }

  public boolean expectsChunks() {
    return expectsChunks;
  }

  /**
   * Get expected output types for this form.
   */
  public Set<OutputType> getExpectedOutputs(boolean vectorizationEnabled) {
    Set<OutputType> outputs = EnumSet.noneOf(OutputType.class);
    if (expectsMetadata) {
      outputs.add(OutputType.METADATA);
    }
    if (expectsFacts) {
      outputs.add(OutputType.FACTS);
    }
    if (expectsContexts) {
      outputs.add(OutputType.CONTEXTS);
    }
    if (expectsRelationships) {
      outputs.add(OutputType.RELATIONSHIPS);
    }
    if (expectsMda) {
      outputs.add(OutputType.MDA);
    }
    if (expectsInsider) {
      outputs.add(OutputType.INSIDER);
    }
    if (expectsEarnings) {
      outputs.add(OutputType.EARNINGS);
    }
    if (expectsChunks && vectorizationEnabled) {
      outputs.add(OutputType.CHUNKS);
    }
    return outputs;
  }

  /**
   * Parse form type from string.
   */
  public static FormType fromString(String form) {
    if (form == null) {
      return FORM_OTHER;
    }
    String normalized = form.toUpperCase().trim();
    if (normalized.equals("10-K") || normalized.equals("10K")) {
      return FORM_10K;
    }
    if (normalized.equals("10-K/A") || normalized.equals("10K/A")) {
      return FORM_10K_A;
    }
    if (normalized.equals("10-Q") || normalized.equals("10Q")) {
      return FORM_10Q;
    }
    if (normalized.equals("10-Q/A") || normalized.equals("10Q/A")) {
      return FORM_10Q_A;
    }
    if (normalized.equals("8-K") || normalized.equals("8K")) {
      return FORM_8K;
    }
    if (normalized.equals("8-K/A") || normalized.equals("8K/A")) {
      return FORM_8K_A;
    }
    if (normalized.equals("DEF 14A") || normalized.equals("DEF14A")) {
      return FORM_DEF14A;
    }
    if (normalized.equals("3")) {
      return FORM_3;
    }
    if (normalized.equals("4")) {
      return FORM_4;
    }
    if (normalized.equals("5")) {
      return FORM_5;
    }
    return FORM_OTHER;
  }

  /**
   * Check if this is an insider form (3, 4, or 5).
   */
  public boolean isInsiderForm() {
    return this == FORM_3 || this == FORM_4 || this == FORM_5;
  }

  /**
   * Output file types.
   */
  public enum OutputType {
    METADATA("metadata"),
    FACTS("facts"),
    CONTEXTS("contexts"),
    RELATIONSHIPS("relationships"),
    MDA("mda"),
    INSIDER("insider"),
    EARNINGS("earnings"),
    CHUNKS("chunks");

    private final String suffix;

    OutputType(String suffix) {
      this.suffix = suffix;
    }

    public String getSuffix() {
      return suffix;
    }

    public String getFileName(String cik, String accession) {
      return cik + "_" + accession + "_" + suffix + ".parquet";
    }
  }
}
