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
  FORM_10K("10-K", true, true, true, true, true, false, false, true, false, false),
  FORM_10K_A("10-K/A", true, true, true, true, true, false, false, true, false, false),
  FORM_10Q("10-Q", true, true, true, true, true, false, false, true, false, false),
  FORM_10Q_A("10-Q/A", true, true, true, true, true, false, false, true, false, false),
  FORM_8K("8-K", true, false, false, false, false, false, true, true, false, false),
  FORM_8K_A("8-K/A", true, false, false, false, false, false, true, true, false, false),
  FORM_DEF14A("DEF 14A", true, true, true, false, false, false, false, false, false, false),
  FORM_3("3", true, false, false, false, false, true, false, false, false, false),
  FORM_4("4", true, false, false, false, false, true, false, false, false, false),
  FORM_5("5", true, false, false, false, false, true, false, false, false, false),
  // 13F-HR: holdings live in an INFORMATION TABLE xml (no XBRL) -> _13f.parquet (+ metadata).
  FORM_13F_HR("13F-HR", true, false, false, false, false, false, false, false, true, false),
  FORM_13F_HR_A("13F-HR/A", true, false, false, false, false, false, false, false, true, false),
  // SC 13D/G: beneficial ownership HTML (no XBRL) -> _13dg.parquet (+ metadata).
  FORM_SC_13D("SC 13D", true, false, false, false, false, false, false, false, false, true),
  FORM_SC_13D_A("SC 13D/A", true, false, false, false, false, false, false, false, false, true),
  FORM_SC_13G("SC 13G", true, false, false, false, false, false, false, false, false, true),
  FORM_SC_13G_A("SC 13G/A", true, false, false, false, false, false, false, false, false, true),
  FORM_OTHER("OTHER", true, false, false, false, false, false, false, false, false, false);

  private final String formName;
  private final boolean expectsMetadata;
  private final boolean expectsFacts;
  private final boolean expectsContexts;
  private final boolean expectsRelationships;
  private final boolean expectsMda;
  private final boolean expectsInsider;
  private final boolean expectsEarnings;
  private final boolean expectsChunks;
  private final boolean expectsInstitutionalHoldings;
  private final boolean expectsBeneficialOwnership;

  FormType(String formName, boolean expectsMetadata, boolean expectsFacts,
      boolean expectsContexts, boolean expectsRelationships, boolean expectsMda,
      boolean expectsInsider, boolean expectsEarnings, boolean expectsChunks,
      boolean expectsInstitutionalHoldings, boolean expectsBeneficialOwnership) {
    this.formName = formName;
    this.expectsMetadata = expectsMetadata;
    this.expectsFacts = expectsFacts;
    this.expectsContexts = expectsContexts;
    this.expectsRelationships = expectsRelationships;
    this.expectsMda = expectsMda;
    this.expectsInsider = expectsInsider;
    this.expectsEarnings = expectsEarnings;
    this.expectsChunks = expectsChunks;
    this.expectsInstitutionalHoldings = expectsInstitutionalHoldings;
    this.expectsBeneficialOwnership = expectsBeneficialOwnership;
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

  public boolean expectsInstitutionalHoldings() {
    return expectsInstitutionalHoldings;
  }

  public boolean expectsBeneficialOwnership() {
    return expectsBeneficialOwnership;
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
    if (expectsInstitutionalHoldings) {
      outputs.add(OutputType.INSTITUTIONAL_HOLDINGS);
    }
    if (expectsBeneficialOwnership) {
      outputs.add(OutputType.BENEFICIAL_OWNERSHIP);
    }
    return outputs;
  }

  /**
   * Get expected output types for this form, narrowed by the filing's EDGAR item numbers.
   *
   * <p>An 8-K reports whatever event its items name — a director's departure, a material
   * agreement, an acquisition — and only Item 2.02, Results of Operations and Financial Condition,
   * is an earnings release. Expecting a transcript from every 8-K makes roughly four in five
   * permanently incomplete: measured across 2019-2022, between 18% and 22% of 8-Ks produced one,
   * every year. Those filings are re-tested and reprocessed on each restart, and can never pass.
   *
   * <p>Unknown items are treated as expecting earnings, which is the behaviour without this
   * narrowing at all. EDGAR's submissions payload covers every filing, so a null here means the
   * lookup failed rather than that the filing reports nothing — and a failed lookup must not be
   * allowed to retire a filing that genuinely owes a transcript. Reprocessing costs time; wrongly
   * declaring a filing complete loses the data quietly, which is the more expensive mistake.
   *
   * @param items comma-separated EDGAR item numbers, empty when the filing reports none, or null
   *              when EDGAR's payload did not cover the filing
   */
  public Set<OutputType> getExpectedOutputs(boolean vectorizationEnabled, String items) {
    Set<OutputType> outputs = getExpectedOutputs(vectorizationEnabled);
    if (outputs.contains(OutputType.EARNINGS) && items != null && !reportsEarningsItem(items)) {
      outputs.remove(OutputType.EARNINGS);
    }
    return outputs;
  }

  /**
   * Whether an item list names Item 2.02, the earnings release item.
   *
   * <p>Matched on the whole item rather than as a substring: the list is comma separated and
   * {@code 2.02} must not be found inside a hypothetical {@code 12.02}, nor {@code 2.0} inside
   * {@code 2.02}.
   */
  private static boolean reportsEarningsItem(String items) {
    for (String item : items.split(",")) {
      if ("2.02".equals(item.trim())) {
        return true;
      }
    }
    return false;
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
    if (normalized.equals("13F-HR")) {
      return FORM_13F_HR;
    }
    if (normalized.equals("13F-HR/A")) {
      return FORM_13F_HR_A;
    }
    if (normalized.equals("SC 13D")) {
      return FORM_SC_13D;
    }
    if (normalized.equals("SC 13D/A")) {
      return FORM_SC_13D_A;
    }
    if (normalized.equals("SC 13G")) {
      return FORM_SC_13G;
    }
    if (normalized.equals("SC 13G/A")) {
      return FORM_SC_13G_A;
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
    CHUNKS("chunks"),
    INSTITUTIONAL_HOLDINGS("13f"),
    BENEFICIAL_OWNERSHIP("13dg");

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
