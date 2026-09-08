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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Tracks which output files exist for a filing.
 */
public class FileInventory {
  private final boolean hasNoXbrl;
  private final boolean hasMetadata;
  private final boolean hasFacts;
  private final boolean hasContexts;
  private final boolean hasRelationships;
  private final boolean hasMda;
  private final boolean hasRiskFactors;
  private final boolean hasInsider;
  private final boolean hasEarnings;
  private final boolean hasChunks;
  private final boolean hasInstitutionalHoldings;
  private final boolean hasBeneficialOwnership;

  private FileInventory(Builder builder) {
    this.hasNoXbrl = builder.hasNoXbrl;
    this.hasMetadata = builder.hasMetadata;
    this.hasFacts = builder.hasFacts;
    this.hasContexts = builder.hasContexts;
    this.hasRelationships = builder.hasRelationships;
    this.hasMda = builder.hasMda;
    this.hasRiskFactors = builder.hasRiskFactors;
    this.hasInsider = builder.hasInsider;
    this.hasEarnings = builder.hasEarnings;
    this.hasChunks = builder.hasChunks;
    this.hasInstitutionalHoldings = builder.hasInstitutionalHoldings;
    this.hasBeneficialOwnership = builder.hasBeneficialOwnership;
  }

  public boolean hasNoXbrl() {
    return hasNoXbrl;
  }

  public boolean hasMetadata() {
    return hasMetadata;
  }

  public boolean hasFacts() {
    return hasFacts;
  }

  public boolean hasContexts() {
    return hasContexts;
  }

  public boolean hasRelationships() {
    return hasRelationships;
  }

  public boolean hasMda() {
    return hasMda;
  }

  /**
   * Whether Item 1A (Risk Factors) text was extracted for this filing.
   *
   * <p>Deliberately absent from {@link FormType#getExpectedOutputs(boolean, String)}, so it never
   * participates in {@link #isComplete(FormType, boolean, String)}. Item 1A is not universal even
   * among 10-Ks — smaller reporting companies
   * are exempt from it — so requiring it would leave those filings permanently incomplete and
   * reprocessed on every restart, which is the failure {@code getExpectedOutputs(…, items)}
   * documents for 8-K earnings transcripts. Its purpose here is the staging marker: the
   * materializer resolves "has this table's source data changed?" from the {@code risk_factors}
   * marker rather than by listing the year partition.
   */
  public boolean hasRiskFactors() {
    return hasRiskFactors;
  }

  public boolean hasInsider() {
    return hasInsider;
  }

  public boolean hasEarnings() {
    return hasEarnings;
  }

  public boolean hasChunks() {
    return hasChunks;
  }

  public boolean hasInstitutionalHoldings() {
    return hasInstitutionalHoldings;
  }

  public boolean hasBeneficialOwnership() {
    return hasBeneficialOwnership;
  }

  /**
   * Check if inventory is complete for the given form type.
   */
  public boolean isComplete(FormType formType, boolean vectorizationEnabled) {
    return isComplete(formType, vectorizationEnabled, null);
  }

  /**
   * Check if inventory is complete, judged against what this filing's items say it owes.
   *
   * @param items the filing's EDGAR item numbers, or null when they are not known
   */
  public boolean isComplete(FormType formType, boolean vectorizationEnabled, String items) {
    Set<FormType.OutputType> expected = formType.getExpectedOutputs(vectorizationEnabled, items);
    for (FormType.OutputType output : expected) {
      if (!has(output)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if this inventory has the given output type.
   */
  public boolean has(FormType.OutputType outputType) {
    switch (outputType) {
    case METADATA:
      return hasMetadata;
    case FACTS:
      return hasFacts;
    case CONTEXTS:
      return hasContexts;
    case RELATIONSHIPS:
      return hasRelationships;
    case MDA:
      return hasMda;
    case INSIDER:
      return hasInsider;
    case EARNINGS:
      return hasEarnings;
    case CHUNKS:
      return hasChunks;
    case INSTITUTIONAL_HOLDINGS:
      return hasInstitutionalHoldings;
    case BENEFICIAL_OWNERSHIP:
      return hasBeneficialOwnership;
    default:
      return false;
    }
  }

  /**
   * Check if any files exist.
   */
  public boolean hasAnyFiles() {
    return hasMetadata || hasFacts || hasContexts || hasRelationships
        || hasMda || hasRiskFactors || hasInsider || hasEarnings || hasChunks
        || hasInstitutionalHoldings || hasBeneficialOwnership;
  }

  /**
   * Get list of missing outputs for a form type.
   */
  public List<FormType.OutputType> getMissingOutputs(FormType formType,
      boolean vectorizationEnabled) {
    List<FormType.OutputType> missing = new ArrayList<>();
    Set<FormType.OutputType> expected = formType.getExpectedOutputs(vectorizationEnabled);
    for (FormType.OutputType output : expected) {
      if (!has(output)) {
        missing.add(output);
      }
    }
    return missing;
  }

  /**
   * Create empty inventory.
   */
  public static FileInventory empty() {
    return new Builder().build();
  }

  /**
   * Create new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("FileInventory{");
    List<String> present = new ArrayList<>();
    if (hasNoXbrl) {
      present.add("no_xbrl");
    }
    if (hasMetadata) {
      present.add("metadata");
    }
    if (hasFacts) {
      present.add("facts");
    }
    if (hasContexts) {
      present.add("contexts");
    }
    if (hasRelationships) {
      present.add("relationships");
    }
    if (hasMda) {
      present.add("mda");
    }
    if (hasRiskFactors) {
      present.add("risk_factors");
    }
    if (hasInsider) {
      present.add("insider");
    }
    if (hasEarnings) {
      present.add("earnings");
    }
    if (hasChunks) {
      present.add("chunks");
    }
    if (hasInstitutionalHoldings) {
      present.add("13f");
    }
    if (hasBeneficialOwnership) {
      present.add("13dg");
    }
    sb.append(String.join(", ", present));
    sb.append("}");
    return sb.toString();
  }

  /**
   * Builder for FileInventory.
   */
  public static class Builder {
    private boolean hasNoXbrl;
    private boolean hasMetadata;
    private boolean hasFacts;
    private boolean hasContexts;
    private boolean hasRelationships;
    private boolean hasMda;
    private boolean hasRiskFactors;
    private boolean hasInsider;
    private boolean hasEarnings;
    private boolean hasChunks;
    private boolean hasInstitutionalHoldings;
    private boolean hasBeneficialOwnership;

    public Builder hasNoXbrl(boolean hasNoXbrl) {
      this.hasNoXbrl = hasNoXbrl;
      return this;
    }

    public Builder hasMetadata(boolean hasMetadata) {
      this.hasMetadata = hasMetadata;
      return this;
    }

    public Builder hasFacts(boolean hasFacts) {
      this.hasFacts = hasFacts;
      return this;
    }

    public Builder hasContexts(boolean hasContexts) {
      this.hasContexts = hasContexts;
      return this;
    }

    public Builder hasRelationships(boolean hasRelationships) {
      this.hasRelationships = hasRelationships;
      return this;
    }

    public Builder hasMda(boolean hasMda) {
      this.hasMda = hasMda;
      return this;
    }

    public Builder hasRiskFactors(boolean hasRiskFactors) {
      this.hasRiskFactors = hasRiskFactors;
      return this;
    }

    public Builder hasInsider(boolean hasInsider) {
      this.hasInsider = hasInsider;
      return this;
    }

    public Builder hasEarnings(boolean hasEarnings) {
      this.hasEarnings = hasEarnings;
      return this;
    }

    public Builder hasChunks(boolean hasChunks) {
      this.hasChunks = hasChunks;
      return this;
    }

    public Builder hasInstitutionalHoldings(boolean hasInstitutionalHoldings) {
      this.hasInstitutionalHoldings = hasInstitutionalHoldings;
      return this;
    }

    public Builder hasBeneficialOwnership(boolean hasBeneficialOwnership) {
      this.hasBeneficialOwnership = hasBeneficialOwnership;
      return this;
    }

    public FileInventory build() {
      return new FileInventory(this);
    }
  }
}
