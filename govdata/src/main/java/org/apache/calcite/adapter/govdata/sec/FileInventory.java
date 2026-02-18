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
  private final boolean hasMetadata;
  private final boolean hasFacts;
  private final boolean hasContexts;
  private final boolean hasRelationships;
  private final boolean hasMda;
  private final boolean hasInsider;
  private final boolean hasEarnings;
  private final boolean hasChunks;

  private FileInventory(Builder builder) {
    this.hasMetadata = builder.hasMetadata;
    this.hasFacts = builder.hasFacts;
    this.hasContexts = builder.hasContexts;
    this.hasRelationships = builder.hasRelationships;
    this.hasMda = builder.hasMda;
    this.hasInsider = builder.hasInsider;
    this.hasEarnings = builder.hasEarnings;
    this.hasChunks = builder.hasChunks;
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

  public boolean hasInsider() {
    return hasInsider;
  }

  public boolean hasEarnings() {
    return hasEarnings;
  }

  public boolean hasChunks() {
    return hasChunks;
  }

  /**
   * Check if inventory is complete for the given form type.
   */
  public boolean isComplete(FormType formType, boolean vectorizationEnabled) {
    Set<FormType.OutputType> expected = formType.getExpectedOutputs(vectorizationEnabled);
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
    default:
      return false;
    }
  }

  /**
   * Check if any files exist.
   */
  public boolean hasAnyFiles() {
    return hasMetadata || hasFacts || hasContexts || hasRelationships
        || hasMda || hasInsider || hasEarnings || hasChunks;
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
    if (hasInsider) {
      present.add("insider");
    }
    if (hasEarnings) {
      present.add("earnings");
    }
    if (hasChunks) {
      present.add("chunks");
    }
    sb.append(String.join(", ", present));
    sb.append("}");
    return sb.toString();
  }

  /**
   * Builder for FileInventory.
   */
  public static class Builder {
    private boolean hasMetadata;
    private boolean hasFacts;
    private boolean hasContexts;
    private boolean hasRelationships;
    private boolean hasMda;
    private boolean hasInsider;
    private boolean hasEarnings;
    private boolean hasChunks;

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

    public FileInventory build() {
      return new FileInventory(this);
    }
  }
}
