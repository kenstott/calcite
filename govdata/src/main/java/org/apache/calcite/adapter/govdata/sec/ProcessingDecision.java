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

/**
 * Decision about whether and how to process a filing.
 */
public class ProcessingDecision {

  /**
   * Processing action to take.
   */
  public enum Action {
    /** Don't process, already complete. */
    SKIP,
    /** Full processing needed. */
    PROCESS,
    /** Only add vectorized chunks. */
    PROCESS_CHUNKS_ONLY
  }

  private final Action action;
  private final String reason;

  private ProcessingDecision(Action action, String reason) {
    this.action = action;
    this.reason = reason;
  }

  public Action getAction() {
    return action;
  }

  public String getReason() {
    return reason;
  }

  /**
   * Should this filing be processed?
   */
  public boolean shouldProcess() {
    return action != Action.SKIP;
  }

  /**
   * Should only chunks be processed (not full reprocessing)?
   */
  public boolean isChunksOnly() {
    return action == Action.PROCESS_CHUNKS_ONLY;
  }

  /**
   * Create a skip decision.
   */
  public static ProcessingDecision skip(String reason) {
    return new ProcessingDecision(Action.SKIP, reason);
  }

  /**
   * Create a process decision.
   */
  public static ProcessingDecision process(String reason) {
    return new ProcessingDecision(Action.PROCESS, reason);
  }

  /**
   * Create a process-chunks-only decision.
   */
  public static ProcessingDecision processChunksOnly(String reason) {
    return new ProcessingDecision(Action.PROCESS_CHUNKS_ONLY, reason);
  }

  @Override
  public String toString() {
    return action + ": " + reason;
  }
}
