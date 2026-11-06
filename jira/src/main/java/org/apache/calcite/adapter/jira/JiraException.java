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
package org.apache.calcite.adapter.jira;

/** A Jira request failed, or what it returned cannot be used. */
public class JiraException extends RuntimeException {
  /** The HTTP status, or 0 when the failure is not an HTTP response. */
  public final int status;

  public JiraException(String message) {
    super(message);
    this.status = 0;
  }

  public JiraException(String message, Throwable cause) {
    super(message, cause);
    this.status = 0;
  }

  public JiraException(int status, String message) {
    super(message);
    this.status = status;
  }
}
