/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.etl;

/**
 * Result of row validation by a {@link Validator}.
 *
 * <p>ValidationResult indicates whether a row is valid and, if not, what action
 * should be taken. The action types are:
 * <ul>
 *   <li>{@link Action#VALID} - Row passes validation, include in output</li>
 *   <li>{@link Action#DROP} - Row fails validation, silently exclude from output</li>
 *   <li>{@link Action#WARN} - Row fails validation, log warning but include in output</li>
 *   <li>{@link Action#FAIL} - Row fails validation, stop processing with error</li>
 * </ul>
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * public class DataValidator implements Validator {
 *     public ValidationResult validate(Map<String, Object> row) {
 *         Object geoFips = row.get("geo_fips");
 *         if (geoFips == null) {
 *             return ValidationResult.drop("Missing required field: geo_fips");
 *         }
 *         if (!isValidFips(geoFips.toString())) {
 *             return ValidationResult.warn("Invalid FIPS code: " + geoFips);
 *         }
 *         return ValidationResult.valid();
 *     }
 * }
 * }</pre>
 *
 * @see Validator
 */
public class ValidationResult {

  /**
   * Actions that can be taken when validation fails.
   */
  public enum Action {
    /** Row is valid, include in output. */
    VALID,
    /** Row is invalid, silently drop from output. */
    DROP,
    /** Row is invalid, log warning but include in output. */
    WARN,
    /** Row is invalid, fail the entire operation. */
    FAIL
  }

  private static final ValidationResult VALID_RESULT =
      new ValidationResult(Action.VALID, null);

  private final Action action;
  private final String message;

  private ValidationResult(Action action, String message) {
    this.action = action;
    this.message = message;
  }

  /**
   * Returns a valid result indicating the row passes validation.
   *
   * @return A valid ValidationResult
   */
  public static ValidationResult valid() {
    return VALID_RESULT;
  }

  /**
   * Returns a drop result indicating the row should be silently excluded.
   *
   * @param message Description of why the row is invalid
   * @return A drop ValidationResult
   */
  public static ValidationResult drop(String message) {
    return new ValidationResult(Action.DROP, message);
  }

  /**
   * Returns a warn result indicating the row is invalid but should be included.
   *
   * @param message Warning message to log
   * @return A warn ValidationResult
   */
  public static ValidationResult warn(String message) {
    return new ValidationResult(Action.WARN, message);
  }

  /**
   * Returns a fail result indicating processing should stop with an error.
   *
   * @param message Error message describing the validation failure
   * @return A fail ValidationResult
   */
  public static ValidationResult fail(String message) {
    return new ValidationResult(Action.FAIL, message);
  }

  /**
   * Returns the validation action.
   *
   * @return The action to take
   */
  public Action getAction() {
    return action;
  }

  /**
   * Returns the validation message.
   *
   * @return The message, or null for valid results
   */
  public String getMessage() {
    return message;
  }

  /**
   * Returns whether this result indicates the row is valid.
   *
   * @return true if the action is VALID
   */
  public boolean isValid() {
    return action == Action.VALID;
  }

  /**
   * Returns whether processing should continue after this result.
   *
   * @return true if the action is not FAIL
   */
  public boolean shouldContinue() {
    return action != Action.FAIL;
  }

  /**
   * Returns whether the row should be included in output.
   *
   * @return true if the action is VALID or WARN
   */
  public boolean shouldInclude() {
    return action == Action.VALID || action == Action.WARN;
  }

  @Override public String toString() {
    if (action == Action.VALID) {
      return "ValidationResult{VALID}";
    }
    return "ValidationResult{" + action + ", message='" + message + "'}";
  }
}
