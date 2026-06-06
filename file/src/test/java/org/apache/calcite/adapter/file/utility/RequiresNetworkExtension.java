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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

import java.net.Socket;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;

/**
 * Enables to activate test conditionally if the specified host is reachable.
 * Note: it is recommended to avoid creating tests that depend on external servers.
 */
public class RequiresNetworkExtension implements ExecutionCondition {
  @Override public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    return context.getElement()
        .flatMap(element -> AnnotationSupport.findAnnotation(element, RequiresNetwork.class))
        .map(net -> {
          try (Socket ignored = new Socket(net.host(), net.port())) {
            return enabled(net.host() + ":" + net.port() + " is reachable");
          } catch (Exception e) {
            return disabled(net.host() + ":" + net.port() + " is unreachable: " + e.getMessage());
          }
        })
        .orElseGet(() -> enabled("@RequiresNetwork is not found"));
  }
}
