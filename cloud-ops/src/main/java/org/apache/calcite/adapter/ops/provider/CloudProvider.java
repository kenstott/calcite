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
package org.apache.calcite.adapter.ops.provider;

import java.util.List;
import java.util.Map;

/**
 * Interface for cloud provider implementations.
 */
public interface CloudProvider {
  /**
   * Query Kubernetes clusters.
   */
  List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds);

  /**
   * Query storage resources.
   */
  List<Map<String, Object>> queryStorageResources(List<String> accountIds);

  /**
   * Query compute instances.
   */
  List<Map<String, Object>> queryComputeInstances(List<String> accountIds);

  /**
   * Query network resources.
   */
  List<Map<String, Object>> queryNetworkResources(List<String> accountIds);

  /**
   * Query IAM resources.
   */
  List<Map<String, Object>> queryIAMResources(List<String> accountIds);

  /**
   * Query database resources.
   */
  List<Map<String, Object>> queryDatabaseResources(List<String> accountIds);

  /**
   * Query container registries.
   */
  List<Map<String, Object>> queryContainerRegistries(List<String> accountIds);
}
