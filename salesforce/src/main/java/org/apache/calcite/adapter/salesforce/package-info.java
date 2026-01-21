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

/**
 * Salesforce adapter for Apache Calcite.
 *
 * <p>This adapter allows Calcite to connect to Salesforce and execute SOQL queries.
 * It provides read-only access to Salesforce objects and implements push-down
 * capabilities for filters, projections, sorts, and limits.
 */
package org.apache.calcite.adapter.salesforce;
