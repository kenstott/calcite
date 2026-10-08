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
 * ServiceNow adapter for Apache Calcite.
 *
 * <p>Reads ServiceNow tables through the Table API ({@code /api/now/table}). Read-only: no
 * INSERT, UPDATE or DELETE. Projection is pushed down as {@code sysparm_fields}; filters, sorts
 * and limits are evaluated by Calcite, because ServiceNow ignores an invalid {@code sysparm_query}
 * term instead of rejecting it (see {@link org.apache.calcite.adapter.servicenow.PredicatePushdown}).
 * The table list and column types come from the instance's own metadata tables
 * ({@code sys_db_object}, {@code sys_dictionary}, {@code sys_glide_object}).
 */
package org.apache.calcite.adapter.servicenow;
