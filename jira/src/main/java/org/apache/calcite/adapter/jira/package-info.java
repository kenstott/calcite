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
 * Jira Cloud adapter for Apache Calcite.
 *
 * <p>Serves Jira "forms" (a project, an issue type and chosen fields) as tables, read with JQL
 * through REST v3 and written through it: INSERT, UPDATE (a status change is a workflow
 * transition) and DELETE. {@code issue_links} serves the links between issues;
 * {@code _jira_forms} and {@code _jira_fields} list what can be served. See
 * {@link org.apache.calcite.adapter.jira.JiraSchemaFactory}.
 */
package org.apache.calcite.adapter.jira;
