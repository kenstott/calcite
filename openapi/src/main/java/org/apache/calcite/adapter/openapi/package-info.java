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
 * Calcite adapter for OpenAPI-based data sources.
 *
 * <p>This adapter allows querying data from RESTful APIs that conform
 * to the OpenAPI specification. It translates SQL queries into HTTP
 * requests and transforms the JSON responses into relational data.
 */
@PackageMarker
package org.apache.calcite.adapter.openapi;

import org.apache.calcite.avatica.util.PackageMarker;
