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
 * Geographic data adapter for Apache Calcite.
 *
 * <p>Provides access to U.S. government geographic data including:
 * <ul>
 *   <li>Census Bureau TIGER/Line boundary files (states, counties, places, ZCTAs)</li>
 *   <li>Census Bureau demographic and economic data via API</li>
 *   <li>HUD-USPS ZIP code to Census geography crosswalk</li>
 *   <li>Census geocoding services</li>
 * </ul>
 *
 * <p>All data sources are free government services. Some require registration
 * for API keys but have no cost.
 */
package org.apache.calcite.adapter.govdata.geo;
