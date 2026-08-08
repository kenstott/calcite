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
 * Reference and cross-referencing data adapter support for the {@code ref} govdata schema.
 *
 * <p>Provides data providers, response transformers, and lifecycle hooks backing
 * {@code ref-schema.yaml}: GLEIF LEI/CIK cross-references, OpenFIGI instrument mapping,
 * calendar/holiday reference tables, and the cross-schema entity-resolution bridge
 * ({@link org.apache.calcite.adapter.govdata.ref.EntityBridgeListener}).
 */
package org.apache.calcite.adapter.govdata.ref;
