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
 * Splunk query provider.
 *
 * <p>There is a single table, called "Splunk". It has fixed columns
 * "host", "index", "source", "sourcetype". It has a variable type, so other
 * fields are held in a map field called "_others".
 */
package org.apache.calcite.adapter.splunk;
