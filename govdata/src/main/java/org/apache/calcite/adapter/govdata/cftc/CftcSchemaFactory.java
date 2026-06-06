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
package org.apache.calcite.adapter.govdata.cftc;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Factory for the CFTC swap data schema.
 *
 * <p>Provides access to public swap trade dissemination data mandated by
 * Dodd-Frank (17 CFR Part 43), sourced from DTCC's Global Trade Repository
 * (GTR) daily EOD files. Covers five CFTC asset classes:
 * RATES (interest rate swaps, OIS), CREDITS (CDS, index CDS),
 * FOREX (FX swaps, cross-currency, NDFs), EQUITIES (TRS, equity options),
 * and COMMODITIES (energy, agricultural, metals swaps).
 *
 * <p>Tables:
 * <ul>
 *   <li>{@code cftc_trades} — all swap dissemination events partitioned by
 *       asset_class/year/month; 110-column 17 CFR Part 43 schema</li>
 * </ul>
 *
 * <p>Data availability: 2024-01-01 onward (post Part 43/45 rewrite).
 * Source URL: https://kgc0418-tdw-data-0.s3.amazonaws.com/cftc/eod/
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "name": "cftc",
 *   "type": "custom",
 *   "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *   "operand": {
 *     "dataSource": "cftc",
 *     "directory": "${GOVDATA_PARQUET_DIR}",
 *     "cacheDirectory": "${GOVDATA_CACHE_DIR}"
 *   }
 * }
 * </pre>
 */
public class CftcSchemaFactory implements GovDataSubSchemaFactory {

  @Override public String getSchemaResourceName() {
    return "/cftc/cftc-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    // No source filtering — cftc_trades is the single table covering all asset classes
    // via the asset_class partition dimension.
  }
}
