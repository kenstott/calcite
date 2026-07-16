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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory for the U.S. federal fiscal schema.
 *
 * <p>Pairs the revenue side (IRS Statistics of Income) with the outlay side
 * (USAspending, SBA):
 * <ul>
 *   <li><b>IRS SOI</b> — {@code soi_income_by_zip}, {@code soi_income_by_county},
 *       {@code county_migration_flows} (bulk CSV by 2-digit year),
 *       {@code exempt_org_master} (EO BMF regional shards),
 *       {@code exempt_org_990} (Form 990 e-file XML zips)</li>
 *   <li><b>USAspending</b> — {@code usaspending_by_agency},
 *       {@code usaspending_by_state} (POST-JSON aggregate API)</li>
 *   <li><b>SBA</b> — {@code sba_loan_approvals} (DKAN FOIA CSV)</li>
 * </ul>
 *
 * <p>No source needs a secret. The optional {@code enabledSources} operand
 * ({@code irs} / {@code usaspending} / {@code sba}) narrows the schema to one
 * source for targeted DQ/backfill runs.
 */
public class FiscalSchemaFactory implements GovDataSubSchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiscalSchemaFactory.class);

  private static final Map<String, String> TABLE_SOURCE;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("soi_income_by_zip", "irs");
    m.put("soi_income_by_county", "irs");
    m.put("county_migration_flows", "irs");
    m.put("exempt_org_master", "irs");
    m.put("exempt_org_990", "irs");
    m.put("usaspending_by_agency", "usaspending");
    m.put("usaspending_by_state", "usaspending");
    m.put("sba_loan_approvals", "sba");
    m.put("ssa_benefits_by_geography", "ssa");
    m.put("ssa_benefits_by_geography_acs", "ssa");
    TABLE_SOURCE = Collections.unmodifiableMap(m);
  }

  @Override public String getSchemaResourceName() {
    return "/fiscal/fiscal-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    Set<String> enabledSources = parseEnabledSources(operand);
    if (enabledSources == null) {
      return;
    }
    for (Map.Entry<String, String> entry : TABLE_SOURCE.entrySet()) {
      final String tableName = entry.getKey();
      final String source = entry.getValue();
      builder.isEnabled(tableName, ctx -> enabledSources.contains(source));
    }
  }

  private Set<String> parseEnabledSources(Map<String, Object> operand) {
    Object sourcesObj = operand.get("enabledSources");
    if (sourcesObj == null) {
      return null;
    }
    Set<String> sources = new HashSet<String>();
    if (sourcesObj instanceof List) {
      for (Object source : (List<?>) sourcesObj) {
        if (source instanceof String) {
          sources.add(((String) source).toLowerCase());
        }
      }
    } else if (sourcesObj instanceof String[]) {
      for (String source : (String[]) sourcesObj) {
        sources.add(source.toLowerCase());
      }
    }
    LOGGER.info("FISCAL enabled sources: {}", sources);
    return sources;
  }
}
