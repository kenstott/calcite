/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Factory for the ECON_REFERENCE schema containing reference/dimension tables
 * for economic data.
 *
 * <p>This schema contains lookup tables that are loaded BEFORE the main ECON schema:
 * <ul>
 *   <li>JOLTS industry and data element codes (BLS)</li>
 *   <li>BLS geography mappings (states, metros, regions)</li>
 *   <li>NAICS sector codes</li>
 *   <li>NIPA table catalog (BEA)</li>
 *   <li>Regional LineCode catalog (BEA)</li>
 *   <li>FRED series catalog (Federal Reserve)</li>
 * </ul>
 *
 * <p>The ECON schema depends on ECON_REFERENCE for dimension lookups.
 *
 * <p>Table enablement is controlled by the {@code enabled} field in econ-reference-schema.yaml
 * using variable substitution (e.g., {@code enabled: "${BEA_API_KEY:}"}). Tables are
 * skipped when their enabled field evaluates to empty/false.
 */
public class EconReferenceSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(EconReferenceSchemaFactory.class);

  @Override
  public String getSchemaResourceName() {
    return "/econ/econ-reference-schema.yaml";
  }

  @Override
  public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for ECON_REFERENCE schema");
    // Reference tables have no special hooks - enablement is controlled via YAML
    // The 'enabled' field in the schema uses variable substitution for API key checks
  }
}
