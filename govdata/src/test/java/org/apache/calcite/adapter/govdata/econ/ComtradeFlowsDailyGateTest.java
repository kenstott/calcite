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

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.govdata.GovDataUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The econ daily skips comtrade_flows when COMTRADE_FLOWS_DAILY is false, so the HS-6 reprocess
 * can own the Comtrade key's call quota; every other econ table is unaffected.
 */
@Tag("unit")
public class ComtradeFlowsDailyGateTest {
  private static final String VAR = "COMTRADE_FLOWS_DAILY";

  @AfterEach void clear() {
    System.clearProperty(VAR);
  }

  private static Map<String, Object> table(String name) {
    List<Map<String, Object>> tables =
        GovDataUtils.loadTableDefinitions(EconSchemaFactory.class, "/econ/econ-schema.yaml");
    for (Map<String, Object> t : tables) {
      if (name.equals(t.get("name"))) {
        return new HashMap<String, Object>(t);
      }
    }
    throw new AssertionError("no table " + name);
  }

  private static boolean enabled(String name) {
    Map<String, Object> t = table(name);
    Object e = t.get("enabled");
    if (e instanceof String) {
      t.put("enabled", VariableResolver.resolveEnvVars((String) e));
    }
    return EtlPipelineConfig.fromMap(t).isEnabled();
  }

  @Test public void comtradeFlowsEnabledByDefault() {
    assertNotNull(table("comtrade_flows").get("enabled"));
    assertTrue(enabled("comtrade_flows"));
  }

  @Test public void comtradeFlowsSkippedWhenOperandFalse() {
    System.setProperty(VAR, "false");
    assertFalse(enabled("comtrade_flows"));
  }

  @Test public void comtradeFlowsEnabledWhenOperandTrue() {
    System.setProperty(VAR, "true");
    assertTrue(enabled("comtrade_flows"));
  }

  @Test public void otherTablesAreNotGated() {
    System.setProperty(VAR, "false");
    int gated = 0;
    for (Map<String, Object> t : GovDataUtils.loadTableDefinitions(EconSchemaFactory.class,
        "/econ/econ-schema.yaml")) {
      Object e = t.get("enabled");
      if (e != null && String.valueOf(e).contains(VAR)) {
        gated++;
        assertEquals("comtrade_flows", t.get("name"));
      }
    }
    assertEquals(1, gated);
  }
}
