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
package org.apache.calcite.adapter.govdata.health;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Product aggregation and csv reading for the CMS Uniform Rate Review public use file. */
@Tag("unit")
class AcaRateReviewTransformerTest {

  private static final String HEADER =
      "SUB_TRK_NUM,COMPANY,ISSUER_ID,EFF_DT,STATE,MARKET,PROD_NAME,PROD_ID,PLAN_ID,EXCHANGE,"
      + "RT_CHG_CUM_PROD,RT_CHG_CUM_SUB,CUR_ENR,DETERMINATION\n";

  private static List<Map<String, Object>> run(String body) throws IOException {
    AcaRateReviewTransformer.Aggregator agg = new AcaRateReviewTransformer.Aggregator(2027);
    AcaRateReviewTransformer.parseCsv(
        new ByteArrayInputStream((HEADER + body).getBytes(StandardCharsets.UTF_8)), agg);
    return agg.finish("t");
  }

  @Test void plansRollUpToProductAndSmallGroupIsDropped() throws IOException {
    List<Map<String, Object>> out = run(
        "URR-1,\"Acme, Inc.\",11111,01/01/2027,MI,Individual,Gold PPO,11111MI001,11111MI0010001,"
            + "Yes,0.2373,0.24,\"4,148\",Not Unreasonable\n"
            + "URR-1,\"Acme, Inc.\",11111,01/01/2027,MI,Individual,Gold PPO,11111MI001,11111MI0010002,"
            + "No,0.2373,0.24,52,Not Unreasonable\n"
            + "URR-1,\"Acme, Inc.\",11111,01/01/2027,MI,Small Group,SG,11111MI009,11111MI0090001,"
            + "No,0.1,0.1,10,\n");
    assertEquals(1, out.size());
    Map<String, Object> r = out.get(0);
    assertEquals(2027, r.get("plan_year"));
    assertEquals("Acme, Inc.", r.get("issuer_name"));
    assertEquals("2027-01-01", r.get("effective_date"));
    assertEquals(23.73, (Double) r.get("rate_change_product_pct"), 1e-9);
    assertEquals(24.0, (Double) r.get("rate_change_submission_pct"), 1e-9);
    assertEquals(2, r.get("plan_count"));
    assertEquals(4200L, r.get("enrollment"));
    assertEquals("Not Unreasonable", r.get("determination"));
  }

  @Test void duplicatePlanRowCountsOnce() throws IOException {
    List<Map<String, Object>> out = run(
        "URR-1,A,1,01/01/2027,MI,Individual,P,1MI001,1MI0010001,Yes,0.05,0.05,10,\n"
            + "URR-1,A,1,01/01/2027,MI,Individual,P,1MI001,1MI0010001,Yes,0.05,0.05,10,\n");
    assertEquals(1, out.get(0).get("plan_count"));
    assertEquals(10L, out.get(0).get("enrollment"));
    assertNull(out.get(0).get("determination"));
  }

  @Test void negativeChangeAndQuotedNewlineSurvive() throws IOException {
    List<Map<String, Object>> out = run(
        "URR-2,\"Two\nLines\",2,04/01/2027,AK,Individual,\"P \"\"x\"\"\",2AK001,2AK0010001,"
            + "Yes,-0.0618,-0.06,0,Unreasonable\n");
    assertEquals(-6.18, (Double) out.get(0).get("rate_change_product_pct"), 1e-9);
    assertEquals("Two\nLines", out.get(0).get("issuer_name"));
    assertEquals("P \"x\"", out.get(0).get("product_name"));
    assertEquals(0L, out.get(0).get("enrollment"));
  }

  @Test void missingRequiredHeaderFails() {
    AcaRateReviewTransformer.Aggregator agg = new AcaRateReviewTransformer.Aggregator(2027);
    assertThrows(IllegalStateException.class, () -> agg.accept(Arrays.asList("SUB_TRK_NUM", "STATE")));
  }

  @Test void noIndividualRowsFails() {
    assertThrows(IllegalStateException.class, () -> run(
        "URR-1,A,1,01/01/2027,MI,Small Group,P,1MI001,1MI0010001,Yes,0.05,0.05,10,\n"));
  }

  @Test void numberFormats() {
    assertEquals(4148.0, AcaRateReviewTransformer.number("4,148"), 0);
    assertEquals(0.05, AcaRateReviewTransformer.number("5%"), 1e-12);
    assertNull(AcaRateReviewTransformer.number(null));
  }
}
