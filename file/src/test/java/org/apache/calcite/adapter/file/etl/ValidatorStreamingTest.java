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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the streaming Validator capability ({@link EtlPipeline#applyValidators} and
 * {@link EtlPipeline#loadValidators}) — see kenstott/govdata-ops#207: this hook was fully
 * specified ({@link Validator}, {@link ValidationResult}) but had zero callers anywhere in the
 * codebase before it was wired up here.
 */
@Tag("unit")
public class ValidatorStreamingTest {

  // ── test validators (public + no-arg ctor so they load reflectively) ────────

  /** DROPs rows with no "id"; VALID otherwise. */
  public static class RequireId implements Validator {
    @Override public ValidationResult validate(Map<String, Object> row) {
      return row.get("id") == null
          ? ValidationResult.drop("missing id")
          : ValidationResult.valid();
    }
  }

  /** WARNs on rows flagged "suspect"; VALID otherwise. Row is kept either way. */
  public static class WarnOnSuspect implements Validator {
    @Override public ValidationResult validate(Map<String, Object> row) {
      return row.get("suspect") != null
          ? ValidationResult.warn("suspect row: " + row.get("id"))
          : ValidationResult.valid();
    }
  }

  /** FAILs the whole pipeline on rows flagged "fatal". */
  public static class FailOnFatal implements Validator {
    @Override public ValidationResult validate(Map<String, Object> row) {
      return row.get("fatal") != null
          ? ValidationResult.fail("fatal row: " + row.get("id"))
          : ValidationResult.valid();
    }
  }

  /** Throws a RuntimeException on rows flagged "boom". */
  public static class BoomOnFlag implements Validator {
    @Override public ValidationResult validate(Map<String, Object> row) {
      if (row.get("boom") != null) {
        throw new IllegalStateException("boom on " + row.get("id"));
      }
      return ValidationResult.valid();
    }
  }

  // ── helpers ─────────────────────────────────────────────────────────────────

  private static Map<String, Object> row(String id) {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    m.put("id", id);
    return m;
  }

  private static EtlPipelineConfig configWithValidatorAction(String action) {
    Map<String, Object> hooksMap = new LinkedHashMap<String, Object>();
    Map<String, Object> errMap = new LinkedHashMap<String, Object>();
    errMap.put("validator", action);
    hooksMap.put("errorHandling", errMap);
    HooksConfig hooks = HooksConfig.fromMap(hooksMap);
    EtlPipelineConfig config = mock(EtlPipelineConfig.class);
    when(config.getHooks()).thenReturn(hooks);
    return config;
  }

  private static List<Map<String, Object>> drain(Iterator<Map<String, Object>> it) {
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    while (it.hasNext()) {
      out.add(it.next());
    }
    return out;
  }

  // ── DROP / WARN / VALID semantics ───────────────────────────────────────────

  @Test void dropRemovesRowSilently() {
    List<Map<String, Object>> src = new ArrayList<Map<String, Object>>();
    src.add(row("a"));
    src.add(new LinkedHashMap<String, Object>()); // no id
    src.add(row("b"));

    List<Map<String, Object>> out = drain(EtlPipeline.applyValidators(
        src.iterator(), Collections.<Validator>singletonList(new RequireId()),
        configWithValidatorAction("continue"), "test_pipeline"));

    assertEquals(2, out.size());
    assertEquals("a", out.get(0).get("id"));
    assertEquals("b", out.get(1).get("id"));
  }

  @Test void warnKeepsRow() {
    List<Map<String, Object>> src = new ArrayList<Map<String, Object>>();
    Map<String, Object> suspectRow = row("a");
    suspectRow.put("suspect", Boolean.TRUE);
    src.add(suspectRow);

    List<Map<String, Object>> out = drain(EtlPipeline.applyValidators(
        src.iterator(), Collections.<Validator>singletonList(new WarnOnSuspect()),
        configWithValidatorAction("continue"), "test_pipeline"));

    assertEquals(1, out.size(), "WARN keeps the row, unlike DROP");
    assertEquals("a", out.get(0).get("id"));
  }

  @Test void failThrowsAndEndsProcessing() {
    List<Map<String, Object>> src = new ArrayList<Map<String, Object>>();
    Map<String, Object> fatalRow = row("a");
    fatalRow.put("fatal", Boolean.TRUE);
    src.add(fatalRow);

    Iterator<Map<String, Object>> it = EtlPipeline.applyValidators(
        src.iterator(), Collections.<Validator>singletonList(new FailOnFatal()),
        configWithValidatorAction("continue"), "test_pipeline");

    assertThrows(IllegalStateException.class, it::hasNext);
  }

  @Test void firstNonValidResultWinsRemainingValidatorsSkipped() {
    List<Validator> chain = new ArrayList<Validator>();
    chain.add(new RequireId()); // would DROP the no-id row
    chain.add(new BoomOnFlag()); // would throw if reached — must not be reached for the dropped row

    List<Map<String, Object>> src = new ArrayList<Map<String, Object>>();
    Map<String, Object> noIdButBoom = new LinkedHashMap<String, Object>();
    noIdButBoom.put("boom", Boolean.TRUE);
    src.add(noIdButBoom);

    List<Map<String, Object>> out = drain(EtlPipeline.applyValidators(
        src.iterator(), chain, configWithValidatorAction("continue"), "test_pipeline"));

    assertTrue(out.isEmpty(), "row dropped by the first validator; BoomOnFlag must never run");
  }

  // ── validator error handling honours the configured action ─────────────────

  @Test void validatorErrorFailActionPropagates() {
    List<Map<String, Object>> src = new ArrayList<Map<String, Object>>();
    Map<String, Object> bad = row("a");
    bad.put("boom", Boolean.TRUE);
    src.add(bad);

    Iterator<Map<String, Object>> it = EtlPipeline.applyValidators(
        src.iterator(), Collections.<Validator>singletonList(new BoomOnFlag()),
        configWithValidatorAction("fail"), "test_pipeline");

    assertThrows(IllegalStateException.class, it::hasNext);
  }

  @Test void validatorErrorContinueActionTreatsRowAsValid() {
    List<Map<String, Object>> src = new ArrayList<Map<String, Object>>();
    Map<String, Object> bad = row("a");
    bad.put("boom", Boolean.TRUE);
    src.add(bad);
    src.add(row("b"));

    List<Map<String, Object>> out = drain(EtlPipeline.applyValidators(
        src.iterator(), Collections.<Validator>singletonList(new BoomOnFlag()),
        configWithValidatorAction("continue"), "test_pipeline"));

    assertEquals(2, out.size(), "a validator error under 'continue' keeps the row, not drops it");
  }

  // ── reflective loading ──────────────────────────────────────────────────────

  @Test void loadsClassBasedValidatorsInOrder() {
    List<HooksConfig.ValidatorConfig> configs = new ArrayList<HooksConfig.ValidatorConfig>();
    configs.add(HooksConfig.ValidatorConfig.ofClass(RequireId.class.getName()));
    configs.add(HooksConfig.ValidatorConfig.ofClass(WarnOnSuspect.class.getName()));
    HooksConfig hooks = HooksConfig.builder().validators(configs).build();

    List<Validator> loaded = EtlPipeline.loadValidators(hooks);

    assertEquals(2, loaded.size());
    assertInstanceOf(RequireId.class, loaded.get(0));
    assertInstanceOf(WarnOnSuspect.class, loaded.get(1));
  }

  @Test void loadReturnsEmptyWhenNoHooks() {
    assertTrue(EtlPipeline.loadValidators(null).isEmpty());
  }

  /**
   * Expression-based validators have no evaluator implementation — must fail loudly rather
   * than silently no-op (the trap this hook shipped with before this test existed: config
   * parsed fine, {@code onFailure: DROP} kept every row unconditionally).
   */
  @Test void expressionBasedValidatorConfigThrows() {
    List<HooksConfig.ValidatorConfig> configs = new ArrayList<HooksConfig.ValidatorConfig>();
    configs.add(HooksConfig.ValidatorConfig.ofExpression("row.cik != null", "drop"));
    HooksConfig hooks = HooksConfig.builder().validators(configs).build();

    assertThrows(IllegalArgumentException.class, () -> EtlPipeline.loadValidators(hooks));
  }
}
