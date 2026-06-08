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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the one-to-many streaming RowTransformer capability
 * ({@link EtlPipeline#applyRowTransformers} and {@link EtlPipeline#loadRowTransformers}).
 */
@Tag("unit")
public class RowTransformerStreamingTest {

  // ── test transformers (public + no-arg ctor so they load reflectively) ──────

  /** Expands one input row into {@code n} rows (n from the "n" field, default 2). */
  public static class FanOut implements RowTransformer {
    @Override public List<Map<String, Object>> transform(Map<String, Object> row,
        RowContext context) {
      int n = row.get("n") != null ? (Integer) row.get("n") : 2;
      List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
      for (int i = 0; i < n; i++) {
        Map<String, Object> r = new LinkedHashMap<String, Object>(row);
        r.put("idx", i);
        out.add(r);
      }
      return out;
    }
  }

  /** Drops every row (returns an empty list). */
  public static class DropAll implements RowTransformer {
    @Override public List<Map<String, Object>> transform(Map<String, Object> row,
        RowContext context) {
      return Collections.emptyList();
    }
  }

  /** Pass-through that tags each row. */
  public static class Tagger implements RowTransformer {
    @Override public List<Map<String, Object>> transform(Map<String, Object> row,
        RowContext context) {
      row.put("tagged", Boolean.TRUE);
      return Collections.singletonList(row);
    }
  }

  /** Throws on rows flagged "boom"; passes others through. */
  public static class BoomOnFlag implements RowTransformer {
    @Override public List<Map<String, Object>> transform(Map<String, Object> row,
        RowContext context) {
      if (row.get("boom") != null) {
        throw new IllegalStateException("boom on " + row.get("id"));
      }
      return Collections.singletonList(row);
    }
  }

  // ── helpers ─────────────────────────────────────────────────────────────────

  private static Map<String, Object> row(String id) {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    m.put("id", id);
    return m;
  }

  /** A mock config whose hooks carry the given row-transformer error action. */
  private static EtlPipelineConfig configWithRowAction(String action) {
    Map<String, Object> hooksMap = new LinkedHashMap<String, Object>();
    Map<String, Object> errMap = new LinkedHashMap<String, Object>();
    errMap.put("rowTransformer", action);
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

  // ── one-to-many fan-out ─────────────────────────────────────────────────────

  @Test void fanOutExpandsEachRow() {
    List<Map<String, Object>> src = new ArrayList<Map<String, Object>>();
    src.add(row("a"));
    src.add(row("b"));
    List<RowTransformer> transformers = Collections.<RowTransformer>singletonList(new FanOut());

    List<Map<String, Object>> out = drain(EtlPipeline.applyRowTransformers(
        src.iterator(), transformers, configWithRowAction("fail"),
        Collections.<String, String>emptyMap()));

    assertEquals(4, out.size(), "2 inputs x fan-out 2 = 4 rows");
    assertEquals("a", out.get(0).get("id"));
    assertEquals(0, out.get(0).get("idx"));
    assertEquals(1, out.get(1).get("idx"));
    assertEquals("b", out.get(2).get("id"));
  }

  @Test void emptyListDropsRow() {
    List<Map<String, Object>> src = new ArrayList<Map<String, Object>>();
    src.add(row("a"));
    src.add(row("b"));

    List<Map<String, Object>> out = drain(EtlPipeline.applyRowTransformers(
        src.iterator(), Collections.<RowTransformer>singletonList(new DropAll()),
        configWithRowAction("fail"), Collections.<String, String>emptyMap()));

    assertTrue(out.isEmpty(), "DropAll must drop every row");
  }

  // ── chaining: each transformer sees the previous one's output rows ──────────

  @Test void transformersChainAndComposeFanOut() {
    Map<String, Object> r = row("a");
    r.put("n", 3);
    List<RowTransformer> chain = new ArrayList<RowTransformer>();
    chain.add(new FanOut());
    chain.add(new Tagger());

    List<Map<String, Object>> out = drain(EtlPipeline.applyRowTransformers(
        Collections.singletonList(r).iterator(), chain, configWithRowAction("fail"),
        Collections.<String, String>emptyMap()));

    assertEquals(3, out.size(), "fan-out 3 then 1:1 tag = 3 rows");
    for (Map<String, Object> o : out) {
      assertEquals(Boolean.TRUE, o.get("tagged"), "every exploded row must be tagged by the chain");
    }
  }

  // ── streaming: source consumed lazily, not drained up front ─────────────────

  @Test void consumesSourceLazily() {
    final AtomicInteger pulls = new AtomicInteger(0);
    final Iterator<Map<String, Object>> counting = new Iterator<Map<String, Object>>() {
      private int i;
      @Override public boolean hasNext() {
        return i < 5;
      }
      @Override public Map<String, Object> next() {
        pulls.incrementAndGet();
        return row("r" + (i++));
      }
    };

    Iterator<Map<String, Object>> it = EtlPipeline.applyRowTransformers(
        counting, Collections.<RowTransformer>singletonList(new FanOut()),
        configWithRowAction("fail"), Collections.<String, String>emptyMap());

    it.next(); // first fan-out row
    assertEquals(1, pulls.get(),
        "only one source row should be pulled to yield the first output (fan-out buffered)");
    it.next(); // second half of the first source row — still no new pull
    assertEquals(1, pulls.get(), "second output comes from the buffered fan-out, no new source pull");
    it.next(); // now the buffer is empty — pulls the next source row
    assertEquals(2, pulls.get(), "third output requires pulling the second source row");
  }

  // ── error handling honours the configured action ───────────────────────────

  @Test void failActionPropagates() {
    List<Map<String, Object>> src = new ArrayList<Map<String, Object>>();
    Map<String, Object> bad = row("a");
    bad.put("boom", Boolean.TRUE);
    src.add(bad);

    Iterator<Map<String, Object>> it = EtlPipeline.applyRowTransformers(
        src.iterator(), Collections.<RowTransformer>singletonList(new BoomOnFlag()),
        configWithRowAction("fail"), Collections.<String, String>emptyMap());

    assertThrows(IllegalStateException.class, it::hasNext);
  }

  @Test void skipRowActionDropsOffendingRow() {
    List<Map<String, Object>> src = new ArrayList<Map<String, Object>>();
    src.add(row("good1"));
    Map<String, Object> bad = row("bad");
    bad.put("boom", Boolean.TRUE);
    src.add(bad);
    src.add(row("good2"));

    List<Map<String, Object>> out = drain(EtlPipeline.applyRowTransformers(
        src.iterator(), Collections.<RowTransformer>singletonList(new BoomOnFlag()),
        configWithRowAction("skip_row"), Collections.<String, String>emptyMap()));

    assertEquals(2, out.size(), "the throwing row is dropped, the rest pass");
    assertEquals("good1", out.get(0).get("id"));
    assertEquals("good2", out.get(1).get("id"));
  }

  // ── reflective loading ──────────────────────────────────────────────────────

  @Test void loadsClassBasedTransformersInOrderSkippingExpressions() {
    List<HooksConfig.TransformerConfig> configs = new ArrayList<HooksConfig.TransformerConfig>();
    configs.add(HooksConfig.TransformerConfig.ofClass(FanOut.class.getName()));
    configs.add(HooksConfig.TransformerConfig.ofExpression("col", "1+1")); // skipped
    configs.add(HooksConfig.TransformerConfig.ofClass(Tagger.class.getName()));
    HooksConfig hooks = HooksConfig.builder().rowTransformers(configs).build();

    List<RowTransformer> loaded = EtlPipeline.loadRowTransformers(hooks);

    assertEquals(2, loaded.size(), "only the two class-based configs load");
    assertInstanceOf(FanOut.class, loaded.get(0));
    assertInstanceOf(Tagger.class, loaded.get(1));
  }

  @Test void loadReturnsEmptyWhenNoHooks() {
    assertTrue(EtlPipeline.loadRowTransformers(null).isEmpty());
    assertFalse(EtlPipeline.loadRowTransformers(null).iterator().hasNext());
  }
}
