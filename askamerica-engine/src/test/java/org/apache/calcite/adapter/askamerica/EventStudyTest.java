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
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates {@link StatsEngine#eventStudy} against panels built with a known treatment effect
 * and, separately, a known pre-existing divergence.
 *
 * <p>The pre-trend test is the reason this tool exists, so the assertions below are mostly
 * about it: a panel with parallel pre-trends must not raise one, and a panel where the treated
 * units were already pulling away must. A pre-trend test that quietly passed on the second
 * panel would hand a caller exactly the false confidence diff_in_diff already gives.
 *
 * <p>The panels carry a residual that alternates by unit-period parity with an even number of
 * units per group. It sums to zero within every group-period cell, every unit, and every
 * period, so it leaves the coefficients at their exact construction values while still giving
 * the model non-zero residual variance to compute standard errors from.
 */
@Tag("unit")
class EventStudyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int TREATED = 6;
    private static final int CONTROL = 6;
    private static final int PERIODS = 10;
    private static final int TREAT_AT = 5;
    private static final double EFFECT = 5.0;

    /** A balanced panel with unit and period effects, a step effect, and an optional drift. */
    private static Panel panel(double effect, double preTrend, int treatAt, int treatAtSecond) {
        List<Double> y = new ArrayList<>();
        List<String> units = new ArrayList<>();
        List<String> times = new ArrayList<>();
        List<Integer> relative = new ArrayList<>();
        int totalUnits = TREATED + CONTROL;
        for (int u = 0; u < totalUnits; u++) {
            boolean treated = u < TREATED;
            // A second adoption date for half the treated units, when one is asked for.
            int adopt = treated && treatAtSecond > 0 && u >= TREATED / 2 ? treatAtSecond
                : treatAt;
            for (int t = 0; t < PERIODS; t++) {
                double v = 10.0 * u + 2.0 * t + 0.1 * ((u + t) % 2 == 0 ? 1 : -1);
                if (treated) {
                    v += preTrend * t;
                    if (t >= adopt) {
                        v += effect;
                    }
                }
                y.add(Double.valueOf(v));
                units.add("u" + u);
                times.add(String.valueOf(2010 + t));
                relative.add(treated ? Integer.valueOf(t - adopt) : null);
            }
        }
        return new Panel(y, units, times, relative);
    }

    private static Panel standardPanel() {
        return panel(EFFECT, 0.0, TREAT_AT, 0);
    }

    @Test void recoversTheStepEffectAndFindsNoPreTrendWhenThereIsNone() {
        JsonNode out = standardPanel().run(5, 5, -1);

        // Every pre-treatment coefficient is zero by construction, so each must come back
        // zero and the joint test must not fire.
        for (JsonNode e : out.path("effects")) {
            if (e.path("is_pre_treatment").asBoolean()) {
                assertEquals(0.0, e.path("coefficient").asDouble(), 1e-9,
                    e.path("term").asText() + " must be flat");
            }
        }
        assertEquals(EFFECT, effect(out, "lag_0").path("coefficient").asDouble(), 1e-9);
        assertEquals(EFFECT, effect(out, "lag_4").path("coefficient").asDouble(), 1e-9);

        JsonNode pre = out.path("pre_trend_test");
        assertEquals(4, pre.path("leads_tested").asInt(), "r = -5 through -2, reference -1");
        assertFalse(pre.path("pre_trends_detected").asBoolean());
        assertTrue(pre.path("p_value").asDouble() > 0.5,
            "exactly-zero leads must not read as divergence, p was "
                + pre.path("p_value").asDouble());
        assertTrue(pre.path("verdict").asText().contains("does not prove"),
            "a passing test must not be reported as proof of parallel trends");
    }

    @Test void detectsTreatedUnitsThatWereAlreadyDiverging() {
        // The same step effect, but the treated units also drift up by 1.0 per period the
        // whole time. diff_in_diff would report the step and say nothing about the drift.
        JsonNode out = panel(EFFECT, 1.0, TREAT_AT, 0).run(5, 5, -1);

        JsonNode pre = out.path("pre_trend_test");
        assertTrue(pre.path("pre_trends_detected").asBoolean(),
            "a linear pre-existing divergence must be caught");
        assertTrue(pre.path("p_value").asDouble() < 0.01,
            "p was " + pre.path("p_value").asDouble());
        assertTrue(pre.path("verdict").asText().contains("already diverging"));

        // Measured against t-1, the drift makes each earlier period progressively lower.
        assertEquals(-1.0, effect(out, "lead_2").path("coefficient").asDouble(), 1e-9);
        assertEquals(-4.0, effect(out, "lead_5").path("coefficient").asDouble(), 1e-9);
    }

    @Test void referencePeriodHasNoCoefficientAndEverythingIsMeasuredAgainstIt() {
        JsonNode out = standardPanel().run(5, 5, -1);
        assertEquals(-1, out.path("reference_period").asInt());
        for (JsonNode e : out.path("effects")) {
            assertFalse("lead_1".equals(e.path("term").asText()),
                "the reference period must not appear as an estimated effect");
        }
        // Shifting the reference to r = -2 shifts every coefficient by the same amount, which
        // for a flat pre-period means nothing moves.
        JsonNode shifted = standardPanel().run(5, 5, -2);
        assertEquals(EFFECT, effect(shifted, "lag_0").path("coefficient").asDouble(), 1e-9);
        assertEquals(0.0, effect(shifted, "lead_1").path("coefficient").asDouble(), 1e-9,
            "with the reference at -2, period -1 becomes an estimated lead");
    }

    @Test void periodsOutsideTheWindowAreBinnedNotDropped() {
        JsonNode out = standardPanel().run(2, 2, -1);
        assertEquals(120, out.path("n").asInt(),
            "narrowing the window must not shrink the sample");

        JsonNode preBeyond = effect(out, "pre_beyond_2");
        assertTrue(preBeyond.path("is_pre_treatment").asBoolean());
        assertEquals(0.0, preBeyond.path("coefficient").asDouble(), 1e-9);
        assertEquals(EFFECT, effect(out, "post_beyond_2").path("coefficient").asDouble(), 1e-9);
        assertEquals(2, out.path("pre_trend_test").path("leads_tested").asInt(),
            "lead_2 plus the binned pre_beyond_2 term; -1 is the reference, and -5..-3 are "
                + "inside the bin rather than tested separately");
    }

    @Test void staggeredAdoptionIsFlaggedWithItsCaveat() {
        JsonNode simultaneous = standardPanel().run(5, 5, -1);
        assertFalse(simultaneous.path("staggered_adoption").asBoolean());
        assertEquals(1, simultaneous.path("distinct_adoption_periods").asInt());
        assertFalse(simultaneous.path("note").asText().contains("Goodman-Bacon"));

        JsonNode staggered = panel(EFFECT, 0.0, 4, 6).run(5, 5, -1);
        assertTrue(staggered.path("staggered_adoption").asBoolean());
        assertEquals(2, staggered.path("distinct_adoption_periods").asInt());
        assertTrue(staggered.path("note").asText().contains("Goodman-Bacon"),
            "the staggered-timing bias must be named, not left implied");
    }

    @Test void neverTreatedUnitsAreTheComparisonGroupAndAreCounted() {
        JsonNode out = standardPanel().run(5, 5, -1);
        assertEquals(TREATED, out.path("treated_units").asInt());
        assertEquals(CONTROL, out.path("never_treated_units").asInt());
        assertEquals(TREATED + CONTROL, out.path("num_entities").asInt());
        assertEquals(PERIODS, out.path("num_time_periods").asInt());
        assertFalse(out.path("note").asText().contains("NO never-treated units"));
    }

    @Test void withNoNeverTreatedUnitsTheThinnerIdentificationIsStated() {
        // Every unit treated, at two different times — identification comes only from timing.
        List<Double> y = new ArrayList<>();
        List<String> units = new ArrayList<>();
        List<String> times = new ArrayList<>();
        List<Integer> relative = new ArrayList<>();
        for (int u = 0; u < 8; u++) {
            int adopt = u < 4 ? 4 : 6;
            for (int t = 0; t < PERIODS; t++) {
                double v = 10.0 * u + 2.0 * t + 0.1 * ((u + t) % 2 == 0 ? 1 : -1);
                if (t >= adopt) {
                    v += EFFECT;
                }
                y.add(Double.valueOf(v));
                units.add("u" + u);
                times.add(String.valueOf(2010 + t));
                relative.add(Integer.valueOf(t - adopt));
            }
        }
        JsonNode out = new Panel(y, units, times, relative).run(3, 3, -1);
        assertEquals(0, out.path("never_treated_units").asInt());
        assertTrue(out.path("note").asText().contains("NO never-treated units"));
    }

    @Test void refusesDesignsThatCannotIdentifyWhatIsAsked() {
        Panel p = standardPanel();

        IllegalArgumentException postRef = assertThrows(IllegalArgumentException.class,
            () -> p.run(5, 5, 2));
        assertTrue(postRef.getMessage().contains("pre-treatment period"), postRef.getMessage());

        // A reference period no observation sits at leaves nothing to normalize against.
        IllegalArgumentException absentRef = assertThrows(IllegalArgumentException.class,
            () -> p.run(2, 2, -9));
        assertTrue(absentRef.getMessage().contains("nothing to normalize against"),
            absentRef.getMessage());

        // No treated rows at all.
        List<Integer> allNull = new ArrayList<>();
        for (int i = 0; i < p.relative.size(); i++) {
            allNull.add(null);
        }
        Panel untreated = new Panel(p.y, p.units, p.times, allNull);
        IllegalArgumentException none = assertThrows(IllegalArgumentException.class,
            () -> untreated.run(5, 5, -1));
        assertTrue(none.getMessage().contains("no event to study"), none.getMessage());
    }

    @Test void aPreTrendTestThatCouldNotRunIsNotReportedAsPassing() {
        // Treatment lands in the panel's second period, so the only pre-treatment observation
        // any treated unit has IS the reference period. Nothing is left to test — the case a
        // caller hits when the intervention predates the data.
        JsonNode out = panel(EFFECT, 0.0, 1, 0).run(1, 5, -1);
        JsonNode pre = out.path("pre_trend_test");
        assertEquals(0, pre.path("leads_tested").asInt());
        assertEquals("no_pre_periods", pre.path("status").asText());
        assertTrue(pre.path("verdict").asText().contains("NOT the same as one that passed"));
        assertFalse(pre.has("p_value"), "no p-value may be reported for a test that not run");
    }

    private static JsonNode effect(JsonNode out, String term) {
        for (JsonNode e : out.path("effects")) {
            if (term.equals(e.path("term").asText())) {
                return e;
            }
        }
        throw new AssertionError("no effect reported for " + term + " in " + out.path("effects"));
    }

    /** A unit-period panel ready to hand to the estimator. */
    private static final class Panel {
        final List<Double> y;
        final List<String> units;
        final List<String> times;
        final List<Integer> relative;

        Panel(List<Double> y, List<String> units, List<String> times, List<Integer> relative) {
            this.y = y;
            this.units = units;
            this.times = times;
            this.relative = relative;
        }

        JsonNode run(int maxLead, int maxLag, int reference) {
            double[] ys = new double[y.size()];
            for (int i = 0; i < y.size(); i++) {
                ys[i] = y.get(i).doubleValue();
            }
            return StatsEngine.eventStudy(ys, units.toArray(new String[0]),
                times.toArray(new String[0]), relative.toArray(new Integer[0]),
                maxLead, maxLag, reference).toJson(MAPPER);
        }
    }
}
