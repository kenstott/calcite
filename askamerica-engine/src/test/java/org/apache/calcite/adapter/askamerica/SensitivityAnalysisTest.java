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
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates {@link StatsEngine#leaveOneGroupOut} against data built so the right answer is
 * known in advance — a sample where one group demonstrably carries the whole effect, and one
 * where every group agrees.
 *
 * <p>The tool exists to catch a specific failure: a headline coefficient supplied entirely by
 * one jurisdiction. A sensitivity check that reported "robust" on the first sample here would
 * be worse than no check at all, since its whole value to a reader is the assurance that
 * someone looked.
 */
@Tag("unit")
class SensitivityAnalysisTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final double EPS = 1e-9;

    /**
     * Four groups with no x-y relationship at all, plus one group sitting far out along x
     * with a much higher y — the high-leverage shape a real outlier jurisdiction takes.
     *
     * <p>Within the four ordinary groups the slope is exactly zero. The whole apparent
     * relationship is the line drawn between the cluster and the distant group, which is
     * what makes the pooled estimate both highly significant and entirely dependent on one
     * unit.
     */
    private static Sample oneOutlierCarriesIt() {
        List<double[]> rows = new ArrayList<>();
        List<String> groups = new ArrayList<>();
        String[] flat = {"AL", "BR", "CO", "DE"};
        for (String g : flat) {
            for (int x = 1; x <= 5; x++) {
                // Deterministic alternating residual, so the flat groups have variance to
                // estimate against but a slope of exactly zero.
                rows.add(new double[]{10.0 + (x % 2 == 0 ? 0.5 : -0.5), x});
                groups.add(g);
            }
        }
        for (int x = 30; x <= 34; x++) {
            rows.add(new double[]{200.0 + (x % 2 == 0 ? 0.5 : -0.5), x});
            groups.add("DC");
        }
        return new Sample(rows, groups);
    }

    @Test void namesTheGroupThatCarriesTheEffect() {
        Sample s = oneOutlierCarriesIt();
        StatsEngine.SensitivityResult r = StatsEngine.leaveOneGroupOut(
            s.y(), s.x(), new String[]{"x"}, s.groups(), "x");
        JsonNode out = r.toJson(MAPPER);

        JsonNode summary = out.path("summary");
        assertEquals("DC", summary.path("most_influential_group").asText(),
            "the steep group is the one holding the estimate up");
        assertEquals(5, summary.path("groups_tested").asInt());
        assertEquals(5, summary.path("groups_estimated").asInt());
        assertFalse(summary.path("robust").asBoolean(),
            "an estimate supplied by one group must never be reported robust");
        assertTrue(summary.path("significance_flips_at_0_05").asBoolean(),
            "dropping DC must take the result across p=0.05");
        assertTrue(summary.path("max_abs_influence").asDouble() > 1.0,
            "DC moves the coefficient by more than its own standard error");

        // Full sample says there is a relationship; without DC there is none.
        assertTrue(out.path("full_sample").path("coefficient").asDouble() > 1.0,
            "pooled slope should be pulled well above zero by DC");
        JsonNode dcDrop = dropFor(out, "DC");
        assertEquals(0.0, dcDrop.path("coefficient").asDouble(), 0.05,
            "without DC the slope is flat");
        assertTrue(dcDrop.path("p_value").asDouble() > 0.05,
            "without DC nothing is significant");
        assertEquals(5, dcDrop.path("rows_dropped").asInt());
        assertEquals(20, dcDrop.path("rows_kept").asInt());
    }

    @Test void influenceIsTheStandardizedChangeFromRefittingWithoutTheGroup() {
        Sample s = oneOutlierCarriesIt();
        StatsEngine.SensitivityResult r = StatsEngine.leaveOneGroupOut(
            s.y(), s.x(), new String[]{"x"}, s.groups(), "x");
        JsonNode out = r.toJson(MAPPER);
        double baseCoef = out.path("full_sample").path("coefficient").asDouble();

        // Refit by hand without DC and check the reported numbers against it, rather than
        // trusting the loop that produced them.
        List<double[]> kept = new ArrayList<>();
        for (int i = 0; i < s.groups.size(); i++) {
            if (!"DC".equals(s.groups.get(i))) {
                kept.add(s.rows.get(i));
            }
        }
        double[] y = new double[kept.size()];
        double[][] x = new double[kept.size()][1];
        for (int i = 0; i < kept.size(); i++) {
            y[i] = kept.get(i)[0];
            x[i][0] = kept.get(i)[1];
        }
        StatsEngine.OlsResult manual = StatsEngine.ols(y, x, new String[]{"x"});

        JsonNode dcDrop = dropFor(out, "DC");
        assertEquals(manual.coef[1], dcDrop.path("coefficient").asDouble(), EPS);
        assertEquals(manual.se[1], dcDrop.path("std_error").asDouble(), EPS);
        assertEquals((baseCoef - manual.coef[1]) / manual.se[1],
            dcDrop.path("influence").asDouble(), 1e-6,
            "influence is the coefficient change in leave-one-out standard-error units");
    }

    @Test void agreeingGroupsReportRobust() {
        // Both groups follow y = 3 + 2x with the same symmetric deterministic residual, so
        // dropping either changes nothing that matters.
        List<double[]> rows = new ArrayList<>();
        List<String> groups = new ArrayList<>();
        for (String g : new String[]{"NC", "SC"}) {
            for (int x = 1; x <= 10; x++) {
                rows.add(new double[]{3.0 + 2.0 * x + (x % 2 == 0 ? 0.2 : -0.2), x});
                groups.add(g);
            }
        }
        Sample s = new Sample(rows, groups);
        StatsEngine.SensitivityResult r = StatsEngine.leaveOneGroupOut(
            s.y(), s.x(), new String[]{"x"}, s.groups(), "x");
        JsonNode out = r.toJson(MAPPER);
        JsonNode summary = out.path("summary");

        assertTrue(summary.path("robust").asBoolean(), "identical groups must read as robust");
        assertFalse(summary.path("sign_flips").asBoolean());
        assertFalse(summary.path("significance_flips_at_0_05").asBoolean());
        assertTrue(summary.path("coefficient_range").asDouble() < 0.01,
            "the coefficient should barely move");
        assertEquals(2.0, out.path("full_sample").path("coefficient").asDouble(), 0.05);
    }

    @Test void aGroupTooLargeToDropIsReportedNotSkipped() {
        // Removing "big" leaves 2 rows for 1 predictor + intercept — nothing to estimate
        // against. That is the strongest possible statement of influence, so it must appear
        // in the output rather than being quietly passed over.
        List<double[]> rows = new ArrayList<>();
        List<String> groups = new ArrayList<>();
        for (int x = 1; x <= 8; x++) {
            rows.add(new double[]{2.0 * x + (x % 2 == 0 ? 0.3 : -0.3), x});
            groups.add("big");
        }
        for (int x = 1; x <= 2; x++) {
            rows.add(new double[]{2.0 * x, x});
            groups.add("small");
        }
        Sample s = new Sample(rows, groups);
        JsonNode out = StatsEngine.leaveOneGroupOut(
            s.y(), s.x(), new String[]{"x"}, s.groups(), "x").toJson(MAPPER);

        JsonNode bigDrop = dropFor(out, "big");
        assertEquals("not_estimable", bigDrop.path("status").asText());
        assertTrue(bigDrop.path("note").asText().contains("depends on it entirely"));
        assertEquals(1, out.path("summary").path("groups_estimated").asInt());
        assertEquals(2, out.path("summary").path("groups_tested").asInt());
        assertFalse(out.path("summary").path("robust").asBoolean(),
            "a group that cannot be dropped rules out a robust verdict");
    }

    @Test void refusesGroupingsItCannotMeaningfullyTest() {
        List<double[]> rows = new ArrayList<>();
        List<String> groups = new ArrayList<>();
        for (int x = 1; x <= 10; x++) {
            rows.add(new double[]{2.0 * x + (x % 2 == 0 ? 0.3 : -0.3), x});
            groups.add("only");
        }
        Sample single = new Sample(rows, groups);
        IllegalArgumentException tooFew = assertThrows(IllegalArgumentException.class,
            () -> StatsEngine.leaveOneGroupOut(single.y(), single.x(), new String[]{"x"},
                single.groups(), "x"));
        assertTrue(tooFew.getMessage().contains("at least 2 groups"), tooFew.getMessage());

        List<double[]> many = new ArrayList<>();
        List<String> manyGroups = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            many.add(new double[]{i * 2.0, i});
            manyGroups.add("g" + i);
        }
        Sample wide = new Sample(many, manyGroups);
        IllegalArgumentException tooMany = assertThrows(IllegalArgumentException.class,
            () -> StatsEngine.leaveOneGroupOut(wide.y(), wide.x(), new String[]{"x"},
                wide.groups(), "x"));
        assertTrue(tooMany.getMessage().contains("400 distinct values"), tooMany.getMessage());
        assertTrue(tooMany.getMessage().contains("Aggregate"), "the error must say what to do");
    }

    @Test void rejectsATermThatIsNotInTheModel() {
        Sample s = oneOutlierCarriesIt();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> StatsEngine.leaveOneGroupOut(s.y(), s.x(), new String[]{"x"}, s.groups(),
                "not_a_column"));
        assertTrue(e.getMessage().contains("not_a_column"), e.getMessage());
    }

    @Test void tracksTheInterceptWhenAsked() {
        Sample s = oneOutlierCarriesIt();
        ObjectNode out = StatsEngine.leaveOneGroupOut(
            s.y(), s.x(), new String[]{"x"}, s.groups(), "intercept").toJson(MAPPER);
        assertEquals("intercept", out.path("term").asText());
        assertEquals(5, out.path("leave_one_out").size());
    }

    private static JsonNode dropFor(JsonNode out, String group) {
        for (JsonNode n : out.path("leave_one_out")) {
            if (group.equals(n.path("group_omitted").asText())) {
                return n;
            }
        }
        throw new AssertionError("no leave_one_out entry for " + group);
    }

    /** Rows as {outcome, predictor} pairs alongside their group labels. */
    private static final class Sample {
        final List<double[]> rows;
        final List<String> groups;

        Sample(List<double[]> rows, List<String> groups) {
            this.rows = rows;
            this.groups = groups;
        }

        double[] y() {
            double[] out = new double[rows.size()];
            for (int i = 0; i < rows.size(); i++) {
                out[i] = rows.get(i)[0];
            }
            return out;
        }

        double[][] x() {
            double[][] out = new double[rows.size()][1];
            for (int i = 0; i < rows.size(); i++) {
                out[i][0] = rows.get(i)[1];
            }
            return out;
        }

        String[] groups() {
            return groups.toArray(new String[0]);
        }
    }
}
