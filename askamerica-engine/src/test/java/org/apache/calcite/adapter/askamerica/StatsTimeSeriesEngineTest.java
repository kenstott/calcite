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

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks {@link StatsTimeSeriesEngine} against series simulated with known parameters, so an
 * estimate that drifts from the construction values, or an interval that fails to widen, fails
 * here rather than in front of a caller.
 */
@Tag("unit")
class StatsTimeSeriesEngineTest {

    private static double[] ar1(double phi, double mean, int n, long seed) {
        Random rnd = new Random(seed);
        double[] x = new double[n];
        double prev = mean;
        for (int i = 0; i < n + 200; i++) {
            prev = mean + phi * (prev - mean) + rnd.nextGaussian();
            if (i >= 200) {
                x[i - 200] = prev;
            }
        }
        return x;
    }

    private static double[] randomWalk(int n, long seed) {
        Random rnd = new Random(seed);
        double[] x = new double[n];
        double v = 100;
        for (int i = 0; i < n; i++) {
            v += rnd.nextGaussian();
            x[i] = v;
        }
        return x;
    }

    /** Percent returns from a GARCH(1,1) with the given parameters. */
    private static double[] simulateGarch(double omega, double alpha, double beta, int n,
            long seed) {
        Random rnd = new Random(seed);
        double s2 = omega / (1 - alpha - beta);
        double[] r = new double[n];
        for (int i = 0; i < n + 500; i++) {
            double e = Math.sqrt(s2) * rnd.nextGaussian();
            if (i >= 500) {
                r[i - 500] = e;
            }
            s2 = omega + alpha * e * e + beta * s2;
        }
        return r;
    }

    @Test
    void differenceAndIntegrateAreInverses() {
        double[] x = {1, 4, 9, 16, 25, 36};
        assertArrayEquals(new double[]{3, 5, 7, 9, 11}, StatsTimeSeriesEngine.difference(x, 1),
            0);
        assertArrayEquals(new double[]{2, 2, 2, 2}, StatsTimeSeriesEngine.difference(x, 2), 0);
        // A constant second difference of 2 continues the squares: 49, 64.
        assertArrayEquals(new double[]{49, 64},
            StatsTimeSeriesEngine.integrate(x, 2, new double[]{2, 2}), 1e-12);
        assertArrayEquals(new double[]{47, 58},
            StatsTimeSeriesEngine.integrate(x, 1, new double[]{11, 11}), 1e-12);
    }

    @Test
    void psiWeightsMatchClosedForms() {
        double[] ar1 = StatsTimeSeriesEngine.psiWeights(new double[]{0.5}, new double[0], 0, 4);
        assertArrayEquals(new double[]{1, 0.5, 0.25, 0.125}, ar1, 1e-12);
        // Random walk: every shock persists forever, so all psi weights are 1.
        double[] rw = StatsTimeSeriesEngine.psiWeights(new double[0], new double[0], 1, 4);
        assertArrayEquals(new double[]{1, 1, 1, 1}, rw, 1e-12);
        double[] ma1 = StatsTimeSeriesEngine.psiWeights(new double[0], new double[]{0.6}, 0, 3);
        assertArrayEquals(new double[]{1, 0.6, 0}, ma1, 1e-12);
    }

    @Test
    void arimaRecoversAr1AndRevertsToTheMean() {
        double[] x = ar1(0.7, 10.0, 2000, 42);
        StatsTimeSeriesEngine.ArimaResult r =
            StatsTimeSeriesEngine.arima(x, 1, 0, 0, 3, 20, 0.95);
        assertEquals(0.7, r.ar[0], 0.06, "AR(1) coefficient");
        assertEquals(20, r.forecast.length);
        double last = x[x.length - 1];
        assertTrue(Math.abs(r.forecast[19] - 10.0) < Math.abs(last - 10.0) + 0.5,
            "the long-horizon forecast must approach the process mean");
        assertEquals(10.0, r.forecast[19], 1.0);
        // For a stationary process the forecast standard error converges, never explodes.
        assertTrue(r.se[19] > r.se[0]);
        assertTrue(r.se[19] < 1.6 * Math.sqrt(r.sigma2 / (1 - 0.49)),
            "AR(1) forecast error variance is bounded by the process variance");
    }

    @Test
    void arimaRandomWalkForecastsLastValueWithSqrtHorizonIntervals() {
        double[] x = randomWalk(1000, 7);
        StatsTimeSeriesEngine.ArimaResult r =
            StatsTimeSeriesEngine.arima(x, 0, 1, 0, 3, 16, 0.95);
        // The (0,1,0) intercept is the sample drift; over 16 steps it is tiny next to the
        // interval, so the forecast stays within a fraction of a sigma of the last value.
        assertEquals(x[x.length - 1], r.forecast[0], 0.2);
        assertEquals(r.se[0] * 4, r.se[15], 1e-9 * r.se[15] + 1e-9);
        assertTrue(r.lower[15] < r.forecast[15] && r.forecast[15] < r.upper[15]);
    }

    @Test
    void arimaSelectsAnOrderAndReportsTheSearch() {
        double[] x = ar1(0.6, 0.0, 1500, 11);
        StatsTimeSeriesEngine.ArimaResult r =
            StatsTimeSeriesEngine.arima(x, null, 0, null, 2, 5, 0.9);
        assertTrue(r.orderSelected);
        assertEquals(9, r.candidatesTried);
        assertTrue(r.p + r.q >= 1, "an AR(1) process must not be fit as white noise");
        assertTrue(r.ljungBoxP > 0.01, "residuals of the selected model should be white");
    }

    @Test
    void arimaRejectsShortSeriesAndBadInputs() {
        assertThrows(IllegalArgumentException.class,
            () -> StatsTimeSeriesEngine.arima(new double[10], 1, 0, 0, 3, 5, 0.95));
        double[] x = randomWalk(100, 1);
        assertThrows(IllegalArgumentException.class,
            () -> StatsTimeSeriesEngine.arima(x, 1, 3, 0, 3, 5, 0.95));
        assertThrows(IllegalArgumentException.class,
            () -> StatsTimeSeriesEngine.arima(x, 1, 1, 0, 3, 0, 0.95));
        assertThrows(IllegalArgumentException.class,
            () -> StatsTimeSeriesEngine.arima(x, 1, 1, 0, 3, 5, 1.5));
    }

    @Test
    void garchRecoversPersistenceAndRevertsToUnconditionalVolatility() {
        double omega = 0.05;
        double alpha = 0.08;
        double beta = 0.90;
        double[] r = simulateGarch(omega, alpha, beta, 6000, 123);
        StatsTimeSeriesEngine.GarchResult g = StatsTimeSeriesEngine.garch(r, false);
        assertEquals(alpha + beta, g.persistence, 0.04, "persistence");
        assertEquals(alpha, g.alpha, 0.04, "alpha");
        assertEquals(Math.sqrt(omega / (1 - alpha - beta)), g.unconditionalVolPct, 0.35,
            "unconditional volatility");
        assertTrue(g.lbSquaredStdResidP > 0.01,
            "no ARCH effect should remain in the standardized residuals");
        double[] path = StatsTimeSeriesEngine.garchVarianceForecast(g, 250);
        double uncond = g.omega / (1 - g.persistence);
        assertEquals(uncond, path[249], 0.05 * uncond, "long-run variance");
        // The path moves monotonically from the current level toward the unconditional one.
        boolean rising = path[1] > path[0];
        for (int i = 1; i < path.length; i++) {
            assertEquals(rising, path[i] >= path[i - 1] - 1e-12, "monotone at " + i);
        }
    }

    @Test
    void garchStudentTFitsFatTailedReturns() {
        Random rnd = new Random(5);
        double omega = 0.05;
        double alpha = 0.08;
        double beta = 0.90;
        double nu = 6;
        double s2 = omega / (1 - alpha - beta);
        double[] r = new double[5000];
        for (int i = 0; i < 5500; i++) {
            // Standardized t: t_nu scaled to unit variance.
            double chi = 0;
            for (int k = 0; k < (int) nu; k++) {
                double g = rnd.nextGaussian();
                chi += g * g;
            }
            double t = rnd.nextGaussian() / Math.sqrt(chi / nu) * Math.sqrt((nu - 2) / nu);
            double e = Math.sqrt(s2) * t;
            if (i >= 500) {
                r[i - 500] = e;
            }
            s2 = omega + alpha * e * e + beta * s2;
        }
        StatsTimeSeriesEngine.GarchResult g = StatsTimeSeriesEngine.garch(r, true);
        assertEquals(nu, g.nu, 2.5, "degrees of freedom");
        StatsTimeSeriesEngine.GarchResult normal = StatsTimeSeriesEngine.garch(r, false);
        assertTrue(g.logLik > normal.logLik, "t likelihood must beat normal on t data");
    }

    @Test
    void garchRejectsShortOrConstantSeries() {
        assertThrows(IllegalArgumentException.class,
            () -> StatsTimeSeriesEngine.garch(new double[50], false));
        assertThrows(IllegalArgumentException.class,
            () -> StatsTimeSeriesEngine.garch(new double[500], false));
    }

    @Test
    void volatilityForecastBuildsAnOrderedNormalizedBand() {
        double[] r = simulateGarch(0.05, 0.08, 0.90, 1500, 99);
        double[] prices = new double[r.length + 1];
        String[] labels = new String[prices.length];
        prices[0] = 50;
        for (int i = 0; i < r.length; i++) {
            prices[i + 1] = prices[i] * Math.exp(r[i] / 100.0);
        }
        for (int i = 0; i < labels.length; i++) {
            labels[i] = String.format("d%05d", i);
        }
        for (String method : new String[]{"garch", "ewma", "historical"}) {
            ObjectNode o = StatsTimeSeriesEngine.volatilityForecast(prices, labels, 1, 0.95,
                252, method, 0.94, 60, false);
            ObjectNode band = (ObjectNode) o.get("price_band");
            double lo = band.get("normalized_lower").asDouble();
            double med = band.get("normalized_median").asDouble();
            double hi = band.get("normalized_upper").asDouble();
            assertTrue(lo < med && med < hi, method + " band ordering");
            assertTrue(lo > 0.8 && hi < 1.2, method + " one-day band should be a few percent");
            assertEquals(prices[prices.length - 1] * med,
                band.get("price_median").asDouble(), 1e-9);
            assertTrue(o.get("methods").has("garch") == "garch".equals(method));
        }
        ObjectNode wide = StatsTimeSeriesEngine.volatilityForecast(prices, labels, 20, 0.95,
            252, "ewma", 0.94, 60, false);
        ObjectNode narrow = StatsTimeSeriesEngine.volatilityForecast(prices, labels, 1, 0.95,
            252, "ewma", 0.94, 60, false);
        assertTrue(wide.get("price_band").get("cumulative_vol_pct").asDouble()
            > narrow.get("price_band").get("cumulative_vol_pct").asDouble());
    }

    @Test
    void volatilityForecastRejectsUnknownMethodAndNonPositivePrices() {
        double[] prices = new double[200];
        String[] labels = new String[200];
        for (int i = 0; i < 200; i++) {
            prices[i] = 10 + i % 7;
            labels[i] = String.format("d%03d", i);
        }
        assertThrows(IllegalArgumentException.class,
            () -> StatsTimeSeriesEngine.volatilityForecast(prices, labels, 1, 0.95, 252,
                "magic", 0.94, 60, false));
        prices[50] = 0;
        assertThrows(IllegalArgumentException.class,
            () -> StatsTimeSeriesEngine.volatilityForecast(prices, labels, 1, 0.95, 252,
                "ewma", 0.94, 60, false));
    }

    @Test
    void sortByLabelOrdersChronologicallyAndRejectsDuplicates() {
        double[] v = {3, 1, 2};
        String[] labels = {"2026-03-01", "2026-01-01", "2026-02-01"};
        assertArrayEquals(new double[]{1, 2, 3}, StatsTimeSeriesEngine.sortByLabel(v, labels),
            0);
        assertThrows(IllegalArgumentException.class, () -> StatsTimeSeriesEngine.sortByLabel(
            new double[]{1, 2}, new String[]{"2026-01-01", "2026-01-01"}));
    }

    private static double[] pricesFrom(double[] r, double start) {
        double[] prices = new double[r.length + 1];
        prices[0] = start;
        for (int i = 0; i < r.length; i++) {
            prices[i + 1] = prices[i] * Math.exp(r[i] / 100.0);
        }
        return prices;
    }

    private static String[] labels(int n) {
        String[] l = new String[n];
        for (int i = 0; i < n; i++) {
            l[i] = String.format("d%05d", i);
        }
        return l;
    }

    @Test
    void kupiecTestAcceptsNominalAndRejectsGrossMiscoverage() {
        // 5 misses in 100 at a 95% level is exactly nominal.
        assertTrue(StatsTimeSeriesEngine.kupiecPValue(5, 100, 0.95) > 0.9);
        // 25 misses in 100 is wildly off; 0 misses in 200 is far too conservative.
        assertTrue(StatsTimeSeriesEngine.kupiecPValue(25, 100, 0.95) < 0.001);
        assertTrue(StatsTimeSeriesEngine.kupiecPValue(0, 200, 0.95) < 0.01);
    }

    @Test
    void backtestOnGarchDataIsCalibratedForGarchAndScoresEveryMethod() {
        double[] prices = pricesFrom(simulateGarch(0.05, 0.10, 0.85, 2500, 2024), 100);
        ObjectNode o = StatsTimeSeriesEngine.backtestVolatility(prices, labels(prices.length),
            1, 0.95, new String[]{"garch", "ewma", "historical"}, 500, 10, 0.94, 60, false);
        assertEquals(200, o.get("evaluations_per_method").asInt());
        for (String m : new String[]{"garch", "ewma", "historical"}) {
            ObjectNode r = (ObjectNode) o.get("methods").get(m);
            assertEquals(200, r.get("violations").asInt()
                + Math.round(200 * r.get("observed_coverage").asDouble()));
            assertTrue(r.get("mean_band_width_pct").asDouble() > 0, m);
            assertTrue(r.get("mean_interval_score_pct").asDouble()
                >= r.get("mean_band_width_pct").asDouble(), m + " score >= width");
        }
        ObjectNode g = (ObjectNode) o.get("methods").get("garch");
        assertEquals(0.95, g.get("observed_coverage").asDouble(), 0.04);
        assertTrue(g.get("kupiec_p_value").asDouble() > 0.01);
    }

    @Test
    void backtestIsWalkForwardAndDoesNotPeekAhead() {
        // Two series identical up to index 1100 and different after it. Origin t uses prices
        // through t and is scored against price t+1, so every origin before 1099 depends only
        // on the shared prefix: its verdict must be the same in both runs. A fit that read
        // the whole series would let the later shock change earlier verdicts.
        double[] a = new double[1200];
        double[] b = new double[1200];
        java.util.Random rnd = new java.util.Random(3);
        for (int i = 0; i < a.length; i++) {
            double e = rnd.nextGaussian();
            a[i] = 0.5 * e;
            b[i] = (i >= 1100 ? 6.0 : 0.5) * e;
        }
        java.util.List<String> early = new java.util.ArrayList<>();
        java.util.List<String> earlyB = new java.util.ArrayList<>();
        for (String s : violationOrigins(a)) {
            if (s.compareTo(String.format("d%05d", 1099)) < 0) {
                early.add(s);
            }
        }
        for (String s : violationOrigins(b)) {
            if (s.compareTo(String.format("d%05d", 1099)) < 0) {
                earlyB.add(s);
            }
        }
        assertEquals(early, earlyB);
        assertTrue(violationOrigins(b).size() > violationOrigins(a).size(),
            "the shock itself must breach a band fit on calm data");
    }

    private static java.util.List<String> violationOrigins(double[] r) {
        double[] prices = pricesFrom(r, 100);
        ObjectNode o = StatsTimeSeriesEngine.backtestVolatility(prices, labels(prices.length),
            1, 0.95, new String[]{"historical"}, 500, 2, 0.94, 60, false);
        java.util.List<String> out = new java.util.ArrayList<>();
        for (com.fasterxml.jackson.databind.JsonNode n
                : o.get("methods").get("historical").get("violation_origins")) {
            out.add(n.asText());
        }
        return out;
    }

    @Test
    void backtestEnforcesEvaluationCapAndInputChecks() {
        double[] prices = pricesFrom(simulateGarch(0.05, 0.08, 0.90, 1500, 5), 100);
        String[] lab = labels(prices.length);
        assertThrows(IllegalArgumentException.class, () ->
            StatsTimeSeriesEngine.backtestVolatility(prices, lab, 1, 0.95,
                new String[]{"ewma"}, 200, 1, 0.94, 60, false));
        assertThrows(IllegalArgumentException.class, () ->
            StatsTimeSeriesEngine.backtestVolatility(prices, lab, 1, 0.95,
                new String[]{"ewma"}, 50, 20, 0.94, 60, false));
        assertThrows(IllegalArgumentException.class, () ->
            StatsTimeSeriesEngine.backtestVolatility(prices, lab, 1, 0.95,
                new String[]{"magic"}, 500, 20, 0.94, 60, false));
        assertThrows(IllegalArgumentException.class, () ->
            StatsTimeSeriesEngine.backtestVolatility(prices, lab, 1, 0.95,
                new String[]{"ewma"}, 1500, 20, 0.94, 60, false));
    }
}
