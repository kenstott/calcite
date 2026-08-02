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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates {@link StatsEngine} against known-answer synthetic data — not just that it
 * compiles and runs, but that the numbers it reports are actually correct. This matters more
 * here than for most utility code: a wrong standard error or a biased-but-confidently-reported
 * coefficient is exactly the kind of failure the askamerica-comparative-eval bank's
 * citation-integrity principle exists to catch, applied to this server's own output.
 */
@Tag("unit")
class StatsEngineTest {

    private static final double EPS = 1e-6;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ─── OLS ───────────────────────────────────────────────────────────────────

    @Test void olsRecoversExactCoefficientsWithNoNoise() {
        // y = 5 + 2*x1 - 3*x2, exactly, no noise -- OLS must recover (5, 2, -3) exactly
        // (up to floating point) and R^2 must be 1.
        int n = 20;
        double[] y = new double[n];
        double[][] x = new double[n][2];
        Random rnd = new Random(42);
        for (int i = 0; i < n; i++) {
            double x1 = rnd.nextDouble() * 10;
            double x2 = rnd.nextDouble() * 10;
            x[i][0] = x1;
            x[i][1] = x2;
            y[i] = 5 + 2 * x1 - 3 * x2;
        }
        StatsEngine.OlsResult result = StatsEngine.ols(y, x, new String[]{"x1", "x2"});
        assertEquals(5.0, result.coef[0], EPS, "intercept");
        assertEquals(2.0, result.coef[1], EPS, "x1 coefficient");
        assertEquals(-3.0, result.coef[2], EPS, "x2 coefficient");
        assertEquals(1.0, result.rSquared, EPS, "perfect fit means R^2 = 1");
    }

    @Test void olsWithNoiseRecoversTrueCoefficientsApproximately() {
        // y = 1 + 4*x + noise. With enough observations, the estimated slope should land
        // close to the true 4, and its 95%-ish CI (coef +/- 2*SE) should contain it.
        int n = 500;
        double[] y = new double[n];
        double[][] x = new double[n][1];
        Random rnd = new Random(7);
        for (int i = 0; i < n; i++) {
            double xi = rnd.nextDouble() * 20 - 10;
            x[i][0] = xi;
            y[i] = 1 + 4 * xi + rnd.nextGaussian() * 2;
        }
        StatsEngine.OlsResult result = StatsEngine.ols(y, x, new String[]{"x"});
        assertTrue(Math.abs(result.coef[1] - 4.0) < 0.3,
            "estimated slope " + result.coef[1] + " should be close to true slope 4.0");
        double lower = result.coef[1] - 2 * result.se[1];
        double upper = result.coef[1] + 2 * result.se[1];
        assertTrue(lower < 4.0 && 4.0 < upper,
            "true slope 4.0 should fall within ~95% CI [" + lower + ", " + upper + "]");
    }

    @Test void olsRejectsTooFewObservations() {
        double[] y = {1, 2, 3};
        double[][] x = {{1, 1}, {2, 1}, {3, 1}};
        assertThrows(IllegalArgumentException.class,
            () -> StatsEngine.ols(y, x, new String[]{"a", "b"}));
    }

    // ─── 2SLS ──────────────────────────────────────────────────────────────────

    @Test void iv2slsCorrectsOlsEndogeneityBias() {
        // Classic simulated endogeneity: true model y = 2 + 3*xTrue + u. We only observe
        // xObserved = xTrue + correlated noise that ALSO affects y (so xObserved is
        // endogenous -- correlated with the error term). z is a valid instrument: correlated
        // with xTrue, uncorrelated with u.
        //
        // Naive OLS of y on xObserved will be biased away from the true slope 3. 2SLS using
        // z as an instrument for xObserved should land much closer to 3 than naive OLS does.
        int n = 2000;
        Random rnd = new Random(123);
        double[] y = new double[n];
        double[] xObserved = new double[n];
        double[][] z = new double[n][1];
        for (int i = 0; i < n; i++) {
            double xTrue = rnd.nextGaussian() * 5;
            double confound = rnd.nextGaussian() * 3; // drives both xObserved and y -> endogeneity
            double zi = xTrue + rnd.nextGaussian() * 0.5; // instrument: correlated with xTrue only
            double u = rnd.nextGaussian() * 1.0;
            xObserved[i] = xTrue + confound;
            y[i] = 2 + 3 * xTrue + 2 * confound + u; // confound leaks into y directly too
            z[i][0] = zi;
        }
        double[][] noControls = new double[n][0];

        double[][] xNaive = new double[n][1];
        for (int i = 0; i < n; i++) {
            xNaive[i][0] = xObserved[i];
        }
        StatsEngine.OlsResult naive = StatsEngine.ols(y, xNaive, new String[]{"x"});

        StatsEngine.Iv2slsResult iv = StatsEngine.iv2sls(y, xObserved, z, noControls,
            new String[]{"z"}, new String[]{});

        double trueSlope = 3.0;
        double naiveError = Math.abs(naive.coef[1] - trueSlope);
        double ivError = Math.abs(iv.stage2.coef[1] - trueSlope);
        assertTrue(ivError < naiveError,
            "2SLS error (" + ivError + ") should be smaller than naive OLS error ("
            + naiveError + ") under this endogeneity design — naive slope="
            + naive.coef[1] + ", 2SLS slope=" + iv.stage2.coef[1]);
        assertTrue(ivError < 0.5,
            "2SLS slope " + iv.stage2.coef[1] + " should be reasonably close to true slope 3.0");
        assertTrue(iv.firstStageF > 10, "instrument should not be flagged weak in this design");

        // The corrected SE must differ from what naively trusting the fitted-on-fitted
        // regression's own SE would give -- verified against the standard 2SLS reference
        // (the naive fitted-residual error variance is asymptotically biased UPWARD, i.e.
        // converges to true sigma^2 plus a positive semidefinite term -- see e.g. Davidson &
        // MacKinnon's 2SLS derivation) rather than asserting a specific direction here: the
        // sign of naive-vs-corrected in any ONE finite simulated sample depends on how the
        // confound happens to load onto x vs. y, so the only thing safe (and meaningful) to
        // assert on a single draw is that the correction actually changes the SE -- i.e. this
        // code path is not a no-op -- not which direction it moves in this particular sample.
        double[][] stage1XForCheck = z; // no controls in this test
        StatsEngine.OlsResult stage1 = StatsEngine.ols(xObserved, stage1XForCheck,
            new String[]{"z"});
        double[] fittedX = new double[n];
        for (int i = 0; i < n; i++) {
            fittedX[i] = stage1.coef[0] + stage1.coef[1] * z[i][0];
        }
        double[][] fittedDesign = new double[n][1];
        for (int i = 0; i < n; i++) {
            fittedDesign[i][0] = fittedX[i];
        }
        StatsEngine.OlsResult naiveFittedOls = StatsEngine.ols(y, fittedDesign,
            new String[]{"x_fitted"});
        assertTrue(Math.abs(iv.stage2.se[1] - naiveFittedOls.se[1]) > EPS,
            "corrected 2SLS SE (" + iv.stage2.se[1] + ") should differ from the naive "
            + "OLS-on-fitted-values SE (" + naiveFittedOls.se[1] + ") -- if they're equal, "
            + "the error-variance correction isn't actually being applied");
        // Point estimates from both regressions on the identical fitted design must match
        // exactly -- the correction only touches the reported SE, never the coefficient.
        assertEquals(naiveFittedOls.coef[1], iv.stage2.coef[1], EPS,
            "2SLS point estimate must equal plain OLS-on-fitted-values -- only the SE differs");
    }

    @Test void iv2slsNaiveFittedSeOverstatesInStandardSimultaneityDesign() {
        // The textbook simultaneity setup (the one the "asymptotically biased upward" result
        // is usually illustrated with): the SAME confound v drives both the endogenous
        // regressor and the structural error directly, X = z + v and y = a + b*X + u where
        // u = rho*v + independent noise. Here the naive fitted-residual SE should come out
        // larger than the corrected one, matching the standard reference direction -- unlike
        // the other 2SLS test above, whose asymmetric confound loadings deliberately avoid
        // asserting a direction because the sign isn't guaranteed in every design.
        int n = 3000;
        Random rnd = new Random(99);
        double[] y = new double[n];
        double[] x = new double[n];
        double[][] z = new double[n][1];
        double rho = 0.7;
        for (int i = 0; i < n; i++) {
            double v = rnd.nextGaussian();
            double zi = rnd.nextGaussian() * 4;
            double u = rho * v + Math.sqrt(1 - rho * rho) * rnd.nextGaussian();
            x[i] = zi + v;
            y[i] = 1 + 2 * x[i] + u;
            z[i][0] = zi;
        }
        double[][] noControls = new double[n][0];
        StatsEngine.Iv2slsResult iv = StatsEngine.iv2sls(y, x, z, noControls,
            new String[]{"z"}, new String[]{});

        StatsEngine.OlsResult stage1 = StatsEngine.ols(x, z, new String[]{"z"});
        double[][] fittedDesign = new double[n][1];
        for (int i = 0; i < n; i++) {
            fittedDesign[i][0] = stage1.coef[0] + stage1.coef[1] * z[i][0];
        }
        StatsEngine.OlsResult naiveFittedOls = StatsEngine.ols(y, fittedDesign,
            new String[]{"x_fitted"});

        assertTrue(iv.stage2.se[1] < naiveFittedOls.se[1],
            "in this standard simultaneity design the corrected 2SLS SE ("
            + iv.stage2.se[1] + ") should be smaller than the naive OLS-on-fitted-values SE ("
            + naiveFittedOls.se[1] + "), matching the documented asymptotic-upward-bias "
            + "direction of the naive estimator");
    }

    @Test void iv2slsSeProducesValidConfidenceInterval() {
        // Regression test for a real bug caught during robust_regression testing: Commons
        // Math's estimateRegressionParametersVariance() returns (X'X)^-1 ALONE (verified via
        // javap on AbstractMultipleLinearRegression -- calculateBetaVariance(), no error-
        // variance scaling), not errorVariance*(X'X)^-1 as this codebase originally assumed.
        // The original rescaling code divided paramVariance by stage2Fitted.errorVariance
        // before multiplying by the correct error variance -- an extra, wrong division that
        // silently collapsed every corrected SE by roughly a factor of sqrt(errorVariance).
        // Neither existing 2SLS test above catches this: both only check a SE comparison
        // (naive vs. corrected), and a systematic scale error on the corrected side alone
        // doesn't necessarily flip that comparison. This test instead checks the corrected SE
        // is actually large enough that a coef +/- 3*SE interval contains the true slope --
        // the failure mode an understated SE produces directly.
        int n = 2000;
        Random rnd = new Random(77);
        double[] y = new double[n];
        double[] x = new double[n];
        double[][] z = new double[n][1];
        double trueSlope = 3.0;
        double rho = 0.7;
        for (int i = 0; i < n; i++) {
            double v = rnd.nextGaussian();
            double zi = rnd.nextGaussian() * 4;
            double u = rho * v + Math.sqrt(1 - rho * rho) * rnd.nextGaussian();
            x[i] = zi + v;
            y[i] = 1 + trueSlope * x[i] + u;
            z[i][0] = zi;
        }
        double[][] noControls = new double[n][0];
        StatsEngine.Iv2slsResult iv = StatsEngine.iv2sls(y, x, z, noControls,
            new String[]{"z"}, new String[]{});
        double lower = iv.stage2.coef[1] - 3 * iv.stage2.se[1];
        double upper = iv.stage2.coef[1] + 3 * iv.stage2.se[1];
        assertTrue(lower < trueSlope && trueSlope < upper,
            "true slope " + trueSlope + " should fall within the 2SLS coef +/- 3*SE interval ["
            + lower + ", " + upper + "] -- SE=" + iv.stage2.se[1] + "; an understated SE (the "
            + "recovery-trick bug) produces an implausibly narrow interval that excludes truth");
    }

    // ─── Diff-in-diff ──────────────────────────────────────────────────────────

    @Test void diffInDiffRecoversExactEffectWithNoNoise() {
        // Classic 2x2 design, no noise, 5 units per cell (20 total -- a single unit per cell
        // is exactly identified but leaves zero degrees of freedom to estimate a standard
        // error from, which ols() correctly rejects): control-pre=10, control-post=12
        // (common trend +2), treated-pre=10, treated-post=17 (+2 common trend, +5 treatment
        // effect). DiD estimate should be exactly 5.
        int perCell = 5;
        int n = perCell * 4;
        double[] y = new double[n];
        double[] treatment = new double[n];
        double[] post = new double[n];
        double[][] cells = {{0, 0, 10}, {0, 1, 12}, {1, 0, 10}, {1, 1, 17}};
        int idx = 0;
        for (double[] cell : cells) {
            for (int r = 0; r < perCell; r++) {
                treatment[idx] = cell[0];
                post[idx] = cell[1];
                y[idx] = cell[2];
                idx++;
            }
        }
        double[][] noControls = new double[n][0];
        StatsEngine.DiffInDiffResult result =
            StatsEngine.diffInDiff(y, treatment, post, noControls, new String[]{});
        assertEquals(5.0, result.reg.coef[3], EPS, "DiD (treatment x post) estimate");
    }

    // ─── Panel fixed effects ───────────────────────────────────────────────────

    @Test void panelFixedEffectsRecoversExactSlopeWithNoNoise() {
        // Two-way fixed effects: y = entityEffect[e] + timeEffect[t] + 2*x, exactly, no noise.
        // The demeaning estimator should recover the true slope 2 exactly regardless of the
        // (arbitrary) entity/time effects, since those are fully absorbed rather than estimated
        // as free parameters.
        String[] entities = {"A", "B", "C"};
        String[] times = {"2020", "2021", "2022", "2023"};
        Map<String, Double> entityEffect = new LinkedHashMap<>();
        entityEffect.put("A", 10.0);
        entityEffect.put("B", -5.0);
        entityEffect.put("C", 20.0);
        Map<String, Double> timeEffect = new LinkedHashMap<>();
        timeEffect.put("2020", 0.0);
        timeEffect.put("2021", 3.0);
        timeEffect.put("2022", -2.0);
        timeEffect.put("2023", 7.0);

        List<Double> yList = new ArrayList<>();
        List<Double> xList = new ArrayList<>();
        List<String> entityList = new ArrayList<>();
        List<String> timeList = new ArrayList<>();
        Random rnd = new Random(11);
        for (String e : entities) {
            for (String t : times) {
                double xi = rnd.nextDouble() * 10;
                yList.add(entityEffect.get(e) + timeEffect.get(t) + 2 * xi);
                xList.add(xi);
                entityList.add(e);
                timeList.add(t);
            }
        }
        int n = yList.size();
        double[] y = new double[n];
        double[][] x = new double[n][1];
        String[] entityIds = new String[n];
        String[] timeIds = new String[n];
        for (int i = 0; i < n; i++) {
            y[i] = yList.get(i);
            x[i][0] = xList.get(i);
            entityIds[i] = entityList.get(i);
            timeIds[i] = timeList.get(i);
        }

        StatsEngine.PanelFixedEffectsResult result =
            StatsEngine.panelFixedEffects(y, x, new String[]{"x"}, entityIds, timeIds);
        assertEquals(2.0, result.coef[0], EPS, "slope on x");
        assertEquals(3, result.numEntities);
        assertEquals(4, result.numTimes);
        // n=12, k=1, correctDof = 12 - 1 - (3+4-1) = 5
        assertEquals(5, result.dof);
    }

    @Test void panelFixedEffectsSeProducesValidConfidenceInterval() {
        // Regression test for the same (X'X)^-1-recovery scaling bug caught in
        // iv2slsSeProducesValidConfidenceInterval (see that test's comment for the root
        // cause). The no-noise test above can't catch it: with near-zero noise, both the
        // correct SE and the bugged (silently collapsed) SE are near zero, so the exact-
        // coefficient assertion passes either way. This test adds real noise and checks the
        // resulting SE is large enough that a coef +/- 3*SE interval actually contains the
        // true slope, and isn't implausibly close to zero.
        int numEntities = 20;
        int numTimes = 10;
        String[] entities = new String[numEntities];
        for (int e = 0; e < numEntities; e++) {
            entities[e] = "E" + e;
        }
        String[] times = new String[numTimes];
        for (int t = 0; t < numTimes; t++) {
            times[t] = "T" + t;
        }
        Random rnd = new Random(55);
        Map<String, Double> entityEffect = new LinkedHashMap<>();
        for (String e : entities) {
            entityEffect.put(e, rnd.nextGaussian() * 10);
        }
        Map<String, Double> timeEffect = new LinkedHashMap<>();
        for (String t : times) {
            timeEffect.put(t, rnd.nextGaussian() * 5);
        }
        double trueSlope = 3.0;
        List<Double> yList = new ArrayList<>();
        List<Double> xList = new ArrayList<>();
        List<String> entityList = new ArrayList<>();
        List<String> timeList = new ArrayList<>();
        for (String e : entities) {
            for (String t : times) {
                double xi = rnd.nextDouble() * 10;
                double yi = entityEffect.get(e) + timeEffect.get(t) + trueSlope * xi
                    + rnd.nextGaussian() * 2;
                yList.add(yi);
                xList.add(xi);
                entityList.add(e);
                timeList.add(t);
            }
        }
        int n = yList.size();
        double[] y = new double[n];
        double[][] x = new double[n][1];
        String[] entityIds = new String[n];
        String[] timeIds = new String[n];
        for (int i = 0; i < n; i++) {
            y[i] = yList.get(i);
            x[i][0] = xList.get(i);
            entityIds[i] = entityList.get(i);
            timeIds[i] = timeList.get(i);
        }
        StatsEngine.PanelFixedEffectsResult result =
            StatsEngine.panelFixedEffects(y, x, new String[]{"x"}, entityIds, timeIds);
        double lower = result.coef[0] - 3 * result.se[0];
        double upper = result.coef[0] + 3 * result.se[0];
        assertTrue(lower < trueSlope && trueSlope < upper,
            "true slope " + trueSlope + " should fall within coef +/- 3*SE [" + lower + ", "
            + upper + "] -- SE=" + result.se[0] + "; an understated SE (the recovery-trick "
            + "bug) produces an implausibly narrow interval that excludes the true value");
        assertTrue(result.se[0] > 0.01,
            "SE (" + result.se[0] + ") should not collapse to near-zero given real noise in "
            + "the data -- a near-zero SE here is the signature of the (X'X)^-1-recovery "
            + "scaling bug");
    }

    @Test void panelFixedEffectsRejectsInsufficientDegreesOfFreedom() {
        // 1 entity, 2 time periods -- no room left to separate entity/time effects from the
        // single predictor's slope.
        double[] y = {1, 2};
        double[][] x = {{1}, {2}};
        String[] entityIds = {"A", "A"};
        String[] timeIds = {"2020", "2021"};
        assertThrows(IllegalArgumentException.class,
            () -> StatsEngine.panelFixedEffects(y, x, new String[]{"x"}, entityIds, timeIds));
    }

    // ─── Robust regression ─────────────────────────────────────────────────────

    @Test void robustRegressionCoefficientsMatchPlainOls() {
        int n = 200;
        double[] y = new double[n];
        double[][] x = new double[n][1];
        Random rnd = new Random(5);
        for (int i = 0; i < n; i++) {
            double xi = rnd.nextDouble() * 10;
            x[i][0] = xi;
            y[i] = 3 + 2 * xi + rnd.nextGaussian();
        }
        StatsEngine.OlsResult plain = StatsEngine.ols(y, x, new String[]{"x"});
        StatsEngine.RobustRegressionResult robust =
            StatsEngine.robustRegression(y, x, new String[]{"x"}, null);
        assertEquals(plain.coef[0], robust.reg.coef[0], EPS, "intercept must match plain OLS");
        assertEquals(plain.coef[1], robust.reg.coef[1], EPS, "slope must match plain OLS");
    }

    @Test void robustRegressionHc1SeDiffersUnderHeteroskedasticity() {
        // Error variance grows with x -- a textbook heteroskedasticity design. HC1 SEs should
        // differ from the (misspecified, constant-variance-assuming) plain OLS SEs.
        int n = 1000;
        double[] y = new double[n];
        double[][] x = new double[n][1];
        Random rnd = new Random(17);
        for (int i = 0; i < n; i++) {
            double xi = rnd.nextDouble() * 10 + 1;
            x[i][0] = xi;
            double noise = rnd.nextGaussian() * xi; // variance grows with x
            y[i] = 1 + 2 * xi + noise;
        }
        StatsEngine.OlsResult plain = StatsEngine.ols(y, x, new String[]{"x"});
        StatsEngine.RobustRegressionResult robust =
            StatsEngine.robustRegression(y, x, new String[]{"x"}, null);
        assertTrue(Math.abs(robust.reg.se[1] - plain.se[1]) > EPS,
            "HC1 SE (" + robust.reg.se[1] + ") should differ from plain OLS SE (" + plain.se[1]
            + ") under heteroskedasticity -- if equal, the correction isn't being applied");
    }

    @Test void robustRegressionClusterRobustRejectsTooFewClusters() {
        double[] y = {1, 2, 3, 4};
        double[][] x = {{1}, {2}, {3}, {4}};
        String[] clusterIds = {"only", "only", "only", "only"};
        assertThrows(IllegalArgumentException.class,
            () -> StatsEngine.robustRegression(y, x, new String[]{"x"}, clusterIds));
    }

    @Test void robustRegressionClusterRobustSeDiffersFromPlainUnderWithinClusterCorrelation() {
        // Moulton-style design (Moulton 1990): x has a strong CLUSTER-level component -- nearly
        // constant within a cluster, as with a state-level policy variable observed over many
        // years of that state -- and the error also carries a shared cluster-level shock.
        // Clustering errors alone (with x independently varying within cluster) does NOT
        // reliably inflate the naive SE; it's the combination of a cluster-correlated
        // regressor AND cluster-correlated errors that makes ignoring clustering understate
        // uncertainty, because effectively only clustersCount independent data points exist
        // for identifying the slope, not n. clustersCount must be large (not just perCluster):
        // with too few clusters (e.g. 20), OLS's 1-parameter slope can overfit the
        // cluster-level (xCluster, clusterShock) relationship by chance in a single draw,
        // artificially shrinking cluster-level residuals and making the cluster-robust SE
        // come out SMALLER than naive on that particular draw -- the actual finite-sample
        // failure mode CR1 is known for with few clusters, not a bug in the formula.
        int clustersCount = 200;
        int perCluster = 5;
        int n = clustersCount * perCluster;
        double[] y = new double[n];
        double[][] x = new double[n][1];
        String[] clusterIds = new String[n];
        Random rnd = new Random(31);
        int idx = 0;
        for (int g = 0; g < clustersCount; g++) {
            double xCluster = rnd.nextDouble() * 10; // cluster-level regressor value
            double clusterShock = rnd.nextGaussian() * 5; // shared within-cluster error component
            for (int r = 0; r < perCluster; r++) {
                double xi = xCluster + rnd.nextGaussian() * 0.1; // small within-cluster jitter
                x[idx][0] = xi;
                y[idx] = 1 + 2 * xi + clusterShock + rnd.nextGaussian() * 0.1;
                clusterIds[idx] = "cluster" + g;
                idx++;
            }
        }
        StatsEngine.OlsResult plain = StatsEngine.ols(y, x, new String[]{"x"});
        StatsEngine.RobustRegressionResult robust =
            StatsEngine.robustRegression(y, x, new String[]{"x"}, clusterIds);
        assertTrue(robust.reg.se[1] > plain.se[1],
            "cluster-robust SE (" + robust.reg.se[1] + ") should be larger than plain OLS SE ("
            + plain.se[1] + ") when errors are strongly correlated within cluster");
    }

    // ─── Hypothesis tests ──────────────────────────────────────────────────────

    @Test void tTestDetectsObviousDifference() {
        Map<String, double[]> groups = new LinkedHashMap<>();
        groups.put("a", new double[]{1, 2, 1, 2, 1, 2, 1, 2, 1, 2});
        groups.put("b", new double[]{100, 101, 99, 102, 98, 100, 101, 99, 100, 101});
        ObjectNode out = StatsEngine.hypothesisTest(MAPPER, "t_test", groups, null, null);
        assertTrue(out.get("p_value").asDouble() < 0.001,
            "an obviously huge mean difference must be significant: " + out);
    }

    @Test void tTestDoesNotFlagIdenticalGroupsAsSignificant() {
        Map<String, double[]> groups = new LinkedHashMap<>();
        groups.put("a", new double[]{5, 5, 5, 5, 5});
        groups.put("b", new double[]{5, 5, 5, 5, 5});
        ObjectNode out = StatsEngine.hypothesisTest(MAPPER, "t_test", groups, null, null);
        // Identical constant groups -> t-statistic is 0/0 (NaN); assert it's NOT a spuriously
        // "significant" low p-value, which would be the dangerous failure mode.
        double p = out.get("p_value").asDouble();
        assertTrue(Double.isNaN(p) || p > 0.05,
            "identical groups must not be reported as significantly different: p=" + p);
    }

    @Test void anovaDetectsGroupDifference() {
        Map<String, double[]> groups = new LinkedHashMap<>();
        groups.put("low", new double[]{1, 2, 1, 2, 1});
        groups.put("mid", new double[]{10, 11, 9, 10, 11});
        groups.put("high", new double[]{20, 21, 19, 20, 21});
        ObjectNode out = StatsEngine.hypothesisTest(MAPPER, "anova", groups, null, null);
        assertTrue(out.get("p_value").asDouble() < 0.001,
            "three clearly separated groups must be significant: " + out);
    }

    @Test void chiSquareDetectsDependence() {
        // Strong dependence: row A always col X, row B always col Y.
        long[][] table = {
            {100, 0},
            {0, 100},
        };
        ObjectNode out = StatsEngine.hypothesisTest(MAPPER, "chi_square",
            java.util.Collections.emptyMap(), null, table);
        assertTrue(out.get("p_value").asDouble() < 0.001,
            "a perfectly dependent contingency table must be significant: " + out);
    }

    @Test void ksTestDetectsDistributionDifference() {
        Map<String, double[]> groups = new LinkedHashMap<>();
        double[] a = new double[100];
        double[] b = new double[100];
        Random rnd = new Random(1);
        for (int i = 0; i < 100; i++) {
            a[i] = rnd.nextGaussian();
            b[i] = rnd.nextGaussian() + 5; // shifted distribution
        }
        groups.put("a", a);
        groups.put("b", b);
        ObjectNode out = StatsEngine.hypothesisTest(MAPPER, "ks_test", groups, null, null);
        assertTrue(out.get("p_value").asDouble() < 0.001,
            "two clearly different distributions must be significant: " + out);
    }
}
