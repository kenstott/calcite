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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates {@link StatsMlEngine} against known-answer synthetic data, mirroring the rigor of
 * {@link StatsEngineTest}: not just that Smile's RF/GBM models run, but that they actually
 * recover the signal (and are actually more resistant to confounding, for double ML) that the
 * synthetic design was built to test for.
 */
@Tag("unit")
class StatsMlEngineTest {

    // ─── Flexible regression ───────────────────────────────────────────────────

    @Test void flexibleRegressionRandomForestFitsStrongNonlinearRelationship() {
        int n = 400;
        double[] y = new double[n];
        double[][] x = new double[n][1];
        Random rnd = new Random(3);
        for (int i = 0; i < n; i++) {
            double xi = rnd.nextDouble() * 10 - 5;
            x[i][0] = xi;
            y[i] = xi * xi + rnd.nextGaussian() * 0.5; // y = x^2 -- a linear model would fail badly
        }
        StatsMlEngine.FlexibleRegressionResult result = StatsMlEngine.flexibleRegression(
            y, x, "y", new String[]{"x"}, "random_forest");
        assertTrue(result.r2 > 0.8,
            "random forest should fit a strongly nonlinear relationship well in-sample: r2="
            + result.r2);
    }

    @Test void flexibleRegressionGradientBoostingFitsStrongNonlinearRelationship() {
        int n = 400;
        double[] y = new double[n];
        double[][] x = new double[n][1];
        Random rnd = new Random(4);
        for (int i = 0; i < n; i++) {
            double xi = rnd.nextDouble() * 10 - 5;
            x[i][0] = xi;
            y[i] = xi * xi + rnd.nextGaussian() * 0.5;
        }
        StatsMlEngine.FlexibleRegressionResult result = StatsMlEngine.flexibleRegression(
            y, x, "y", new String[]{"x"}, "gradient_boosting");
        assertTrue(result.r2 > 0.8,
            "gradient boosting should fit a strongly nonlinear relationship well in-sample: r2="
            + result.r2);
    }

    @Test void flexibleRegressionRejectsUnknownMethod() {
        double[] y = {1, 2, 3, 4, 5};
        double[][] x = {{1}, {2}, {3}, {4}, {5}};
        assertThrows(IllegalArgumentException.class,
            () -> StatsMlEngine.flexibleRegression(y, x, "y", new String[]{"x"}, "linear"));
    }

    @Test void featureImportanceRanksRelevantPredictorAboveNoise() {
        // x0 actually drives y; x1 is pure noise unrelated to y. Importance should reflect that.
        int n = 500;
        double[] y = new double[n];
        double[][] x = new double[n][2];
        Random rnd = new Random(9);
        for (int i = 0; i < n; i++) {
            double x0 = rnd.nextDouble() * 10;
            double x1 = rnd.nextDouble() * 10; // unrelated to y
            x[i][0] = x0;
            x[i][1] = x1;
            y[i] = 3 * x0 + rnd.nextGaussian() * 0.5;
        }
        StatsMlEngine.FlexibleRegressionResult result = StatsMlEngine.flexibleRegression(
            y, x, "y", new String[]{"x0", "x1"}, "random_forest");
        assertTrue(result.importance[0] > result.importance[1],
            "predictor actually driving y (importance=" + result.importance[0]
            + ") should rank above pure noise (importance=" + result.importance[1] + ")");
    }

    // ─── Double ML ATE ──────────────────────────────────────────────────────────

    @Test void doubleMlAteRecoversApproximateTreatmentEffectUnderConfounding() {
        // Classic confounded design: a control variable w drives BOTH treatment assignment
        // and the outcome nonlinearly, so a naive unadjusted comparison would be badly biased.
        // True ATE is 4. DML should land much closer to 4 than an unadjusted mean-difference
        // comparison would.
        int n = 1000;
        Random rnd = new Random(21);
        double[] y = new double[n];
        double[] treatment = new double[n];
        double[][] controls = new double[n][1];
        double trueAte = 4.0;
        for (int i = 0; i < n; i++) {
            double w = rnd.nextDouble() * 10;
            double treatProb = 1.0 / (1.0 + Math.exp(-(w - 5))); // w drives treatment assignment
            double d = rnd.nextDouble() < treatProb ? 1.0 : 0.0;
            double outcomeNoise = rnd.nextGaussian();
            y[i] = w * w * 0.3 + trueAte * d + outcomeNoise; // w also drives outcome nonlinearly
            treatment[i] = d;
            controls[i][0] = w;
        }
        StatsMlEngine.DoubleMlResult result = StatsMlEngine.doubleMlAte(
            y, treatment, controls, new String[]{"w"}, 5, "random_forest");

        double naiveMeanDiff = naiveMeanDifference(y, treatment);
        double dmlError = Math.abs(result.ate - trueAte);
        double naiveError = Math.abs(naiveMeanDiff - trueAte);
        assertTrue(dmlError < naiveError,
            "DML ATE error (" + dmlError + ", ate=" + result.ate + ") should be smaller than "
            + "the naive unadjusted mean-difference error (" + naiveError + ", diff="
            + naiveMeanDiff + ") under this confounded design");
    }

    @Test void doubleMlAteRejectsNoControls() {
        double[] y = {1, 2, 3, 4, 5, 6};
        double[] treatment = {0, 1, 0, 1, 0, 1};
        double[][] controls = new double[6][0];
        assertThrows(IllegalArgumentException.class,
            () -> StatsMlEngine.doubleMlAte(y, treatment, controls, new String[]{}, 2,
                "random_forest"));
    }

    @Test void doubleMlAteRejectsTooManyFolds() {
        double[] y = {1, 2, 3, 4, 5, 6};
        double[] treatment = {0, 1, 0, 1, 0, 1};
        double[][] controls = {{1}, {2}, {3}, {4}, {5}, {6}};
        assertThrows(IllegalArgumentException.class,
            () -> StatsMlEngine.doubleMlAte(y, treatment, controls, new String[]{"w"}, 10,
                "random_forest"));
    }

    private static double naiveMeanDifference(double[] y, double[] treatment) {
        double sumTreated = 0;
        double sumControl = 0;
        int nTreated = 0;
        int nControl = 0;
        for (int i = 0; i < y.length; i++) {
            if (treatment[i] > 0.5) {
                sumTreated += y[i];
                nTreated++;
            } else {
                sumControl += y[i];
                nControl++;
            }
        }
        return (sumTreated / nTreated) - (sumControl / nControl);
    }
}
