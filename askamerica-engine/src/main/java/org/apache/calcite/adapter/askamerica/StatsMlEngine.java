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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.formula.Formula;
import smile.data.type.StructType;
import smile.data.vector.DoubleVector;
import smile.data.vector.ValueVector;
import smile.regression.GradientTreeBoost;
import smile.regression.RandomForest;

import java.util.ArrayList;
import java.util.List;

/**
 * Nonlinear regression (random forest / gradient boosting) and Double Machine Learning, via
 * Smile — kept in its own file/dependency boundary from {@link StatsEngine} (Commons Math
 * only) since this is the one tool family that needs a real ML runtime rather than closed-form
 * matrix algebra, mirroring how the read-only engine already isolates the ETL-only/ML-only
 * dependencies it strips from the shadow jar (see build.gradle.kts) — this dependency is real
 * and used at query time, unlike those.
 */
final class StatsMlEngine {

    private StatsMlEngine() {}

    private static final String[] METHODS = {"random_forest", "gradient_boosting"};

    // ─── Flexible (nonlinear) regression + feature importance ─────────────────

    /** Fits a random forest or gradient boosting regression of {@code outcome} on
     *  {@code predictors}, reporting in-sample fit diagnostics (R²/RMSE — NOT held-out
     *  performance; a tree ensemble can overfit its own training rows, so a high in-sample R²
     *  alone doesn't establish the model generalizes) and variable importance (impurity
     *  decrease summed across trees — captures nonlinear/interaction effects a bivariate
     *  {@code corr()} can't, but is a "how much did this variable help the trees split," not a
     *  causal or even necessarily monotonic effect size). */
    static FlexibleRegressionResult flexibleRegression(double[] y, double[][] x,
            String outcome, String[] predictors, String method) {
        validateMethod(method);
        int n = y.length;
        DataFrame data = buildDataFrame(y, outcome, x, predictors, allIndices(n));

        double[] predicted = new double[n];
        double[] importance;
        StructType schema = controlsSchema(x, predictors, allIndices(n));
        if ("gradient_boosting".equals(method)) {
            GradientTreeBoost model = GradientTreeBoost.fit(Formula.lhs(outcome), data);
            importance = model.importance();
            for (int i = 0; i < n; i++) {
                predicted[i] = model.predict(rowTuple(x[i], schema));
            }
        } else {
            RandomForest model = RandomForest.fit(Formula.lhs(outcome), data);
            importance = model.importance();
            for (int i = 0; i < n; i++) {
                predicted[i] = model.predict(rowTuple(x[i], schema));
            }
        }
        double r2 = rSquared(y, predicted);
        double rmse = rmse(y, predicted);
        return new FlexibleRegressionResult(method, predictors, importance, r2, rmse, n);
    }

    static final class FlexibleRegressionResult {
        final String method;
        final String[] predictors;
        final double[] importance;
        final double r2;
        final double rmse;
        final int n;

        FlexibleRegressionResult(String method, String[] predictors, double[] importance,
                double r2, double rmse, int n) {
            this.method = method;
            this.predictors = predictors;
            this.importance = importance;
            this.r2 = r2;
            this.rmse = rmse;
            this.n = n;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = mapper.createObjectNode();
            out.put("method", method);
            out.put("n", n);
            out.put("in_sample_r_squared", r2);
            out.put("in_sample_rmse", rmse);
            ArrayNode imp = mapper.createArrayNode();
            for (int i = 0; i < predictors.length; i++) {
                ObjectNode row = mapper.createObjectNode();
                row.put("predictor", predictors[i]);
                row.put("importance", importance[i]);
                imp.add(row);
            }
            out.set("importance", imp);
            out.put("note", "R²/RMSE are IN-SAMPLE — the model is scored on the same rows it "
                + "was trained on, so this measures fit, not generalization; a tree ensemble "
                + "can reach a high in-sample R² by memorizing noise. Importance is impurity "
                + "decrease summed across trees, not a causal effect size or even guaranteed "
                + "monotonic direction — pair with domain reasoning, not a substitute for it.");
            return out;
        }
    }

    // ─── Double Machine Learning: average treatment effect ────────────────────

    /**
     * Partialling-out Double/Debiased ML (Chernozhukov, Chetverikov, Demirer, Duflo, Hansen,
     * Newey &amp; Robins, "Double/Debiased Machine Learning for Treatment and Structural
     * Parameters," <i>The Econometrics Journal</i>, 2018): cross-fit random-forest or
     * gradient-boosting nuisance models for the treatment and the outcome (each trained on
     * K-1 folds, predicted on the held-out fold — so every observation's residual comes from a
     * model that never saw that observation, avoiding overfitting bias), orthogonalize both to
     * their ML-predicted values, then regress the outcome residual on the treatment residual.
     * The resulting slope is Neyman-orthogonal to first-stage estimation error, which is what
     * makes this valid even though the nuisance models themselves may have real bias/variance
     * from being flexible ML fits rather than parametric regressions.
     *
     * <p>Fold assignment is deterministic ({@code row index % folds}), not random — this
     * server has no fixed-seed RNG contract, and a reproducible tool result matters more here
     * than a "properly randomized" split would for a one-off analysis.
     *
     * <p>This estimates an average treatment effect ASSUMING UNCONFOUNDEDNESS (no omitted
     * variable affects both treatment and outcome beyond what's in {@code controls}) — DML
     * corrects for how the nuisance functions are estimated, not for a fundamentally
     * insufficient control set. It is not a substitute for a valid research design.
     */
    static DoubleMlResult doubleMlAte(double[] y, double[] treatment, double[][] controls,
            String[] controlNames, int folds, String method) {
        validateMethod(method);
        int n = y.length;
        if (controlNames.length < 1) {
            throw new IllegalArgumentException("double ML needs at least one control variable");
        }
        if (folds < 2 || folds > n / 2) {
            throw new IllegalArgumentException(
                "folds must be at least 2 and leave a reasonable number of rows per fold "
                + "(got folds=" + folds + " for n=" + n + ")");
        }

        double[] treatmentResid = new double[n];
        double[] outcomeResid = new double[n];

        for (int fold = 0; fold < folds; fold++) {
            List<Integer> trainList = new ArrayList<>();
            List<Integer> testList = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                if (i % folds == fold) {
                    testList.add(i);
                } else {
                    trainList.add(i);
                }
            }
            int[] trainIdx = toArray(trainList);
            int[] testIdx = toArray(testList);

            DataFrame treatTrainDf =
                buildDataFrame(treatment, "__treatment__", controls, controlNames, trainIdx);
            DataFrame outcomeTrainDf =
                buildDataFrame(y, "__outcome__", controls, controlNames, trainIdx);
            StructType schema = controlsSchema(controls, controlNames, trainIdx);

            if ("gradient_boosting".equals(method)) {
                GradientTreeBoost treatModel =
                    GradientTreeBoost.fit(Formula.lhs("__treatment__"), treatTrainDf);
                GradientTreeBoost outcomeModel =
                    GradientTreeBoost.fit(Formula.lhs("__outcome__"), outcomeTrainDf);
                for (int idx : testIdx) {
                    Tuple row = rowTuple(controls[idx], schema);
                    treatmentResid[idx] = treatment[idx] - treatModel.predict(row);
                    outcomeResid[idx] = y[idx] - outcomeModel.predict(row);
                }
            } else {
                RandomForest treatModel =
                    RandomForest.fit(Formula.lhs("__treatment__"), treatTrainDf);
                RandomForest outcomeModel =
                    RandomForest.fit(Formula.lhs("__outcome__"), outcomeTrainDf);
                for (int idx : testIdx) {
                    Tuple row = rowTuple(controls[idx], schema);
                    treatmentResid[idx] = treatment[idx] - treatModel.predict(row);
                    outcomeResid[idx] = y[idx] - outcomeModel.predict(row);
                }
            }
        }

        // theta_hat = sum(D_tilde * Y_tilde) / sum(D_tilde^2) -- OLS-through-origin of the
        // outcome residual on the treatment residual (both already centered by construction).
        double num = 0;
        double den = 0;
        for (int i = 0; i < n; i++) {
            num += treatmentResid[i] * outcomeResid[i];
            den += treatmentResid[i] * treatmentResid[i];
        }
        if (den <= 0) {
            throw new IllegalArgumentException(
                "treatment residuals have zero variance after partialling out controls — "
                + "the controls may fully determine the treatment, or the treatment has no "
                + "variation left to estimate an effect from");
        }
        double theta = num / den;

        // Standard partialling-out DML asymptotic variance (Chernozhukov et al. 2018, eq. for
        // the Neyman-orthogonal score psi_i = (Y_tilde_i - theta*D_tilde_i) * D_tilde_i):
        // Var(theta_hat) ~= (1/n) * mean(psi_i^2) / mean(D_tilde_i^2)^2.
        double meanD2 = den / n;
        double sumPsi2 = 0;
        for (int i = 0; i < n; i++) {
            double psi = (outcomeResid[i] - theta * treatmentResid[i]) * treatmentResid[i];
            sumPsi2 += psi * psi;
        }
        double meanPsi2 = sumPsi2 / n;
        double variance = meanPsi2 / (meanD2 * meanD2) / n;
        double se = Math.sqrt(variance);
        double tStat = theta / se;
        org.apache.commons.math3.distribution.NormalDistribution normal =
            new org.apache.commons.math3.distribution.NormalDistribution();
        double pValue = 2 * (1 - normal.cumulativeProbability(Math.abs(tStat)));

        return new DoubleMlResult(theta, se, tStat, pValue, n, folds, method,
            controlNames.length);
    }

    static final class DoubleMlResult {
        final double ate;
        final double se;
        final double tStat;
        final double pValue;
        final int n;
        final int folds;
        final String method;
        final int numControls;

        DoubleMlResult(double ate, double se, double tStat, double pValue, int n, int folds,
                String method, int numControls) {
            this.ate = ate;
            this.se = se;
            this.tStat = tStat;
            this.pValue = pValue;
            this.n = n;
            this.folds = folds;
            this.method = method;
            this.numControls = numControls;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = mapper.createObjectNode();
            out.put("ate", ate);
            out.put("std_error", se);
            out.put("t_stat", tStat);
            out.put("p_value", pValue);
            out.put("n", n);
            out.put("folds", folds);
            out.put("nuisance_learner", method);
            out.put("num_controls", numControls);
            out.put("note", "Cross-fit Double ML average treatment effect (Chernozhukov et "
                + "al. 2018). ASSUMES UNCONFOUNDEDNESS — no factor beyond the given controls "
                + "affects both treatment and outcome. DML corrects for using flexible ML to "
                + "estimate the nuisance functions; it does NOT validate that the control set "
                + "is sufficient for a causal interpretation — that's a substantive claim "
                + "about the data-generating process, not something this tool can verify.");
            return out;
        }
    }

    // ─── shared helpers ─────────────────────────────────────────────────────────

    private static void validateMethod(String method) {
        if (!"random_forest".equals(method) && !"gradient_boosting".equals(method)) {
            throw new IllegalArgumentException("method must be one of "
                + java.util.Arrays.toString(METHODS) + ", got '" + method + "'");
        }
    }

    private static int[] allIndices(int n) {
        int[] out = new int[n];
        for (int i = 0; i < n; i++) {
            out[i] = i;
        }
        return out;
    }

    private static int[] toArray(List<Integer> list) {
        int[] out = new int[list.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = list.get(i);
        }
        return out;
    }

    /** Builds a Smile DataFrame with {@code col0} (named {@code col0Name}) plus every column
     *  in {@code others}/{@code otherNames}, restricted to {@code indices} rows — the shape
     *  {@code Formula.lhs(col0Name)} expects (target column + every other column as predictor,
     *  by name). */
    private static DataFrame buildDataFrame(double[] col0, String col0Name, double[][] others,
            String[] otherNames, int[] indices) {
        int n = indices.length;
        double[] c0 = new double[n];
        for (int r = 0; r < n; r++) {
            c0[r] = col0[indices[r]];
        }
        ValueVector[] vectors = new ValueVector[1 + otherNames.length];
        vectors[0] = new DoubleVector(col0Name, c0);
        for (int j = 0; j < otherNames.length; j++) {
            double[] col = new double[n];
            for (int r = 0; r < n; r++) {
                col[r] = others[indices[r]][j];
            }
            vectors[1 + j] = new DoubleVector(otherNames[j], col);
        }
        return new DataFrame(vectors);
    }

    /** Schema for a controls-only (no outcome/treatment column) row, for {@link #rowTuple} at
     *  prediction time — built once per fold/call rather than per row. */
    private static StructType controlsSchema(double[][] x, String[] names, int[] indices) {
        int n = indices.length;
        ValueVector[] vectors = new ValueVector[names.length];
        for (int j = 0; j < names.length; j++) {
            double[] col = new double[n];
            for (int r = 0; r < n; r++) {
                col[r] = x[indices[r]][j];
            }
            vectors[j] = new DoubleVector(names[j], col);
        }
        return new DataFrame(vectors).schema();
    }

    private static Tuple rowTuple(double[] row, StructType schema) {
        return Tuple.of(schema, row);
    }

    private static double rSquared(double[] actual, double[] predicted) {
        double mean = 0;
        for (double v : actual) {
            mean += v;
        }
        mean /= actual.length;
        double ssTot = 0;
        double ssRes = 0;
        for (int i = 0; i < actual.length; i++) {
            ssTot += (actual[i] - mean) * (actual[i] - mean);
            double e = actual[i] - predicted[i];
            ssRes += e * e;
        }
        return ssTot > 0 ? 1 - ssRes / ssTot : Double.NaN;
    }

    private static double rmse(double[] actual, double[] predicted) {
        double sum = 0;
        for (int i = 0; i < actual.length; i++) {
            double e = actual[i] - predicted[i];
            sum += e * e;
        }
        return Math.sqrt(sum / actual.length);
    }
}
