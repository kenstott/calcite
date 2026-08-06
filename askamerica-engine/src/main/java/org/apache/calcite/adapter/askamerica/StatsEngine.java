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

import org.apache.commons.math3.distribution.FDistribution;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.commons.math3.stat.inference.OneWayAnova;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Multivariate regression and hypothesis-testing tools backed by Apache Commons Math, for
 * MCP tools that need real matrix algebra (multiple covariates, proper multi-instrument
 * 2SLS) beyond what DuckDB's single-pass SQL aggregates ({@code corr}, {@code regr_slope},
 * ...) can express — see {@code FileAdapterFunctions}/{@code DuckDBStatsFunctions} for those.
 *
 * <p>Every entry point here takes already-extracted numeric columns (see
 * {@link #extractColumns}), never touches SQL parsing itself, and returns a structured JSON
 * result (coefficients / standard errors / diagnostics) — never raw rows. Callers extract the
 * FULL result set of their SQL (not the client-facing row-limited {@code query()} path — a
 * regression needs every row, not a preview capped for LLM readability).
 */
final class StatsEngine {

    private StatsEngine() {}

    /** Safety ceiling on rows pulled into memory for a single stats computation — generous
     *  for any realistic state/county/year panel, bounded so a runaway query can't exhaust
     *  heap. Distinct from {@code query()}'s client-facing {@code MAX_LIMIT}, which caps what
     *  the LLM sees, not what a regression is allowed to compute over. */
    private static final int STATS_MAX_ROWS = 200_000;

    // ─── Data extraction ───────────────────────────────────────────────────────

    /** Runs {@code sql} and extracts the named columns as an {@link Extraction}, one row per
     *  observation in {@code columns} order. Rows where ANY requested column is null are
     *  dropped (regression and hypothesis tests need complete cases) — the returned
     *  extraction reports how many were dropped so a tool can surface that rather than
     *  silently changing the effective sample size underneath the caller. */
    static Extraction extractColumns(Connection conn, String sql, String[] columns)
            throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = stmt.executeQuery(sql);
            try {
                int[] idx = new int[columns.length];
                for (int i = 0; i < columns.length; i++) {
                    idx[i] = rs.findColumn(columns[i]);
                }
                List<double[]> rows = new ArrayList<>();
                int totalRows = 0;
                int droppedForNull = 0;
                while (rs.next()) {
                    totalRows++;
                    if (totalRows > STATS_MAX_ROWS) {
                        throw new SQLException("Query returned more than " + STATS_MAX_ROWS
                            + " rows — narrow the SQL (add a WHERE/GROUP BY) before running a "
                            + "statistical model over it.");
                    }
                    double[] row = new double[columns.length];
                    boolean hasNull = false;
                    for (int i = 0; i < columns.length; i++) {
                        double v = rs.getDouble(idx[i]);
                        if (rs.wasNull()) {
                            hasNull = true;
                            break;
                        }
                        row[i] = v;
                    }
                    if (hasNull) {
                        droppedForNull++;
                    } else {
                        rows.add(row);
                    }
                }
                return new Extraction(columns, rows.toArray(new double[0][]), totalRows,
                    droppedForNull);
            } finally {
                rs.close();
            }
        } finally {
            stmt.close();
        }
    }

    /** Runs {@code sql} and extracts {@code numericColumns} (via {@code getDouble}) alongside
     *  {@code labelColumns} (categorical, via {@code getString}) — for panel_fixed_effects'
     *  entity/time labels and robust_regression's cluster labels, where the row-alignment
     *  policy needs to match {@link #extractColumns}' (drop any row null in ANY requested
     *  column, numeric or categorical) but a label column can't go through the double-only
     *  path. */
    static LabeledExtraction extractColumnsWithLabels(Connection conn, String sql,
            String[] numericColumns, String[] labelColumns) throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = stmt.executeQuery(sql);
            try {
                int[] numIdx = new int[numericColumns.length];
                for (int i = 0; i < numericColumns.length; i++) {
                    numIdx[i] = rs.findColumn(numericColumns[i]);
                }
                int[] labelIdx = new int[labelColumns.length];
                for (int i = 0; i < labelColumns.length; i++) {
                    labelIdx[i] = rs.findColumn(labelColumns[i]);
                }
                List<double[]> rows = new ArrayList<>();
                List<String[]> labelRows = new ArrayList<>();
                int totalRows = 0;
                int droppedForNull = 0;
                while (rs.next()) {
                    totalRows++;
                    if (totalRows > STATS_MAX_ROWS) {
                        throw new SQLException("Query returned more than " + STATS_MAX_ROWS
                            + " rows — narrow the SQL before running a statistical model over "
                            + "it.");
                    }
                    double[] row = new double[numericColumns.length];
                    boolean hasNull = false;
                    for (int i = 0; i < numericColumns.length; i++) {
                        double v = rs.getDouble(numIdx[i]);
                        if (rs.wasNull()) {
                            hasNull = true;
                            break;
                        }
                        row[i] = v;
                    }
                    String[] labelRow = new String[labelColumns.length];
                    if (!hasNull) {
                        for (int i = 0; i < labelColumns.length; i++) {
                            String v = rs.getString(labelIdx[i]);
                            if (v == null) {
                                hasNull = true;
                                break;
                            }
                            labelRow[i] = v;
                        }
                    }
                    if (hasNull) {
                        droppedForNull++;
                    } else {
                        rows.add(row);
                        labelRows.add(labelRow);
                    }
                }
                return new LabeledExtraction(numericColumns, rows.toArray(new double[0][]),
                    labelColumns, labelRows.toArray(new String[0][]), totalRows, droppedForNull);
            } finally {
                rs.close();
            }
        } finally {
            stmt.close();
        }
    }

    static final class LabeledExtraction {
        final String[] numericColumns;
        final double[][] rows;
        final String[] labelColumns;
        final String[][] labelRows;
        final int totalRows;
        final int droppedForNull;

        LabeledExtraction(String[] numericColumns, double[][] rows, String[] labelColumns,
                String[][] labelRows, int totalRows, int droppedForNull) {
            this.numericColumns = numericColumns;
            this.rows = rows;
            this.labelColumns = labelColumns;
            this.labelRows = labelRows;
            this.totalRows = totalRows;
            this.droppedForNull = droppedForNull;
        }

        double[] column(String name) {
            int idx = indexOf(numericColumns, name);
            double[] out = new double[rows.length];
            for (int r = 0; r < rows.length; r++) {
                out[r] = rows[r][idx];
            }
            return out;
        }

        double[][] columnsFor(String[] names) {
            int[] idx = new int[names.length];
            for (int i = 0; i < names.length; i++) {
                idx[i] = indexOf(numericColumns, names[i]);
            }
            double[][] out = new double[rows.length][names.length];
            for (int r = 0; r < rows.length; r++) {
                for (int c = 0; c < idx.length; c++) {
                    out[r][c] = rows[r][idx[c]];
                }
            }
            return out;
        }

        String[] labelColumn(String name) {
            int idx = indexOf(labelColumns, name);
            String[] out = new String[labelRows.length];
            for (int r = 0; r < labelRows.length; r++) {
                out[r] = labelRows[r][idx];
            }
            return out;
        }

        private static int indexOf(String[] haystack, String name) {
            for (int i = 0; i < haystack.length; i++) {
                if (haystack[i].equals(name)) {
                    return i;
                }
            }
            throw new IllegalArgumentException(
                "column '" + name + "' not found in " + Arrays.toString(haystack));
        }

        int n() {
            return rows.length;
        }
    }

    /** Runs {@code sql} and buckets {@code valueCol} (numeric) by the distinct string values
     *  of {@code groupCol} (categorical — read via {@code getString}, not {@code getDouble}),
     *  for t-test/ANOVA/KS-test group construction. Rows with a null in either column are
     *  dropped. Preserves first-seen group order (a {@link LinkedHashMap}) so a two-group
     *  t-test's "group_a"/"group_b" labeling is deterministic rather than hash-order-dependent. */
    static Map<String, double[]> extractGroupedColumn(Connection conn, String sql,
            String groupCol, String valueCol) throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = stmt.executeQuery(sql);
            try {
                int groupIdx = rs.findColumn(groupCol);
                int valueIdx = rs.findColumn(valueCol);
                Map<String, List<Double>> buckets = new java.util.LinkedHashMap<>();
                int rowCount = 0;
                while (rs.next()) {
                    if (++rowCount > STATS_MAX_ROWS) {
                        throw new SQLException("Query returned more than " + STATS_MAX_ROWS
                            + " rows — narrow the SQL before running a statistical test over "
                            + "it.");
                    }
                    String g = rs.getString(groupIdx);
                    double v = rs.getDouble(valueIdx);
                    if (rs.wasNull() || g == null) {
                        continue;
                    }
                    buckets.computeIfAbsent(g, k -> new ArrayList<>()).add(v);
                }
                Map<String, double[]> out = new java.util.LinkedHashMap<>();
                for (Map.Entry<String, List<Double>> e : buckets.entrySet()) {
                    double[] arr = new double[e.getValue().size()];
                    for (int i = 0; i < arr.length; i++) {
                        arr[i] = e.getValue().get(i);
                    }
                    out.put(e.getKey(), arr);
                }
                return out;
            } finally {
                rs.close();
            }
        } finally {
            stmt.close();
        }
    }

    /** Runs {@code sql} and cross-tabulates two categorical columns into a contingency table
     *  (row label -> col label -> count) for a chi-square test of independence. Rows with a
     *  null in either column are dropped. */
    static ContingencyTable extractContingencyTable(Connection conn, String sql, String rowCol,
            String colCol) throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = stmt.executeQuery(sql);
            try {
                int rowIdx = rs.findColumn(rowCol);
                int colIdx = rs.findColumn(colCol);
                List<String> rowLabels = new ArrayList<>();
                List<String> colLabels = new ArrayList<>();
                Map<String, Integer> rowIndex = new java.util.LinkedHashMap<>();
                Map<String, Integer> colIndex = new java.util.LinkedHashMap<>();
                Map<Long, Long> counts = new java.util.LinkedHashMap<>();
                int rowCount = 0;
                while (rs.next()) {
                    if (++rowCount > STATS_MAX_ROWS) {
                        throw new SQLException("Query returned more than " + STATS_MAX_ROWS
                            + " rows — narrow the SQL before running a chi-square test over "
                            + "it.");
                    }
                    String r = rs.getString(rowIdx);
                    String c = rs.getString(colIdx);
                    if (r == null || c == null) {
                        continue;
                    }
                    int ri = rowIndex.computeIfAbsent(r, k -> {
                        rowLabels.add(k);
                        return rowLabels.size() - 1;
                    });
                    int ci = colIndex.computeIfAbsent(c, k -> {
                        colLabels.add(k);
                        return colLabels.size() - 1;
                    });
                    long key = ((long) ri << 32) | (ci & 0xffffffffL);
                    counts.merge(key, 1L, (a, b) -> a + b);
                }
                long[][] table = new long[rowLabels.size()][colLabels.size()];
                for (Map.Entry<Long, Long> e : counts.entrySet()) {
                    int ri = (int) (e.getKey() >> 32);
                    int ci = (int) (long) e.getKey();
                    table[ri][ci] = e.getValue();
                }
                return new ContingencyTable(rowLabels, colLabels, table);
            } finally {
                rs.close();
            }
        } finally {
            stmt.close();
        }
    }

    static final class ContingencyTable {
        final List<String> rowLabels;
        final List<String> colLabels;
        final long[][] counts;

        ContingencyTable(List<String> rowLabels, List<String> colLabels, long[][] counts) {
            this.rowLabels = rowLabels;
            this.colLabels = colLabels;
            this.counts = counts;
        }
    }

    /** Column-major-accessible result of {@link #extractColumns}, plus how many source rows
     *  were seen vs. kept after dropping incomplete cases. */
    static final class Extraction {
        final String[] columns;
        final double[][] rows;   // rows[i] is one observation, in `columns` order
        final int totalRows;
        final int droppedForNull;

        Extraction(String[] columns, double[][] rows, int totalRows, int droppedForNull) {
            this.columns = columns;
            this.rows = rows;
            this.totalRows = totalRows;
            this.droppedForNull = droppedForNull;
        }

        double[] column(String name) {
            return column(indexOf(name));
        }

        double[] column(int i) {
            double[] out = new double[rows.length];
            for (int r = 0; r < rows.length; r++) {
                out[r] = rows[r][i];
            }
            return out;
        }

        double[][] columnsFor(String[] names) {
            int[] idx = new int[names.length];
            for (int i = 0; i < names.length; i++) {
                idx[i] = indexOf(names[i]);
            }
            double[][] out = new double[rows.length][names.length];
            for (int r = 0; r < rows.length; r++) {
                for (int c = 0; c < idx.length; c++) {
                    out[r][c] = rows[r][idx[c]];
                }
            }
            return out;
        }

        int indexOf(String name) {
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].equals(name)) {
                    return i;
                }
            }
            throw new IllegalArgumentException(
                "column '" + name + "' not found in " + Arrays.toString(columns));
        }

        int n() {
            return rows.length;
        }
    }

    // ─── OLS ───────────────────────────────────────────────────────────────────

    /** Multivariate OLS: {@code y ~ intercept + x[0] + x[1] + ...}, via Commons Math's
     *  {@link OLSMultipleLinearRegression} (QR decomposition — no explicit matrix inverse).
     *  Returns coefficients, standard errors, t-stats, two-sided p-values, R²/adjusted R²,
     *  and the overall F-test. */
    static OlsResult ols(double[] y, double[][] x, String[] xNames) {
        if (y.length != x.length) {
            throw new IllegalArgumentException(
                "y has " + y.length + " rows, x has " + x.length);
        }
        int n = y.length;
        int k = xNames.length; // predictors, not counting intercept
        int dof = n - k - 1;
        if (dof < 1) {
            throw new IllegalArgumentException("only " + n + " complete observations for "
                + k + " predictors + intercept — need at least " + (k + 2)
                + " to estimate standard errors");
        }
        OLSMultipleLinearRegression reg = new OLSMultipleLinearRegression();
        reg.newSampleData(y, x);
        double[] beta = reg.estimateRegressionParameters();
        double[] se = reg.estimateRegressionParametersStandardErrors();
        double errorVariance = reg.estimateErrorVariance();
        double[][] paramVariance = reg.estimateRegressionParametersVariance();

        TDistribution tDist = new TDistribution(dof);
        String[] names = new String[k + 1];
        names[0] = "intercept";
        System.arraycopy(xNames, 0, names, 1, k);
        double[] tStat = new double[k + 1];
        double[] pValue = new double[k + 1];
        for (int i = 0; i <= k; i++) {
            tStat[i] = beta[i] / se[i];
            pValue[i] = 2 * (1 - tDist.cumulativeProbability(Math.abs(tStat[i])));
        }
        double rSquared = reg.calculateRSquared();
        double adjRSquared = reg.calculateAdjustedRSquared();
        double fStat;
        double fPValue;
        if (k > 0 && rSquared < 1.0) {
            fStat = (rSquared / k) / ((1 - rSquared) / dof);
            FDistribution fDist = new FDistribution(k, dof);
            fPValue = 1 - fDist.cumulativeProbability(fStat);
        } else {
            fStat = Double.NaN;
            fPValue = Double.NaN;
        }
        return new OlsResult(names, beta, se, tStat, pValue, rSquared, adjRSquared, fStat,
            fPValue, n, dof, errorVariance, paramVariance);
    }

    static final class OlsResult {
        final String[] names;
        final double[] coef;
        final double[] se;
        final double[] tStat;
        final double[] pValue;
        final double rSquared;
        final double adjRSquared;
        final double fStat;
        final double fPValue;
        final int n;
        final int dof;
        /** sigma-hat-squared of THIS regression's own residuals — for 2SLS this is the
         *  fitted-on-fitted variance, not the correct one; see {@link #iv2sls}. */
        final double errorVariance;
        /** {@code (X'X)^-1} ALONE, not multiplied by errorVariance — this is exactly what
         *  Commons Math's {@code estimateRegressionParametersVariance()} returns (verified via
         *  javap on {@code AbstractMultipleLinearRegression}: it's {@code calculateBetaVariance()}
         *  with no error-variance scaling; {@code estimateRegressionParametersStandardErrors()}
         *  does its own separate {@code sqrt(errorVariance * betaVariance[i][i])}). Exposed so
         *  {@link #iv2sls}, {@link #panelFixedEffects}, and {@link #robustRegression} can plug
         *  in a DIFFERENT (correctly-computed) error variance by directly multiplying this
         *  matrix, without redoing the matrix inversion Commons Math already did internally —
         *  do NOT divide this by errorVariance first, it is not scaled by it to begin with. */
        final double[][] paramVariance;

        OlsResult(String[] names, double[] coef, double[] se, double[] tStat, double[] pValue,
                double rSquared, double adjRSquared, double fStat, double fPValue, int n,
                int dof, double errorVariance, double[][] paramVariance) {
            this.names = names;
            this.coef = coef;
            this.se = se;
            this.tStat = tStat;
            this.pValue = pValue;
            this.rSquared = rSquared;
            this.adjRSquared = adjRSquared;
            this.fStat = fStat;
            this.fPValue = fPValue;
            this.n = n;
            this.dof = dof;
            this.errorVariance = errorVariance;
            this.paramVariance = paramVariance;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = mapper.createObjectNode();
            ArrayNode coefs = mapper.createArrayNode();
            for (int i = 0; i < names.length; i++) {
                ObjectNode c = mapper.createObjectNode();
                c.put("term", names[i]);
                c.put("coefficient", coef[i]);
                c.put("std_error", se[i]);
                c.put("t_stat", tStat[i]);
                c.put("p_value", pValue[i]);
                coefs.add(c);
            }
            out.set("coefficients", coefs);
            out.put("r_squared", rSquared);
            out.put("adj_r_squared", adjRSquared);
            out.put("f_statistic", fStat);
            out.put("f_p_value", fPValue);
            out.put("n", n);
            out.put("degrees_of_freedom", dof);
            return out;
        }
    }

    // ─── 2SLS (instrumental variables) ─────────────────────────────────────────

    /**
     * Two-stage least squares for a single endogenous regressor with one or more instruments
     * and optional exogenous controls (controls enter both stages).
     *
     * <p>Stage 1: {@code endogenous ~ instruments + controls} — the fitted values from this
     * regression are the "exogenous part" of the endogenous variable.
     * Stage 2 point estimates: {@code y ~ fitted(endogenous) + controls}. This equals the
     * textbook 2SLS coefficient formula {@code (Ẑ'Ẑ)^-1 Ẑ'y}, so the point estimates from
     * regressing y on the fitted design are correct as-is.
     *
     * <p>Standard errors are NOT simply this second regression's own reported SEs — Commons
     * Math computes those from fitted-on-fitted residuals (y - Ẑβ), whose implied error
     * variance is asymptotically biased UPWARD (it converges to the true σ² plus a positive
     * semidefinite term — see e.g. the standard 2SLS derivation in Davidson & MacKinnon,
     * <i>Econometric Theory and Methods</i>), not downward — a naive "two OLS calls" 2SLS
     * implementation that trusts the second regression's own SEs directly reports
     * systematically OVERSTATED uncertainty, not understated. The correct formula
     * (e.g. Wooldridge, <i>Introductory Econometrics</i>, ch. 15) rescales the SAME
     * (Ẑ'Ẑ)^-1 matrix Commons Math already computed by the CORRECT error variance — estimated
     * from residuals against the ACTUAL endogenous variable, not the fitted one, using the
     * 2SLS coefficients. Commons Math's own {@code paramVariance} IS (Ẑ'Ẑ)^-1 already (see
     * {@link OlsResult#paramVariance}) — this method multiplies it directly by the correct
     * error variance, avoiding a second manual matrix inversion.
     */
    static Iv2slsResult iv2sls(double[] y, double[] endogenous, double[][] instruments,
            double[][] controls, String[] instrumentNames, String[] controlNames) {
        int n = y.length;

        // Stage 1: endogenous ~ instruments + controls.
        double[][] stage1X = concatColumns(instruments, controls);
        String[] stage1Names = concatNames(instrumentNames, controlNames);
        OlsResult stage1 = ols(endogenous, stage1X, stage1Names);
        double[] fitted = predict(stage1, stage1X);

        // Weak-instrument screen: the stage-1 overall F-stat. This is conservative (an
        // overall F including controls, not a textbook partial-F on the instruments alone)
        // — Stock & Yogo's F < 10 rule of thumb is the standard threshold to flag.
        double firstStageF = stage1.fStat;

        // Stage 2, fitted design — gives the correct 2SLS point estimates, and a
        // parameter-variance matrix we'll rescale rather than trust directly.
        double[][] stage2XFitted = prependColumn(fitted, controls);
        String[] stage2Names = prependName("endogenous_2sls", controlNames);
        OlsResult stage2Fitted = ols(y, stage2XFitted, stage2Names);

        // Correct error variance: residuals against the ACTUAL endogenous variable, using
        // the 2SLS coefficients from stage2Fitted.
        double[][] stage2XActual = prependColumn(endogenous, controls);
        double[] beta = stage2Fitted.coef;
        int k = stage2Names.length;
        int dof = n - k - 1;
        double ssrActual = 0;
        for (int i = 0; i < n; i++) {
            double predicted = beta[0];
            for (int j = 0; j < k; j++) {
                predicted += beta[j + 1] * stage2XActual[i][j];
            }
            double resid = y[i] - predicted;
            ssrActual += resid * resid;
        }
        double correctErrorVariance = ssrActual / dof;

        // stage2Fitted.paramVariance IS (Ẑ'Ẑ)^-1 already (Commons Math does not scale it by
        // its own error variance — see OlsResult#paramVariance) — multiply directly by the
        // correct error variance, then take sqrt(diag(.)) for the correct standard errors.
        double[] correctSe = new double[k + 1];
        double[] correctTStat = new double[k + 1];
        double[] correctPValue = new double[k + 1];
        TDistribution tDist = new TDistribution(dof);
        for (int i = 0; i <= k; i++) {
            double correctVar = stage2Fitted.paramVariance[i][i] * correctErrorVariance;
            correctSe[i] = Math.sqrt(correctVar);
            correctTStat[i] = beta[i] / correctSe[i];
            correctPValue[i] = 2 * (1 - tDist.cumulativeProbability(Math.abs(correctTStat[i])));
        }

        OlsResult corrected = new OlsResult(stage2Fitted.names, beta, correctSe, correctTStat,
            correctPValue, stage2Fitted.rSquared, stage2Fitted.adjRSquared, stage2Fitted.fStat,
            stage2Fitted.fPValue, n, dof, correctErrorVariance, stage2Fitted.paramVariance);
        return new Iv2slsResult(corrected, firstStageF, instrumentNames.length);
    }

    static final class Iv2slsResult {
        final OlsResult stage2;
        final double firstStageF;
        final int numInstruments;

        Iv2slsResult(OlsResult stage2, double firstStageF, int numInstruments) {
            this.stage2 = stage2;
            this.firstStageF = firstStageF;
            this.numInstruments = numInstruments;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = stage2.toJson(mapper);
            out.put("first_stage_f_statistic", firstStageF);
            out.put("num_instruments", numInstruments);
            out.put("weak_instrument_warning", firstStageF < 10);
            out.put("note", "Standard errors use the corrected 2SLS formula (residuals "
                + "against the actual endogenous variable, not the first-stage fitted "
                + "values) — not naive OLS-on-fitted-values SEs, whose implied error "
                + "variance is asymptotically biased upward (overstated), not understated.");
            return out;
        }
    }

    // ─── Difference-in-differences ─────────────────────────────────────────────

    /** DiD as OLS with a treatment×post interaction term: {@code y ~ treatment + post +
     *  treatment*post + controls}. The interaction coefficient IS the DiD estimate — the
     *  average treatment effect on the treated, under the parallel-trends assumption (which
     *  this method does not itself test; that requires pre-period data this single
     *  regression doesn't see). */
    static DiffInDiffResult diffInDiff(double[] y, double[] treatment, double[] post,
            double[][] controls, String[] controlNames) {
        int n = y.length;
        double[] interaction = new double[n];
        for (int i = 0; i < n; i++) {
            interaction[i] = treatment[i] * post[i];
        }
        // Build [treatment, post, interaction, ...controls] explicitly.
        int k = 3 + controlNames.length;
        double[][] fullX = new double[n][k];
        for (int i = 0; i < n; i++) {
            fullX[i][0] = treatment[i];
            fullX[i][1] = post[i];
            fullX[i][2] = interaction[i];
            for (int c = 0; c < controlNames.length; c++) {
                fullX[i][3 + c] = controls[i][c];
            }
        }
        String[] names = new String[k];
        names[0] = "treatment";
        names[1] = "post";
        names[2] = "treatment_x_post";
        System.arraycopy(controlNames, 0, names, 3, controlNames.length);
        OlsResult reg = ols(y, fullX, names);
        return new DiffInDiffResult(reg);
    }

    static final class DiffInDiffResult {
        final OlsResult reg;

        DiffInDiffResult(OlsResult reg) {
            this.reg = reg;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = reg.toJson(mapper);
            // treatment_x_post is always index 3 in the coefficients array (intercept,
            // treatment, post, treatment_x_post, ...controls).
            out.put("did_estimate", reg.coef[3]);
            out.put("did_std_error", reg.se[3]);
            out.put("did_p_value", reg.pValue[3]);
            out.put("note", "did_estimate is the treatment x post interaction coefficient — "
                + "the estimated average treatment effect on the treated, valid under the "
                + "parallel-trends assumption. This regression does not itself test parallel "
                + "trends; that requires checking pre-period trends separately.");
            return out;
        }
    }

    // ─── Panel (two-way) fixed effects ─────────────────────────────────────────

    /**
     * Two-way (entity + time) fixed-effects panel regression via the within (demeaning)
     * estimator: subtract each variable's entity mean and time mean, add back the grand mean,
     * then run OLS on the demeaned data. This is the Frisch-Waugh-Lovell equivalent of
     * including an entity dummy and a time dummy for every entity/period — same coefficients
     * as the full dummy-variable regression, without actually building N_entities + N_periods
     * dummy columns.
     *
     * <p>Point estimates from demeaned OLS are correct as-is. Standard errors are NOT simply
     * Commons Math's own reported SEs for that demeaned regression — it has no way to know
     * (N_entities - 1) + (N_periods - 1) additional degrees of freedom were absorbed by the
     * demeaning rather than actually estimated as free slope parameters, so it would use the
     * wrong residual degrees of freedom. This method recomputes the sum of squared residuals
     * directly and divides by the CORRECT degrees of freedom
     * ({@code n - k - (numEntities + numTimes - 1)}), then multiplies that correct sigma²
     * directly by Commons Math's own {@code (X'X)^-1} — which is what
     * {@code estimateRegressionParametersVariance()} already returns unscaled (see
     * {@link OlsResult#paramVariance}) — same approach as {@link #iv2sls}.
     */
    static PanelFixedEffectsResult panelFixedEffects(double[] y, double[][] x, String[] xNames,
            String[] entityIds, String[] timeIds) {
        int n = y.length;
        int k = xNames.length;
        if (n != entityIds.length || n != timeIds.length) {
            throw new IllegalArgumentException("y/x, entity, and time arrays must be the same length");
        }

        double grandMeanY = mean(y);
        Map<String, Double> entityMeanY = groupMeans(entityIds, y);
        Map<String, Double> timeMeanY = groupMeans(timeIds, y);
        double[] yTilde = new double[n];
        for (int i = 0; i < n; i++) {
            yTilde[i] = y[i] - entityMeanY.get(entityIds[i]) - timeMeanY.get(timeIds[i])
                + grandMeanY;
        }

        double[][] xTilde = new double[n][k];
        for (int j = 0; j < k; j++) {
            double[] col = new double[n];
            for (int i = 0; i < n; i++) {
                col[i] = x[i][j];
            }
            double grandMeanX = mean(col);
            Map<String, Double> entityMeanX = groupMeans(entityIds, col);
            Map<String, Double> timeMeanX = groupMeans(timeIds, col);
            for (int i = 0; i < n; i++) {
                xTilde[i][j] = col[i] - entityMeanX.get(entityIds[i]) - timeMeanX.get(timeIds[i])
                    + grandMeanX;
            }
        }

        int numEntities = new LinkedHashSet<>(Arrays.asList(entityIds)).size();
        int numTimes = new LinkedHashSet<>(Arrays.asList(timeIds)).size();
        int correctDof = n - k - (numEntities + numTimes - 1);
        if (correctDof < 1) {
            throw new IllegalArgumentException("only " + n + " observations for " + k
                + " predictors across " + numEntities + " entities and " + numTimes
                + " periods — need at least " + (k + numEntities + numTimes) + " to estimate "
                + "two-way fixed effects with any residual degrees of freedom");
        }

        OLSMultipleLinearRegression reg = new OLSMultipleLinearRegression();
        reg.setNoIntercept(true);
        reg.newSampleData(yTilde, xTilde);
        double[] beta = reg.estimateRegressionParameters();
        // (X'X)^-1 on the demeaned design — Commons Math's estimateRegressionParametersVariance()
        // returns this UNSCALED (see OlsResult#paramVariance), so it's multiplied directly by
        // the correct sigma2 below, with no division by reg's own (wrong-dof) error variance.
        double[][] xTildeXTildeInv = reg.estimateRegressionParametersVariance();

        double ssr = 0;
        for (int i = 0; i < n; i++) {
            double predicted = 0;
            for (int j = 0; j < k; j++) {
                predicted += beta[j] * xTilde[i][j];
            }
            double resid = yTilde[i] - predicted;
            ssr += resid * resid;
        }
        double correctSigma2 = ssr / correctDof;

        TDistribution tDist = new TDistribution(correctDof);
        double[] se = new double[k];
        double[] tStat = new double[k];
        double[] pValue = new double[k];
        for (int i = 0; i < k; i++) {
            se[i] = Math.sqrt(xTildeXTildeInv[i][i] * correctSigma2);
            tStat[i] = beta[i] / se[i];
            pValue[i] = 2 * (1 - tDist.cumulativeProbability(Math.abs(tStat[i])));
        }

        return new PanelFixedEffectsResult(xNames, beta, se, tStat, pValue, n, correctDof,
            numEntities, numTimes);
    }

    static final class PanelFixedEffectsResult {
        final String[] names;
        final double[] coef;
        final double[] se;
        final double[] tStat;
        final double[] pValue;
        final int n;
        final int dof;
        final int numEntities;
        final int numTimes;

        PanelFixedEffectsResult(String[] names, double[] coef, double[] se, double[] tStat,
                double[] pValue, int n, int dof, int numEntities, int numTimes) {
            this.names = names;
            this.coef = coef;
            this.se = se;
            this.tStat = tStat;
            this.pValue = pValue;
            this.n = n;
            this.dof = dof;
            this.numEntities = numEntities;
            this.numTimes = numTimes;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = mapper.createObjectNode();
            ArrayNode coefs = mapper.createArrayNode();
            for (int i = 0; i < names.length; i++) {
                ObjectNode c = mapper.createObjectNode();
                c.put("term", names[i]);
                c.put("coefficient", coef[i]);
                c.put("std_error", se[i]);
                c.put("t_stat", tStat[i]);
                c.put("p_value", pValue[i]);
                coefs.add(c);
            }
            out.set("coefficients", coefs);
            out.put("n", n);
            out.put("degrees_of_freedom", dof);
            out.put("num_entities", numEntities);
            out.put("num_time_periods", numTimes);
            out.put("note", "Two-way fixed effects (within/demeaning estimator) — no "
                + "intercept term is reported because it's absorbed into the entity and time "
                + "effects, not because none exists.");
            return out;
        }
    }

    private static Map<String, Double> groupMeans(String[] groupIds, double[] values) {
        Map<String, Double> sum = new LinkedHashMap<>();
        Map<String, Integer> count = new LinkedHashMap<>();
        for (int i = 0; i < groupIds.length; i++) {
            sum.merge(groupIds[i], values[i], (a, b) -> a + b);
            count.merge(groupIds[i], 1, (a, b) -> a + b);
        }
        Map<String, Double> out = new LinkedHashMap<>();
        for (Map.Entry<String, Double> e : sum.entrySet()) {
            out.put(e.getKey(), e.getValue() / count.get(e.getKey()));
        }
        return out;
    }

    // ─── Heteroskedasticity- / cluster-robust standard errors ──────────────────

    /**
     * Re-estimates OLS standard errors with a robust "sandwich" covariance estimator,
     * keeping the same point estimates. With {@code clusterIds == null}: White/HC1
     * heteroskedasticity-robust SEs (valid when error variance differs across observations,
     * without assuming a specific form). With {@code clusterIds} given: cluster-robust SEs
     * (valid when errors are correlated WITHIN a cluster, e.g. multiple years for the same
     * state) — degrees of freedom for the cluster case use {@code numClusters - 1}, the
     * standard convention (asymptotics run in the number of clusters, not the number of
     * observations).
     */
    static RobustRegressionResult robustRegression(double[] y, double[][] x, String[] xNames,
            String[] clusterIds) {
        OlsResult base = ols(y, x, xNames);
        int n = y.length;
        int k = xNames.length;
        int p = k + 1; // including intercept

        // base.paramVariance IS (X'X)^-1 already (Commons Math does not scale it by its own
        // error variance — see OlsResult#paramVariance) — used directly in the sandwich below.
        RealMatrix xtxInv = new Array2DRowRealMatrix(base.paramVariance);

        double[][] xAugArr = new double[n][p];
        double[] resid = new double[n];
        for (int i = 0; i < n; i++) {
            xAugArr[i][0] = 1.0;
            double predicted = base.coef[0];
            for (int j = 0; j < k; j++) {
                xAugArr[i][1 + j] = x[i][j];
                predicted += base.coef[1 + j] * x[i][j];
            }
            resid[i] = y[i] - predicted;
        }
        RealMatrix xAug = new Array2DRowRealMatrix(xAugArr);

        RealMatrix meat;
        double correction;
        int dof;
        String method;
        if (clusterIds == null) {
            RealMatrix meatAcc = new Array2DRowRealMatrix(p, p);
            for (int i = 0; i < n; i++) {
                RealMatrix xi = xAug.getRowMatrix(i).transpose(); // p x 1
                meatAcc = meatAcc.add(xi.multiply(xi.transpose()).scalarMultiply(resid[i] * resid[i]));
            }
            meat = meatAcc;
            correction = (double) n / (n - p);
            dof = n - p;
            method = "HC1 heteroskedasticity-robust";
        } else {
            if (clusterIds.length != n) {
                throw new IllegalArgumentException("clusterIds must be the same length as y");
            }
            Map<String, RealMatrix> clusterScore = new LinkedHashMap<>();
            for (int i = 0; i < n; i++) {
                String g = clusterIds[i];
                RealMatrix xi = xAug.getRowMatrix(i).transpose().scalarMultiply(resid[i]);
                clusterScore.merge(g, xi, RealMatrix::add);
            }
            RealMatrix meatAcc = new Array2DRowRealMatrix(p, p);
            for (RealMatrix score : clusterScore.values()) {
                meatAcc = meatAcc.add(score.multiply(score.transpose()));
            }
            meat = meatAcc;
            int numClusters = clusterScore.size();
            if (numClusters < 2) {
                throw new IllegalArgumentException(
                    "cluster-robust SEs need at least 2 clusters, got " + numClusters);
            }
            correction = ((double) numClusters / (numClusters - 1)) * ((double) (n - 1) / (n - p));
            dof = numClusters - 1;
            method = "cluster-robust (" + numClusters + " clusters)";
        }

        RealMatrix sandwich = xtxInv.multiply(meat).multiply(xtxInv).scalarMultiply(correction);
        TDistribution tDist = new TDistribution(dof);
        double[] robustSe = new double[p];
        double[] robustT = new double[p];
        double[] robustP = new double[p];
        for (int i = 0; i < p; i++) {
            robustSe[i] = Math.sqrt(sandwich.getEntry(i, i));
            robustT[i] = base.coef[i] / robustSe[i];
            robustP[i] = 2 * (1 - tDist.cumulativeProbability(Math.abs(robustT[i])));
        }

        OlsResult corrected = new OlsResult(base.names, base.coef, robustSe, robustT, robustP,
            base.rSquared, base.adjRSquared, base.fStat, base.fPValue, n, dof, base.errorVariance,
            base.paramVariance);
        return new RobustRegressionResult(corrected, method);
    }

    static final class RobustRegressionResult {
        final OlsResult reg;
        final String method;

        RobustRegressionResult(OlsResult reg, String method) {
            this.reg = reg;
            this.method = method;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = reg.toJson(mapper);
            out.put("se_method", method);
            out.put("note", "Coefficients are identical to plain OLS — only the standard "
                + "errors (and therefore t-stats/p-values) differ. r_squared/f_statistic "
                + "above are still the plain-OLS versions, not robust F-tests.");
            return out;
        }
    }

    // ─── Leave-one-group-out sensitivity ────────────────────────────────────────

    /** Refits beyond this and a single tool call turns into a multi-minute scan. The caller
     *  is told to aggregate rather than being silently given a truncated sweep. */
    private static final int MAX_SENSITIVITY_GROUPS = 200;

    /**
     * Refit the same OLS once per group with that group's rows removed, and report how far
     * the tracked coefficient moves.
     *
     * <p>A regression over jurisdictions is routinely carried by one of them — DC, a census
     * region with a single large state, one outlier county. The full-sample estimate cannot
     * show this: it reports one number whether every unit agrees or a single unit supplies
     * the whole effect. Dropping each group in turn is the cheapest test that distinguishes
     * the two, and the only diagnostic here that can invalidate a headline result rather
     * than decorate it.
     *
     * <p>{@code influence} is the standardized change in the tracked coefficient — the
     * baseline estimate minus the leave-one-out estimate, divided by the leave-one-out
     * standard error (DFBETA in standard-error units). It answers "would omitting this one
     * group have moved the published number by more than its own uncertainty".
     *
     * <p>Groups whose removal leaves too few observations to estimate are reported as such,
     * not skipped: a group large enough to break the model is the strongest possible
     * statement about its influence.
     */
    static SensitivityResult leaveOneGroupOut(double[] y, double[][] x, String[] xNames,
            String[] groupIds, String trackedTerm) {
        if (y.length != groupIds.length) {
            throw new IllegalArgumentException(
                "y has " + y.length + " rows, group column has " + groupIds.length);
        }
        OlsResult baseline = ols(y, x, xNames);
        int termIdx = indexOfName(baseline.names, trackedTerm);

        // First-seen order, so the report reads in the order the SQL returned.
        LinkedHashSet<String> groupSet = new LinkedHashSet<>(Arrays.asList(groupIds));
        if (groupSet.size() > MAX_SENSITIVITY_GROUPS) {
            throw new IllegalArgumentException("group column has " + groupSet.size()
                + " distinct values — leave-one-out would run that many regressions. "
                + "Aggregate to a coarser grouping (state rather than county, say) or "
                + "restrict the SQL to the groups in question; the limit is "
                + MAX_SENSITIVITY_GROUPS + ".");
        }
        if (groupSet.size() < 2) {
            throw new IllegalArgumentException("group column has " + groupSet.size()
                + " distinct value(s) — leave-one-out needs at least 2 groups to compare");
        }

        List<LeaveOneOut> drops = new ArrayList<>();
        for (String group : groupSet) {
            int kept = 0;
            for (int i = 0; i < groupIds.length; i++) {
                if (!group.equals(groupIds[i])) {
                    kept++;
                }
            }
            int dropped = y.length - kept;
            // ols() needs n - k - 1 >= 1 to have any residual degrees of freedom left.
            if (kept < xNames.length + 2) {
                drops.add(new LeaveOneOut(group, dropped, kept));
                continue;
            }
            double[] subY = new double[kept];
            double[][] subX = new double[kept][];
            int w = 0;
            for (int i = 0; i < groupIds.length; i++) {
                if (!group.equals(groupIds[i])) {
                    subY[w] = y[i];
                    subX[w] = x[i];
                    w++;
                }
            }
            OlsResult refit = ols(subY, subX, xNames);
            double coef = refit.coef[termIdx];
            double se = refit.se[termIdx];
            double influence = se == 0 ? Double.NaN
                : (baseline.coef[termIdx] - coef) / se;
            drops.add(new LeaveOneOut(group, dropped, kept, coef, se,
                refit.pValue[termIdx], influence));
        }
        return new SensitivityResult(baseline, baseline.names[termIdx], drops);
    }

    private static int indexOfName(String[] names, String term) {
        for (int i = 0; i < names.length; i++) {
            if (names[i].equals(term)) {
                return i;
            }
        }
        throw new IllegalArgumentException("term '" + term + "' is not one of "
            + Arrays.toString(names));
    }

    /** One refit: the group held out, and the tracked coefficient without it. */
    static final class LeaveOneOut {
        final String group;
        final int rowsDropped;
        final int rowsKept;
        final boolean estimable;
        final double coef;
        final double se;
        final double pValue;
        final double influence;

        /** A group whose removal leaves too few rows to estimate. */
        LeaveOneOut(String group, int rowsDropped, int rowsKept) {
            this(group, rowsDropped, rowsKept, false, Double.NaN, Double.NaN, Double.NaN,
                Double.NaN);
        }

        LeaveOneOut(String group, int rowsDropped, int rowsKept, double coef, double se,
                double pValue, double influence) {
            this(group, rowsDropped, rowsKept, true, coef, se, pValue, influence);
        }

        private LeaveOneOut(String group, int rowsDropped, int rowsKept, boolean estimable,
                double coef, double se, double pValue, double influence) {
            this.group = group;
            this.rowsDropped = rowsDropped;
            this.rowsKept = rowsKept;
            this.estimable = estimable;
            this.coef = coef;
            this.se = se;
            this.pValue = pValue;
            this.influence = influence;
        }
    }

    static final class SensitivityResult {
        final OlsResult baseline;
        final String term;
        final List<LeaveOneOut> drops;

        SensitivityResult(OlsResult baseline, String term, List<LeaveOneOut> drops) {
            this.baseline = baseline;
            this.term = term;
            this.drops = drops;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = mapper.createObjectNode();
            out.put("term", term);
            int termIdx = indexOfName(baseline.names, term);
            double baseCoef = baseline.coef[termIdx];
            double baseP = baseline.pValue[termIdx];

            ObjectNode base = mapper.createObjectNode();
            base.put("coefficient", baseCoef);
            base.put("std_error", baseline.se[termIdx]);
            base.put("p_value", baseP);
            base.put("n", baseline.n);
            out.set("full_sample", base);

            ArrayNode arr = mapper.createArrayNode();
            String maxGroup = null;
            double maxAbsInfluence = 0;
            double minCoef = Double.POSITIVE_INFINITY;
            double maxCoef = Double.NEGATIVE_INFINITY;
            boolean signFlips = false;
            boolean significanceFlips = false;
            boolean anyInestimable = false;
            int estimated = 0;
            for (LeaveOneOut d : drops) {
                ObjectNode n = mapper.createObjectNode();
                n.put("group_omitted", d.group);
                n.put("rows_dropped", d.rowsDropped);
                n.put("rows_kept", d.rowsKept);
                if (!d.estimable) {
                    anyInestimable = true;
                    n.put("status", "not_estimable");
                    n.put("note", "Omitting this group leaves too few observations to fit "
                        + "the model — the estimate depends on it entirely.");
                    arr.add(n);
                    continue;
                }
                estimated++;
                n.put("coefficient", d.coef);
                n.put("std_error", d.se);
                n.put("p_value", d.pValue);
                n.put("influence", d.influence);
                boolean flipped = (baseCoef > 0 && d.coef < 0) || (baseCoef < 0 && d.coef > 0);
                if (flipped) {
                    signFlips = true;
                    n.put("sign_flipped", true);
                }
                // 0.05 is a convention, not a threshold this tool endorses — it is reported
                // because a result that crosses it when one group leaves is exactly the case
                // a reader needs told.
                boolean sigFlip = (baseP < 0.05) != (d.pValue < 0.05);
                if (sigFlip) {
                    significanceFlips = true;
                    n.put("significance_flipped_at_0_05", true);
                }
                minCoef = Math.min(minCoef, d.coef);
                maxCoef = Math.max(maxCoef, d.coef);
                if (!Double.isNaN(d.influence) && Math.abs(d.influence) > maxAbsInfluence) {
                    maxAbsInfluence = Math.abs(d.influence);
                    maxGroup = d.group;
                }
                arr.add(n);
            }
            out.set("leave_one_out", arr);

            ObjectNode summary = mapper.createObjectNode();
            summary.put("groups_tested", drops.size());
            summary.put("groups_estimated", estimated);
            if (estimated > 0) {
                summary.put("coefficient_min", minCoef);
                summary.put("coefficient_max", maxCoef);
                summary.put("coefficient_range", maxCoef - minCoef);
            }
            if (maxGroup != null) {
                summary.put("most_influential_group", maxGroup);
                summary.put("max_abs_influence", maxAbsInfluence);
            }
            summary.put("sign_flips", signFlips);
            summary.put("significance_flips_at_0_05", significanceFlips);
            summary.put("robust", !signFlips && !significanceFlips && !anyInestimable
                && maxAbsInfluence < 1.0);
            out.set("summary", summary);

            out.put("note", "influence is (full-sample coefficient − leave-one-out "
                + "coefficient) / leave-one-out standard error: how far omitting one group "
                + "moves the estimate, in that estimate's own standard-error units. "
                + "|influence| above 1 means a single group moves the answer by more than "
                + "its uncertainty. 'robust' is a summary of the checks run here (no sign "
                + "flip, no crossing of p=0.05, every group droppable, all |influence| < 1) "
                + "— it is not a general claim that the specification is correct.");
            return out;
        }
    }

    // ─── Hypothesis tests ───────────────────────────────────────────────────────

    /** Two-sample or one-sample t-test, one-way ANOVA, chi-square test of independence, or
     *  two-sample Kolmogorov-Smirnov test, via Commons Math's {@code stat.inference} package.
     *  {@code groups} maps a group label to its sample values (t-test needs exactly 2 groups
     *  unless {@code oneSampleMu} is given; ANOVA needs 2+; KS needs exactly 2; chi-square
     *  takes {@code counts} as a contingency table instead of {@code groups}). */
    static ObjectNode hypothesisTest(ObjectMapper mapper, String test,
            Map<String, double[]> groups, Double oneSampleMu, long[][] contingencyTable) {
        ObjectNode out = mapper.createObjectNode();
        out.put("test", test);
        switch (test) {
            case "t_test": {
                TTest tTest = new TTest();
                if (oneSampleMu != null) {
                    if (groups.size() != 1) {
                        throw new IllegalArgumentException(
                            "one-sample t-test needs exactly 1 group of values");
                    }
                    double[] sample = groups.values().iterator().next();
                    double stat = tTest.t(oneSampleMu, sample);
                    double p = tTest.tTest(oneSampleMu, sample);
                    out.put("t_statistic", stat);
                    out.put("p_value", p);
                    out.put("n", sample.length);
                    out.put("sample_mean", mean(sample));
                    out.put("hypothesized_mean", oneSampleMu);
                } else {
                    if (groups.size() != 2) {
                        throw new IllegalArgumentException(
                            "two-sample t-test needs exactly 2 groups, got " + groups.size());
                    }
                    List<double[]> vals = new ArrayList<>(groups.values());
                    List<String> keys = new ArrayList<>(groups.keySet());
                    double[] a = vals.get(0);
                    double[] b = vals.get(1);
                    // Welch's t-test (unequal variances) — the safer default; doesn't
                    // assume the two groups have the same variance, which a homoscedastic
                    // pooled-variance test would silently assume.
                    double stat = tTest.t(a, b);
                    double p = tTest.tTest(a, b);
                    out.put("t_statistic", stat);
                    out.put("p_value", p);
                    out.put("group_a", keys.get(0));
                    out.put("group_a_n", a.length);
                    out.put("group_a_mean", mean(a));
                    out.put("group_b", keys.get(1));
                    out.put("group_b_n", b.length);
                    out.put("group_b_mean", mean(b));
                    out.put("method", "Welch's t-test (unequal variances assumed)");
                }
                break;
            }
            case "anova": {
                if (groups.size() < 2) {
                    throw new IllegalArgumentException(
                        "ANOVA needs at least 2 groups, got " + groups.size());
                }
                OneWayAnova anova = new OneWayAnova();
                List<double[]> classes = new ArrayList<>(groups.values());
                double fStat = anova.anovaFValue(classes);
                double p = anova.anovaPValue(classes);
                out.put("f_statistic", fStat);
                out.put("p_value", p);
                out.put("num_groups", groups.size());
                ArrayNode groupStats = mapper.createArrayNode();
                for (Map.Entry<String, double[]> e : groups.entrySet()) {
                    ObjectNode g = mapper.createObjectNode();
                    g.put("group", e.getKey());
                    g.put("n", e.getValue().length);
                    g.put("mean", mean(e.getValue()));
                    groupStats.add(g);
                }
                out.set("groups", groupStats);
                break;
            }
            case "chi_square": {
                if (contingencyTable == null || contingencyTable.length < 2
                        || contingencyTable[0].length < 2) {
                    throw new IllegalArgumentException(
                        "chi-square test needs a contingency table with at least 2 rows and "
                        + "2 columns");
                }
                ChiSquareTest chi = new ChiSquareTest();
                double stat = chi.chiSquare(contingencyTable);
                double p = chi.chiSquareTest(contingencyTable);
                out.put("chi_square_statistic", stat);
                out.put("p_value", p);
                out.put("rows", contingencyTable.length);
                out.put("cols", contingencyTable[0].length);
                break;
            }
            case "ks_test": {
                if (groups.size() != 2) {
                    throw new IllegalArgumentException(
                        "Kolmogorov-Smirnov test needs exactly 2 groups, got " + groups.size());
                }
                List<double[]> vals = new ArrayList<>(groups.values());
                KolmogorovSmirnovTest ks = new KolmogorovSmirnovTest();
                double stat = ks.kolmogorovSmirnovStatistic(vals.get(0), vals.get(1));
                double p = ks.kolmogorovSmirnovTest(vals.get(0), vals.get(1));
                out.put("d_statistic", stat);
                out.put("p_value", p);
                break;
            }
            default:
                throw new IllegalArgumentException("unknown test '" + test
                    + "' — expected one of: t_test, anova, chi_square, ks_test");
        }
        return out;
    }

    // ─── shared helpers ─────────────────────────────────────────────────────────

    private static double mean(double[] a) {
        double sum = 0;
        for (double v : a) {
            sum += v;
        }
        return sum / a.length;
    }

    private static double[] predict(OlsResult reg, double[][] x) {
        double[] out = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            double v = reg.coef[0];
            for (int j = 0; j < x[i].length; j++) {
                v += reg.coef[j + 1] * x[i][j];
            }
            out[i] = v;
        }
        return out;
    }

    private static double[][] concatColumns(double[][] a, double[][] b) {
        int n = a.length > 0 ? a.length : b.length;
        int aCols = a.length > 0 ? a[0].length : 0;
        int bCols = b.length > 0 ? b[0].length : 0;
        double[][] out = new double[n][aCols + bCols];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < aCols; j++) {
                out[i][j] = a[i][j];
            }
            for (int j = 0; j < bCols; j++) {
                out[i][aCols + j] = b[i][j];
            }
        }
        return out;
    }

    private static String[] concatNames(String[] a, String[] b) {
        String[] out = new String[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static double[][] prependColumn(double[] col, double[][] rest) {
        int n = col.length;
        int restCols = rest.length > 0 ? rest[0].length : 0;
        double[][] out = new double[n][1 + restCols];
        for (int i = 0; i < n; i++) {
            out[i][0] = col[i];
            for (int j = 0; j < restCols; j++) {
                out[i][1 + j] = rest[i][j];
            }
        }
        return out;
    }

    private static String[] prependName(String name, String[] rest) {
        String[] out = new String[1 + rest.length];
        out[0] = name;
        System.arraycopy(rest, 0, out, 1, rest.length);
        return out;
    }
}
