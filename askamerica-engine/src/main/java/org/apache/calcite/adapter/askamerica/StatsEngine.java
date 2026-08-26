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
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.commons.math3.stat.inference.OneWayAnova;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    static final int STATS_MAX_ROWS = 200_000;

    // ─── Data extraction ───────────────────────────────────────────────────────

    /** Runs {@code sql} and extracts the named columns as an {@link Extraction}, one row per
     *  observation in {@code columns} order. Rows where ANY requested column is null are
     *  dropped (regression and hypothesis tests need complete cases) — the returned
     *  extraction reports how many were dropped so a tool can surface that rather than
     *  silently changing the effective sample size underneath the caller. */
    static Extraction extractColumns(Connection conn, String sql, String[] columns)
            throws Exception {
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = McpServer.executeWithRepair(stmt, sql);
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
            String[] numericColumns, String[] labelColumns) throws Exception {
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = McpServer.executeWithRepair(stmt, sql);
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
            String groupCol, String valueCol) throws Exception {
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = McpServer.executeWithRepair(stmt, sql);
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
            String colCol) throws Exception {
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = McpServer.executeWithRepair(stmt, sql);
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
        return panelFixedEffects(y, x, xNames, entityIds, timeIds, null);
    }

    /**
     * As above, with optionally cluster-robust standard errors.
     *
     * <p>Conventional panel standard errors assume the residuals are independent across
     * observations. In a state-year panel they are not: whatever the model misses about a
     * state in one year it usually also misses the next, so the same information is counted
     * many times over and the reported precision is too high — often by a large factor
     * (Bertrand, Duflo & Mullainathan 2004, on exactly this design). Clustering by the unit
     * lets residuals correlate freely within a unit and only assumes independence between
     * units.
     *
     * <p>Two things change together, and reporting one without the other would understate
     * the correction: the covariance becomes the CR1 sandwich, and inference moves to
     * {@code G - 1} degrees of freedom, where G is the number of clusters — not the residual
     * degrees of freedom, which are far larger.
     *
     * <p>{@code clusterIds == null} reproduces the conventional estimator exactly.
     */
    static PanelFixedEffectsResult panelFixedEffects(double[] y, double[][] x, String[] xNames,
            String[] entityIds, String[] timeIds, String[] clusterIds) {
        int n = y.length;
        int k = xNames.length;
        if (clusterIds != null && n != clusterIds.length) {
            throw new IllegalArgumentException("cluster column has " + clusterIds.length
                + " rows, the model has " + n);
        }
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
        double[] resid = new double[n];
        for (int i = 0; i < n; i++) {
            double predicted = 0;
            for (int j = 0; j < k; j++) {
                predicted += beta[j] * xTilde[i][j];
            }
            resid[i] = yTilde[i] - predicted;
            ssr += resid[i] * resid[i];
        }
        double correctSigma2 = ssr / correctDof;

        // The covariance actually used for inference, fully scaled either way, so callers
        // (and the joint pre-trend test) read one matrix without knowing which path built it.
        double[][] covariance;
        int inferenceDof;
        String seMethod;
        int numClusters;
        if (clusterIds == null) {
            covariance = new double[k][k];
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < k; j++) {
                    covariance[i][j] = xTildeXTildeInv[i][j] * correctSigma2;
                }
            }
            inferenceDof = correctDof;
            seMethod = "conventional";
            numClusters = 0;
        } else {
            // CR1 sandwich on the demeaned design: A · (Σ_g s_g s_g') · A, where s_g is the
            // cluster's score vector Σ_{i∈g} x̃_i û_i. Same estimator robustRegression uses
            // for OLS, applied to the within-transformed design instead of the raw one.
            Map<String, double[]> scores = new LinkedHashMap<>();
            for (int i = 0; i < n; i++) {
                double[] s = scores.get(clusterIds[i]);
                if (s == null) {
                    s = new double[k];
                    scores.put(clusterIds[i], s);
                }
                for (int j = 0; j < k; j++) {
                    s[j] += xTilde[i][j] * resid[i];
                }
            }
            numClusters = scores.size();
            if (numClusters < 2) {
                throw new IllegalArgumentException("cluster column has " + numClusters
                    + " distinct value(s) — clustered standard errors need at least 2, and "
                    + "are only meaningful with many more. Cluster on the unit (state, "
                    + "county) rather than on something constant across the sample.");
            }
            RealMatrix meat = new Array2DRowRealMatrix(k, k);
            for (double[] s : scores.values()) {
                RealMatrix sm = new Array2DRowRealMatrix(s.length, 1);
                for (int j = 0; j < k; j++) {
                    sm.setEntry(j, 0, s[j]);
                }
                meat = meat.add(sm.multiply(sm.transpose()));
            }
            RealMatrix bread = new Array2DRowRealMatrix(xTildeXTildeInv);
            // Finite-sample correction, counting the absorbed fixed effects in K the same
            // way correctDof does — otherwise the correction disagrees with the model's own
            // notion of how many parameters it fitted.
            int params = n - correctDof;
            double c = ((double) numClusters / (numClusters - 1))
                * ((double) (n - 1) / (n - params));
            covariance = bread.multiply(meat).multiply(bread).scalarMultiply(c).getData();
            // Inference on the number of clusters, not the number of observations. With 50
            // states this is 49 rather than several hundred, and the p-values grow to match.
            inferenceDof = numClusters - 1;
            seMethod = "cluster-robust (CR1)";
        }

        TDistribution tDist = new TDistribution(inferenceDof);
        double[] se = new double[k];
        double[] tStat = new double[k];
        double[] pValue = new double[k];
        for (int i = 0; i < k; i++) {
            se[i] = Math.sqrt(covariance[i][i]);
            tStat[i] = beta[i] / se[i];
            pValue[i] = 2 * (1 - tDist.cumulativeProbability(Math.abs(tStat[i])));
        }

        return new PanelFixedEffectsResult(xNames, beta, se, tStat, pValue, n, correctDof,
            numEntities, numTimes, covariance, correctSigma2, inferenceDof, seMethod,
            numClusters);
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
        /** Fully-scaled coefficient covariance, conventional or CR1 per {@link #seMethod}. */
        final double[][] covariance;
        /** Residual variance on the correct two-way-FE degrees of freedom. */
        final double sigma2;
        /** Degrees of freedom the reported t and p use: residual, or clusters - 1. */
        final int inferenceDof;
        final String seMethod;
        /** Zero when standard errors are conventional. */
        final int numClusters;

        PanelFixedEffectsResult(String[] names, double[] coef, double[] se, double[] tStat,
                double[] pValue, int n, int dof, int numEntities, int numTimes,
                double[][] covariance, double sigma2, int inferenceDof, String seMethod,
                int numClusters) {
            this.names = names;
            this.coef = coef;
            this.se = se;
            this.tStat = tStat;
            this.pValue = pValue;
            this.n = n;
            this.dof = dof;
            this.numEntities = numEntities;
            this.numTimes = numTimes;
            this.covariance = covariance;
            this.sigma2 = sigma2;
            this.inferenceDof = inferenceDof;
            this.seMethod = seMethod;
            this.numClusters = numClusters;
        }

        /**
         * Below this, CR1 is known to understate uncertainty badly — the asymptotics are in
         * the number of clusters, not observations, and 20 states is not many. Reported
         * rather than enforced: too few clusters is a caveat on the answer, not a reason to
         * refuse one.
         */
        static final int FEW_CLUSTERS = 40;

        boolean fewClusters() {
            return numClusters > 0 && numClusters < FEW_CLUSTERS;
        }

        /** Names the estimator and, when clustered, what it was clustered on. */
        void describeStandardErrors(ObjectNode out, String clusterColumn) {
            out.put("se_method", seMethod);
            out.put("inference_degrees_of_freedom", inferenceDof);
            if (numClusters > 0) {
                out.put("num_clusters", numClusters);
                out.put("clustered_on", clusterColumn);
                if (fewClusters()) {
                    out.put("few_clusters_warning", "Only " + numClusters + " clusters. "
                        + "Cluster-robust standard errors rely on having many clusters, and "
                        + "below roughly " + FEW_CLUSTERS + " they are themselves biased "
                        + "downward — so a p-value near a threshold here is weaker than it "
                        + "looks, not stronger.");
                }
            }
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

    // ─── Event study ────────────────────────────────────────────────────────────

    /** Past this the design is dummies all the way down; the caller is told to bin instead. */
    private static final int MAX_EVENT_DUMMIES = 40;

    /**
     * Two-way fixed-effects event study: the outcome regressed on one indicator per period
     * relative to treatment, with unit and time effects absorbed.
     *
     * <p>This is the credibility companion to {@link #diffInDiff}. A difference-in-differences
     * estimate rests on parallel trends, which that tool can assert but not test — it collapses
     * everything before treatment into a single "pre" indicator, so a treated group already
     * diverging beforehand produces exactly the same number as one that was not. Estimating a
     * separate coefficient per pre-period makes that divergence visible, and lets it be tested
     * jointly rather than eyeballed.
     *
     * <p>{@code relativeTime[i]} is the period of observation {@code i} counted from its unit's
     * treatment (0 = the period treatment begins, -1 = the period before), or null for a unit
     * never treated. Never-treated units get zeros across every indicator, which is what makes
     * them the comparison group. The reference period is omitted from the design, so every
     * coefficient reads as a difference from it — there is no "effect at the reference period"
     * to report and one is not invented.
     *
     * <p>Periods outside {@code [-maxLead, maxLag]} are binned into single endpoint indicators
     * rather than dropped: dropping them would silently change which units are in the sample.
     */
    static EventStudyResult eventStudy(double[] y, String[] entityIds, String[] timeIds,
            Integer[] relativeTime, int maxLead, int maxLag, int referencePeriod,
            String[] clusterIds) {
        int n = y.length;
        if (n != entityIds.length || n != timeIds.length || n != relativeTime.length) {
            throw new IllegalArgumentException("outcome, entity, time, and relative-time "
                + "arrays must be the same length");
        }
        if (maxLead < 1 || maxLag < 0) {
            throw new IllegalArgumentException("maxLead must be at least 1 and maxLag at "
                + "least 0; got " + maxLead + " and " + maxLag);
        }
        if (referencePeriod > 0) {
            throw new IllegalArgumentException("reference_period must be a pre-treatment "
                + "period (<= 0); got " + referencePeriod + ". Normalizing against a "
                + "post-treatment period measures every other period against the treatment "
                + "effect itself.");
        }

        // Which indicators the data actually supports — a period no observation falls in
        // would be a column of zeros, which is not estimable and must not be requested.
        LinkedHashSet<Integer> occupied = new LinkedHashSet<>();
        boolean anyBelow = false;
        boolean anyAbove = false;
        int treatedUnits = 0;
        LinkedHashSet<String> treatedSet = new LinkedHashSet<>();
        LinkedHashSet<String> neverTreatedSet = new LinkedHashSet<>();
        for (int i = 0; i < n; i++) {
            if (relativeTime[i] == null) {
                neverTreatedSet.add(entityIds[i]);
                continue;
            }
            treatedSet.add(entityIds[i]);
            int r = relativeTime[i].intValue();
            if (r < -maxLead) {
                anyBelow = true;
            } else if (r > maxLag) {
                anyAbove = true;
            } else {
                occupied.add(Integer.valueOf(r));
            }
        }
        neverTreatedSet.removeAll(treatedSet);
        treatedUnits = treatedSet.size();
        if (treatedUnits == 0) {
            throw new IllegalArgumentException("no observation has a treatment time — every "
                + "row is never-treated, so there is no event to study. Check that the "
                + "treatment-time column is populated for the treated units.");
        }
        if (!occupied.contains(Integer.valueOf(referencePeriod))) {
            throw new IllegalArgumentException("no observation sits at the reference period "
                + referencePeriod + ", so there is nothing to normalize against. Observed "
                + "relative periods inside the window: " + new java.util.TreeSet<>(occupied));
        }

        List<Integer> periods = new ArrayList<>(occupied);
        periods.remove(Integer.valueOf(referencePeriod));
        Collections.sort(periods);
        int dummies = periods.size() + (anyBelow ? 1 : 0) + (anyAbove ? 1 : 0);
        if (dummies > MAX_EVENT_DUMMIES) {
            throw new IllegalArgumentException("the window spans " + dummies + " indicators — "
                + "narrow max_lead/max_lag so the periods beyond them are binned into the "
                + "endpoints; the limit is " + MAX_EVENT_DUMMIES + ".");
        }
        if (dummies == 0) {
            throw new IllegalArgumentException("only the reference period is occupied — there "
                + "is nothing to estimate against it");
        }

        List<String> names = new ArrayList<>();
        if (anyBelow) {
            names.add("pre_beyond_" + maxLead);
        }
        for (Integer p : periods) {
            names.add(eventTermName(p.intValue()));
        }
        if (anyAbove) {
            names.add("post_beyond_" + maxLag);
        }

        double[][] x = new double[n][names.size()];
        for (int i = 0; i < n; i++) {
            if (relativeTime[i] == null) {
                continue;
            }
            int r = relativeTime[i].intValue();
            String term;
            if (r < -maxLead) {
                term = "pre_beyond_" + maxLead;
            } else if (r > maxLag) {
                term = "post_beyond_" + maxLag;
            } else if (r == referencePeriod) {
                continue;
            } else {
                term = eventTermName(r);
            }
            x[i][names.indexOf(term)] = 1.0;
        }

        PanelFixedEffectsResult fit = panelFixedEffects(y, x, names.toArray(new String[0]),
            entityIds, timeIds, clusterIds);

        // Every indicator strictly before the reference period is a pre-trend test: under
        // parallel trends each is zero, so their joint significance is the test.
        List<Integer> leadIdx = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            Integer p = eventTermPeriod(names.get(i));
            if (names.get(i).startsWith("pre_beyond_")
                    || (p != null && p.intValue() < referencePeriod)) {
                leadIdx.add(Integer.valueOf(i));
            }
        }
        double[] pretrend = jointFTest(fit, leadIdx);
        return new EventStudyResult(fit, names, referencePeriod, maxLead, maxLag,
            leadIdx.size(), pretrend[0], pretrend[1], treatedUnits, neverTreatedSet.size(),
            countDistinctAdoptionTimes(timeIds, relativeTime));
    }

    private static String eventTermName(int r) {
        return r < 0 ? ("lead_" + (-r)) : ("lag_" + r);
    }

    /** The relative period an indicator name encodes, or null for a binned endpoint. */
    private static Integer eventTermPeriod(String name) {
        if (name.startsWith("lead_")) {
            return Integer.valueOf(-Integer.parseInt(name.substring(5)));
        }
        if (name.startsWith("lag_")) {
            return Integer.valueOf(Integer.parseInt(name.substring(4)));
        }
        return null;
    }

    /**
     * How many distinct calendar periods units are first treated in. More than one means
     * adoption is staggered, which is what makes the two-way-FE estimator here potentially
     * biased — reported so the caller can say so rather than discover it later.
     *
     * <p>Derived as period minus relative period on every treated row, not by looking for
     * rows at relative period 0: a unit treated before the panel starts, or in a year the
     * panel happens to skip, has no such row, and counting only those would report staggered
     * adoption as simultaneous.
     */
    private static int countDistinctAdoptionTimes(String[] timeIds, Integer[] relativeTime) {
        LinkedHashSet<Integer> adoptionPeriods = new LinkedHashSet<>();
        for (int i = 0; i < timeIds.length; i++) {
            if (relativeTime[i] == null) {
                continue;
            }
            int t;
            try {
                t = Integer.parseInt(timeIds[i].trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("the time column must hold ordered numeric "
                    + "periods for an event study — '" + timeIds[i] + "' is not one");
            }
            adoptionPeriods.add(Integer.valueOf(t - relativeTime[i].intValue()));
        }
        return adoptionPeriods.size();
    }

    /**
     * Joint F-test that every coefficient in {@code idx} is zero, from the fitted covariance
     * matrix. Returns {F, p}, or {NaN, NaN} when the restricted covariance cannot be inverted
     * — a singular block means the test is not identified, which is reported as such rather
     * than as a passing test.
     */
    private static double[] jointFTest(PanelFixedEffectsResult fit, List<Integer> idx) {
        int q = idx.size();
        if (q == 0) {
            return new double[]{Double.NaN, Double.NaN};
        }
        // The same covariance the coefficients' own standard errors come from, so a
        // clustered event study gets a clustered pre-trend test rather than a conventional
        // one — reporting a clustered effect beside an unclustered credibility check would
        // hold the headline to a stricter standard than the test that vouches for it.
        double[][] cov = fit.covariance;
        double[][] sub = new double[q][q];
        double[] b = new double[q];
        for (int i = 0; i < q; i++) {
            b[i] = fit.coef[idx.get(i).intValue()];
            for (int j = 0; j < q; j++) {
                sub[i][j] = cov[idx.get(i).intValue()][idx.get(j).intValue()];
            }
        }
        try {
            RealMatrix inv = new org.apache.commons.math3.linear.LUDecomposition(
                new Array2DRowRealMatrix(sub)).getSolver().getInverse();
            double wald = 0;
            for (int i = 0; i < q; i++) {
                for (int j = 0; j < q; j++) {
                    wald += b[i] * inv.getEntry(i, j) * b[j];
                }
            }
            double f = wald / q;
            // Denominator degrees of freedom follow the same rule as the coefficients':
            // clusters - 1 when clustered, residual dof otherwise.
            FDistribution dist = new FDistribution(q, fit.inferenceDof);
            return new double[]{f, 1 - dist.cumulativeProbability(f)};
        } catch (org.apache.commons.math3.linear.SingularMatrixException e) {
            return new double[]{Double.NaN, Double.NaN};
        }
    }

    static final class EventStudyResult {
        final PanelFixedEffectsResult fit;
        final List<String> names;
        final int referencePeriod;
        final int maxLead;
        final int maxLag;
        final int leadCount;
        final double pretrendF;
        final double pretrendP;
        final int treatedUnits;
        final int neverTreatedUnits;
        final int distinctAdoptionTimes;
        /** What the standard errors were clustered on, for the report; null when they were not. */
        String clusterColumn;

        EventStudyResult(PanelFixedEffectsResult fit, List<String> names, int referencePeriod,
                int maxLead, int maxLag, int leadCount, double pretrendF, double pretrendP,
                int treatedUnits, int neverTreatedUnits, int distinctAdoptionTimes) {
            this.fit = fit;
            this.names = names;
            this.referencePeriod = referencePeriod;
            this.maxLead = maxLead;
            this.maxLag = maxLag;
            this.leadCount = leadCount;
            this.pretrendF = pretrendF;
            this.pretrendP = pretrendP;
            this.treatedUnits = treatedUnits;
            this.neverTreatedUnits = neverTreatedUnits;
            this.distinctAdoptionTimes = distinctAdoptionTimes;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = mapper.createObjectNode();
            ArrayNode effects = mapper.createArrayNode();
            for (int i = 0; i < names.size(); i++) {
                ObjectNode c = mapper.createObjectNode();
                c.put("term", names.get(i));
                Integer p = eventTermPeriod(names.get(i));
                if (p != null) {
                    c.put("relative_period", p.intValue());
                }
                c.put("is_pre_treatment", names.get(i).startsWith("pre_beyond_")
                    || (p != null && p.intValue() < referencePeriod));
                c.put("coefficient", fit.coef[i]);
                c.put("std_error", fit.se[i]);
                c.put("t_stat", fit.tStat[i]);
                c.put("p_value", fit.pValue[i]);
                effects.add(c);
            }
            out.set("effects", effects);
            out.put("reference_period", referencePeriod);
            out.put("max_lead", maxLead);
            out.put("max_lag", maxLag);
            out.put("n", fit.n);
            out.put("degrees_of_freedom", fit.dof);
            out.put("num_entities", fit.numEntities);
            out.put("num_time_periods", fit.numTimes);
            out.put("treated_units", treatedUnits);
            out.put("never_treated_units", neverTreatedUnits);

            ObjectNode pre = mapper.createObjectNode();
            pre.put("leads_tested", leadCount);
            if (Double.isNaN(pretrendF)) {
                pre.put("status", leadCount == 0 ? "no_pre_periods" : "not_identified");
                pre.put("verdict", "No parallel-trends test could be run, which is NOT the "
                    + "same as one that passed. Without pre-period estimates the "
                    + "identifying assumption is untested.");
            } else {
                pre.put("f_statistic", pretrendF);
                pre.put("p_value", pretrendP);
                pre.put("pre_trends_detected", pretrendP < 0.05);
                pre.put("verdict", pretrendP < 0.05
                    ? "Pre-treatment coefficients are jointly different from zero (p < 0.05): "
                        + "the groups were already diverging before treatment, so the "
                        + "post-treatment estimates cannot be read as the treatment's effect."
                    : "Pre-treatment coefficients are not jointly distinguishable from zero. "
                        + "This is consistent with parallel trends; it does not prove them, "
                        + "and a test with few pre-periods or wide standard errors will fail "
                        + "to detect divergence that is really there.");
            }
            out.set("pre_trend_test", pre);

            out.put("staggered_adoption", distinctAdoptionTimes > 1);
            out.put("distinct_adoption_periods", distinctAdoptionTimes);
            StringBuilder note = new StringBuilder();
            note.append("Coefficients are differences from the reference period ")
                .append(referencePeriod)
                .append(", with unit and time fixed effects absorbed. The reference period "
                    + "itself has no coefficient by construction. Periods outside the window "
                    + "are binned into pre_beyond_/post_beyond_ terms rather than dropped. ");
            if (distinctAdoptionTimes > 1) {
                note.append("Adoption is STAGGERED (")
                    .append(distinctAdoptionTimes)
                    .append(" distinct treatment periods). With staggered timing and effects "
                        + "that differ across units or over time, this two-way fixed-effects "
                        + "estimator uses already-treated units as controls and can be biased "
                        + "— including, in the worst case, carrying the wrong sign "
                        + "(Goodman-Bacon 2021; Callaway & Sant'Anna 2021). Treat the shape "
                        + "of the path as indicative and say so. ");
            }
            if (neverTreatedUnits == 0) {
                note.append("There are NO never-treated units: identification comes entirely "
                    + "from differences in treatment timing, so the estimates lean harder on "
                    + "the staggered-adoption caveat above. ");
            }
            fit.describeStandardErrors(out, clusterColumn);
            if (fit.numClusters > 0) {
                note.append("Standard errors and the pre-trend test are cluster-robust on ")
                    .append(clusterColumn)
                    .append(" (")
                    .append(fit.numClusters)
                    .append(" clusters), so residuals may correlate freely within a unit; "
                        + "inference uses clusters - 1 degrees of freedom, not the residual "
                        + "count. ");
                if (fit.fewClusters()) {
                    note.append("With this few clusters the correction is itself unreliable "
                        + "and errs toward overstating precision — see "
                        + "few_clusters_warning. ");
                }
            } else {
                note.append("Standard errors are conventional and NOT clustered — with "
                    + "repeated observations of the same unit they are likely too small; "
                    + "pass cluster_col to correct this, and read a marginal p-value "
                    + "accordingly until you have. ");
            }
            out.put("note", note.toString());
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

    // ─── Correlation, decomposition, and scenario tools ─────────────────────────

    /** Pairwise Pearson correlation matrix plus each column's variance inflation factor (VIF)
     *  — how much of that column's variance is explained by the OTHER columns via OLS. VIF
     *  answers a question the correlation matrix alone can't: a variable can be weakly
     *  correlated with every other variable individually and still be almost entirely
     *  redundant with the GROUP of them together. VIF > 10 is the conventional flag for
     *  problematic multicollinearity (some texts use 5); {@link Double#POSITIVE_INFINITY}
     *  means a column is an exact linear combination of the others. */
    static CorrelationMatrixResult correlationMatrix(double[][] data, String[] names) {
        int n = data.length;
        int k = names.length;
        if (k < 2) {
            throw new IllegalArgumentException("correlation_matrix needs at least 2 columns");
        }
        if (n < k + 2) {
            throw new IllegalArgumentException("only " + n + " complete observations for "
                + k + " columns — need at least " + (k + 2) + " for a stable VIF");
        }
        PearsonsCorrelation pc = new PearsonsCorrelation(data);
        double[][] r = pc.getCorrelationMatrix().getData();
        double[][] pValues;
        try {
            pValues = pc.getCorrelationPValues().getData();
        } catch (RuntimeException e) {
            // The p-value's own t-distribution needs n > 2; the correlations themselves are
            // still valid without it.
            pValues = null;
        }
        double[] vif = new double[k];
        for (int j = 0; j < k; j++) {
            double[] y = new double[n];
            double[][] x = new double[n][k - 1];
            for (int row = 0; row < n; row++) {
                y[row] = data[row][j];
                int c = 0;
                for (int col = 0; col < k; col++) {
                    if (col != j) {
                        x[row][c++] = data[row][col];
                    }
                }
            }
            OLSMultipleLinearRegression reg = new OLSMultipleLinearRegression();
            reg.newSampleData(y, x);
            double r2 = reg.calculateRSquared();
            vif[j] = r2 >= 1.0 ? Double.POSITIVE_INFINITY : 1.0 / (1.0 - r2);
        }
        return new CorrelationMatrixResult(names, r, pValues, vif, n);
    }

    static final class CorrelationMatrixResult {
        final String[] names;
        final double[][] r;
        final double[][] pValues; // nullable — see correlationMatrix()
        final double[] vif;
        final int n;

        CorrelationMatrixResult(String[] names, double[][] r, double[][] pValues, double[] vif,
                int n) {
            this.names = names;
            this.r = r;
            this.pValues = pValues;
            this.vif = vif;
            this.n = n;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = mapper.createObjectNode();
            out.put("n", n);
            ArrayNode pairs = mapper.createArrayNode();
            for (int i = 0; i < names.length; i++) {
                for (int j = i + 1; j < names.length; j++) {
                    ObjectNode p = mapper.createObjectNode();
                    p.put("var_a", names[i]);
                    p.put("var_b", names[j]);
                    p.put("correlation", r[i][j]);
                    if (pValues != null) {
                        p.put("p_value", pValues[i][j]);
                    }
                    pairs.add(p);
                }
            }
            out.set("pairwise_correlations", pairs);
            ArrayNode vifArr = mapper.createArrayNode();
            double maxVif = 0;
            String worstVar = null;
            for (int i = 0; i < names.length; i++) {
                ObjectNode v = mapper.createObjectNode();
                v.put("variable", names[i]);
                v.put("vif", vif[i]);
                vifArr.add(v);
                if (vif[i] > maxVif) {
                    maxVif = vif[i];
                    worstVar = names[i];
                }
            }
            out.set("variance_inflation_factors", vifArr);
            if (worstVar != null) {
                out.put("most_collinear_variable", worstVar);
                out.put("max_vif", maxVif);
                out.put("multicollinearity_flag", maxVif > 10
                    ? "severe (VIF > 10) — treating these as independent evidence is not "
                        + "justified; consider dropping one or combining them"
                    : maxVif > 5 ? "moderate (VIF > 5) — worth noting" : "none");
            }
            return out;
        }
    }

    /** Bins a continuous predictor into equal-count quantile groups and tests whether the mean
     *  outcome trends monotonically across bins — the check a single linear correlation can't
     *  make: a relationship can be non-monotonic (a threshold, a U-shape) and still show a
     *  middling linear r, or be genuinely monotonic but nonlinear and understate one. Trend
     *  significance comes from a simple regression of bin mean on bin rank (1..bins), the same
     *  construction Cochran-Armitage / Cuzick-style trend tests use — it tests the BIN-LEVEL
     *  trend, not a re-run of the underlying observation-level relationship. */
    static QuantileBinningResult quantileBinningTest(double[] outcome, double[] predictor,
            int bins) {
        int n = outcome.length;
        if (n != predictor.length) {
            throw new IllegalArgumentException("outcome and predictor must be the same length");
        }
        if (bins < 3) {
            throw new IllegalArgumentException("bins must be at least 3 to test a trend");
        }
        if (n < bins * 2) {
            throw new IllegalArgumentException("only " + n + " observations for " + bins
                + " bins — need at least " + (bins * 2) + " (2 per bin) for a stable bin mean");
        }
        Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (a, b) -> Double.compare(predictor[a], predictor[b]));
        List<BinStat> binStats = new ArrayList<>();
        SimpleRegression trend = new SimpleRegression();
        for (int b = 0; b < bins; b++) {
            int lo = (int) Math.floor((double) b * n / bins);
            int hi = (int) Math.floor((double) (b + 1) * n / bins);
            double sumY = 0;
            double sumX = 0;
            double minX = Double.POSITIVE_INFINITY;
            double maxX = Double.NEGATIVE_INFINITY;
            for (int i = lo; i < hi; i++) {
                int idx = order[i];
                sumY += outcome[idx];
                sumX += predictor[idx];
                minX = Math.min(minX, predictor[idx]);
                maxX = Math.max(maxX, predictor[idx]);
            }
            int count = hi - lo;
            double meanY = sumY / count;
            double meanX = sumX / count;
            binStats.add(new BinStat(b + 1, count, minX, maxX, meanX, meanY));
            trend.addData(b + 1, meanY);
        }
        double slope = trend.getSlope();
        double pValue;
        try {
            pValue = trend.getSignificance();
        } catch (RuntimeException e) {
            pValue = Double.NaN;
        }
        boolean monotonicIncreasing = true;
        boolean monotonicDecreasing = true;
        for (int i = 1; i < binStats.size(); i++) {
            if (binStats.get(i).meanOutcome < binStats.get(i - 1).meanOutcome) {
                monotonicIncreasing = false;
            }
            if (binStats.get(i).meanOutcome > binStats.get(i - 1).meanOutcome) {
                monotonicDecreasing = false;
            }
        }
        return new QuantileBinningResult(binStats, slope, pValue,
            monotonicIncreasing || monotonicDecreasing, monotonicIncreasing, n);
    }

    static final class BinStat {
        final int bin;
        final int n;
        final double min;
        final double max;
        final double meanPredictor;
        final double meanOutcome;

        BinStat(int bin, int n, double min, double max, double meanPredictor,
                double meanOutcome) {
            this.bin = bin;
            this.n = n;
            this.min = min;
            this.max = max;
            this.meanPredictor = meanPredictor;
            this.meanOutcome = meanOutcome;
        }
    }

    static final class QuantileBinningResult {
        final List<BinStat> bins;
        final double trendSlope;
        final double trendPValue;
        final boolean monotonic;
        final boolean increasing;
        final int n;

        QuantileBinningResult(List<BinStat> bins, double trendSlope, double trendPValue,
                boolean monotonic, boolean increasing, int n) {
            this.bins = bins;
            this.trendSlope = trendSlope;
            this.trendPValue = trendPValue;
            this.monotonic = monotonic;
            this.increasing = increasing;
            this.n = n;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = mapper.createObjectNode();
            out.put("n", n);
            ArrayNode arr = mapper.createArrayNode();
            for (BinStat b : bins) {
                ObjectNode bn = mapper.createObjectNode();
                bn.put("bin", b.bin);
                bn.put("n", b.n);
                bn.put("predictor_min", b.min);
                bn.put("predictor_max", b.max);
                bn.put("predictor_mean", b.meanPredictor);
                bn.put("outcome_mean", b.meanOutcome);
                arr.add(bn);
            }
            out.set("bins", arr);
            out.put("trend_slope_per_bin", trendSlope);
            out.put("trend_p_value", trendPValue);
            out.put("monotonic", monotonic);
            if (monotonic) {
                out.put("direction", increasing ? "increasing" : "decreasing");
            }
            out.put("note", "trend_p_value tests whether bin means move consistently with bin "
                + "rank (a dose-response test) — it can be significant even when 'monotonic' "
                + "is false if only one bin breaks the pattern; read both together, not "
                + "trend_p_value alone.");
            return out;
        }
    }

    /** Each distinct group's share of a total, and what the total would be with that group
     *  removed — "how much of X does group G account for" otherwise requires a full-sample SUM
     *  alongside a per-group SUM and a subtraction, done by hand. */
    static SubgroupContributionResult subgroupContribution(double[] value, String[] group) {
        int n = value.length;
        if (n != group.length) {
            throw new IllegalArgumentException("value and group must be the same length");
        }
        Map<String, double[]> agg = new LinkedHashMap<>(); // group -> {sum, count}
        double total = 0;
        for (int i = 0; i < n; i++) {
            total += value[i];
            double[] slot = agg.computeIfAbsent(group[i], g -> new double[2]);
            slot[0] += value[i];
            slot[1] += 1;
        }
        List<Subgroup> groups = new ArrayList<>();
        for (Map.Entry<String, double[]> e : agg.entrySet()) {
            double sum = e.getValue()[0];
            int count = (int) e.getValue()[1];
            groups.add(new Subgroup(e.getKey(), sum, count,
                total == 0 ? Double.NaN : sum / total, total - sum));
        }
        groups.sort((a, b) -> Double.compare(b.sum, a.sum));
        return new SubgroupContributionResult(groups, total, n);
    }

    static final class Subgroup {
        final String label;
        final double sum;
        final int n;
        final double share;
        final double totalExcluding;

        Subgroup(String label, double sum, int n, double share, double totalExcluding) {
            this.label = label;
            this.sum = sum;
            this.n = n;
            this.share = share;
            this.totalExcluding = totalExcluding;
        }
    }

    static final class SubgroupContributionResult {
        final List<Subgroup> groups;
        final double total;
        final int n;

        SubgroupContributionResult(List<Subgroup> groups, double total, int n) {
            this.groups = groups;
            this.total = total;
            this.n = n;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = mapper.createObjectNode();
            out.put("total", total);
            out.put("n", n);
            out.put("groups_found", groups.size());
            ArrayNode arr = mapper.createArrayNode();
            for (Subgroup g : groups) {
                ObjectNode gn = mapper.createObjectNode();
                gn.put("group", g.label);
                gn.put("sum", g.sum);
                gn.put("n", g.n);
                gn.put("share_of_total", g.share);
                gn.put("total_excluding_this_group", g.totalExcluding);
                arr.add(gn);
            }
            out.set("groups", arr);
            if (!groups.isEmpty()) {
                Subgroup top = groups.get(0);
                out.put("largest_group", top.label);
                out.put("largest_group_share", top.share);
            }
            return out;
        }
    }

    /** Gini coefficient and Lorenz curve for a distribution of nonnegative amounts (funding per
     *  institution, income per household, market share per firm) — quantifies HOW concentrated a
     *  total is across its units, where subgroup_contribution's top-N shares only show
     *  concentration at the specific cut points asked for. 0 = perfectly even, 1 = one unit holds
     *  everything. Computed via the standard rank-weighted-sum formula (equivalent to the area
     *  between the Lorenz curve and the line of equality), which needs the full sorted
     *  distribution rather than a handful of top-N shares. */
    static GiniResult giniCoefficient(double[] value) {
        int n = value.length;
        if (n < 2) {
            throw new IllegalArgumentException("gini_coefficient needs at least 2 observations, "
                + "got " + n);
        }
        double[] sorted = value.clone();
        Arrays.sort(sorted);
        double total = 0;
        for (double v : sorted) {
            if (v < 0) {
                throw new IllegalArgumentException("gini_coefficient requires nonnegative "
                    + "values — a negative amount (e.g. a net loss) has no defined share of a "
                    + "total and breaks the concentration measure; filter it out or clip to 0 "
                    + "before calling");
            }
            total += v;
        }
        if (total == 0) {
            throw new IllegalArgumentException("gini_coefficient: values sum to 0 — nothing to "
                + "measure concentration over");
        }
        double weightedSum = 0;
        for (int i = 0; i < n; i++) {
            weightedSum += (i + 1) * sorted[i];
        }
        double gini = (2.0 * weightedSum) / (n * total) - (n + 1.0) / n;

        List<double[]> lorenz = new ArrayList<>();
        lorenz.add(new double[]{0, 0});
        double cumValue = 0;
        for (int i = 0; i < n; i++) {
            cumValue += sorted[i];
            double popShare = (i + 1.0) / n;
            lorenz.add(new double[]{popShare, cumValue / total});
        }
        return new GiniResult(gini, total, n, lorenz);
    }

    static final class GiniResult {
        final double gini;
        final double total;
        final int n;
        final List<double[]> lorenzPoints;

        GiniResult(double gini, double total, int n, List<double[]> lorenzPoints) {
            this.gini = gini;
            this.total = total;
            this.n = n;
            this.lorenzPoints = lorenzPoints;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = mapper.createObjectNode();
            out.put("gini", gini);
            out.put("total", total);
            out.put("n", n);
            String band = gini < 0.3 ? "low" : gini < 0.5 ? "moderate" : gini < 0.7 ? "high"
                : "extreme";
            out.put("concentration_band", band);
            // Lorenz curve is downsampled for LLM readability — deciles plus the exact endpoints,
            // not every one of n points, mirroring the row-capped query() convention elsewhere.
            ArrayNode arr = mapper.createArrayNode();
            int last = lorenzPoints.size() - 1;
            for (int i = 0; i <= 10; i++) {
                int idx = Math.min(last, (int) Math.round(i / 10.0 * last));
                double[] pt = lorenzPoints.get(idx);
                ObjectNode pn = mapper.createObjectNode();
                pn.put("cumulative_population_share", pt[0]);
                pn.put("cumulative_value_share", pt[1]);
                arr.add(pn);
            }
            out.set("lorenz_curve_deciles", arr);
            out.put("note", "gini is the area between the Lorenz curve and perfect equality, "
                + "doubled — 0 means every unit holds an equal share, 1 means one unit holds the "
                + "whole total. Compare across two periods/populations directly; a rising gini "
                + "alongside a rising unit count means new entrants are NOT absorbing share "
                + "evenly, even if top-N shares alone look flat.");
            return out;
        }
    }

    /** Pearson correlation between x and y AFTER regressing out a set of control variables from
     *  each — the correlation that would remain if the controls were held fixed. With no
     *  controls this is exactly the ordinary Pearson correlation and its significance test.
     *  Answers "does this relationship survive controlling for Z" as a single legible number,
     *  where ols_regression answers it only indirectly, as one coefficient inside a multi-term
     *  fit the caller has to interpret themselves. */
    static PartialCorrelationResult partialCorrelation(double[] x, double[] y,
            double[][] controls, String[] controlNames) {
        int n = x.length;
        if (n != y.length) {
            throw new IllegalArgumentException("x and y must be the same length");
        }
        int k = controlNames.length;
        double[] xResid = x;
        double[] yResid = y;
        if (k > 0) {
            if (n < k + 3) {
                throw new IllegalArgumentException("only " + n + " complete observations for "
                    + k + " controls — need at least " + (k + 3)
                    + " to estimate a partial correlation");
            }
            OLSMultipleLinearRegression regX = new OLSMultipleLinearRegression();
            regX.newSampleData(x, controls);
            xResid = regX.estimateResiduals();
            OLSMultipleLinearRegression regY = new OLSMultipleLinearRegression();
            regY.newSampleData(y, controls);
            yResid = regY.estimateResiduals();
        } else if (n < 3) {
            throw new IllegalArgumentException("only " + n + " complete observations — need "
                + "at least 3 to estimate a correlation's significance");
        }
        double r = new PearsonsCorrelation().correlation(xResid, yResid);
        int dof = n - 2 - k;
        double t = r * Math.sqrt(dof / (1 - r * r));
        double p = dof > 0
            ? 2 * (1 - new TDistribution(dof).cumulativeProbability(Math.abs(t))) : Double.NaN;
        double zeroOrderR = new PearsonsCorrelation().correlation(x, y);
        return new PartialCorrelationResult(r, p, dof, n, k, zeroOrderR);
    }

    static final class PartialCorrelationResult {
        final double r;
        final double pValue;
        final int dof;
        final int n;
        final int controlsUsed;
        final double zeroOrderCorrelation;

        PartialCorrelationResult(double r, double pValue, int dof, int n, int controlsUsed,
                double zeroOrderCorrelation) {
            this.r = r;
            this.pValue = pValue;
            this.dof = dof;
            this.n = n;
            this.controlsUsed = controlsUsed;
            this.zeroOrderCorrelation = zeroOrderCorrelation;
        }

        ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode out = mapper.createObjectNode();
            out.put("partial_correlation", r);
            out.put("p_value", pValue);
            out.put("degrees_of_freedom", dof);
            out.put("n", n);
            out.put("controls_used", controlsUsed);
            out.put("zero_order_correlation", zeroOrderCorrelation);
            if (controlsUsed > 0) {
                out.put("note", "zero_order_correlation is the plain (unconditional) "
                    + "correlation between x and y for comparison — the gap between it and "
                    + "partial_correlation is how much of the raw relationship the controls "
                    + "explain away.");
            }
            return out;
        }
    }

    /** Sums/averages/counts/min/max a column — the aggregate {@code scenario_sweep} reports per
     *  swept parameter value. */
    static double aggregate(double[] col, String agg) {
        if (col.length == 0) {
            return Double.NaN;
        }
        switch (agg) {
            case "sum": {
                double s = 0;
                for (double v : col) {
                    s += v;
                }
                return s;
            }
            case "count":
                return col.length;
            case "min": {
                double m = Double.POSITIVE_INFINITY;
                for (double v : col) {
                    m = Math.min(m, v);
                }
                return m;
            }
            case "max": {
                double m = Double.NEGATIVE_INFINITY;
                for (double v : col) {
                    m = Math.max(m, v);
                }
                return m;
            }
            case "avg":
            default: {
                double s = 0;
                for (double v : col) {
                    s += v;
                }
                return s / col.length;
            }
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
