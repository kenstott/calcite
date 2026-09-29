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

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.CholeskyDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.NonPositiveDefiniteMatrixException;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.optim.InitialGuess;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.SimpleBounds;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.nonlinear.scalar.ObjectiveFunction;
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.BOBYQAOptimizer;
import org.apache.commons.math3.special.Gamma;

import smile.timeseries.AR;
import smile.timeseries.ARMA;
import smile.timeseries.BoxTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ARIMA point/interval forecasts, GARCH(1,1) volatility models, and the EWMA / historical
 * volatility baselines they are compared against.
 *
 * <p>ARMA estimation is Smile's {@link ARMA#fit}; differencing, integration back to levels,
 * the psi-weight prediction intervals, GARCH maximum likelihood, and the volatility
 * forecasts are implemented here because Smile ships neither an I term nor any conditional
 * heteroskedasticity model. Kept beside {@link StatsEngine} / {@link StatsMlEngine} as pure
 * array-in / result-out code; SQL extraction lives in {@link McpServer}.
 */
final class StatsTimeSeriesEngine {

    static final int ARIMA_MIN_OBS = 30;
    static final int GARCH_MIN_OBS = 100;
    static final int MAX_HORIZON = 250;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private StatsTimeSeriesEngine() {}

    // ─── Shared helpers ────────────────────────────────────────────────────────

    /** Sorts {@code values} by ascending {@code labels} (ISO dates / timestamps sort
     *  chronologically as strings). Two rows sharing a label mean the SQL returned more than
     *  one series (typically several tickers), which no single-series model can interpret. */
    static double[] sortByLabel(double[] values, String[] labels) {
        int n = values.length;
        Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (a, b) -> labels[a].compareTo(labels[b]));
        double[] out = new double[n];
        for (int i = 0; i < n; i++) {
            out[i] = values[order[i]];
            if (i > 0 && labels[order[i]].equals(labels[order[i - 1]])) {
                throw new IllegalArgumentException("the time column repeats the value '"
                    + labels[order[i]] + "' — the SQL returned more than one series (filter to "
                    + "a single ticker/entity) or duplicate rows for one period");
            }
        }
        return out;
    }

    static String[] sortedLabels(String[] labels) {
        String[] out = labels.clone();
        Arrays.sort(out);
        return out;
    }

    /** Log returns in percent: 100 * ln(p_t / p_{t-1}). Every price must be positive. */
    static double[] logReturnsPercent(double[] prices) {
        double[] r = new double[prices.length - 1];
        for (int i = 1; i < prices.length; i++) {
            if (!(prices[i] > 0) || !(prices[i - 1] > 0)) {
                throw new IllegalArgumentException("price series contains a non-positive "
                    + "value, so log returns are undefined — check the column (use "
                    + "input_type='return' if it already holds returns)");
            }
            r[i - 1] = 100.0 * Math.log(prices[i] / prices[i - 1]);
        }
        return r;
    }

    static double mean(double[] x) {
        double s = 0;
        for (double v : x) {
            s += v;
        }
        return s / x.length;
    }

    static double variance(double[] x) {
        double m = mean(x);
        double s = 0;
        for (double v : x) {
            s += (v - m) * (v - m);
        }
        return s / (x.length - 1);
    }

    private static double zFor(double level) {
        if (!(level > 0 && level < 1)) {
            throw new IllegalArgumentException("level must be strictly between 0 and 1, got "
                + level);
        }
        return new NormalDistribution().inverseCumulativeProbability(0.5 + level / 2.0);
    }

    private static void checkHorizon(int horizon) {
        if (horizon < 1 || horizon > MAX_HORIZON) {
            throw new IllegalArgumentException("horizon must be between 1 and " + MAX_HORIZON
                + ", got " + horizon);
        }
    }

    private static ArrayNode arr(double[] v) {
        ArrayNode a = MAPPER.createArrayNode();
        for (double d : v) {
            a.add(d);
        }
        return a;
    }

    // ─── ARIMA ─────────────────────────────────────────────────────────────────

    static final class ArimaResult {
        int p;
        int d;
        int q;
        double[] ar;
        double[] ma;
        double intercept;
        double sigma2;
        double aic;
        int nObs;
        int nDifferenced;
        double[] forecast;
        double[] lower;
        double[] upper;
        double[] se;
        double level;
        double ljungBoxP;
        int ljungBoxLag;
        boolean orderSelected;
        int candidatesTried;
        List<String> candidatesFailed = new ArrayList<>();
        double lastObserved;

        ObjectNode toJson(ObjectMapper m) {
            ObjectNode o = m.createObjectNode();
            o.put("model", "ARIMA(" + p + "," + d + "," + q + ")");
            o.put("estimator", "Smile ARMA.fit on the differenced series (conditional least "
                + "squares family — not exact MLE)");
            o.put("order_selected_by_aic", orderSelected);
            if (orderSelected) {
                o.put("candidates_tried", candidatesTried);
                ArrayNode failed = o.putArray("candidates_failed_to_fit");
                for (String s : candidatesFailed) {
                    failed.add(s);
                }
            }
            o.put("n_obs", nObs);
            o.put("n_after_differencing", nDifferenced);
            o.set("ar_coefficients", arr(ar));
            o.set("ma_coefficients", arr(ma));
            o.put("intercept_differenced_scale", intercept);
            o.put("innovation_variance", sigma2);
            o.put("aic_approx", aic);
            o.put("ljung_box_residual_p_value", ljungBoxP);
            o.put("ljung_box_lag", ljungBoxLag);
            o.put("last_observed", lastObserved);
            o.put("interval_level", level);
            o.set("forecast", arr(forecast));
            o.set("lower", arr(lower));
            o.set("upper", arr(upper));
            o.set("forecast_std_error", arr(se));
            o.put("interval_basis", "Gaussian innovations; variance from the psi-weights of "
                + "the full integrated model, so it widens with the horizon and, for d>=1, "
                + "grows without bound");
            return o;
        }
    }

    /** Difference {@code x} {@code d} times. */
    static double[] difference(double[] x, int d) {
        double[] w = x;
        for (int k = 0; k < d; k++) {
            double[] next = new double[w.length - 1];
            for (int i = 1; i < w.length; i++) {
                next[i - 1] = w[i] - w[i - 1];
            }
            w = next;
        }
        return w;
    }

    private static final class Fit {
        final int p;
        final int q;
        final double[] ar;
        final double[] ma;
        final double intercept;
        final double sigma2;
        final double aic;
        final double[] residuals;
        final double[] forecast;

        Fit(int p, int q, double[] ar, double[] ma, double intercept, double sigma2,
                double aic, double[] residuals, double[] forecast) {
            this.p = p;
            this.q = q;
            this.ar = ar;
            this.ma = ma;
            this.intercept = intercept;
            this.sigma2 = sigma2;
            this.aic = aic;
            this.residuals = residuals;
            this.forecast = forecast;
        }
    }

    private static Fit fitArma(double[] w, int p, int q, int horizon) {
        int n = w.length;
        if (p == 0 && q == 0) {
            double m = mean(w);
            double[] res = new double[n];
            double rss = 0;
            for (int i = 0; i < n; i++) {
                res[i] = w[i] - m;
                rss += res[i] * res[i];
            }
            double[] f = new double[horizon];
            Arrays.fill(f, m);
            return new Fit(0, 0, new double[0], new double[0], m, rss / (n - 1),
                n * Math.log(rss / n) + 2, res, f);
        }
        if (q == 0) {
            // Smile's ARMA.fit rejects q = 0; a pure AR model has its own estimator.
            AR ar = AR.fit(w, p);
            double rss = ar.RSS();
            return new Fit(p, 0, ar.ar(), new double[0], ar.intercept(), ar.variance(),
                n * Math.log(rss / n) + 2.0 * (p + 1), ar.residuals(), ar.forecast(horizon));
        }
        ARMA model = ARMA.fit(w, p, q);
        double rss = model.RSS();
        double aic = n * Math.log(rss / n) + 2.0 * (p + q + 1);
        return new Fit(p, q, model.ar(), model.ma(), model.intercept(), model.variance(),
            aic, model.residuals(), model.forecast(horizon));
    }

    /** Integrates forecasts of the d-times-differenced series back to the level of
     *  {@code x}. */
    static double[] integrate(double[] x, int d, double[] wForecast) {
        if (d == 0) {
            return wForecast.clone();
        }
        // last[k] = most recent value of the k-times-differenced series.
        double[] last = new double[d];
        double[] cur = x;
        for (int k = 0; k < d; k++) {
            last[k] = cur[cur.length - 1];
            cur = difference(cur, 1);
        }
        double[] out = new double[wForecast.length];
        for (int h = 0; h < wForecast.length; h++) {
            last[d - 1] += wForecast[h];
            for (int k = d - 2; k >= 0; k--) {
                last[k] += last[k + 1];
            }
            out[h] = last[0];
        }
        return out;
    }

    /** Psi weights of the integrated ARMA: the MA(inf) representation of the level series,
     *  which is what the forecast-error variance is built from. */
    static double[] psiWeights(double[] ar, double[] ma, int d, int count) {
        // phi*(B) = (1 - sum ar_i B^i) * (1 - B)^d, as a polynomial in B.
        double[] poly = new double[ar.length + 1];
        poly[0] = 1;
        for (int i = 0; i < ar.length; i++) {
            poly[i + 1] = -ar[i];
        }
        for (int k = 0; k < d; k++) {
            double[] next = new double[poly.length + 1];
            for (int i = 0; i < poly.length; i++) {
                next[i] += poly[i];
                next[i + 1] -= poly[i];
            }
            poly = next;
        }
        double[] psi = new double[count];
        psi[0] = 1;
        for (int j = 1; j < count; j++) {
            double v = j <= ma.length ? ma[j - 1] : 0;
            for (int i = 1; i < poly.length && i <= j; i++) {
                v -= poly[i] * psi[j - i];
            }
            psi[j] = v;
        }
        return psi;
    }

    /** Fits ARIMA(p,d,q) to {@code x} and forecasts {@code horizon} steps ahead. A null
     *  {@code p} or {@code q} selects both by approximate AIC over 0..maxOrder. */
    static ArimaResult arima(double[] x, Integer p, int d, Integer q, int maxOrder, int horizon,
            double level) {
        checkHorizon(horizon);
        if (d < 0 || d > 2) {
            throw new IllegalArgumentException("d must be 0, 1 or 2, got " + d);
        }
        if (x.length < ARIMA_MIN_OBS) {
            throw new IllegalArgumentException("ARIMA needs at least " + ARIMA_MIN_OBS
                + " observations, got " + x.length);
        }
        double z = zFor(level);
        double[] w = difference(x, d);
        ArimaResult r = new ArimaResult();
        Fit best = null;
        if (p != null && q != null) {
            if (p < 0 || q < 0 || p > 10 || q > 10) {
                throw new IllegalArgumentException("p and q must be between 0 and 10");
            }
            best = fitArma(w, p, q, horizon);
        } else {
            if (maxOrder < 0 || maxOrder > 5) {
                throw new IllegalArgumentException("max_order must be between 0 and 5");
            }
            int pLo = p != null ? p : 0;
            int pHi = p != null ? p : maxOrder;
            int qLo = q != null ? q : 0;
            int qHi = q != null ? q : maxOrder;
            r.orderSelected = true;
            for (int pp = pLo; pp <= pHi; pp++) {
                for (int qq = qLo; qq <= qHi; qq++) {
                    r.candidatesTried++;
                    Fit f;
                    try {
                        f = fitArma(w, pp, qq, horizon);
                    } catch (RuntimeException e) {
                        // Reported in the result (candidates_failed_to_fit), not dropped.
                        r.candidatesFailed.add("(" + pp + "," + qq + "): " + e.getMessage());
                        continue;
                    }
                    if (Double.isNaN(f.aic) || Double.isInfinite(f.aic)) {
                        r.candidatesFailed.add("(" + pp + "," + qq + "): non-finite AIC");
                        continue;
                    }
                    if (best == null || f.aic < best.aic) {
                        best = f;
                    }
                }
            }
            if (best == null) {
                throw new IllegalArgumentException("no ARMA candidate could be fit: "
                    + r.candidatesFailed);
            }
        }
        r.p = best.p;
        r.q = best.q;
        r.d = d;
        r.ar = best.ar;
        r.ma = best.ma;
        r.intercept = best.intercept;
        r.sigma2 = best.sigma2;
        r.aic = best.aic;
        r.nObs = x.length;
        r.nDifferenced = w.length;
        r.level = level;
        r.lastObserved = x[x.length - 1];
        r.forecast = integrate(x, d, best.forecast);
        double[] psi = psiWeights(best.ar, best.ma, d, horizon);
        r.se = new double[horizon];
        r.lower = new double[horizon];
        r.upper = new double[horizon];
        double cum = 0;
        for (int h = 0; h < horizon; h++) {
            cum += psi[h] * psi[h];
            r.se[h] = Math.sqrt(best.sigma2 * cum);
            r.lower[h] = r.forecast[h] - z * r.se[h];
            r.upper[h] = r.forecast[h] + z * r.se[h];
        }
        int lag = Math.max(1, Math.min(10, best.residuals.length / 5));
        r.ljungBoxLag = lag;
        r.ljungBoxP = BoxTest.ljung(best.residuals, lag).pvalue;
        return r;
    }

    // ─── GARCH(1,1) ────────────────────────────────────────────────────────────

    static final class GarchResult {
        boolean studentT;
        double mu;
        double omega;
        double alpha;
        double beta;
        double nu = Double.NaN;
        double logLik;
        double aic;
        double bic;
        int nObs;
        double persistence;
        double halfLifePeriods;
        double unconditionalVolPct;
        double lastConditionalVolPct;
        double nextVolPct;
        double lbSquaredStdResidP;
        int lbLag;
        String[] paramNames;
        double[] paramValues;
        double[] paramSe;          // null when the Hessian is unusable
        String seNote;
        // Kept for forecasting.
        double lastEpsSq;
        double lastSigma2;

        ObjectNode toJson(ObjectMapper m) {
            ObjectNode o = m.createObjectNode();
            o.put("model", "GARCH(1,1)");
            o.put("innovation_distribution", studentT ? "student_t" : "normal");
            o.put("estimator", "Gaussian/Student-t quasi-MLE, BOBYQA, best of 3 starts");
            o.put("units", "returns and volatilities are in percent per period");
            o.put("n_obs", nObs);
            ObjectNode params = o.putObject("parameters");
            for (int i = 0; i < paramNames.length; i++) {
                ObjectNode pn = params.putObject(paramNames[i]);
                pn.put("estimate", paramValues[i]);
                if (paramSe != null) {
                    pn.put("std_error", paramSe[i]);
                    pn.put("z", paramValues[i] / paramSe[i]);
                }
            }
            if (paramSe == null) {
                o.put("std_errors_unavailable", seNote);
            }
            o.put("log_likelihood", logLik);
            o.put("aic", aic);
            o.put("bic", bic);
            o.put("persistence_alpha_plus_beta", persistence);
            o.put("volatility_half_life_periods", halfLifePeriods);
            o.put("unconditional_vol_pct", unconditionalVolPct);
            o.put("last_conditional_vol_pct", lastConditionalVolPct);
            o.put("next_period_vol_pct", nextVolPct);
            o.put("ljung_box_squared_std_residuals_p_value", lbSquaredStdResidP);
            o.put("ljung_box_lag", lbLag);
            return o;
        }
    }

    private static double[] filterSigma2(double[] r, double mu, double omega, double alpha,
            double beta, double startVar) {
        int n = r.length;
        double[] s2 = new double[n];
        s2[0] = startVar;
        for (int t = 1; t < n; t++) {
            double e = r[t - 1] - mu;
            s2[t] = omega + alpha * e * e + beta * s2[t - 1];
        }
        return s2;
    }

    /** Negative log-likelihood; parameters are [mu, omega, alpha, beta(, nu)]. */
    private static double negLogLik(double[] r, double[] th, boolean t, double startVar) {
        double mu = th[0];
        double omega = th[1];
        double alpha = th[2];
        double beta = th[3];
        if (omega <= 0 || alpha < 0 || beta < 0 || alpha + beta >= 0.9999) {
            return 1e12;
        }
        double nu = t ? th[4] : Double.NaN;
        if (t && nu <= 2.01) {
            return 1e12;
        }
        double[] s2 = filterSigma2(r, mu, omega, alpha, beta, startVar);
        double ll = 0;
        double c = 0;
        if (t) {
            c = Gamma.logGamma((nu + 1) / 2) - Gamma.logGamma(nu / 2)
                - 0.5 * Math.log(Math.PI * (nu - 2));
        }
        for (int i = 0; i < r.length; i++) {
            double e = r[i] - mu;
            if (t) {
                ll += c - 0.5 * Math.log(s2[i])
                    - (nu + 1) / 2 * Math.log(1 + e * e / (s2[i] * (nu - 2)));
            } else {
                ll += -0.5 * (Math.log(2 * Math.PI) + Math.log(s2[i]) + e * e / s2[i]);
            }
        }
        return -ll;
    }

    /** Fits GARCH(1,1) to returns in percent. */
    static GarchResult garch(double[] r, boolean studentT) {
        if (r.length < GARCH_MIN_OBS) {
            throw new IllegalArgumentException("GARCH needs at least " + GARCH_MIN_OBS
                + " return observations, got " + r.length);
        }
        double v = variance(r);
        if (!(v > 0)) {
            throw new IllegalArgumentException("the return series has zero variance");
        }
        double sd = Math.sqrt(v);
        int k = studentT ? 5 : 4;
        // Optimise in a unit-free space: mu/sd, omega/var, alpha, beta, nu/100.
        double[] lo = {-1, 1e-6, 1e-6, 1e-6, 0.0205};
        double[] hi = {1, 1.0, 0.5, 0.9998, 1.0};
        double[] lo2 = Arrays.copyOf(lo, k);
        double[] hi2 = Arrays.copyOf(hi, k);
        double[][] starts = {{0.05, 0.90}, {0.10, 0.80}, {0.15, 0.70}};
        PointValuePair best = null;
        for (double[] ab : starts) {
            double[] z0 = new double[k];
            z0[0] = mean(r) / sd;
            z0[2] = ab[0];
            z0[3] = ab[1];
            z0[1] = Math.max(1e-5, 1 - ab[0] - ab[1]);
            if (studentT) {
                z0[4] = 0.08;
            }
            final boolean t = studentT;
            ObjectiveFunction f = new ObjectiveFunction(z -> {
                double[] th = new double[k];
                th[0] = z[0] * sd;
                th[1] = z[1] * v;
                th[2] = z[2];
                th[3] = z[3];
                if (t) {
                    th[4] = z[4] * 100;
                }
                return negLogLik(r, th, t, v);
            });
            PointValuePair res = new BOBYQAOptimizer(2 * k + 1, 0.05, 1e-9).optimize(
                new MaxEval(20000), f, GoalType.MINIMIZE, new InitialGuess(z0),
                new SimpleBounds(lo2, hi2));
            if (best == null || res.getValue() < best.getValue()) {
                best = res;
            }
        }
        double[] z = best.getPoint();
        double[] th = new double[k];
        th[0] = z[0] * sd;
        th[1] = z[1] * v;
        th[2] = z[2];
        th[3] = z[3];
        if (studentT) {
            th[4] = z[4] * 100;
        }
        if (best.getValue() >= 1e11) {
            throw new IllegalArgumentException("GARCH optimisation found no admissible "
                + "parameters (stationarity constraint alpha+beta<1 never satisfied)");
        }
        GarchResult g = new GarchResult();
        g.studentT = studentT;
        g.mu = th[0];
        g.omega = th[1];
        g.alpha = th[2];
        g.beta = th[3];
        if (studentT) {
            g.nu = th[4];
        }
        g.nObs = r.length;
        g.logLik = -best.getValue();
        g.aic = 2.0 * k - 2 * g.logLik;
        g.bic = k * Math.log(r.length) - 2 * g.logLik;
        g.persistence = g.alpha + g.beta;
        g.halfLifePeriods = Math.log(0.5) / Math.log(g.persistence);
        double uncondVar = g.omega / (1 - g.persistence);
        g.unconditionalVolPct = Math.sqrt(uncondVar);
        double[] s2 = filterSigma2(r, g.mu, g.omega, g.alpha, g.beta, v);
        int n = r.length;
        double eLast = r[n - 1] - g.mu;
        g.lastEpsSq = eLast * eLast;
        g.lastSigma2 = s2[n - 1];
        g.lastConditionalVolPct = Math.sqrt(s2[n - 1]);
        g.nextVolPct = Math.sqrt(g.omega + g.alpha * g.lastEpsSq + g.beta * g.lastSigma2);
        // Diagnostics: a well-specified model leaves no ARCH effect in the standardized
        // residuals, so squared standardized residuals should look like white noise.
        double[] zsq = new double[n];
        for (int i = 0; i < n; i++) {
            double e = (r[i] - g.mu) / Math.sqrt(s2[i]);
            zsq[i] = e * e;
        }
        g.lbLag = Math.max(1, Math.min(10, n / 5));
        g.lbSquaredStdResidP = BoxTest.ljung(zsq, g.lbLag).pvalue;
        fillParams(g, r, th, k, v);
        return g;
    }

    private static void fillParams(GarchResult g, double[] r, double[] th, int k, double v) {
        g.paramNames = studentTNames(k);
        g.paramValues = th.clone();
        double[][] h = new double[k][k];
        double[] step = new double[k];
        for (int i = 0; i < k; i++) {
            step[i] = 1e-3 * Math.max(Math.abs(th[i]), 1e-2);
        }
        boolean t = k == 5;
        double f0 = negLogLik(r, th, t, v);
        for (int i = 0; i < k; i++) {
            for (int j = i; j < k; j++) {
                double[] pp = th.clone();
                double[] pm = th.clone();
                double[] mp = th.clone();
                double[] mm = th.clone();
                pp[i] += step[i];
                pp[j] += step[j];
                pm[i] += step[i];
                pm[j] -= step[j];
                mp[i] -= step[i];
                mp[j] += step[j];
                mm[i] -= step[i];
                mm[j] -= step[j];
                double d2;
                if (i == j) {
                    double[] up = th.clone();
                    double[] dn = th.clone();
                    up[i] += step[i];
                    dn[i] -= step[i];
                    d2 = (negLogLik(r, up, t, v) - 2 * f0 + negLogLik(r, dn, t, v))
                        / (step[i] * step[i]);
                } else {
                    d2 = (negLogLik(r, pp, t, v) - negLogLik(r, pm, t, v)
                        - negLogLik(r, mp, t, v) + negLogLik(r, mm, t, v))
                        / (4 * step[i] * step[j]);
                }
                h[i][j] = d2;
                h[j][i] = d2;
            }
        }
        try {
            RealMatrix hm = new Array2DRowRealMatrix(h);
            new CholeskyDecomposition(hm);
            RealMatrix cov = MatrixUtils.inverse(hm);
            double[] se = new double[k];
            for (int i = 0; i < k; i++) {
                se[i] = Math.sqrt(cov.getEntry(i, i));
                if (Double.isNaN(se[i]) || Double.isInfinite(se[i])) {
                    throw new IllegalStateException("non-finite variance for "
                        + g.paramNames[i]);
                }
            }
            g.paramSe = se;
        } catch (NonPositiveDefiniteMatrixException | IllegalStateException
                 | org.apache.commons.math3.linear.SingularMatrixException e) {
            // Surfaced to the caller as std_errors_unavailable, not swallowed: a parameter on
            // its bound (alpha or beta near 0) makes the numerical Hessian unusable.
            g.paramSe = null;
            g.seNote = "numerical Hessian at the optimum is not positive definite ("
                + e.getClass().getSimpleName() + ") — typically a parameter sits on its bound; "
                + "treat the point estimates as descriptive";
        }
    }

    private static String[] studentTNames(int k) {
        return k == 5 ? new String[]{"mu", "omega", "alpha", "beta", "nu"}
            : new String[]{"mu", "omega", "alpha", "beta"};
    }

    /** Conditional variance path for periods T+1..T+horizon, in percent^2. */
    static double[] garchVarianceForecast(GarchResult g, int horizon) {
        checkHorizon(horizon);
        double uncond = g.omega / (1 - g.persistence);
        double s1 = g.omega + g.alpha * g.lastEpsSq + g.beta * g.lastSigma2;
        double[] out = new double[horizon];
        for (int h = 1; h <= horizon; h++) {
            out[h - 1] = uncond + Math.pow(g.persistence, h - 1) * (s1 - uncond);
        }
        return out;
    }

    static ObjectNode garchForecastJson(GarchResult g, int horizon, double periodsPerYear,
            double level) {
        double[] var = garchVarianceForecast(g, horizon);
        ObjectNode o = g.toJson(MAPPER);
        double[] daily = new double[horizon];
        double[] cumulative = new double[horizon];
        double cum = 0;
        for (int i = 0; i < horizon; i++) {
            daily[i] = Math.sqrt(var[i]);
            cum += var[i];
            cumulative[i] = Math.sqrt(cum);
        }
        o.put("horizon", horizon);
        o.put("periods_per_year", periodsPerYear);
        o.set("forecast_period_vol_pct", arr(daily));
        o.set("forecast_cumulative_vol_pct", arr(cumulative));
        double[] annual = new double[horizon];
        for (int i = 0; i < horizon; i++) {
            annual[i] = daily[i] * Math.sqrt(periodsPerYear);
        }
        o.set("forecast_annualized_vol_pct", arr(annual));
        double z = zFor(level);
        double[] lo = new double[horizon];
        double[] hi = new double[horizon];
        for (int i = 0; i < horizon; i++) {
            lo[i] = g.mu * (i + 1) - z * cumulative[i];
            hi[i] = g.mu * (i + 1) + z * cumulative[i];
        }
        o.put("interval_level", level);
        o.set("cumulative_return_lower_pct", arr(lo));
        o.set("cumulative_return_upper_pct", arr(hi));
        o.put("interval_basis", "normal quantiles of the cumulative log return; exact for "
            + "normal innovations at h=1, an approximation otherwise (the sum of h "
            + "innovations is closer to normal than any one)");
        return o;
    }

    // ─── Volatility forecaster ─────────────────────────────────────────────────

    /** RiskMetrics EWMA conditional variance path; returns the one-step-ahead variance. */
    static double ewmaNextVariance(double[] r, double lambda) {
        if (!(lambda > 0 && lambda < 1)) {
            throw new IllegalArgumentException("ewma_lambda must be strictly between 0 and 1");
        }
        double m = mean(r);
        int seed = Math.min(r.length, 30);
        double s2 = 0;
        for (int i = 0; i < seed; i++) {
            s2 += (r[i] - m) * (r[i] - m);
        }
        s2 /= seed;
        for (int i = 0; i < r.length; i++) {
            double e = r[i] - m;
            s2 = lambda * s2 + (1 - lambda) * e * e;
        }
        return s2;
    }

    /**
     * Forecasts volatility with GARCH(1,1), EWMA and trailing historical estimates, and turns
     * the chosen method into a normalized band for the price {@code horizon} periods ahead
     * (band edges divided by the last price, so 1.0 is unchanged).
     */
    static ObjectNode volatilityForecast(double[] prices, String[] sortedLabels, int horizon,
            double level, double periodsPerYear, String method, double ewmaLambda, int window,
            boolean studentT) {
        checkHorizon(horizon);
        if (!"garch".equals(method) && !"ewma".equals(method) && !"historical".equals(method)) {
            throw new IllegalArgumentException("method must be 'garch', 'ewma' or "
                + "'historical', got '" + method + "'");
        }
        double[] r = logReturnsPercent(prices);
        if (window < 5 || window > r.length) {
            throw new IllegalArgumentException("window must be between 5 and the number of "
                + "returns (" + r.length + "), got " + window);
        }
        double sqrtAnn = Math.sqrt(periodsPerYear);
        ObjectNode o = MAPPER.createObjectNode();
        o.put("last_date", sortedLabels[sortedLabels.length - 1]);
        o.put("last_price", prices[prices.length - 1]);
        o.put("n_returns", r.length);
        o.put("horizon", horizon);
        o.put("periods_per_year", periodsPerYear);
        o.put("units", "volatilities in percent per period unless named annualized");

        double histSd = Math.sqrt(variance(Arrays.copyOfRange(r, r.length - window, r.length)));
        double ewmaVar = ewmaNextVariance(r, ewmaLambda);
        ObjectNode methods = o.putObject("methods");
        ObjectNode hist = methods.putObject("historical");
        hist.put("window", window);
        hist.put("period_vol_pct", histSd);
        hist.put("annualized_vol_pct", histSd * sqrtAnn);
        ObjectNode ew = methods.putObject("ewma");
        ew.put("lambda", ewmaLambda);
        ew.put("period_vol_pct", Math.sqrt(ewmaVar));
        ew.put("annualized_vol_pct", Math.sqrt(ewmaVar) * sqrtAnn);

        double chosenCumVar;
        double drift;
        if ("garch".equals(method)) {
            GarchResult g = garch(r, studentT);
            double[] var = garchVarianceForecast(g, horizon);
            double cum = 0;
            for (double x : var) {
                cum += x;
            }
            chosenCumVar = cum;
            drift = g.mu * horizon;
            ObjectNode gj = methods.putObject("garch");
            gj.put("distribution", studentT ? "student_t" : "normal");
            gj.put("persistence", g.persistence);
            gj.put("half_life_periods", g.halfLifePeriods);
            gj.put("unconditional_vol_pct", g.unconditionalVolPct);
            gj.put("period_vol_pct", Math.sqrt(var[0]));
            gj.put("annualized_vol_pct", Math.sqrt(var[0]) * sqrtAnn);
            gj.put("ljung_box_squared_std_residuals_p_value", g.lbSquaredStdResidP);
            gj.set("period_vol_path_pct", arr(sqrtAll(var)));
        } else if ("ewma".equals(method)) {
            chosenCumVar = ewmaVar * horizon;
            drift = mean(r) * horizon;
        } else {
            chosenCumVar = histSd * histSd * horizon;
            drift = mean(r) * horizon;
        }
        o.put("band_method", method);
        double z = zFor(level);
        double cumSd = Math.sqrt(chosenCumVar);
        double last = prices[prices.length - 1];
        double loN = Math.exp((drift - z * cumSd) / 100.0);
        double hiN = Math.exp((drift + z * cumSd) / 100.0);
        double medN = Math.exp(drift / 100.0);
        ObjectNode band = o.putObject("price_band");
        band.put("level", level);
        band.put("horizon_periods", horizon);
        band.put("cumulative_vol_pct", cumSd);
        band.put("drift_pct", drift);
        band.put("normalized_lower", loN);
        band.put("normalized_median", medN);
        band.put("normalized_upper", hiN);
        band.put("price_lower", last * loN);
        band.put("price_median", last * medN);
        band.put("price_upper", last * hiN);
        band.put("note", "A volatility band, not a directional prediction: the drift term is "
            + "the sample mean return, which is statistically indistinguishable from zero over "
            + "any horizon this short. The band says how far the price is likely to move, "
            + "not which way.");
        return o;
    }

    private static double[] sqrtAll(double[] v) {
        double[] out = new double[v.length];
        for (int i = 0; i < v.length; i++) {
            out[i] = Math.sqrt(v[i]);
        }
        return out;
    }

    // ─── Backtest ──────────────────────────────────────────────────────────────

    static final int BACKTEST_MAX_EVALS = 400;

    /** Cumulative variance (percent^2) and drift (percent) over {@code horizon} periods from
     *  returns {@code r}, by the named method. */
    private static double[] cumVarAndDrift(double[] r, String method, int horizon,
            double ewmaLambda, int window, boolean studentT) {
        if ("garch".equals(method)) {
            GarchResult g = garch(r, studentT);
            double cum = 0;
            for (double v : garchVarianceForecast(g, horizon)) {
                cum += v;
            }
            return new double[]{cum, g.mu * horizon};
        }
        if ("ewma".equals(method)) {
            return new double[]{ewmaNextVariance(r, ewmaLambda) * horizon, mean(r) * horizon};
        }
        if ("historical".equals(method)) {
            double sd = Math.sqrt(variance(Arrays.copyOfRange(r, r.length - window, r.length)));
            return new double[]{sd * sd * horizon, mean(r) * horizon};
        }
        throw new IllegalArgumentException("method must be 'garch', 'ewma' or 'historical', "
            + "got '" + method + "'");
    }

    /** Kupiec proportion-of-failures likelihood-ratio p-value: are {@code violations} out of
     *  {@code n} consistent with a miss rate of {@code 1 - level}? */
    static double kupiecPValue(int violations, int n, double level) {
        double p0 = 1 - level;
        double phat = (double) violations / n;
        double ll0 = (n - violations) * Math.log(1 - p0) + violations * Math.log(p0);
        double ll1 = (n - violations) * Math.log(1 - phat)
            + (violations == 0 ? 0 : violations * Math.log(phat));
        double lr = Math.max(0, -2 * (ll0 - ll1));
        return 1 - new org.apache.commons.math3.distribution.ChiSquaredDistribution(1)
            .cumulativeProbability(lr);
    }

    /**
     * Walk-forward test of the volatility bands: at each origin, fit on prices up to it only,
     * build the {@code horizon}-period band, and compare it with what the price actually did.
     * A 95% band should miss about 5% of the time (Kupiec test) and be as narrow as it can
     * while doing so (mean width, Winkler interval score — lower is better). Origins are
     * spaced {@code step} apart so refitting GARCH each time stays affordable.
     */
    static ObjectNode backtestVolatility(double[] prices, String[] sortedLabels, int horizon,
            double level, String[] methods, int minTrain, int step, double ewmaLambda,
            int window, boolean studentT) {
        checkHorizon(horizon);
        if (step < 1) {
            throw new IllegalArgumentException("step must be at least 1");
        }
        if (minTrain < GARCH_MIN_OBS + 1 || minTrain < window + 1) {
            throw new IllegalArgumentException("min_train must exceed both "
                + GARCH_MIN_OBS + " and the historical window (" + window + ")");
        }
        int lastOrigin = prices.length - 1 - horizon;
        if (lastOrigin < minTrain) {
            throw new IllegalArgumentException("series has " + prices.length + " prices; need "
                + "more than min_train + horizon = " + (minTrain + horizon));
        }
        int evals = (lastOrigin - minTrain) / step + 1;
        if (evals > BACKTEST_MAX_EVALS) {
            throw new IllegalArgumentException("that setting means " + evals + " evaluations "
                + "per method (limit " + BACKTEST_MAX_EVALS + "); raise step to at least "
                + (int) Math.ceil((double) (lastOrigin - minTrain + 1) / BACKTEST_MAX_EVALS));
        }
        double z = zFor(level);
        double alpha = 1 - level;
        ObjectNode o = MAPPER.createObjectNode();
        o.put("first_period", sortedLabels[0]);
        o.put("last_period", sortedLabels[sortedLabels.length - 1]);
        o.put("horizon", horizon);
        o.put("level", level);
        o.put("min_train", minTrain);
        o.put("step", step);
        o.put("evaluations_per_method", evals);
        o.put("first_origin", sortedLabels[minTrain]);
        o.put("last_origin", sortedLabels[minTrain + (evals - 1) * step]);
        o.put("units", "band width and interval score in percent of the last price");
        ObjectNode res = o.putObject("methods");
        for (String method : methods) {
            int below = 0;
            int above = 0;
            double widthSum = 0;
            double winklerSum = 0;
            List<String> violationOrigins = new ArrayList<>();
            for (int e = 0; e < evals; e++) {
                int t = minTrain + e * step;
                double[] r = logReturnsPercent(Arrays.copyOfRange(prices, 0, t + 1));
                double[] cv = cumVarAndDrift(r, method, horizon, ewmaLambda, window, studentT);
                double sd = Math.sqrt(cv[0]);
                double lo = cv[1] - z * sd;
                double hi = cv[1] + z * sd;
                double realized = 100.0 * Math.log(prices[t + horizon] / prices[t]);
                double miss = 0;
                if (realized < lo) {
                    below++;
                    miss = lo - realized;
                    violationOrigins.add(sortedLabels[t]);
                } else if (realized > hi) {
                    above++;
                    miss = realized - hi;
                    violationOrigins.add(sortedLabels[t]);
                }
                widthSum += hi - lo;
                winklerSum += (hi - lo) + (2.0 / alpha) * miss;
            }
            int viol = below + above;
            ObjectNode m = res.putObject(method);
            m.put("violations", viol);
            m.put("violations_below_lower", below);
            m.put("violations_above_upper", above);
            ArrayNode origins = m.putArray("violation_origins");
            for (String lbl : violationOrigins) {
                origins.add(lbl);
            }
            m.put("observed_coverage", 1.0 - (double) viol / evals);
            m.put("expected_coverage", level);
            m.put("kupiec_p_value", kupiecPValue(viol, evals, level));
            m.put("mean_band_width_pct", widthSum / evals);
            m.put("mean_interval_score_pct", winklerSum / evals);
            m.put("reading", evals < 30
                ? "fewer than 30 evaluations: coverage is too noisy to judge"
                : kupiecPValue(viol, evals, level) < 0.05
                    ? "coverage differs from nominal at the 5% level — band is mis-calibrated"
                    : "coverage is consistent with nominal");
        }
        o.put("note", "Walk-forward on data through last_period only. This validates how "
            + "well each method's band was calibrated historically; it says nothing about "
            + "periods after last_period. Overlapping horizons (horizon > step) make the "
            + "evaluations dependent, so the Kupiec p-value is optimistic in that case.");
        return o;
    }
}
