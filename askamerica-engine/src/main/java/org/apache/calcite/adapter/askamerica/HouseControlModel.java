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

import org.apache.commons.math3.distribution.NormalDistribution;

/**
 * A fitted, documented model of which party controls the U.S. House after an election. The
 * coefficients are constants from one ordinary-least-squares fit; nothing is fitted at run
 * time.
 *
 * <p>Fit: the Democratic share of the 435 seats, by cycle, 1978-2024 (n = 24; 1976 drops out
 * because the 1972 sitting president is not in the corpus). Seats come from the decisive round
 * of {@code officials.house_election_results}. Regressors:
 * <ul>
 *   <li>{@code inc}: +1 when the sitting president is a Democrat, -1 when a Republican;</li>
 *   <li>{@code mid}: 1 in a midterm year (no presidential election), 0 otherwise;</li>
 *   <li>{@code lag}: the previous cycle's national Democratic two-party House vote share
 *       minus 0.5.</li>
 * </ul>
 * {@code seatShare = 0.501 + 0.002 inc - 0.053 inc mid + 1.47 lag}, residual sd 0.041.
 * P(Democrats control) = P(Normal(seatShare, 0.041) > 218/435).
 *
 * <p>Stated limits, which every use carries in {@link #SOURCE}: out-of-sample, leave one cycle
 * out, the Brier score is 0.109 over 24 cycles (a coin flip scores 0.250, "last control
 * repeats" 0.208) and 22 of 24 controls are called right. The specification was chosen on
 * those same 24 points, so that Brier is optimistic. The model covers the House only. The
 * probability treats the residual as normal, an approximation. Real income growth added
 * nothing out of sample and is not in it.
 */
final class HouseControlModel {
  static final double INTERCEPT = 0.501;
  static final double INCUMBENT = 0.002;
  static final double INCUMBENT_MIDTERM = -0.053;
  static final double LAG = 1.47;
  static final double RESIDUAL_SD = 0.041;
  static final double MAJORITY_SHARE = 218.0 / 435.0;
  static final double BRIER = 0.109;
  static final int CYCLES = 24;

  /** The text every use of the model carries as its probability_source. */
  static final String SOURCE = "house_control_model: OLS fit of Democratic House seat share "
      + "on sitting-president party, midterm and the prior cycle's national Democratic House "
      + "vote share (officials.house_election_results, 1978-2024, n=24). Leave-one-cycle-out "
      + "Brier 0.109 (coin flip 0.250; previous control repeats 0.208), 22 of 24 controls "
      + "called right. The specification was chosen on the same 24 points, so the Brier is "
      + "optimistic. House only; no Senate model. Probability assumes normal residuals "
      + "(sd 0.041 of seat share)";

  private HouseControlModel() {
  }

  /** Predicted Democratic share of House seats. */
  static double seatShare(boolean presidentDemocrat, boolean midterm,
      double priorDemocraticVoteShare) {
    double inc = presidentDemocrat ? 1 : -1;
    double mid = midterm ? 1 : 0;
    return INTERCEPT + INCUMBENT * inc + INCUMBENT_MIDTERM * inc * mid
        + LAG * (priorDemocraticVoteShare - 0.5);
  }

  /** P(Democrats win a majority, 218 of 435 seats). */
  static double probabilityDemocraticControl(boolean presidentDemocrat, boolean midterm,
      double priorDemocraticVoteShare) {
    if (!(priorDemocraticVoteShare > 0 && priorDemocraticVoteShare < 1)) {
      throw new IllegalArgumentException("prior_d_vote_share is the previous cycle's national "
          + "Democratic two-party House vote share, strictly between 0 and 1, got "
          + priorDemocraticVoteShare);
    }
    double mean = seatShare(presidentDemocrat, midterm, priorDemocraticVoteShare);
    return 1 - new NormalDistribution(mean, RESIDUAL_SD).cumulativeProbability(MAJORITY_SHARE);
  }
}
