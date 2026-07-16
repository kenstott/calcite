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
package org.apache.calcite.adapter.govdata.health;

/**
 * Canonicalises the two NDC representations used in the health schema to a single
 * 9-digit, zero-padded product key (labeler[5] + product[4]).
 *
 * <p>{@code fda_ndc_products.product_ndc} is openFDA's hyphenated 2-segment product NDC
 * whose segment lengths vary (4-4, 5-3, 5-4), e.g. {@code 0002-1433}, {@code 24909-042}.
 * {@code medicaid_drug_utilization.ndc} is CMS's 11-digit numeric package NDC in the
 * canonical 5-4-2 layout, e.g. {@code 00002143301}. A raw {@code ndc = product_ndc}
 * join therefore matches nothing.
 *
 * <p>Both sides are reduced to the same product key: pad each product-NDC segment to
 * 5-4, or drop the 2-digit package segment from the 11-digit form. The result
 * ({@code product_ndc9}) is the join column carried by both tables.
 */
public final class NdcNormalizer {

  private NdcNormalizer() {
  }

  /**
   * Normalise a hyphenated openFDA product NDC (e.g. {@code 0002-1433}, {@code 24909-042})
   * to a 9-digit key. Returns {@code null} if the input is not a single-dash, numeric,
   * 2-segment product NDC within the 5-4 canonical widths.
   */
  public static String fromProductNdc(String productNdc) {
    if (productNdc == null) {
      return null;
    }
    String trimmed = productNdc.trim();
    int dash = trimmed.indexOf('-');
    if (dash <= 0 || dash >= trimmed.length() - 1) {
      return null;
    }
    if (trimmed.indexOf('-', dash + 1) >= 0) {
      return null;
    }
    String labeler = trimmed.substring(0, dash);
    String product = trimmed.substring(dash + 1);
    if (!isDigits(labeler) || !isDigits(product)
        || labeler.length() > 5 || product.length() > 4) {
      return null;
    }
    return leftPad(labeler, 5) + leftPad(product, 4);
  }

  /**
   * Normalise a CMS Medicaid 11-digit numeric NDC (5-4-2) to the same 9-digit product
   * key by dropping the 2-digit package segment. Non-digit characters are stripped and
   * leading zeros dropped upstream are restored by padding to 11 first. Returns
   * {@code null} if no digits remain or more than 11 digits are present.
   */
  public static String fromElevenDigitNdc(String ndc) {
    if (ndc == null) {
      return null;
    }
    String digits = ndc.replaceAll("[^0-9]", "");
    if (digits.isEmpty() || digits.length() > 11) {
      return null;
    }
    return leftPad(digits, 11).substring(0, 9);
  }

  private static boolean isDigits(String s) {
    if (s.isEmpty()) {
      return false;
    }
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isDigit(s.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  private static String leftPad(String s, int width) {
    if (s.length() >= width) {
      return s;
    }
    StringBuilder sb = new StringBuilder(width);
    for (int i = s.length(); i < width; i++) {
      sb.append('0');
    }
    sb.append(s);
    return sb.toString();
  }
}
