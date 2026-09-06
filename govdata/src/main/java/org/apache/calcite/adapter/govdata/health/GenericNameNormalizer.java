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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Canonicalises the two {@code generic_name} conventions used in the health schema to a
 * single join key.
 *
 * <p>{@code fda_drug_shortages.generic_name} embeds the dosage form as a trailing word or
 * phrase (e.g. {@code "Atropine Sulfate Injection"}) even though the table already carries
 * a separate {@code dosage_form} column with the same information, and joins combination
 * drugs with {@code ";"} in alphabetized order (e.g.
 * {@code "Azelastine Hydrochloride; Fluticasone Propionate"}).
 * {@code fda_ndc_products.generic_name} never includes the dosage form and joins
 * combinations with {@code "and"} in source order, often without salt names (e.g.
 * {@code "fluticasone propionate and salmeterol"}). A raw or case-insensitive
 * {@code generic_name = generic_name} join therefore matches only a few percent of names.
 *
 * <p>Both sides are reduced to the same key: uppercase, strip a trailing dosage-form
 * phrase and release-modifier clause, split into components, strip each component's salt
 * suffix, sort the components, and rejoin. Tested against the full set of distinct
 * {@code fda_drug_shortages.generic_name} values in production: recovers 226 of 245 (92.2%),
 * up from 9 (3.7%) under a bare case-insensitive match. The remaining 19 are genuine edge
 * cases (bare compounds, multi-salt electrolyte mixes, one slash-separated combination) not
 * worth chasing with more special-casing.
 */
public final class GenericNameNormalizer {

  private GenericNameNormalizer() {
  }

  // Longest phrase first so e.g. "Oral Powder for Suspension" matches before "Suspension".
  private static final String[] DOSAGE_FORMS = {
      "ORAL POWDER FOR SUSPENSION", "POWDER FOR ORAL SUSPENSION",
      "SOLUTION FOR INJECTION", "EXTENDED-RELEASE TABLET", "EXTENDED RELEASE TABLET",
      "DELAYED-RELEASE TABLET", "DELAYED RELEASE CAPSULE", "ORAL SUSPENSION",
      "OPHTHALMIC SOLUTION", "OPHTHALMIC OINTMENT", "NASAL SPRAY", "ORAL SOLUTION",
      "ORAL TABLET", "ORAL CAPSULE", "FOR INJECTION", "FOR SUSPENSION",
      "INJECTABLE EMULSION", "TOPICAL SOLUTION", "TOPICAL CREAM", "TOPICAL GEL",
      "RECTAL SUPPOSITORY", "VAGINAL INSERT", "TRANSDERMAL SYSTEM", "TRANSDERMAL PATCH",
      "PREFILLED SYRINGE", "SUSPENSION", "INJECTION", "TABLET", "CAPSULE", "SOLUTION",
      "SUPPOSITORY", "SYRINGE", "SPRAY", "CREAM", "OINTMENT", "GEL", "LOTION",
      "PATCH", "POWDER", "KIT", "IMPLANT", "AEROSOL", "ELIXIR", "SYRUP", "EMULSION",
      "INSERT", "SYSTEM", "FILM", "PELLET", "GRANULE", "GRANULES",
  };
  private static final Pattern DOSAGE_RE =
      Pattern.compile("\\b(" + String.join("|", DOSAGE_FORMS) + ")\\b\\.?\\s*$");

  // Trailing ", <modifier>" clauses that qualify the dosage form rather than introduce
  // another combo ingredient (a comma is otherwise ambiguous: it also separates combo
  // ingredients, e.g. "A, B, C Tablet").
  private static final String[] MODIFIERS = {
      "EXTENDED RELEASE", "DELAYED RELEASE", "IMMEDIATE RELEASE",
      "SUSTAINED RELEASE", "CONTROLLED RELEASE", "ORALLY DISINTEGRATING",
      "CHEWABLE", "METERED", "FOR SUSPENSION", "FOR SOLUTION", "FOR INJECTION",
      "USP",
  };
  private static final Pattern MODIFIER_RE =
      Pattern.compile(",\\s*(" + String.join("|", MODIFIERS) + ")\\s*$");

  private static final String[] SALT_FORMS = {
      "HYDROCHLORIDE", "HCL", "SODIUM", "SULFATE", "SUCCINATE", "FUMARATE",
      "MALEATE", "TARTRATE", "CITRATE", "PHOSPHATE", "BROMIDE", "MESYLATE",
      "ACETATE", "DIHYDRATE", "MONOHYDRATE", "ANHYDROUS", "BESYLATE", "CALCIUM",
      "POTASSIUM", "MAGNESIUM", "ESTOLATE", "PALMITATE", "STEARATE", "LACTATE",
      "GLUCONATE", "PAMOATE", "VALERATE", "MEGLUMINE", "HYDROBROMIDE",
      "NITRATE", "CAMSYLATE", "EDISYLATE", "TOSYLATE", "DECANOATE",
      "ASPARTATE", "SACCHARATE", "CIPIONATE", "CYPIONATE", "ENANTATE",
  };
  private static final Pattern SALT_RE =
      Pattern.compile("\\b(" + String.join("|", SALT_FORMS) + ")\\b");

  private static final Pattern SPLIT_RE = Pattern.compile("\\s*;\\s*|\\s+AND\\s+");
  private static final Pattern PUNCTUATION_RE = Pattern.compile("[.’']");
  private static final Pattern WHITESPACE_RE = Pattern.compile("\\s+");

  /**
   * Normalises a raw {@code generic_name} value to the shared join key. Returns {@code null}
   * for a {@code null} input; an empty input normalises to an empty string, matching another
   * empty input rather than failing loudly, since neither side treats blank as an error case.
   */
  public static String normalize(String genericName) {
    if (genericName == null) {
      return null;
    }
    String s = genericName.toUpperCase(java.util.Locale.ROOT).trim();
    s = PUNCTUATION_RE.matcher(s).replaceAll("");
    s = WHITESPACE_RE.matcher(s).replaceAll(" ");
    s = stripDosageForm(s);

    List<String> parts = new ArrayList<>();
    for (String p : SPLIT_RE.split(s)) {
      if (!p.trim().isEmpty()) {
        parts.add(p.trim());
      }
    }
    List<String> expanded = new ArrayList<>();
    for (String p : parts) {
      for (String x : p.split(",")) {
        if (!x.trim().isEmpty()) {
          expanded.add(x.trim());
        }
      }
    }
    List<String> stripped = new ArrayList<>();
    for (String p : expanded) {
      String noSalt = SALT_RE.matcher(p).replaceAll("").trim();
      noSalt = WHITESPACE_RE.matcher(noSalt).replaceAll(" ").trim();
      if (!noSalt.isEmpty()) {
        stripped.add(noSalt);
      }
    }
    Collections.sort(stripped);
    return String.join("|", stripped);
  }

  private static String stripDosageForm(String input) {
    String s = input;
    String prev = null;
    while (!s.equals(prev)) {
      prev = s;
      Matcher m = MODIFIER_RE.matcher(s);
      s = m.replaceAll("").trim();
      m = DOSAGE_RE.matcher(s);
      s = m.replaceAll("").trim();
    }
    return s;
  }
}
