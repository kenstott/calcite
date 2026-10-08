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
package org.apache.calcite.adapter.servicenow;

import org.apache.calcite.adapter.servicenow.ServiceNowColumn.Kind;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * The table of filter shapes that may be sent to ServiceNow, and which of them are verified.
 *
 * <p>An <em>entry</em> is one combination of comparison, column type and position in the boolean
 * structure, written {@code OP:TYPE:POSITION}, for example {@code EQ:TEXT:AND} (equality on a text
 * column as a top-level conjunct) or {@code LIKE_PREFIX:TEXT:OR} (a STARTSWITH inside an OR
 * group). Further entries cover the boolean structure ({@code SHAPE:*}) and value text that needs
 * escaping care ({@code VALUE:SPECIAL}).
 *
 * <p><b>Every entry is unverified until proven.</b> ServiceNow drops an invalid {@code
 * sysparm_query} term and runs the rest, and several of its comparisons differ from SQL (empty
 * values, case, time zone). An entry is pushed only if it is in the verified set, which comes from
 * a verification record written by the differential harness after a live run
 * ({@code PushdownHarness}), or from the {@code trustPushdown} operand that names entries
 * explicitly (see {@link PushdownVerification}). With the shipped, empty record nothing is pushed
 * and every filter is evaluated by Calcite. An entry the translator would need but that is not
 * verified makes the whole filter stay in Calcite, never a partial or silent push.
 */
final class PushdownCapabilities {

  /** Column types as the pushdown distinguishes them. */
  enum TypeGroup {
    TEXT, NUMERIC, BOOLEAN, TIMESTAMP, DATE, REFERENCE, GUID;

    static TypeGroup of(Kind kind) {
      switch (kind) {
      case TEXT:
        return TEXT;
      case INTEGER:
      case LONG:
      case DECIMAL:
      case CURRENCY:
      case DOUBLE:
        return NUMERIC;
      case BOOLEAN:
        return BOOLEAN;
      case TIMESTAMP:
        return TIMESTAMP;
      case DATE:
        return DATE;
      case REFERENCE:
        return REFERENCE;
      case GUID:
        return GUID;
      default:
        return null; // TIME: not pushed
      }
    }
  }

  /** The comparisons. */
  enum Op {
    EQ, NE,
    /**
     * {@code <>} written as {@code !=} plus {@code ISNOTEMPTY}. SQL's {@code <>} excludes NULL
     * (empty) rows; if ServiceNow's {@code !=} includes them this is the only exact form.
     */
    NE_NOT_EMPTY,
    LT, LE, GT, GE, IN, NOT_IN,
    /** {@code NOT IN} plus {@code ISNOTEMPTY}, for the same reason as {@link #NE_NOT_EMPTY}. */
    NOT_IN_NOT_EMPTY,
    IS_NULL, IS_NOT_NULL,
    LIKE_EXACT, LIKE_PREFIX, LIKE_SUFFIX, LIKE_CONTAINS
  }

  /** Where in the boolean structure the comparison sits. */
  enum Position {
    /** A top-level conjunct, joined to others with {@code ^}. */
    AND,
    /** A member of an OR group, joined with {@code ^OR}. */
    OR
  }

  /** Shape entries: how terms combine. */
  static final String SHAPE_AND = "SHAPE:AND";
  static final String SHAPE_OR_GROUP = "SHAPE:OR_GROUP";
  static final String SHAPE_OR_WITH_OTHER_TERMS = "SHAPE:OR_WITH_OTHER_TERMS";
  /** Values that contain punctuation which might be read as query syntax. */
  static final String VALUE_SPECIAL = "VALUE:SPECIAL";

  /** Every entry the translator can use, whether verified or not. */
  private static final Set<String> CANDIDATES = new TreeSet<>();

  static {
    CANDIDATES.add(SHAPE_AND);
    CANDIDATES.add(SHAPE_OR_GROUP);
    CANDIDATES.add(SHAPE_OR_WITH_OTHER_TERMS);
    CANDIDATES.add(VALUE_SPECIAL);
    for (TypeGroup group : TypeGroup.values()) {
      for (Op op : Op.values()) {
        for (Position position : Position.values()) {
          if (declared(op, group, position)) {
            CANDIDATES.add(id(op, group, position));
          }
        }
      }
    }
  }

  /** Whether the combination is one the translator may ever generate. */
  static boolean declared(Op op, TypeGroup group, Position position) {
    if (group == TypeGroup.BOOLEAN) {
      // Calcite rewrites <> and NOT IN on a boolean to = or NOT, and IN over both values to
      // IS NOT NULL, so only these reach the scan
      switch (op) {
      case NE:
      case NE_NOT_EMPTY:
      case IN:
      case NOT_IN:
      case NOT_IN_NOT_EMPTY:
        return false;
      default:
        break;
      }
    }
    switch (op) {
    case NE_NOT_EMPTY:
    case NOT_IN_NOT_EMPTY:
      // The extra ISNOTEMPTY term cannot sit inside an OR group
      return position == Position.AND;
    case LT:
    case LE:
    case GT:
    case GE:
      return group == TypeGroup.NUMERIC || group == TypeGroup.TIMESTAMP
          || group == TypeGroup.DATE || group == TypeGroup.TEXT;
    case LIKE_EXACT:
    case LIKE_PREFIX:
    case LIKE_SUFFIX:
    case LIKE_CONTAINS:
      return group == TypeGroup.TEXT;
    default:
      return true;
    }
  }

  static String id(Op op, TypeGroup group, Position position) {
    return op + ":" + group + ":" + position;
  }

  /** The ids of all entries, for documentation, the harness and validation. */
  static Set<String> candidates() {
    return Collections.unmodifiableSet(CANDIDATES);
  }

  /** Pushes nothing: no entry is verified. */
  static final PushdownCapabilities NONE = new PushdownCapabilities(Collections.<String>emptySet());

  private final Set<String> verified;

  /** Creates a table in which exactly {@code verified} entries may be pushed. */
  PushdownCapabilities(Set<String> verified) {
    for (String entry : verified) {
      if (!CANDIDATES.contains(entry)) {
        throw new IllegalArgumentException("Unknown pushdown entry '" + entry
            + "'; the entries are " + CANDIDATES);
      }
    }
    this.verified = Collections.unmodifiableSet(new LinkedHashSet<>(verified));
  }

  boolean isVerified(String entry) {
    return verified.contains(entry);
  }

  Set<String> verified() {
    return verified;
  }
}
