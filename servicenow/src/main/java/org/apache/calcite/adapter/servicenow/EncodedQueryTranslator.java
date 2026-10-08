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

import org.apache.calcite.adapter.servicenow.PushdownCapabilities.Op;
import org.apache.calcite.adapter.servicenow.PushdownCapabilities.Position;
import org.apache.calcite.adapter.servicenow.PushdownCapabilities.TypeGroup;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimestampString;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Translates Calcite filters into ServiceNow encoded-query text ({@code sysparm_query}).
 *
 * <p>What it translates, per filter (a top-level conjunct): comparisons of a column with a
 * literal ({@code = <> < <= > >=}), {@code IN} and {@code NOT IN} (Calcite's SEARCH), a single
 * range such as {@code BETWEEN}, {@code IS NULL}, {@code IS NOT NULL}, a boolean column, {@code
 * NOT} over any of these (by inverting the operator), {@code LIKE} patterns that are exactly a
 * prefix, suffix, contains or equality, and an OR of such comparisons (an OR group). Everything
 * else stays in Calcite: expressions on columns, functions, casts of columns, dot-walked
 * fields, NOT over OR/AND/LIKE, OR over AND (which {@code ^NQ} or parentheses would be needed
 * for), a literal empty string (an empty value is NULL here, so {@code = ''} matches nothing and
 * is left to Calcite), values containing {@code ^}, and a {@code NOT IN} or {@code <>} that
 * would need an entry nobody has verified.
 *
 * <p>Column names come from the discovered metadata and are checked against the name pattern
 * before they are written; display columns are never pushed. The API is never relied on to
 * reject a bad field, because it ignores one.
 *
 * <p>Negated operators exclude NULL (empty) rows in SQL. ServiceNow's {@code !=} reportedly
 * includes them, so a negated comparison has two candidate encodings: plain, and with an added
 * {@code ^<field>ISNOTEMPTY} term ({@code NE_NOT_EMPTY}, {@code NOT_IN_NOT_EMPTY}); whichever is
 * verified is used, the added-term form first.
 *
 * <p>Boolean structure follows the observation (to be confirmed on a live instance) that
 * {@code ^OR} attaches to the term before it and the next {@code ^} starts a new AND clause:
 * {@code a^b^ORc} is {@code a AND (b OR c)}, {@code a^ORb^c^ORd} is {@code (a OR b) AND (c OR d)}.
 * The paging term the reader appends follows the pushed text, so an OR group is always followed
 * by more terms; that is why it needs {@link PushdownCapabilities#SHAPE_OR_WITH_OTHER_TERMS}.
 */
final class EncodedQueryTranslator implements PredicatePushdown {

  /** Longest IN list pushed; a long list could exceed URL limits. */
  static final int MAX_IN_VALUES = 100;

  private static final String SPECIAL_CHARACTERS = "=<>!@%*'\"\\;&+#,";

  private final PushdownCapabilities capabilities;

  EncodedQueryTranslator(PushdownCapabilities capabilities) {
    this.capabilities = capabilities;
  }

  /** A translated filter. */
  private static final class Clause {
    final String text;
    final Set<String> entries;
    final boolean orGroup;

    Clause(String text, Set<String> entries, boolean orGroup) {
      this.text = text;
      this.entries = entries;
      this.orGroup = orGroup;
    }
  }

  /** One encoded-query term (or two, for the added ISNOTEMPTY form). */
  private static final class Term {
    final String text;
    final Set<String> entries;

    Term(String text, Set<String> entries) {
      this.text = text;
      this.entries = entries;
    }
  }

  /** A comparison of a column with literal values, before it is written as text. */
  private static final class Cmp {
    final Op op;
    final int column;
    final List<Comparable> values;

    Cmp(Op op, int column, List<Comparable> values) {
      this.op = op;
      this.column = column;
      this.values = values;
    }
  }

  @Override public Result push(List<RexNode> filters, List<ServiceNowColumn> columns) {
    // A filter that is an AND is several conjuncts; take them one by one
    final List<RexNode> conjuncts = new ArrayList<>();
    for (RexNode filter : filters) {
      conjuncts.addAll(RelOptUtil.conjunctions(filter));
    }
    filters.clear();
    filters.addAll(conjuncts);
    final List<Clause> accepted = new ArrayList<>();
    final List<RexNode> acceptedNodes = new ArrayList<>();
    for (RexNode filter : filters) {
      final Clause clause = clause(filter, columns);
      if (clause != null) {
        accepted.add(clause);
        acceptedNodes.add(filter);
      }
    }
    if (accepted.isEmpty() || !capabilities.isVerified(PushdownCapabilities.SHAPE_AND)) {
      return new Result("", Collections.<String>emptySet());
    }
    final boolean orShapesVerified =
        capabilities.isVerified(PushdownCapabilities.SHAPE_OR_GROUP)
            && capabilities.isVerified(PushdownCapabilities.SHAPE_OR_WITH_OTHER_TERMS);
    final List<String> texts = new ArrayList<>();
    final Set<String> entries = new LinkedHashSet<>();
    entries.add(PushdownCapabilities.SHAPE_AND);
    for (int i = 0; i < accepted.size(); i++) {
      final Clause clause = accepted.get(i);
      if (clause.orGroup && !orShapesVerified) {
        acceptedNodes.set(i, null);
        continue;
      }
      if (clause.orGroup) {
        entries.add(PushdownCapabilities.SHAPE_OR_GROUP);
        entries.add(PushdownCapabilities.SHAPE_OR_WITH_OTHER_TERMS);
      }
      texts.add(clause.text);
      entries.addAll(clause.entries);
    }
    if (texts.isEmpty()) {
      return new Result("", Collections.<String>emptySet());
    }
    for (RexNode node : acceptedNodes) {
      if (node != null) {
        filters.remove(node);
      }
    }
    return new Result(String.join("^", texts), entries);
  }

  /** Translates one filter, or returns null if it must stay in Calcite. */
  private Clause clause(RexNode filter, List<ServiceNowColumn> columns) {
    if (filter.getKind() == SqlKind.OR) {
      final List<String> texts = new ArrayList<>();
      final Set<String> entries = new LinkedHashSet<>();
      for (RexNode member : RelOptUtil.disjunctions(filter)) {
        final List<Term> terms = terms(member, Position.OR, columns);
        if (terms == null || terms.size() != 1) {
          return null;
        }
        texts.add(terms.get(0).text);
        entries.addAll(terms.get(0).entries);
      }
      return new Clause(String.join("^OR", texts), entries, true);
    }
    final List<String> texts = new ArrayList<>();
    final Set<String> entries = new LinkedHashSet<>();
    for (RexNode conjunct : RelOptUtil.conjunctions(filter)) {
      final List<Term> terms = terms(conjunct, Position.AND, columns);
      if (terms == null) {
        return null;
      }
      for (Term term : terms) {
        texts.add(term.text);
        entries.addAll(term.entries);
      }
    }
    return new Clause(String.join("^", texts), entries, false);
  }

  private List<Term> terms(RexNode node, Position position, List<ServiceNowColumn> columns) {
    final List<Cmp> comparisons = comparisons(node, false);
    if (comparisons == null || comparisons.isEmpty()) {
      return null;
    }
    if (position == Position.OR && comparisons.size() != 1) {
      return null;
    }
    final List<Term> terms = new ArrayList<>();
    for (Cmp cmp : comparisons) {
      final Term term = term(cmp, position, columns);
      if (term == null) {
        return null;
      }
      terms.add(term);
    }
    return terms;
  }

  /** Describes the node as comparisons joined by AND, or returns null. */
  private static List<Cmp> comparisons(RexNode node, boolean negated) {
    final SqlKind kind = node.getKind();
    switch (kind) {
    case NOT:
      return negated ? null : comparisons(((RexCall) node).getOperands().get(0), true);
    case INPUT_REF:
      return Collections.singletonList(
          new Cmp(Op.EQ, ((RexInputRef) node).getIndex(),
              Collections.<Comparable>singletonList(!negated)));
    case IS_NULL:
    case IS_NOT_NULL: {
      final RexNode operand = ((RexCall) node).getOperands().get(0);
      if (!(operand instanceof RexInputRef)) {
        return null;
      }
      final boolean isNull = (kind == SqlKind.IS_NULL) != negated;
      return Collections.singletonList(
          new Cmp(isNull ? Op.IS_NULL : Op.IS_NOT_NULL, ((RexInputRef) operand).getIndex(),
              Collections.<Comparable>emptyList()));
    }
    case EQUALS:
    case NOT_EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
      return comparison((RexCall) node, negated);
    case SEARCH:
      return negated ? null : search((RexCall) node);
    case LIKE:
      return negated ? null : like((RexCall) node);
    default:
      return null;
    }
  }

  private static List<Cmp> comparison(RexCall call, boolean negated) {
    final RexNode left = call.getOperands().get(0);
    final RexNode right = call.getOperands().get(1);
    final RexInputRef ref;
    final RexLiteral literal;
    SqlKind kind = call.getKind();
    if (left instanceof RexInputRef && literal(right) != null) {
      ref = (RexInputRef) left;
      literal = literal(right);
    } else if (right instanceof RexInputRef && literal(left) != null) {
      ref = (RexInputRef) right;
      literal = literal(left);
      kind = kind.reverse();
    } else {
      return null;
    }
    if (negated) {
      kind = kind.negate();
    }
    final Op op;
    switch (kind) {
    case EQUALS:
      op = Op.EQ;
      break;
    case NOT_EQUALS:
      op = Op.NE;
      break;
    case LESS_THAN:
      op = Op.LT;
      break;
    case LESS_THAN_OR_EQUAL:
      op = Op.LE;
      break;
    case GREATER_THAN:
      op = Op.GT;
      break;
    case GREATER_THAN_OR_EQUAL:
      op = Op.GE;
      break;
    default:
      return null;
    }
    final Comparable value = comparable(literal);
    if (value == null) {
      return null;
    }
    return Collections.singletonList(new Cmp(op, ref.getIndex(),
        Collections.singletonList(value)));
  }

  /** The literal behind a literal operand, looking through a cast of it. */
  private static RexLiteral literal(RexNode node) {
    if (node instanceof RexLiteral) {
      return (RexLiteral) node;
    }
    if (node.getKind() == SqlKind.CAST
        && ((RexCall) node).getOperands().get(0) instanceof RexLiteral) {
      return (RexLiteral) ((RexCall) node).getOperands().get(0);
    }
    return null;
  }

  private static Comparable comparable(RexLiteral literal) {
    if (literal.isNull()) {
      return null;
    }
    switch (literal.getTypeName()) {
    case CHAR:
    case VARCHAR:
      return literal.getValueAs(NlsString.class);
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DECIMAL:
      return literal.getValueAs(BigDecimal.class);
    case BOOLEAN:
      return literal.getValueAs(Boolean.class);
    case TIMESTAMP:
      return literal.getValueAs(TimestampString.class);
    case DATE:
      return literal.getValueAs(DateString.class);
    default:
      return null;
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static List<Cmp> search(RexCall call) {
    final RexNode operand = call.getOperands().get(0);
    final RexNode second = call.getOperands().get(1);
    if (!(operand instanceof RexInputRef) || !(second instanceof RexLiteral)) {
      return null;
    }
    final Sarg sarg = ((RexLiteral) second).getValueAs(Sarg.class);
    if (sarg == null || sarg.nullAs == org.apache.calcite.rex.RexUnknownAs.TRUE) {
      return null;
    }
    final int column = ((RexInputRef) operand).getIndex();
    if (sarg.isPoints()) {
      final List<Comparable> points = new ArrayList<>();
      for (Object range : sarg.rangeSet.asRanges()) {
        points.add(((Range<Comparable>) range).lowerEndpoint());
      }
      return Collections.singletonList(new Cmp(points.size() == 1 ? Op.EQ : Op.IN, column, points));
    }
    if (sarg.isComplementedPoints()) {
      final List<Comparable> points = new ArrayList<>();
      for (Object range : sarg.rangeSet.complement().asRanges()) {
        points.add(((Range<Comparable>) range).lowerEndpoint());
      }
      return Collections.singletonList(
          new Cmp(points.size() == 1 ? Op.NE : Op.NOT_IN, column, points));
    }
    if (sarg.rangeSet.asRanges().size() != 1) {
      return null;
    }
    final Range<Comparable> range = (Range<Comparable>) sarg.rangeSet.asRanges().iterator().next();
    final List<Cmp> bounds = new ArrayList<>();
    if (range.hasLowerBound()) {
      bounds.add(new Cmp(range.lowerBoundType() == BoundType.CLOSED ? Op.GE : Op.GT, column,
          Collections.<Comparable>singletonList(range.lowerEndpoint())));
    }
    if (range.hasUpperBound()) {
      bounds.add(new Cmp(range.upperBoundType() == BoundType.CLOSED ? Op.LE : Op.LT, column,
          Collections.<Comparable>singletonList(range.upperEndpoint())));
    }
    return bounds;
  }

  private static List<Cmp> like(RexCall call) {
    if (call.getOperands().size() != 2 || !(call.getOperands().get(0) instanceof RexInputRef)
        || !(call.getOperands().get(1) instanceof RexLiteral)) {
      return null; // includes LIKE with an ESCAPE clause
    }
    final Comparable pattern = comparable((RexLiteral) call.getOperands().get(1));
    if (!(pattern instanceof NlsString)) {
      return null;
    }
    final String p = ((NlsString) pattern).getValue();
    if (p.indexOf('_') >= 0) {
      return null;
    }
    final int first = p.indexOf('%');
    final int last = p.lastIndexOf('%');
    final Op op;
    final String piece;
    if (first < 0) {
      op = Op.LIKE_EXACT;
      piece = p;
    } else if (first == 0 && last == p.length() - 1 && p.length() >= 3
        && p.indexOf('%', 1) == last) {
      op = Op.LIKE_CONTAINS;
      piece = p.substring(1, p.length() - 1);
    } else if (first == last && last == p.length() - 1) {
      op = Op.LIKE_PREFIX;
      piece = p.substring(0, p.length() - 1);
    } else if (first == last && first == 0) {
      op = Op.LIKE_SUFFIX;
      piece = p.substring(1);
    } else {
      return null;
    }
    return Collections.singletonList(new Cmp(op, ((RexInputRef) call.getOperands().get(0))
        .getIndex(), Collections.<Comparable>singletonList(new NlsString(piece, null, null))));
  }

  // ---- writing a comparison as text -------------------------------------------------------

  private Term term(Cmp cmp, Position position, List<ServiceNowColumn> columns) {
    final ServiceNowColumn column = cmp.column < columns.size() ? columns.get(cmp.column) : null;
    if (column == null || column.display
        || !ServiceNowCatalog.NAME.matcher(column.field).matches()) {
      return null;
    }
    final TypeGroup group = TypeGroup.of(column.kind);
    if (group == null || !PushdownCapabilities.declared(cmp.op, group, position)) {
      return null;
    }
    final String field = column.field;
    final boolean inList = cmp.op == Op.IN || cmp.op == Op.NOT_IN;
    final List<String> values = new ArrayList<>();
    boolean special = false;
    if (cmp.op != Op.IS_NULL && cmp.op != Op.IS_NOT_NULL) {
      if (inList && cmp.values.size() > MAX_IN_VALUES) {
        return null;
      }
      for (Comparable value : cmp.values) {
        final String text = valueText(group, column.kind, value);
        if (text == null || text.isEmpty() || unsafe(text, inList)) {
          return null;
        }
        special |= isSpecial(text);
        values.add(text);
      }
    }
    final String value = values.isEmpty() ? "" : values.get(0);
    final String list = String.join(",", values);

    switch (cmp.op) {
    case NE:
    case NOT_IN: {
      final boolean in = cmp.op == Op.NOT_IN;
      final String base = field + (in ? "NOT IN" + list : "!=" + value);
      final Op withTerm = in ? Op.NOT_IN_NOT_EMPTY : Op.NE_NOT_EMPTY;
      if (position == Position.AND) {
        final Term added = verified(withTerm, group, position, special,
            base + "^" + field + "ISNOTEMPTY");
        if (added != null) {
          return added;
        }
      }
      return verified(cmp.op, group, position, special, base);
    }
    case EQ:
      return verified(Op.EQ, group, position, special, field + "=" + value);
    case LT:
      return verified(Op.LT, group, position, special, field + "<" + value);
    case LE:
      return verified(Op.LE, group, position, special, field + "<=" + value);
    case GT:
      return verified(Op.GT, group, position, special, field + ">" + value);
    case GE:
      return verified(Op.GE, group, position, special, field + ">=" + value);
    case IN:
      return verified(Op.IN, group, position, special, field + "IN" + list);
    case IS_NULL:
      return verified(Op.IS_NULL, group, position, false, field + "ISEMPTY");
    case IS_NOT_NULL:
      return verified(Op.IS_NOT_NULL, group, position, false, field + "ISNOTEMPTY");
    case LIKE_EXACT:
      return verified(Op.LIKE_EXACT, group, position, special, field + "=" + value);
    case LIKE_PREFIX:
      return verified(Op.LIKE_PREFIX, group, position, special, field + "STARTSWITH" + value);
    case LIKE_SUFFIX:
      return verified(Op.LIKE_SUFFIX, group, position, special, field + "ENDSWITH" + value);
    case LIKE_CONTAINS:
      return verified(Op.LIKE_CONTAINS, group, position, special, field + "LIKE" + value);
    default:
      return null;
    }
  }

  /** The term, if every entry it relies on is verified. */
  private Term verified(Op op, TypeGroup group, Position position, boolean special,
      String text) {
    if (!PushdownCapabilities.declared(op, group, position)) {
      return null;
    }
    final Set<String> entries = new LinkedHashSet<>();
    entries.add(PushdownCapabilities.id(op, group, position));
    if (special) {
      entries.add(PushdownCapabilities.VALUE_SPECIAL);
    }
    for (String entry : entries) {
      if (!capabilities.isVerified(entry)) {
        return null;
      }
    }
    return new Term(text, entries);
  }

  /** The wire text of a literal for a column type, or null if the literal does not fit it. */
  private static String valueText(TypeGroup group, ServiceNowColumn.Kind kind, Comparable value) {
    switch (group) {
    case TEXT:
    case REFERENCE:
    case GUID:
      return value instanceof NlsString ? ((NlsString) value).getValue() : null;
    case NUMERIC:
      if (!(value instanceof BigDecimal)) {
        return null;
      }
      final BigDecimal number = (BigDecimal) value;
      if ((kind == ServiceNowColumn.Kind.INTEGER || kind == ServiceNowColumn.Kind.LONG)
          && number.stripTrailingZeros().scale() > 0) {
        return null;
      }
      return number.toPlainString();
    case BOOLEAN:
      return value instanceof Boolean ? value.toString() : null;
    case TIMESTAMP: {
      if (!(value instanceof TimestampString)) {
        return null;
      }
      final String text = value.toString();
      return text.indexOf('.') >= 0 ? null : text;
    }
    case DATE:
      return value instanceof DateString ? value.toString() : null;
    default:
      return null;
    }
  }

  /** Text that cannot be sent: the query separator, control characters, a comma in a list. */
  private static boolean unsafe(String text, boolean inList) {
    for (int i = 0; i < text.length(); i++) {
      final char c = text.charAt(i);
      if (c == '^' || c < 0x20 || c == 0x7f || inList && c == ',') {
        return true;
      }
    }
    return false;
  }

  private static boolean isSpecial(String text) {
    if (text.startsWith(" ") || text.endsWith(" ")) {
      return true;
    }
    for (int i = 0; i < text.length(); i++) {
      if (SPECIAL_CHARACTERS.indexOf(text.charAt(i)) >= 0) {
        return true;
      }
    }
    return false;
  }
}
