/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Enumerator that pushes {@code ArrowTranslator}'s conjunctive predicates down to the vector
 * level in plain Java, for when Gandiva is unavailable.
 *
 * <p>{@code ArrowTranslator.translateMatch} already only ever produces a conjunction of simple
 * binary/unary comparisons — it throws {@link UnsupportedOperationException} on a genuine
 * disjunction, which {@link ArrowRules.ArrowFilterRule} catches and treats as "don't push this
 * filter" (see the {@code testArrowProjectFieldsWithDisjunctiveFilter} test, gated on the
 * still-open {@code Bug.CALCITE_6293_FIXED}). So the condition shape this class has to evaluate
 * is exactly the same shape {@link GandivaEvaluators#makeFilter} already parses: one string per
 * conjunct, {@code "fieldName operator [literal literalType]"}. Reusing that shape — rather than
 * adopting a different, richer representation — means this class adds a fallback path without
 * changing what {@link ArrowFilter}/{@link ArrowTranslator} can express, which is the only thing
 * that keeps this safe to enable unconditionally in {@link ArrowRules#RULES}.
 */
class ArrowJavaFilterEnumerator extends AbstractArrowEnumerator {
  private final ArrowFileReader arrowFileReader;
  private final List<Condition> conditions;

  ArrowJavaFilterEnumerator(ArrowFileReader arrowFileReader, ImmutableIntList fields,
      Schema schema, List<String> rawConditions) {
    super(arrowFileReader, fields);
    this.arrowFileReader = arrowFileReader;
    this.conditions = new ArrayList<>(rawConditions.size());
    for (String raw : rawConditions) {
      this.conditions.add(Condition.parse(schema, raw));
    }
  }

  @Override void evaluateOperator(ArrowRecordBatch arrowRecordBatch) {
    // Nothing to pre-evaluate: rows are checked against `conditions` one at a time in
    // rowMatches(), not batch-evaluated up front.
  }

  @Override public boolean moveNext() {
    while (true) {
      if (currRowIndex >= rowCount - 1) {
        this.valueVectors.clear();
        loadNextArrowBatch();
        if (rowCount == 0) {
          return false;
        }
        currRowIndex = 0;
      } else {
        currRowIndex++;
      }
      if (rowMatches()) {
        return true;
      }
    }
  }

  private boolean rowMatches() {
    VectorSchemaRoot root;
    try {
      root = arrowFileReader.getVectorSchemaRoot();
    } catch (IOException e) {
      throw Util.toUnchecked(e);
    }
    for (Condition condition : conditions) {
      ValueVector vector = root.getVector(condition.fieldIndex);
      if (!condition.matches(vector, currRowIndex)) {
        return false;
      }
    }
    return true;
  }

  @Override public void close() {
    try {
      arrowFileReader.close();
    } catch (IOException e) {
      throw Util.toUnchecked(e);
    }
  }

  /** One parsed conjunct — the same grammar {@link GandivaEvaluators#makeFilter} parses. */
  private static final class Condition {
    final int fieldIndex;
    final String operator;
    final Object literal;

    private Condition(int fieldIndex, String operator, Object literal) {
      this.fieldIndex = fieldIndex;
      this.operator = operator;
      this.literal = literal;
    }

    static Condition parse(Schema schema, String raw) {
      String[] data = raw.split(" ");
      int fieldIndex = schema.getFields().indexOf(schema.findField(data[0]));
      String operator = data[1];
      Object literal = data.length > 2 ? parseLiteral(data[2], data[3]) : null;
      return new Condition(fieldIndex, operator, literal);
    }

    private static Object parseLiteral(String literal, String type) {
      if (type.startsWith("decimal")) {
        return new BigDecimal(literal);
      } else if (type.equals("integer")) {
        return Integer.parseInt(literal);
      } else if (type.equals("long")) {
        return Long.parseLong(literal);
      } else if (type.equals("float")) {
        return Float.parseFloat(literal);
      } else if (type.equals("double")) {
        return Double.parseDouble(literal);
      } else if (type.equals("string")) {
        return literal.substring(1, literal.length() - 1);
      } else {
        throw new IllegalArgumentException("Invalid literal " + literal + ", type " + type);
      }
    }

    boolean matches(ValueVector vector, int rowIndex) {
      boolean isNull = vector.isNull(rowIndex);
      switch (operator) {
      case "isnull":
        return isNull;
      case "isnotnull":
        return !isNull;
      case "istrue":
        return !isNull && Boolean.TRUE.equals(vector.getObject(rowIndex));
      case "isfalse":
        return !isNull && Boolean.FALSE.equals(vector.getObject(rowIndex));
      case "isnottrue":
        return isNull || !Boolean.TRUE.equals(vector.getObject(rowIndex));
      case "isnotfalse":
        return isNull || !Boolean.FALSE.equals(vector.getObject(rowIndex));
      default:
        break;
      }
      if (isNull) {
        // SQL three-valued logic: any comparison against NULL is neither true nor false.
        return false;
      }
      Object value = vector.getObject(rowIndex);
      int cmp = compare(value, literal);
      switch (operator) {
      case "equal":
        return cmp == 0;
      case "not_equal":
        return cmp != 0;
      case "less_than":
        return cmp < 0;
      case "less_than_or_equal_to":
        return cmp <= 0;
      case "greater_than":
        return cmp > 0;
      case "greater_than_or_equal_to":
        return cmp >= 0;
      default:
        throw new IllegalArgumentException("Unsupported operator " + operator);
      }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compare(Object value, Object literal) {
      if (value instanceof Number && literal instanceof Number) {
        double a = ((Number) value).doubleValue();
        double b = ((Number) literal).doubleValue();
        return Double.compare(a, b);
      }
      if (value instanceof Comparable && literal instanceof Comparable
          && value.getClass() == literal.getClass()) {
        return ((Comparable) value).compareTo(literal);
      }
      // Field value and literal are both Comparable but of different runtime types (e.g. a
      // CharSequence-backed vector value against a String literal) — fall back to string
      // comparison rather than throwing, matching how the raw condition string was built from
      // Object#toString on both sides in the first place.
      return value.toString().compareTo(literal.toString());
    }
  }
}
