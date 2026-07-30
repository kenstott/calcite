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

import org.apache.arrow.gandiva.evaluator.Filter;
import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.Condition;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Double.parseDouble;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

/**
 * Every reference to the optional Gandiva API, kept in one class.
 *
 * <p>Isolation is the point, not tidiness. Gandiva is {@code compileOnly}, so its classes are
 * absent at runtime unless a deployment adds them. A class whose bytecode mentions a Gandiva type —
 * even only in a {@code catch} clause — fails to load with {@link NoClassDefFoundError} when they
 * are absent, and {@link ArrowTable} is reached through {@code Class.forName}, which initialises it.
 * Keeping the API confined here means {@code ArrowTable} loads and serves the non-accelerated path
 * on a JVM that has never heard of Gandiva; this class is only touched once
 * {@link GandivaAvailability#isAvailable()} has said it is safe to.
 *
 * <p>Returns {@code Object} for the same reason: a signature mentioning {@code Projector} would
 * pull the type back into the caller's constant pool.
 */
final class GandivaEvaluators {

  private GandivaEvaluators() {
  }

  /** An identity projector over {@code fields}, for column selection. */
  static Object makeProjector(Schema schema, ImmutableIntList fields) {
    final List<ExpressionTree> expressionTrees = new ArrayList<>();
    for (int fieldOrdinal : fields) {
      Field field = schema.getFields().get(fieldOrdinal);
      TreeNode node = TreeBuilder.makeField(field);
      expressionTrees.add(TreeBuilder.makeExpression(node, field));
    }
    try {
      return Projector.make(schema, expressionTrees);
    } catch (GandivaException e) {
      throw Util.toUnchecked(e);
    }
  }

  /** A filter over the conjunction of {@code conditions}, as produced by ArrowTranslator. */
  static Object makeFilter(Schema schema, List<String> conditions) {
    final List<TreeNode> conditionNodes = new ArrayList<>(conditions.size());
    for (String condition : conditions) {
      String[] data = condition.split(" ");
      List<TreeNode> treeNodes = new ArrayList<>(2);
      treeNodes.add(
          TreeBuilder.makeField(schema.getFields()
              .get(schema.getFields().indexOf(schema.findField(data[0])))));

      // if the split condition has more than two parts it's a binary operator
      // with an additional literal node
      if (data.length > 2) {
        treeNodes.add(makeLiteralNode(data[2], data[3]));
      }

      String operator = data[1];
      conditionNodes.add(
          TreeBuilder.makeFunction(operator, treeNodes, new ArrowType.Bool()));
    }
    final Condition filterCondition;
    if (conditionNodes.size() == 1) {
      filterCondition = TreeBuilder.makeCondition(conditionNodes.get(0));
    } else {
      TreeNode treeNode = TreeBuilder.makeAnd(conditionNodes);
      filterCondition = TreeBuilder.makeCondition(treeNode);
    }

    try {
      return Filter.make(schema, filterCondition);
    } catch (GandivaException e) {
      throw Util.toUnchecked(e);
    }
  }

  private static TreeNode makeLiteralNode(String literal, String type) {
    if (type.startsWith("decimal")) {
      String[] typeParts =
          type.substring(type.indexOf('(') + 1, type.indexOf(')')).split(",");
      int precision = parseInt(typeParts[0]);
      int scale = parseInt(typeParts[1]);
      return TreeBuilder.makeDecimalLiteral(literal, precision, scale);
    } else if (type.equals("integer")) {
      return TreeBuilder.makeLiteral(parseInt(literal));
    } else if (type.equals("long")) {
      return TreeBuilder.makeLiteral(parseLong(literal));
    } else if (type.equals("float")) {
      return TreeBuilder.makeLiteral(parseFloat(literal));
    } else if (type.equals("double")) {
      return TreeBuilder.makeLiteral(parseDouble(literal));
    } else if (type.equals("string")) {
      return TreeBuilder.makeStringLiteral(literal.substring(1, literal.length() - 1));
    } else {
      throw new IllegalArgumentException("Invalid literal " + literal
          + ", type " + type);
    }
  }
}
