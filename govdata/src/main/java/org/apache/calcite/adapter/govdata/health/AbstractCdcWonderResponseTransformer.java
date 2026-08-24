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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import java.io.StringReader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Parses a CDC WONDER (wonder.cdc.gov) {@code &lt;data-table&gt;} response into flat rows.
 *
 * <p>WONDER's grouped cross-tab XML may omit repeated label cells: only the first row of
 * each outer group carries a {@code &lt;c l="..." r="N"/&gt;} cell (the {@code r} is the
 * HTML rowspan of the original results table); subsequent rows in the same group start
 * directly with the next-level label cell. For example, grouping by Year then Race:
 * <pre>{@code
 * <r><c l="1999" r="5"/><c l="American Indian or Alaska Native"/><c v="1,878"/></r>
 * <r><c l="Asian or Pacific Islander"/><c v="8,976"/></r>
 * ...
 * <r><c c="1"/><c dt="563,065"/></r>   <!-- subtotal row for 1999, skipped -->
 * <r><c l="2000" r="5"/><c l="American Indian or Alaska Native"/><c v="1,959"/></r>
 * }</pre>
 * so each level's current label/code is carried forward until a row restates it. (Some
 * responses set {@code show-all-labels="true"} and restate every level on every row — the
 * same carry-forward logic handles that shape too, since every row simply overwrites all
 * levels.) A row whose first cell lacks an {@code l} attribute (it has {@code c=} or
 * {@code dt=} instead) is a subtotal/grand-total row and is dropped — it is redundant with
 * {@code SUM()} over the detail rows and would double-count if included.
 *
 * <p>Subclasses declare the table's group levels (outermost first, matching {@code B_1},
 * {@code B_2}, ...) and measure columns (matching {@code M_1}, {@code M_2}, ... in request
 * order); this class owns the carry-forward parsing shared by every WONDER table.
 */
public abstract class AbstractCdcWonderResponseTransformer implements ResponseTransformer {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** One grouped-by level: the output column for its label, and (if present) its code. */
  protected static final class GroupLevel {
    final String labelColumn;
    final String codeColumn;

    GroupLevel(String labelColumn, String codeColumn) {
      this.labelColumn = labelColumn;
      this.codeColumn = codeColumn;
    }

    GroupLevel(String labelColumn) {
      this(labelColumn, null);
    }
  }

  /** Group levels, outermost (B_1) first. */
  protected abstract GroupLevel[] groupLevels();

  /** Measure output column names, in the order requested (M_1, M_2, ...). */
  protected abstract String[] measureColumns();

  /**
   * Optional extra column name for one group level (e.g. a hierarchy-depth column), or null
   * if that level has none. Overridden together with {@link #extraAttribute}.
   */
  protected String extraColumnName(int level) {
    return null;
  }

  /**
   * Optional extra attribute value read off a level's label cell (e.g. WONDER's {@code h=}
   * hierarchy-depth attribute), or null. Only consulted when {@link #extraColumnName} for the
   * same level is non-null.
   */
  protected String extraAttribute(int level, Element labelCell) {
    return null;
  }

  @Override
  public String transform(String response, RequestContext context) {
    try {
      GroupLevel[] levels = groupLevels();
      String[] measures = measureColumns();
      ArrayNode out = MAPPER.createArrayNode();

      Document doc = parseXml(response);
      NodeList rows = doc.getElementsByTagName("r");
      String[] currentLabels = new String[levels.length];
      String[] currentCodes = new String[levels.length];
      String[] currentExtras = new String[levels.length];

      for (int i = 0; i < rows.getLength(); i++) {
        Element row = (Element) rows.item(i);
        NodeList cells = row.getElementsByTagName("c");
        if (cells.getLength() == 0 || !((Element) cells.item(0)).hasAttribute("l")) {
          // Subtotal/grand-total row (first cell has c= or dt= instead of l=) — skip.
          continue;
        }

        // Leading cells with an "l" attribute are labels; the rest are measure values.
        int labelCellCount = 0;
        while (labelCellCount < cells.getLength()
            && ((Element) cells.item(labelCellCount)).hasAttribute("l")) {
          labelCellCount++;
        }

        // These labelCellCount cells fill the *last* labelCellCount group levels; more
        // coarse (earlier) levels not restated in this row carry forward unchanged.
        int startLevel = levels.length - labelCellCount;
        for (int lvl = 0; lvl < labelCellCount; lvl++) {
          Element cell = (Element) cells.item(lvl);
          int level = startLevel + lvl;
          currentLabels[level] = cell.getAttribute("l");
          currentCodes[level] = cell.hasAttribute("cd") ? cell.getAttribute("cd") : null;
          currentExtras[level] = extraColumnName(level) != null ? extraAttribute(level, cell) : null;
        }

        ObjectNode outRow = MAPPER.createObjectNode();
        for (int lvl = 0; lvl < levels.length; lvl++) {
          putOrNull(outRow, levels[lvl].labelColumn, currentLabels[lvl]);
          if (levels[lvl].codeColumn != null) {
            putOrNull(outRow, levels[lvl].codeColumn, currentCodes[lvl]);
          }
          String extraColumn = extraColumnName(lvl);
          if (extraColumn != null) {
            putOrNull(outRow, extraColumn, currentExtras[lvl]);
          }
        }
        for (int m = 0; m < measures.length; m++) {
          int cellIndex = labelCellCount + m;
          String value = cellIndex < cells.getLength()
              ? measureValue((Element) cells.item(cellIndex)) : null;
          putOrNull(outRow, measures[m], value);
        }
        out.add(outRow);
      }

      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to transform CDC WONDER response", e);
    }
  }

  private static Document parseXml(String response) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    DocumentBuilder builder = factory.newDocumentBuilder();
    return builder.parse(new InputSource(new StringReader(response)));
  }

  /** A measure cell holds its number in {@code v=} (or {@code dt=} for a totals cell). */
  private static String measureValue(Element cell) {
    String raw = cell.hasAttribute("v") ? cell.getAttribute("v")
        : cell.hasAttribute("dt") ? cell.getAttribute("dt") : null;
    return raw == null ? null : raw.replace(",", "");
  }

  private static void putOrNull(ObjectNode row, String key, String value) {
    if (value == null) {
      row.putNull(key);
    } else {
      row.put(key, value);
    }
  }
}
