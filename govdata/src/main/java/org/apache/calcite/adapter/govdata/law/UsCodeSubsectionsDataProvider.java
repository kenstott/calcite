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
package org.apache.calcite.adapter.govdata.law;

import java.util.Iterator;
import java.util.Map;

/**
 * DataProvider for {@code usc_subsections}: the same OLRC USLM XML stream as {@link
 * UsCodeXmlDataProvider}, with each section split at its own structural boundaries into units
 * small enough to embed as one chunk. See {@link UsCodeXmlDataProvider#units} for the rule.
 */
public class UsCodeSubsectionsDataProvider extends UsCodeXmlDataProvider {

  @Override String tableName() {
    return "usc_subsections";
  }

  @Override Iterator<Map<String, Object>> rows(Iterator<SectionData> sections) {
    return new UnitRows(sections);
  }
}
