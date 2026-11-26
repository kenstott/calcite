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
package org.apache.calcite.adapter.govdata.research;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.Normalizer;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code nsf_publications} — one row per (NSF award, publication) from the
 * {@code jrnl} array the NSF Award Search API carries on each award record, fetched by the same
 * (fiscal year, award month) walk as {@link NsfAwardsProvider}.
 *
 * <p>The array is one entry per report submission or Public Access Repository deposit, not one per
 * publication: the same paper recurs across an award's annual reports with differing author, page
 * and journal spellings (confirmed live, about 1,700 of 24,600 September 2010 entries repeat an
 * award's title and identifier). Entries of one award are therefore collapsed on
 * {@code publication_key}:
 * <ul>
 *   <li>the DOI, when the entry's digital object id is a well-formed DOI;
 *   <li>otherwise the normalized title — joining the DOI group carrying that title when exactly
 *       one of the award's DOI groups does;
 *   <li>otherwise (no DOI and no title, e.g. a "see attached PDF" placeholder) the entry's own
 *       remaining fields.
 * </ul>
 * One entry of the group supplies every column (nothing is merged across entries), chosen
 * independently of response order: a DOI before none, a repository id before none, then the most
 * populated fields, then the smallest field text. {@code source_record_count} keeps the number of
 * entries collapsed.
 *
 * <p>A digital object id that is not a well-formed DOI (an ISSN, a bare URL, a DOI with embedded
 * spaces or trailing notes) leaves {@code doi} null rather than being repaired; the reported text
 * stays in {@code digital_object_id}.
 */
public class NsfPublicationsProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(NsfPublicationsProvider.class);
  private static final String TABLE = "nsf_publications";
  /** Resolver prefixes seen in front of a DOI, including repeated ones. */
  private static final Pattern DOI_PREFIX = Pattern.compile(
      "^(?:(?:https?://)?(?:dx\\.)?doi\\.org/+|doi\\s*:\\s*|doi\\s+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern DOI = Pattern.compile("10\\.\\d{4,9}/\\S+");
  private static final Pattern DOI_TRAILER = Pattern.compile("[.,;|]+$");
  private static final Pattern NOT_ALNUM = Pattern.compile("[^\\p{L}\\p{N}]+");

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    int fiscalYear = Integer.parseInt(NsfAwardsProvider.required(variables, "year"));
    int month = Integer.parseInt(NsfAwardsProvider.required(variables, "award_month"));
    YearMonth slice = NsfAwardsProvider.awardMonth(fiscalYear, month);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    NsfAwardsProvider.fetchAwards(TABLE, slice.atDay(1), slice.atEndOfMonth(), rawCache,
        award -> rows.addAll(toRows(award, fiscalYear)));
    LOGGER.info("{}: {} publication links for fy={} month={}", TABLE, rows.size(), fiscalYear,
        month);
    return rows.iterator();
  }

  /** One award's publication entries, collapsed to one row per publication key. */
  static List<Map<String, Object>> toRows(JsonNode award, int fiscalYear) {
    JsonNode entries = award.get("jrnl");
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    if (entries == null || entries.isNull()) {
      return rows;
    }
    if (!entries.isArray()) {
      throw new IllegalStateException(TABLE + ": jrnl is not an array on award "
          + NsfAwardsProvider.text(award, "id") + ": " + entries);
    }
    Map<String, List<JsonNode>> groups = new TreeMap<String, List<JsonNode>>();
    Map<String, Set<String>> doiKeysByTitle = new TreeMap<String, Set<String>>();
    List<JsonNode> withoutDoi = new ArrayList<JsonNode>();
    for (JsonNode entry : entries) {
      String doi = doi(NsfAwardsProvider.text(entry, "dgtlObjId"));
      if (doi == null) {
        withoutDoi.add(entry);
        continue;
      }
      String key = "doi:" + doi;
      group(groups, key).add(entry);
      String title = titleKey(NsfAwardsProvider.text(entry, "artTitl"));
      if (!title.isEmpty()) {
        doiKeysByTitle.computeIfAbsent(title, t -> new TreeSet<String>()).add(key);
      }
    }
    for (JsonNode entry : withoutDoi) {
      String title = titleKey(NsfAwardsProvider.text(entry, "artTitl"));
      String key;
      if (title.isEmpty()) {
        key = "record:" + fieldText(entry);
      } else {
        Set<String> doiKeys = doiKeysByTitle.get(title);
        key = doiKeys != null && doiKeys.size() == 1
            ? doiKeys.iterator().next() : "title:" + title;
      }
      group(groups, key).add(entry);
    }
    for (Map.Entry<String, List<JsonNode>> group : groups.entrySet()) {
      rows.add(toRow(award, fiscalYear, group.getKey(), group.getValue()));
    }
    return rows;
  }

  private static List<JsonNode> group(Map<String, List<JsonNode>> groups, String key) {
    return groups.computeIfAbsent(key, k -> new ArrayList<JsonNode>());
  }

  private static Map<String, Object> toRow(JsonNode award, int fiscalYear, String key,
      List<JsonNode> entries) {
    JsonNode best = entries.get(0);
    for (JsonNode entry : entries) {
      if (preferred(entry, best)) {
        best = entry;
      }
    }
    String objectId = NsfAwardsProvider.text(best, "dgtlObjId");
    String year = NsfAwardsProvider.text(best, "jrnlYr");
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("award_id", NsfAwardsProvider.text(award, "id"));
    row.put("fiscal_year", fiscalYear);
    row.put("directorate", NsfAwardsProvider.text(award, "dirAbbr"));
    row.put("division", NsfAwardsProvider.text(award, "divAbbr"));
    row.put("publication_key", key);
    row.put("doi", doi(objectId));
    row.put("digital_object_id", objectId);
    row.put("par_id", NsfAwardsProvider.text(best, "parPblcId"));
    row.put("title", NsfAwardsProvider.text(best, "artTitl"));
    row.put("authors", NsfAwardsProvider.text(best, "auth"));
    row.put("journal", NsfAwardsProvider.text(best, "jrnlTitl"));
    row.put("journal_volume", NsfAwardsProvider.text(best, "jrnlVol"));
    row.put("pages", NsfAwardsProvider.text(best, "artPageNum"));
    row.put("pub_year", year == null ? null : Integer.valueOf(year));
    row.put("source_record_count", entries.size());
    return row;
  }

  /** Whether {@code a} should represent a group ahead of {@code b} (class javadoc order). */
  private static boolean preferred(JsonNode a, JsonNode b) {
    int cmp = Boolean.compare(doi(NsfAwardsProvider.text(a, "dgtlObjId")) != null,
        doi(NsfAwardsProvider.text(b, "dgtlObjId")) != null);
    if (cmp == 0) {
      cmp = Boolean.compare(NsfAwardsProvider.text(a, "parPblcId") != null,
          NsfAwardsProvider.text(b, "parPblcId") != null);
    }
    if (cmp == 0) {
      cmp = Integer.compare(populated(a), populated(b));
    }
    if (cmp == 0) {
      cmp = fieldText(b).compareTo(fieldText(a));
    }
    return cmp > 0;
  }

  private static int populated(JsonNode entry) {
    int n = 0;
    for (Iterator<String> names = entry.fieldNames(); names.hasNext();) {
      if (NsfAwardsProvider.text(entry, names.next()) != null) {
        n++;
      }
    }
    return n;
  }

  /** The entry's fields in name order, so equal entries render identically. */
  private static String fieldText(JsonNode entry) {
    Map<String, String> fields = new TreeMap<String, String>();
    for (Iterator<String> names = entry.fieldNames(); names.hasNext();) {
      String name = names.next();
      fields.put(name, NsfAwardsProvider.text(entry, name));
    }
    return fields.toString();
  }

  /** The lower-cased DOI when {@code objectId} is one, optionally behind a resolver prefix. */
  static String doi(String objectId) {
    if (objectId == null) {
      return null;
    }
    String s = objectId.replace(' ', ' ').trim();
    for (Matcher m = DOI_PREFIX.matcher(s); m.find(); m = DOI_PREFIX.matcher(s)) {
      s = s.substring(m.end()).trim();
    }
    s = DOI_TRAILER.matcher(s).replaceFirst("").trim();
    return DOI.matcher(s).matches() ? s.toLowerCase(Locale.ROOT) : null;
  }

  /** The title reduced to its letters and digits; empty for a missing or placeholder title. */
  static String titleKey(String title) {
    if (title == null) {
      return "";
    }
    String s = Normalizer.normalize(title, Normalizer.Form.NFKC).toLowerCase(Locale.ROOT);
    return NOT_ALNUM.matcher(s).replaceAll("");
  }
}
