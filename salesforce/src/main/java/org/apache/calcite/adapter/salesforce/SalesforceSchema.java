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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Schema for Salesforce sObjects.
 */
public class SalesforceSchema extends AbstractSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(SalesforceSchema.class);

  private final SalesforceConnection connection;
  private final boolean lowercaseAliases;
  private final Set<String> tables;
  private final LoadingCache<String, SalesforceConnection.SObjectDescription> descriptionCache;
  private Map<String, Table> tableMap;

  /**
   * Creates a Salesforce schema.
   *
   * @param lowercaseAliases also register each sObject under its lower-case
   *     name; turn off for clients such as Trino that match names
   *     case-insensitively and would see the pair as ambiguous
   * @param describeCacheDirectory where describe results are kept between
   *     runs
   * @param describeCacheTimeToLive how long a describe result on disk is used
   *     for; zero keeps nothing on disk
   * @param tables the sObjects served, matched ignoring case; empty serves
   *     every queryable sObject
   */
  public SalesforceSchema(SalesforceConnection connection, int cacheMaxSize,
      boolean lowercaseAliases, Path describeCacheDirectory, Duration describeCacheTimeToLive,
      Set<String> tables) {
    final DescribeCache describeCache =
        new DescribeCache(describeCacheDirectory, connection.describeScope(),
            describeCacheTimeToLive);
    this.connection = connection;
    this.lowercaseAliases = lowercaseAliases;
    this.tables = tables;
    this.descriptionCache = CacheBuilder.newBuilder()
        .maximumSize(cacheMaxSize)
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build(new CacheLoader<String, SalesforceConnection.SObjectDescription>() {
          @Override public SalesforceConnection.SObjectDescription load(String sObjectType)
              throws IOException {
            return connection.parseDescription(
                describeCache.get(sObjectType, connection::describeSObjectJson));
          }
        });
  }

  @Override protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = createTableMap();
    }
    return tableMap;
  }

  private Map<String, Table> createTableMap() {
    try {
      List<SalesforceConnection.SObjectBasicInfo> sObjects = connection.listSObjects();
      ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

      for (String name : served(sObjects)) {
        builder.put(name, new SalesforceTable(this, name));

        // Also add with lowercase name for case-insensitive matching
        String lowerName = name.toLowerCase(Locale.ROOT);
        if (lowercaseAliases && !lowerName.equals(name)) {
          builder.put(lowerName, new SalesforceTable(this, name));
        }
      }

      // Served whatever 'tables' names, for picking the sObjects to name
      builder.put(ListingTables.OBJECTS, new ListingTables.Objects(this));
      builder.put(ListingTables.COLUMNS, new ListingTables.Columns(this));

      return builder.build();
    } catch (IOException e) {
      LOGGER.error("Failed to list Salesforce sObjects", e);
      throw new RuntimeException("Failed to list Salesforce sObjects", e);
    }
  }

  /** Names of the served sObjects: every queryable one, or those named in {@code tables}. */
  private List<String> served(List<SalesforceConnection.SObjectBasicInfo> sObjects) {
    final List<String> names = new ArrayList<>();
    final Map<String, String> byName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (SalesforceConnection.SObjectBasicInfo sObject : sObjects) {
      byName.put(sObject.name, sObject.name);
      if (tables.isEmpty()) {
        names.add(sObject.name);
      }
    }
    if (tables.isEmpty()) {
      return names;
    }
    for (String table : tables) {
      final String name = byName.get(table);
      if (name == null) {
        throw new IllegalArgumentException("'tables' names '" + table
            + "', which is not a queryable sObject in this org");
      }
      names.add(name);
    }
    return names;
  }

  /** Every queryable sObject in the org, from the sObject list. */
  List<SalesforceConnection.SObjectBasicInfo> queryableSObjects() {
    try {
      return connection.listSObjects();
    } catch (IOException e) {
      throw new RuntimeException("Failed to list Salesforce sObjects", e);
    }
  }

  /**
   * Get the cached description for an sObject.
   */
  public SalesforceConnection.SObjectDescription getDescription(String sObjectType) {
    try {
      return descriptionCache.get(sObjectType);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to describe sObject: " + sObjectType, e);
    }
  }

  /**
   * Get the connection to Salesforce.
   */
  public SalesforceConnection getConnection() {
    return connection;
  }
}
