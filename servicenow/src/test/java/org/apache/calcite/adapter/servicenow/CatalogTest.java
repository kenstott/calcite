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

import org.apache.calcite.adapter.servicenow.ServiceNowCatalog.TableDef;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Catalog discovery from the metadata tables, and its on-disk cache. */
class CatalogTest {

  private static List<String> columnNames(TableDef table) {
    final List<String> names = new ArrayList<>();
    for (ServiceNowColumn column : table.columns) {
      names.add(column.name);
    }
    return names;
  }

  private static ServiceNowCatalog load(FixtureServer server) {
    return new ServiceNowCatalog(
        ServiceNowCatalog.load(TableApiClientTest.connection(server), 2),
        Collections.<String>emptySet());
  }

  @Test void listsTablesFromSysDbObject() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      assertThat(load(server).tableNames(),
          contains("bad_table", "incident", "sys_user", "task"));
    }
  }

  @Test void inheritedColumnsComeFromAncestors() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final TableDef incident = load(server).table("incident");
      // sys_id first; then by name, with each reference field followed by its display column.
      // short_description, priority, active, opened_at, start_date and assigned_to are task's;
      // u_retired is inactive in the dictionary and the empty-element rows declare no column
      assertThat(columnNames(incident),
          contains("sys_id", "active", "assigned_to", "assigned_to__display", "caller_id",
              "caller_id__display", "closed_at", "number", "opened_at", "priority",
              "short_description", "start_date", "u_widget"));
    }
  }

  @Test void typesFollowTheFieldTypeAndSysGlideObject() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final TableDef incident = load(server).table("incident");
      final java.util.Map<String, ServiceNowColumn.Kind> kinds = new java.util.HashMap<>();
      for (ServiceNowColumn column : incident.columns) {
        kinds.put(column.name, column.kind);
      }
      assertThat(kinds.get("sys_id"), is(ServiceNowColumn.Kind.GUID));
      assertThat(kinds.get("priority"), is(ServiceNowColumn.Kind.INTEGER));
      assertThat(kinds.get("active"), is(ServiceNowColumn.Kind.BOOLEAN));
      assertThat(kinds.get("opened_at"), is(ServiceNowColumn.Kind.TIMESTAMP));
      assertThat(kinds.get("start_date"), is(ServiceNowColumn.Kind.DATE));
      assertThat(kinds.get("caller_id"), is(ServiceNowColumn.Kind.REFERENCE));
      assertThat(kinds.get("caller_id__display"), is(ServiceNowColumn.Kind.TEXT));
      // x_custom_type is not documented; sys_glide_object says it is stored as a string
      assertThat(kinds.get("u_widget"), is(ServiceNowColumn.Kind.TEXT));
    }
  }

  @Test void anUnmappableTypeFailsOnlyItsTableAndNamesTheType() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final ServiceNowCatalog catalog = load(server);
      catalog.table("incident");
      final ServiceNowException e =
          assertThrows(ServiceNowException.class, () -> catalog.table("bad_table"));
      assertThat(e.getMessage(), containsString("bad_table.u_mystery"));
      assertThat(e.getMessage(), containsString("mystery_type"));
      assertThat(e.getMessage(), containsString("excludeColumnTypes"));
    }
  }

  @Test void excludedTypesAreLeftOutOnlyWhenConfigured() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final ServiceNowCatalog catalog = new ServiceNowCatalog(
          ServiceNowCatalog.load(TableApiClientTest.connection(server), 2),
          Collections.singleton("Mystery_Type"));
      assertThat(columnNames(catalog.table("bad_table")), contains("sys_id"));
    }
  }

  @Test void unreadableMetadataFailsAndNamesTheTable() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      server.override(seen -> seen.table.equals("sys_dictionary")
          ? FixtureServer.error(403, "User Not Authorized", "Access to table denied")
              .withStatus(403)
          : null);
      final ServiceNowException e = assertThrows(ServiceNowException.class,
          () -> ServiceNowCatalog.load(TableApiClientTest.connection(server), 2));
      assertThat(e.getMessage(), containsString("cannot read the metadata table sys_dictionary"));
      assertThat(e.getMessage(), containsString("does not guess columns"));
      assertThat(e.getStatus(), is(403));
    }
  }

  @Test void snapshotRoundTripsThroughJson() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final ServiceNowCatalog.Snapshot snapshot =
          ServiceNowCatalog.load(TableApiClientTest.connection(server), 2);
      final ServiceNowCatalog.Snapshot copy =
          ServiceNowCatalog.Snapshot.fromJson(snapshot.toJson());
      assertThat(copy.toJson(), equalTo(snapshot.toJson()));
    }
  }

  @Test void cacheServesASecondLoadFromDisk(@TempDir Path directory) throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final CatalogCache cache = new CatalogCache(directory, "scope", Duration.ofHours(1));
      cache.get(() -> ServiceNowCatalog.load(TableApiClientTest.connection(server), 2));
      final int requests = server.requests().size();
      assertThat(requests > 0, is(true));
      cache.get(() -> {
        throw new AssertionError("loaded again although the cache is fresh");
      });
      assertThat(server.requests().size(), is(requests));
    }
  }

  @Test void cacheIsScopedAndZeroTtlKeepsNothing(@TempDir Path directory) throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final CatalogCache off = new CatalogCache(directory, "scope", Duration.ZERO);
      off.get(() -> ServiceNowCatalog.load(TableApiClientTest.connection(server), 100));
      assertThat(Files.list(directory).count(), is(0L));

      final CatalogCache a = new CatalogCache(directory, "instance|basic:a", Duration.ofHours(1));
      final CatalogCache b = new CatalogCache(directory, "instance|basic:b", Duration.ofHours(1));
      a.get(() -> ServiceNowCatalog.load(TableApiClientTest.connection(server), 100));
      final int[] loadsForB = {0};
      b.get(() -> {
        loadsForB[0]++;
        return ServiceNowCatalog.load(TableApiClientTest.connection(server), 100);
      });
      assertThat(loadsForB[0], is(1));
      assertThat(Files.list(directory).count(), is(2L));
    }
  }

  @Test void aCorruptCacheFileIsAnErrorNotAReload(@TempDir Path directory) throws Exception {
    final CatalogCache cache = new CatalogCache(directory, "scope", Duration.ofHours(1));
    try (FixtureServer server = new FixtureServer()) {
      cache.get(() -> ServiceNowCatalog.load(TableApiClientTest.connection(server), 100));
      final Path file = Files.walk(directory).filter(p -> p.getFileName().toString()
          .equals("catalog.json")).findFirst().get();
      Files.write(file, "{not json".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      final ServiceNowException e = assertThrows(ServiceNowException.class, () -> cache.get(() -> {
        throw new AssertionError("must not reload");
      }));
      assertThat(e.getMessage(), containsString("is unusable"));
    }
  }

  @Test void tablesWithoutSysIdAreRejected() {
    final ServiceNowCatalog.Snapshot snapshot = new ServiceNowCatalog.Snapshot(
        java.util.Arrays.asList(new ServiceNowCatalog.TableRow("1", "t", "T", "")),
        java.util.Arrays.asList(
            new ServiceNowCatalog.DictionaryRow("t", "a", "string", 10, true)),
        Collections.<String, String>emptyMap());
    final ServiceNowException e = assertThrows(ServiceNowException.class,
        () -> new ServiceNowCatalog(snapshot, Collections.<String>emptySet()).table("t"));
    assertThat(e.getMessage(), containsString("no sys_id column"));
  }

  @Test void circularInheritanceIsRejected() {
    final ServiceNowCatalog.Snapshot snapshot = new ServiceNowCatalog.Snapshot(
        java.util.Arrays.asList(new ServiceNowCatalog.TableRow("1", "a", "A", "2"),
            new ServiceNowCatalog.TableRow("2", "b", "B", "1")),
        Collections.<ServiceNowCatalog.DictionaryRow>emptyList(),
        Collections.<String, String>emptyMap());
    final ServiceNowException e = assertThrows(ServiceNowException.class,
        () -> new ServiceNowCatalog(snapshot, Collections.<String>emptySet()).table("a"));
    assertThat(e.getMessage(), containsString("circular"));
  }

  @Test void aDisplayColumnNameClashIsRejected() {
    final ServiceNowCatalog.Snapshot snapshot = new ServiceNowCatalog.Snapshot(
        java.util.Arrays.asList(new ServiceNowCatalog.TableRow("1", "t", "T", "")),
        java.util.Arrays.asList(
            new ServiceNowCatalog.DictionaryRow("t", "sys_id", "GUID", 32, true),
            new ServiceNowCatalog.DictionaryRow("t", "owner", "reference", 32, true),
            new ServiceNowCatalog.DictionaryRow("t", "owner__display", "string", 32, true)),
        Collections.<String, String>emptyMap());
    final ServiceNowException e = assertThrows(ServiceNowException.class,
        () -> new ServiceNowCatalog(snapshot, Collections.<String>emptySet()).table("t"));
    assertThat(e.getMessage(), containsString("owner__display"));
  }
}
