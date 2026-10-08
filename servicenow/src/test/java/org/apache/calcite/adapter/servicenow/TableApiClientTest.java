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

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The Table API client and the keyset reader, against a local server playing the documented API.
 * The fixtures are derived from ServiceNow's documentation, not captured from an instance.
 */
class TableApiClientTest {

  /** Remembers requested pauses instead of waiting. */
  private static final class RecordingSleeper implements ServiceNowConnection.Sleeper {
    final List<Long> pauses = new ArrayList<>();

    @Override public void sleep(long millis) {
      pauses.add(millis);
    }
  }

  static ServiceNowConnection connection(FixtureServer server, int maxRetries,
      Duration maxRetryWait, ServiceNowConnection.Sleeper sleeper) {
    return new ServiceNowConnection(URI.create(server.url()),
        ServiceNowAuth.basic(FixtureServer.USER, FixtureServer.PASSWORD), 2, maxRetries,
        maxRetryWait, Duration.ofSeconds(10), sleeper);
  }

  static ServiceNowConnection connection(FixtureServer server) {
    return connection(server, 3, Duration.ofSeconds(60), millis -> { });
  }

  private static List<String> numbers(TableReader reader) {
    final List<String> numbers = new ArrayList<>();
    while (reader.hasNext()) {
      numbers.add(Rows.text(reader.next(), "number", "incident", true));
    }
    return numbers;
  }

  @Test void pagesByKeyAndEndsOnlyOnAnEmptyPage() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final TableReader reader = new TableReader(connection(server), "incident",
          Arrays.asList("sys_id", "number"), false, 2, "");
      assertThat(numbers(reader),
          contains("INC0000001", "INC0000002", "INC0000003", "INC0000004", "INC0000005"));

      // Pages of 2, 2, 1, then the empty page that proves the end; the short page of 1 did not end it
      final List<FixtureServer.Seen> requests = server.requests("incident");
      assertThat(requests.size(), is(4));
      assertThat(reader.requestCount(), is(4));
      assertThat(requests.get(0).params.get("sysparm_query"), equalTo("ORDERBYsys_id"));
      assertThat(requests.get(1).params.get("sysparm_query"),
          equalTo("sys_id>" + String.format("%032x", 0xc2) + "^ORDERBYsys_id"));
      for (FixtureServer.Seen seen : requests) {
        assertThat(seen.params.get("sysparm_limit"), equalTo("2"));
        assertThat(seen.params.get("sysparm_fields"), equalTo("sys_id,number"));
        assertThat(seen.params.get("sysparm_no_count"), equalTo("true"));
        assertThat(seen.params.get("sysparm_display_value"), equalTo("false"));
        assertThat(seen.params.get("sysparm_exclude_reference_link"), equalTo("true"));
      }
    }
  }

  @Test void aShortPageIsNotTheEndOfData() throws Exception {
    // ServiceNow applies sysparm_limit before ACLs, so a page may hold fewer rows than the limit
    try (FixtureServer server = new FixtureServer()) {
      final AtomicInteger calls = new AtomicInteger();
      server.override(seen -> {
        if (calls.getAndIncrement() == 0) {
          return FixtureServer.Reply.json("{\"result\":[{\"sys_id\":\""
              + String.format("%032x", 0xc1) + "\",\"number\":\"INC0000001\"}]}");
        }
        return null;
      });
      final TableReader reader = new TableReader(connection(server), "incident",
          Arrays.asList("sys_id", "number"), false, 3, "");
      assertThat(numbers(reader).size(), is(5));
    }
  }

  @Test void displayValuesAreRequestedOnlyWhenAsked() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final TableReader reader = new TableReader(connection(server), "incident",
          Arrays.asList("sys_id", "caller_id"), true, 10, "");
      final JsonNode first = reader.next();
      assertThat(first.get("caller_id").get("display_value").asText(), equalTo("Abel Tuter"));
      assertThat(server.requests("incident").get(0).params.get("sysparm_display_value"),
          equalTo("all"));
    }
  }

  @Test void aPageThatDoesNotAdvanceIsAnError() throws Exception {
    // What ServiceNow would do if it dropped the sys_id> term it could not parse
    try (FixtureServer server = new FixtureServer()) {
      server.override(seen -> FixtureServer.Reply.json("{\"result\":[{\"sys_id\":\""
          + String.format("%032x", 0xc1) + "\",\"number\":\"INC0000001\"}]}"));
      final TableReader reader = new TableReader(connection(server), "incident",
          Arrays.asList("sys_id", "number"), false, 1, "");
      reader.next();
      final ServiceNowException e = assertThrows(ServiceNowException.class, reader::hasNext);
      assertThat(e.getMessage(), containsString("did not advance"));
    }
  }

  @Test void sysIdIsRequiredForPaging() {
    assertThrows(IllegalArgumentException.class, () -> new TableReader(null, "incident",
        Arrays.asList("number"), false, 10, ""));
  }

  @Test void aResponseWithoutResultIsAnError() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      server.override(seen -> FixtureServer.Reply.json("{\"rows\":[]}"));
      final TableReader reader = new TableReader(connection(server), "incident",
          Arrays.asList("sys_id"), false, 10, "");
      final ServiceNowException e = assertThrows(ServiceNowException.class, reader::hasNext);
      assertThat(e.getMessage(), containsString("no 'result' array"));
    }
  }

  @Test void aTruncatedBodyWith200IsAnError() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      server.override(seen -> FixtureServer.Reply.json("{\"result\":[{\"sys_id\":\"abc"));
      final ServiceNowException e = assertThrows(ServiceNowException.class,
          () -> new TableReader(connection(server), "incident", Arrays.asList("sys_id"), false,
              10, "").hasNext());
      assertThat(e.getMessage(), containsString("not valid JSON"));
    }
  }

  @Test void htmlWith200IsAnError() throws Exception {
    // A hibernating instance and a login redirect page both answer HTML
    try (FixtureServer server = new FixtureServer()) {
      server.override(seen -> new FixtureServer.Reply(200, "text/html",
          "<html><body>Instance Hibernating</body></html>"));
      final ServiceNowException e = assertThrows(ServiceNowException.class,
          () -> new TableReader(connection(server), "incident", Arrays.asList("sys_id"), false,
              10, "").hasNext());
      assertThat(e.getMessage(), containsString("instead of JSON"));
      assertThat(e.getMessage(), containsString("Instance Hibernating"));
    }
  }

  @Test void authenticationFailureNamesTheUser() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final ServiceNowConnection bad = new ServiceNowConnection(URI.create(server.url()),
          ServiceNowAuth.basic(FixtureServer.USER, "wrong"), 1, 0, Duration.ZERO,
          Duration.ofSeconds(10));
      final ServiceNowException e = assertThrows(ServiceNowException.class,
          () -> bad.getTable("incident", new LinkedHashMap<String, String>()));
      assertThat(e.getStatus(), is(401));
      assertThat(e.getMessage(), containsString("basic:" + FixtureServer.USER));
      assertThat(e.getMessage(), containsString("User Not Authenticated"));
    }
  }

  @Test void errorBodyMessageIsReported() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final ServiceNowException e = assertThrows(ServiceNowException.class,
          () -> connection(server).getTable("no_such_table", new LinkedHashMap<String, String>()));
      assertThat(e.getStatus(), is(400));
      assertThat(e.getMessage(), containsString("Invalid table no_such_table"));
    }
  }

  @Test void rateLimitIsRetriedAsTheServerInstructs() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      final AtomicInteger calls = new AtomicInteger();
      server.override(seen -> calls.getAndIncrement() < 2
          ? FixtureServer.error(429, "Rate limit exceeded", "").header("Retry-After", "7")
          : null);
      final RecordingSleeper sleeper = new RecordingSleeper();
      final ServiceNowConnection connection =
          connection(server, 3, Duration.ofSeconds(60), sleeper);
      final Map<String, String> params = new LinkedHashMap<>();
      params.put("sysparm_limit", "1");
      connection.getTable("incident", params);
      assertThat(sleeper.pauses, contains(7000L, 7000L));
      assertThat(server.requests().size(), is(3));
    }
  }

  @Test void rateLimitFailsWhenRetriesAreUsedUp() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      server.override(seen ->
          FixtureServer.error(429, "Rate limit exceeded", "").header("Retry-After", "1"));
      final RecordingSleeper sleeper = new RecordingSleeper();
      final ServiceNowException e = assertThrows(ServiceNowException.RateLimited.class,
          () -> connection(server, 2, Duration.ofSeconds(60), sleeper)
              .getTable("incident", new LinkedHashMap<String, String>()));
      assertThat(e.getMessage(), containsString("2 allowed retries are used up"));
      assertThat(e.getMessage(), containsString("Rate limit exceeded"));
      assertThat(sleeper.pauses.size(), is(2));
      assertThat(server.requests().size(), is(3));
    }
  }

  @Test void rateLimitFailsWhenTheWaitBudgetWouldBeExceeded() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      server.override(seen ->
          FixtureServer.error(429, "Rate limit exceeded", "").header("Retry-After", "40"));
      final RecordingSleeper sleeper = new RecordingSleeper();
      final ServiceNowException e = assertThrows(ServiceNowException.RateLimited.class,
          () -> connection(server, 5, Duration.ofSeconds(60), sleeper)
              .getTable("incident", new LinkedHashMap<String, String>()));
      assertThat(e.getMessage(), containsString("retry wait budget"));
      // One wait of 40s fits in the 60s budget; the second would make 80s
      assertThat(sleeper.pauses, contains(40000L));
    }
  }

  @Test void rateLimitWithoutRetryAfterFailsInsteadOfGuessing() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      server.override(seen -> FixtureServer.error(429, "Too many requests", ""));
      final ServiceNowException e = assertThrows(ServiceNowException.RateLimited.class,
          () -> connection(server).getTable("incident", new LinkedHashMap<String, String>()));
      assertThat(e.getMessage(), containsString("without a Retry-After header"));
      assertThat(server.requests().size(), is(1));
    }
  }

  @Test void rateLimitWithAnUnparsableRetryAfterFails() throws Exception {
    try (FixtureServer server = new FixtureServer()) {
      server.override(seen -> FixtureServer.error(429, "Too many requests", "")
          .header("Retry-After", "Wed, 21 Oct 2026 07:28:00 GMT"));
      final ServiceNowException e = assertThrows(ServiceNowException.RateLimited.class,
          () -> connection(server).getTable("incident", new LinkedHashMap<String, String>()));
      assertThat(e.getMessage(), containsString("not a whole number of seconds"));
    }
  }
}
