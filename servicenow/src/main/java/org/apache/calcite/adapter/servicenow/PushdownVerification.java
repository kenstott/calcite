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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Which pushdown entries may be pushed: the union of the entries a verification record marks
 * verified and the entries the {@code trustPushdown} operand names.
 *
 * <p>The record is a JSON file written by the differential harness after a live run:
 * <pre>{"version": 1, "instance": "https://dev1.service-now.com", "mode": "live",
 *  "entries": {"EQ:TEXT:AND": {"status": "verified", "cases": 4, "detail": "..."},
 *              "LT:TEXT:AND": {"status": "mismatch", "cases": 2, "detail": "..."}}}</pre>
 * Only {@code "status": "verified"} enables an entry. The bundled record
 * ({@code servicenow/pushdown-verification.json}) is empty, so by default nothing is pushed. A
 * record that names an instance is accepted only for that instance, because what was proven is
 * the behaviour of one instance; to use it elsewhere, run the harness there or name entries with
 * {@code trustPushdown}. {@code trustPushdown} is the explicit, visible way to trust entries
 * without a record: an unknown entry name is an error, and each trusted entry is logged.
 */
final class PushdownVerification {
  private PushdownVerification() {}

  private static final Logger LOGGER = LoggerFactory.getLogger(PushdownVerification.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Name of the bundled, empty record. */
  static final String BUNDLED_RECORD = "/servicenow/pushdown-verification.json";

  /**
   * Resolves the verified entries.
   *
   * @param recordFile  a record file, or null for the bundled one
   * @param instanceUrl the instance this schema talks to
   * @param trusted     entries named by the operand
   */
  static Set<String> resolve(Path recordFile, URI instanceUrl, Collection<String> trusted) {
    final JsonNode record;
    try {
      if (recordFile == null) {
        try (InputStream in = PushdownVerification.class.getResourceAsStream(BUNDLED_RECORD)) {
          if (in == null) {
            throw new ServiceNowException("Bundled pushdown record " + BUNDLED_RECORD
                + " is missing from the jar");
          }
          record = MAPPER.readTree(in);
        }
      } else {
        record = MAPPER.readTree(Files.readAllBytes(recordFile));
      }
    } catch (IOException e) {
      throw new ServiceNowException("Cannot read the pushdown verification record "
          + (recordFile == null ? BUNDLED_RECORD : recordFile) + ": " + e.getMessage(), e);
    }
    if (record.path("version").asInt(-1) != 1 || !record.path("entries").isObject()) {
      throw new ServiceNowException("Pushdown verification record has the wrong shape "
          + "(expected version 1 and an 'entries' object)");
    }
    final String recordInstance = record.path("instance").asText("");
    if (!recordInstance.isEmpty() && !sameHost(recordInstance, instanceUrl)) {
      throw new ServiceNowException("The pushdown verification record was produced against "
          + recordInstance + " but this schema talks to " + instanceUrl + ". Run the harness "
          + "against this instance, or name the entries to trust in the trustPushdown operand.");
    }
    final Set<String> verified = new LinkedHashSet<>();
    final java.util.Iterator<Map.Entry<String, JsonNode>> it = record.get("entries").fields();
    while (it.hasNext()) {
      final Map.Entry<String, JsonNode> entry = it.next();
      if (!PushdownCapabilities.candidates().contains(entry.getKey())) {
        throw new ServiceNowException("Pushdown verification record names unknown entry '"
            + entry.getKey() + "'");
      }
      final String status = entry.getValue().path("status").asText("");
      if (status.equals("verified")) {
        verified.add(entry.getKey());
      } else if (!status.equals("mismatch")) {
        throw new ServiceNowException("Pushdown entry '" + entry.getKey() + "' has status '"
            + status + "'; expected verified or mismatch");
      }
    }
    for (String name : trusted) {
      if (!PushdownCapabilities.candidates().contains(name)) {
        throw new IllegalArgumentException("trustPushdown names unknown entry '" + name + "'");
      }
      LOGGER.warn("Pushdown entry {} is trusted by the trustPushdown operand, not by a "
          + "verification record", name);
      verified.add(name);
    }
    return verified;
  }

  private static boolean sameHost(String recordInstance, URI instanceUrl) {
    final String host = URI.create(recordInstance).getHost();
    return host != null && host.equalsIgnoreCase(instanceUrl.getHost());
  }

  /** Writes a record in the format {@link #resolve} reads. */
  static String toJson(String instance, String mode, Map<String, String[]> entries) {
    final ObjectNode root = MAPPER.createObjectNode();
    root.put("version", 1);
    root.put("instance", instance);
    root.put("mode", mode);
    final ObjectNode out = root.putObject("entries");
    entries.forEach((id, value) -> out.putObject(id).put("status", value[0])
        .put("cases", Integer.parseInt(value[1])).put("detail", value[2]));
    return root.toPrettyString();
  }
}
