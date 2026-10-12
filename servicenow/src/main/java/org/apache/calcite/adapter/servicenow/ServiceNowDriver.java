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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * JDBC driver for ServiceNow.
 *
 * <p>Accepts {@code jdbc:servicenow:} URLs, parses semicolon-delimited
 * parameters, builds an inline Calcite model targeting
 * {@link ServiceNowSchemaFactory}, and delegates to the Calcite JDBC driver.
 * Parameters are the factory's operands ({@code instanceUrl}, {@code authType},
 * {@code username}, {@code password}, ...); values are URL-decoded. {@code schema} names the SQL
 * schema (default {@code servicenow}). {@code lex} defaults to {@code JAVA}, because ServiceNow
 * table and column names are lower case and the Calcite default would fold unquoted names to
 * upper case.
 *
 * <p>Example URL:
 * {@code jdbc:servicenow:instanceUrl=https://dev12345.service-now.com;authType=basic;username=...;password=...}
 */
public class ServiceNowDriver extends org.apache.calcite.jdbc.Driver {

  static {
    new ServiceNowDriver().register();
  }

  @Override protected String getConnectStringPrefix() {
    return "jdbc:servicenow:";
  }

  @Override public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    String remainder = url.substring(getConnectStringPrefix().length());
    Properties params = new Properties();
    params.putAll(info);
    parseParams(remainder, params);
    String model = buildModel(params);
    // Delegate to a plain Calcite driver; super.connect would reject the
    // jdbc:calcite: URL because acceptsURL uses this driver's prefix.
    final Properties connectProps = new Properties();
    connectProps.putAll(params);
    if (!connectProps.containsKey("lex")) {
      connectProps.setProperty("lex", "JAVA");
    }
    return new org.apache.calcite.jdbc.Driver()
        .connect("jdbc:calcite:model=inline:" + model, connectProps);
  }

  private static void parseParams(String paramStr, Properties props) {
    if (paramStr.isEmpty()) {
      return;
    }
    for (String pair : paramStr.split(";")) {
      int idx = pair.indexOf('=');
      if (idx <= 0) {
        throw new IllegalArgumentException(
            "Malformed jdbc:servicenow: parameter (expected key=value): " + pair);
      }
      String key = pair.substring(0, idx).trim();
      String value = pair.substring(idx + 1).trim();
      props.setProperty(key, URLDecoder.decode(value, StandardCharsets.UTF_8));
    }
  }

  private static String buildModel(Properties props) throws SQLException {
    Map<String, Object> operand = new HashMap<>();
    for (String key : props.stringPropertyNames()) {
      operand.put(key, props.getProperty(key));
    }

    String schemaName = props.getProperty("schema", "servicenow");

    Map<String, Object> schema = new HashMap<>();
    schema.put("name", schemaName);
    schema.put("type", "custom");
    schema.put("factory", ServiceNowSchemaFactory.class.getName());
    schema.put("operand", operand);

    List<Map<String, Object>> schemas = new ArrayList<>();
    schemas.add(schema);

    Map<String, Object> model = new HashMap<>();
    model.put("version", "1.0");
    model.put("defaultSchema", schemaName);
    model.put("schemas", schemas);

    try {
      return new ObjectMapper().writeValueAsString(model);
    } catch (Exception e) {
      throw new SQLException("Failed to build ServiceNow model: " + e.getMessage(), e);
    }
  }
}
