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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * Factory for Salesforce schemas.
 */
public class SalesforceSchemaFactory implements SchemaFactory {

  public static final SalesforceSchemaFactory INSTANCE = new SalesforceSchemaFactory();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    String loginUrl = (String) operand.get("loginUrl");
    if (loginUrl == null) {
      loginUrl = "https://login.salesforce.com";
    }

    String username = (String) operand.get("username");
    String password = (String) operand.get("password");
    String securityToken = (String) operand.get("securityToken");
    String clientId = (String) operand.get("clientId");
    String clientSecret = (String) operand.get("clientSecret");
    String apiVersion = (String) operand.get("apiVersion");

    if (apiVersion == null) {
      apiVersion = "v58.0";
    }

    // Authentication configuration
    SalesforceConnection.AuthConfig authConfig;
    if (username != null && password != null) {
      // Username/password flow
      authConfig = SalesforceConnection.AuthConfig
          .usernamePassword(username, password, securityToken, clientId, clientSecret);
    } else if (clientId != null && clientSecret != null) {
      // Client credentials flow
      authConfig = SalesforceConnection.AuthConfig
          .clientCredentials(clientId, clientSecret);
    } else {
      // OAuth token flow
      String accessToken = (String) operand.get("accessToken");
      String instanceUrl = (String) operand.get("instanceUrl");
      if (accessToken == null || instanceUrl == null) {
        throw new IllegalArgumentException(
            "One of username/password, clientId/clientSecret, or "
                + "accessToken/instanceUrl must be provided");
      }
      authConfig = SalesforceConnection.AuthConfig.accessToken(accessToken, instanceUrl);
    }

    // Cache configuration
    Object lowercaseAliasesOperand = operand.get("lowercaseAliases");
    boolean lowercaseAliases = lowercaseAliasesOperand == null
        || Boolean.parseBoolean(lowercaseAliasesOperand.toString());

    // A model file supplies a number; the JDBC driver supplies URL strings
    Object cacheMaxSizeOperand = operand.get("cacheMaxSize");
    int cacheMaxSize = cacheMaxSizeOperand == null
        ? 1000
        : Integer.parseInt(cacheMaxSizeOperand.toString());

    try {
      SalesforceConnection connection =
          new SalesforceConnection(loginUrl, authConfig, apiVersion);
      return new SalesforceSchema(connection, cacheMaxSize, lowercaseAliases);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Salesforce schema", e);
    }
  }
}
