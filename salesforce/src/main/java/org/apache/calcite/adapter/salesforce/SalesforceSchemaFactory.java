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
    } else {
      // OAuth token flow
      String accessToken = (String) operand.get("accessToken");
      String instanceUrl = (String) operand.get("instanceUrl");
      if (accessToken == null || instanceUrl == null) {
        throw new IllegalArgumentException(
            "Either username/password or accessToken/instanceUrl must be provided");
      }
      authConfig = SalesforceConnection.AuthConfig.accessToken(accessToken, instanceUrl);
    }

    // Cache configuration
    Integer cacheMaxSize = (Integer) operand.get("cacheMaxSize");
    if (cacheMaxSize == null) {
      cacheMaxSize = 1000;
    }

    try {
      SalesforceConnection connection =
          new SalesforceConnection(loginUrl, authConfig, apiVersion);
      return new SalesforceSchema(connection, cacheMaxSize);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Salesforce schema", e);
    }
  }
}
