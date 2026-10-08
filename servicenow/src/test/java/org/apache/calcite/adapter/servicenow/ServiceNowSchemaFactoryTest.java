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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Operand validation. Nothing here reaches a network. */
class ServiceNowSchemaFactoryTest {

  private static Map<String, Object> operand() {
    final Map<String, Object> operand = new HashMap<>();
    operand.put("instanceUrl", "https://dev12345.service-now.com");
    operand.put("authType", "basic");
    operand.put("username", "u");
    operand.put("password", "p");
    return operand;
  }

  private static String failure(Map<String, Object> operand) {
    return assertThrows(IllegalArgumentException.class,
        () -> ServiceNowSchemaFactory.INSTANCE.create(null, "sn", operand)).getMessage();
  }

  @Test void validOperandsCreateASchemaWithoutContactingTheInstance() {
    ServiceNowSchemaFactory.INSTANCE.create(null, "sn", operand());
  }

  @Test void requiredOperandsAreNamed() {
    for (String key : new String[] {"instanceUrl", "authType", "username", "password"}) {
      final Map<String, Object> operand = operand();
      operand.remove(key);
      assertThat(failure(operand), containsString("'" + key + "'"));
    }
  }

  @Test void authTypeIsExplicitAndOauthIsNotImplementedYet() {
    final Map<String, Object> operand = operand();
    operand.put("authType", "oauth_client_credentials");
    assertThat(failure(operand), containsString("not implemented yet"));
    operand.put("authType", "api_key");
    assertThat(failure(operand), containsString("Unknown authType 'api_key'"));
  }

  @Test void plainHttpIsOnlyForLoopback() {
    final Map<String, Object> operand = operand();
    operand.put("instanceUrl", "http://dev12345.service-now.com");
    assertThat(failure(operand), containsString("must be https"));
    operand.put("instanceUrl", "http://localhost:8080");
    ServiceNowSchemaFactory.INSTANCE.create(null, "sn", operand);
  }

  @Test void numbersAreValidated() {
    final Map<String, Object> operand = operand();
    operand.put("pageSize", "many");
    assertThat(failure(operand), containsString("'pageSize' is not a whole number"));
    operand.put("pageSize", 0);
    assertThat(failure(operand), containsString("'pageSize' must be at least 1"));
  }

  @Test void tableNamesAreValidated() {
    final Map<String, Object> operand = operand();
    operand.put("tables", "incident,bad name");
    assertThat(failure(operand), containsString("bad name"));
  }
}
