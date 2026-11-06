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
package org.apache.calcite.adapter.ops.provider;

import org.apache.calcite.adapter.ops.CloudOpsConfig;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Reads AWS through an assumed role, with a key that can do nothing but assume it.
 *
 * <p>{@code scripts/ephemeral_aws_role.py create} makes the user and the role and writes
 * the three {@code aws.assumeRole.*} settings to {@code local-test.properties}; without
 * them the tests are skipped.
 */
@Tag("integration")
class AWSRoleAssumptionLiveTest {
  private static final String FILE = "src/test/resources/local-test.properties";

  private static List<String> accountIds;
  private static String accessKeyId;
  private static String secretAccessKey;
  private static String roleArn;

  @BeforeAll static void readSettings() throws IOException {
    assumeTrue(new File(FILE).isFile(), FILE + " is absent");
    Properties properties = new Properties();
    try (InputStream in = new FileInputStream(FILE)) {
      properties.load(in);
    }
    accessKeyId = properties.getProperty("aws.assumeRole.accessKeyId");
    secretAccessKey = properties.getProperty("aws.assumeRole.secretAccessKey");
    roleArn = properties.getProperty("aws.assumeRole.roleArn");
    assumeTrue(accessKeyId != null && secretAccessKey != null && roleArn != null,
        "aws.assumeRole.* settings are absent from " + FILE);
    accountIds = Collections.singletonList(properties.getProperty("aws.accountIds"));
  }

  private static AWSProvider provider(String role) {
    return new AWSProvider(
        new CloudOpsConfig.AWSConfig(accountIds, "us-east-1", accessKeyId, secretAccessKey,
            role));
  }

  @Test void theKeyAloneReadsNothing() {
    AWSProvider provider = provider(null);
    assertThrows(IllegalStateException.class, () -> provider.queryIAMResources(accountIds));
  }

  @Test void theAssumedRoleReadsTheAccount() {
    List<Map<String, Object>> rows = provider(roleArn).queryIAMResources(accountIds);
    boolean sawItsOwnRole = false;
    String roleName = roleArn.substring(roleArn.lastIndexOf('/') + 1);
    for (Map<String, Object> row : rows) {
      assertThat(row.get("AccountId"), is((Object) accountIds.get(0)));
      if (roleName.equals(row.get("IAMResource"))) {
        sawItsOwnRole = true;
      }
    }
    assertThat("the role assumed is among the " + rows.size() + " IAM rows",
        sawItsOwnRole, is(true));
  }

  @Test void theAccountIdPlaceholderIsFilledIn() {
    String template = roleArn.replace(accountIds.get(0), "{account-id}");
    assertThat(template.contains("{account-id}"), is(true));
    List<Map<String, Object>> rows = provider(template).queryIAMResources(accountIds);
    assertThat(rows.isEmpty(), is(false));
  }
}
