/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.ops.provider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import software.amazon.awssdk.regions.Region;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests the parts of the AWS provider that need no cloud: how the {@code aws.region}
 * setting is read and how a bucket policy is judged.
 */
@Tag("unit")
class AWSProviderSettingsTest {
  @Test void absentBlankOrAllMeansEveryEnabledRegion() {
    assertThat(AWSProvider.regionsIn(null), is(Collections.<Region>emptyList()));
    assertThat(AWSProvider.regionsIn("  "), is(Collections.<Region>emptyList()));
    assertThat(AWSProvider.regionsIn("ALL"), is(Collections.<Region>emptyList()));
  }

  @Test void oneRegion() {
    assertThat(AWSProvider.regionsIn("eu-west-1"),
        is(Collections.singletonList(Region.EU_WEST_1)));
  }

  @Test void severalRegionsKeepTheirOrder() {
    assertThat(AWSProvider.regionsIn("us-west-2, us-east-1"),
        is(Arrays.asList(Region.US_WEST_2, Region.US_EAST_1)));
  }

  @Test void emptyNameIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> AWSProvider.regionsIn("us-east-1,,"));
  }

  @Test void bucketPolicyDenyingInsecureTransportIsHttpsOnly() {
    String policy = "{\"Statement\":[{\"Effect\":\"Deny\",\"Principal\":\"*\","
        + "\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::b/*\","
        + "\"Condition\":{\"Bool\":{\"aws:SecureTransport\":\"false\"}}}]}";
    assertThat(AWSProvider.deniesInsecureTransport(policy), is(true));
  }

  @Test void bucketPolicyWithoutThatDenyIsNotHttpsOnly() {
    String allowOnly = "{\"Statement\":{\"Effect\":\"Allow\",\"Principal\":\"*\","
        + "\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::b/*\","
        + "\"Condition\":{\"Bool\":{\"aws:SecureTransport\":\"false\"}}}}";
    assertThat(AWSProvider.deniesInsecureTransport(allowOnly), is(false));
    assertThat(AWSProvider.deniesInsecureTransport("{\"Statement\":[]}"), is(false));
  }
}
