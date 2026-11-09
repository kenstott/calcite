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
package org.apache.calcite.adapter.govdata.etl;

/**
 * Reads the S3/MinIO credentials for the maintenance command-line tools from the process
 * environment so the secret never appears on a command line (visible in {@code ps} and in
 * {@code bash -x} traces). The tools take {@code --s3-credentials-from-env} instead of
 * {@code --s3-access-key}/{@code --s3-secret-key}.
 */
final class S3EnvCredentials {
  static final String FLAG = "--s3-credentials-from-env";
  private static final String ACCESS_VAR = "AWS_ACCESS_KEY_ID";
  private static final String SECRET_VAR = "AWS_SECRET_ACCESS_KEY";

  final String accessKey;
  final String secretKey;

  private S3EnvCredentials(String accessKey, String secretKey) {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  /** Reads both variables; throws if either is unset or empty. */
  static S3EnvCredentials read() {
    String access = System.getenv(ACCESS_VAR);
    String secret = System.getenv(SECRET_VAR);
    if (access == null || access.isEmpty() || secret == null || secret.isEmpty()) {
      throw new IllegalArgumentException(FLAG + " requires " + ACCESS_VAR + " and " + SECRET_VAR
          + " to be set in the environment");
    }
    return new S3EnvCredentials(access, secret);
  }
}
