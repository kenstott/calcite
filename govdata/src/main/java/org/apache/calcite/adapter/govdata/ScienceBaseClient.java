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
package org.apache.calcite.adapter.govdata;

import java.io.IOException;
import java.io.InputStream;

/** Retrieves ScienceBase resources; a resource the site does not serve is an error. */
public interface ScienceBaseClient {
  /** Returns a text resource, such as a catalog JSON response. */
  String fetchText(String url) throws IOException;

  /** Opens a file download without holding it in memory; the caller closes the stream. */
  InputStream openStream(String url) throws IOException;
}
