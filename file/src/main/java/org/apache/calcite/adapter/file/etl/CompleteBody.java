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
package org.apache.calcite.adapter.file.etl;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketException;

/**
 * The response body of an {@link HttpURLConnection}, failing when the server stops short of the
 * {@code Content-Length} it declared.
 *
 * <p>{@code HttpURLConnection.getInputStream()} reports end-of-stream when a fixed-length body is
 * cut off, with no error. A reader of JSON or ZIP then fails loudly when it parses, but a CSV or
 * line-oriented reader just yields fewer rows. This stream throws a {@link SocketException} (the
 * same failure a mid-body reset gives) so the caller's existing connection retry applies. A body
 * with no declared length (chunked) is passed through; the JDK already raises on a cut chunked body.
 */
public final class CompleteBody {
  private CompleteBody() {
  }

  /** The connection's body, checked against its declared length. */
  public static InputStream of(HttpURLConnection conn) throws IOException {
    InputStream in = conn.getInputStream();
    long declared = conn.getContentLengthLong();
    return declared < 0 ? in : new Checked(in, declared, String.valueOf(conn.getURL()));
  }

  private static final class Checked extends FilterInputStream {
    private final long declared;
    private final String url;
    private long seen;

    Checked(InputStream in, long declared, String url) {
      super(in);
      this.declared = declared;
      this.url = url;
    }

    @Override public int read() throws IOException {
      int b = super.read();
      if (b >= 0) {
        seen++;
      } else {
        checkComplete();
      }
      return b;
    }

    @Override public int read(byte[] buf, int off, int len) throws IOException {
      int n = super.read(buf, off, len);
      if (n > 0) {
        seen += n;
      } else if (n < 0) {
        checkComplete();
      }
      return n;
    }

    @Override public long skip(long n) throws IOException {
      long skipped = super.skip(n);
      seen += Math.max(skipped, 0);
      return skipped;
    }

    private void checkComplete() throws SocketException {
      if (seen < declared) {
        throw new SocketException("truncated body: " + seen + " of " + declared + " bytes from "
            + url);
      }
    }
  }
}
