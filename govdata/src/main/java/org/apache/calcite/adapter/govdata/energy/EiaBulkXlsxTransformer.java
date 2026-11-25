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
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

public abstract class EiaBulkXlsxTransformer extends XlsxCellReader
    implements ResponseTransformer {

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  protected static final Logger LOGGER = LoggerFactory.getLogger(EiaBulkXlsxTransformer.class);

  protected abstract String parseWorkbook(XSSFWorkbook workbook, RequestContext context)
      throws Exception;

  @Override
  public String transform(String response, RequestContext context) {
    // The response is UTF-8 corrupted binary — ignore it and re-download
    String url = context.getUrl();
    try {
      byte[] xlsxBytes = downloadBytes(url);
      XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(xlsxBytes));
      try {
        return parseWorkbook(workbook, context);
      } finally {
        workbook.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("EIA bulk XLSX: failed to parse from " + url, e);
    }
  }

  protected byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    InputStream is = conn.getInputStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
    } finally {
      is.close();
    }
    return baos.toByteArray();
  }
}
