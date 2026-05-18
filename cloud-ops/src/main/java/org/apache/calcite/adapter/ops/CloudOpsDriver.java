/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.ops;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * JDBC driver for Cloud Operations (Azure, AWS, GCP).
 *
 * <p>Accepts {@code jdbc:cloudops:} URLs, parses semicolon-delimited parameters,
 * builds an inline Calcite model targeting {@link CloudOpsSchemaFactory},
 * and delegates to the Calcite JDBC driver.
 *
 * <p>Example URL:
 * {@code jdbc:cloudops:azure.tenantId=xxx;azure.clientId=xxx;azure.clientSecret=xxx;azure.subscriptionIds=sub1,sub2}
 */
public class CloudOpsDriver extends org.apache.calcite.jdbc.Driver {

    static {
        new CloudOpsDriver().register();
    }

    @Override
    protected String getConnectStringPrefix() {
        return "jdbc:cloudops:";
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        String remainder = url.substring(getConnectStringPrefix().length());
        Properties params = new Properties(info);
        parseParams(remainder, params);
        String model = buildModel(params);
        return super.connect("jdbc:calcite:model=inline:" + model, params);
    }

    private void parseParams(String paramStr, Properties props) {
        if (paramStr == null || paramStr.isEmpty()) {
            return;
        }
        for (String pair : paramStr.split(";")) {
            int idx = pair.indexOf('=');
            if (idx > 0) {
                String key = pair.substring(0, idx).trim();
                String value = pair.substring(idx + 1).trim();
                if (value.startsWith("'") && value.endsWith("'")) {
                    value = value.substring(1, value.length() - 1);
                }
                try {
                    props.setProperty(key, URLDecoder.decode(value, "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    props.setProperty(key, value);
                }
            }
        }
    }

    private String buildModel(Properties props) throws SQLException {
        try {
            Map<String, Object> operand = new HashMap<String, Object>();
            for (String key : props.stringPropertyNames()) {
                operand.put(key, props.getProperty(key));
            }

            Map<String, Object> schema = new HashMap<String, Object>();
            schema.put("name", "cloud");
            schema.put("type", "custom");
            schema.put("factory", "org.apache.calcite.adapter.ops.CloudOpsSchemaFactory");
            schema.put("operand", operand);

            List<Map<String, Object>> schemas = new ArrayList<Map<String, Object>>();
            schemas.add(schema);

            Map<String, Object> model = new HashMap<String, Object>();
            model.put("version", "1.0");
            model.put("defaultSchema", "cloud");
            model.put("schemas", schemas);

            return new ObjectMapper().writeValueAsString(model);
        } catch (Exception e) {
            throw new SQLException("Failed to build CloudOps model: " + e.getMessage(), e);
        }
    }
}
