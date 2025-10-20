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
package org.apache.calcite.mcp.protocol;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * MCP JSON-RPC response message.
 */
public class McpResponse {
  private String jsonrpc = "2.0";
  private String id;
  private JsonElement result;
  private JsonObject error;

  public McpResponse(String id) {
    this.id = id;
  }

  public String getJsonrpc() {
    return jsonrpc;
  }

  public void setJsonrpc(String jsonrpc) {
    this.jsonrpc = jsonrpc;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public JsonElement getResult() {
    return result;
  }

  public void setResult(JsonElement result) {
    this.result = result;
  }

  public JsonObject getError() {
    return error;
  }

  public void setError(JsonObject error) {
    this.error = error;
  }

  public static McpResponse success(String id, JsonElement result) {
    McpResponse response = new McpResponse(id);
    response.setResult(result);
    return response;
  }

  public static McpResponse error(String id, int code, String message) {
    McpResponse response = new McpResponse(id);
    JsonObject error = new JsonObject();
    error.addProperty("code", code);
    error.addProperty("message", message);
    response.setError(error);
    return response;
  }
}
