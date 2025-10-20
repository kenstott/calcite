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
package org.apache.calcite.mcp.metadata;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for VectorMetadata parsing.
 */
public class VectorMetadataTest {

  @Test
  public void testParsePattern1() {
    String comment = "Vector embedding for semantic search. "
        + "[VECTOR dimension=384 provider=local model=tf-idf]";

    VectorMetadata vm = VectorMetadata.parseFromComment("embedding", comment);

    assertThat(vm, notNullValue());
    assertThat(vm.getColumnName(), equalTo("embedding"));
    assertThat(vm.getDimension(), equalTo(384));
    assertThat(vm.getProvider(), equalTo("local"));
    assertThat(vm.getModel(), equalTo("tf-idf"));
    assertThat(vm.getDescription(), equalTo("Vector embedding for semantic search."));
    assertThat(vm.getSourceTable(), nullValue());
    assertThat(vm.getSourceTableColumn(), nullValue());
  }

  @Test
  public void testParsePattern2b() {
    String comment = "Vector embedding with logical FK. "
        + "[VECTOR dimension=384 provider=local model=tf-idf source_table=mda_sections source_id_col=section_id]";

    VectorMetadata vm = VectorMetadata.parseFromComment("embedding", comment);

    assertThat(vm, notNullValue());
    assertThat(vm.getDimension(), equalTo(384));
    assertThat(vm.getProvider(), equalTo("local"));
    assertThat(vm.getModel(), equalTo("tf-idf"));
    assertThat(vm.getSourceTable(), equalTo("mda_sections"));
    assertThat(vm.getSourceIdColumn(), equalTo("section_id"));
    assertThat(vm.getSourceTableColumn(), nullValue());
  }

  @Test
  public void testParsePattern3() {
    String comment = "Vector embedding for multi-source aggregation. "
        + "[VECTOR dimension=384 provider=local model=tf-idf source_table_col=source_table source_id_col=source_id]";

    VectorMetadata vm = VectorMetadata.parseFromComment("embedding", comment);

    assertThat(vm, notNullValue());
    assertThat(vm.getDimension(), equalTo(384));
    assertThat(vm.getProvider(), equalTo("local"));
    assertThat(vm.getModel(), equalTo("tf-idf"));
    assertThat(vm.getSourceTableColumn(), equalTo("source_table"));
    assertThat(vm.getSourceIdColumnName(), equalTo("source_id"));
    assertThat(vm.getSourceTable(), nullValue());
  }

  @Test
  public void testParseNoVectorMetadata() {
    String comment = "Regular column without vector metadata";

    VectorMetadata vm = VectorMetadata.parseFromComment("text", comment);

    assertThat(vm, nullValue());
  }

  @Test
  public void testParseNullComment() {
    VectorMetadata vm = VectorMetadata.parseFromComment("embedding", null);

    assertThat(vm, nullValue());
  }

  @Test
  public void testParseOpenAiProvider() {
    String comment = "OpenAI embedding. "
        + "[VECTOR dimension=1536 provider=openai model=text-embedding-ada-002]";

    VectorMetadata vm = VectorMetadata.parseFromComment("embedding", comment);

    assertThat(vm, notNullValue());
    assertThat(vm.getDimension(), equalTo(1536));
    assertThat(vm.getProvider(), equalTo("openai"));
    assertThat(vm.getModel(), equalTo("text-embedding-ada-002"));
  }
}
