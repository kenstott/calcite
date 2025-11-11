#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
cd /Users/kennethstott/calcite-vector-columns
./gradlew :file:compileJava :file:processResources > /dev/null 2>&1

cat > /tmp/TestModelLoad.java << 'JAVA'
import org.apache.calcite.adapter.file.similarity.ONNXEmbeddingProvider;
import java.util.HashMap;
import java.util.Map;

public class TestModelLoad {
  public static void main(String[] args) {
    try {
      System.out.println("Testing ONNX Model Loading...");
      Map<String, Object> config = new HashMap<>();
      ONNXEmbeddingProvider provider = new ONNXEmbeddingProvider(config);
      System.out.println("✓ Provider created: " + provider.getProviderName());
      System.out.println("✓ Dimensions: " + provider.getDimensions());
      double[] embedding = provider.generateEmbedding("test");
      System.out.println("✓ Generated embedding with " + embedding.length + " dimensions");
      double norm = 0.0;
      for (double v : embedding) norm += v * v;
      System.out.println("✓ Vector length: " + Math.sqrt(norm));
      System.out.println("\n✓✓✓ Phase 2 COMPLETE - Model loading verified! ✓✓✓");
    } catch (Exception e) {
      System.err.println("✗ Error: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }
}
JAVA

./gradlew :file:javaexec -PmainClass=TestModelLoad -PclasspathFiles=/tmp/TestModelLoad.java --quiet 2>&1 || \
  (cd /tmp && javac -cp /Users/kennethstott/calcite-vector-columns/file/build/classes/java/main TestModelLoad.java && \
   java -cp ".:/Users/kennethstott/calcite-vector-columns/file/build/classes/java/main:/Users/kennethstott/calcite-vector-columns/file/build/resources/main:$HOME/.gradle/caches/modules-2/files-2.1/com.microsoft.onnxruntime/onnxruntime/*/onnxruntime-*.jar:$HOME/.gradle/caches/modules-2/files-2.1/ai.djl/api/*/api-*.jar:$HOME/.gradle/caches/modules-2/files-2.1/ai.djl.huggingface/tokenizers/*/tokenizers-*.jar:$HOME/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-api/*/slf4j-api-*.jar" TestModelLoad)
