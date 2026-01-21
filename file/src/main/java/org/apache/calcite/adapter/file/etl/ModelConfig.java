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
package org.apache.calcite.adapter.file.etl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Configuration for a model containing multiple schemas.
 *
 * <p>A model defines schemas and their processing order. Schemas can declare
 * dependencies on other schemas, ensuring reference/dimension tables are
 * loaded before fact tables that depend on them.
 *
 * <p>Example usage:
 * <pre>{@code
 * ModelConfig model = ModelConfig.builder()
 *     .name("govdata_econ")
 *     .schema(SchemaRef.of("econ_reference", "/econ/econ-reference-schema.yaml"))
 *     .schema(SchemaRef.of("econ", "/econ/econ-schema.yaml", "econ_reference"))
 *     .build();
 * }</pre>
 */
public class ModelConfig {

  private final String name;
  private final List<SchemaRef> schemas;

  private ModelConfig(Builder builder) {
    this.name = builder.name;
    this.schemas = Collections.unmodifiableList(new ArrayList<>(builder.schemas));
  }

  public String getName() {
    return name;
  }

  public List<SchemaRef> getSchemas() {
    return schemas;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Reference to a schema within the model.
   */
  public static class SchemaRef {
    private final String name;
    private final String resourcePath;
    private final List<String> dependsOn;

    private SchemaRef(String name, String resourcePath, List<String> dependsOn) {
      this.name = name;
      this.resourcePath = resourcePath;
      this.dependsOn = dependsOn != null
          ? Collections.unmodifiableList(new ArrayList<>(dependsOn))
          : Collections.emptyList();
    }

    public static SchemaRef of(String name, String resourcePath) {
      return new SchemaRef(name, resourcePath, null);
    }

    public static SchemaRef of(String name, String resourcePath, String... dependsOn) {
      List<String> deps = new ArrayList<>();
      Collections.addAll(deps, dependsOn);
      return new SchemaRef(name, resourcePath, deps);
    }

    public String getName() {
      return name;
    }

    public String getResourcePath() {
      return resourcePath;
    }

    public List<String> getDependsOn() {
      return dependsOn;
    }

    @Override
    public String toString() {
      return "SchemaRef{name='" + name + "', resource='" + resourcePath + "'"
          + (dependsOn.isEmpty() ? "" : ", dependsOn=" + dependsOn) + "}";
    }
  }

  /**
   * Builder for ModelConfig.
   */
  public static class Builder {
    private String name;
    private final List<SchemaRef> schemas = new ArrayList<>();

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder schema(SchemaRef schema) {
      this.schemas.add(schema);
      return this;
    }

    public Builder schema(String name, String resourcePath) {
      this.schemas.add(SchemaRef.of(name, resourcePath));
      return this;
    }

    public Builder schema(String name, String resourcePath, String... dependsOn) {
      this.schemas.add(SchemaRef.of(name, resourcePath, dependsOn));
      return this;
    }

    public ModelConfig build() {
      if (name == null || name.isEmpty()) {
        throw new IllegalStateException("Model name is required");
      }
      if (schemas.isEmpty()) {
        throw new IllegalStateException("At least one schema is required");
      }
      return new ModelConfig(this);
    }
  }
}
