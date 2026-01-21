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
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for extensibility hooks in the ETL pipeline.
 *
 * <p>HooksConfig allows adapters to inject business-specific logic into the
 * generic ETL pipeline. Hooks are specified as fully qualified class names
 * that implement the appropriate interface.
 *
 * <h3>YAML Configuration Example</h3>
 * <pre>{@code
 * tables:
 *   - name: economic_data
 *     source:
 *       type: http
 *       url: "https://api.example.com/data"
 *
 *     hooks:
 *       # Single response transformer class
 *       responseTransformer: "org.example.MyResponseTransformer"
 *
 *       # List of row transformer configurations
 *       rowTransformers:
 *         - type: class
 *           class: "org.example.MyRowTransformer"
 *         - type: expression
 *           column: data_value
 *           expression: "REPLACE(data_value, '(NA)', '')"
 *
 *       # List of validator configurations
 *       validators:
 *         - type: class
 *           class: "org.example.MyValidator"
 *         - type: expression
 *           condition: "geo_fips IS NOT NULL"
 *           action: "drop"
 *
 *       # Custom dimension resolver class
 *       dimensionResolver: "org.example.MyDimensionResolver"
 *
 *       # Error handling configuration
 *       errorHandling:
 *         responseTransformer: fail
 *         rowTransformer: skip_row
 *         validator: continue
 * }</pre>
 *
 * @see ResponseTransformer
 * @see RowTransformer
 * @see Validator
 * @see DimensionResolver
 */
public class HooksConfig {

  private final boolean enabled;
  private final String responseTransformerClass;
  private final List<TransformerConfig> rowTransformers;
  private final List<ValidatorConfig> validators;
  private final String dimensionResolverClass;
  private final String dataProviderClass;
  private final String tableLifecycleListenerClass;
  private final String variableNormalizerClass;
  private final Map<String, Object> variableNormalizerConfig;
  private final HookErrorHandling errorHandling;

  private HooksConfig(Builder builder) {
    this.enabled = builder.enabled;
    this.responseTransformerClass = builder.responseTransformerClass;
    this.rowTransformers = builder.rowTransformers != null
        ? Collections.unmodifiableList(new ArrayList<TransformerConfig>(builder.rowTransformers))
        : Collections.<TransformerConfig>emptyList();
    this.validators = builder.validators != null
        ? Collections.unmodifiableList(new ArrayList<ValidatorConfig>(builder.validators))
        : Collections.<ValidatorConfig>emptyList();
    this.dimensionResolverClass = builder.dimensionResolverClass;
    this.dataProviderClass = builder.dataProviderClass;
    this.tableLifecycleListenerClass = builder.tableLifecycleListenerClass;
    this.variableNormalizerClass = builder.variableNormalizerClass;
    this.variableNormalizerConfig = builder.variableNormalizerConfig != null
        ? Collections.unmodifiableMap(new HashMap<String, Object>(builder.variableNormalizerConfig))
        : Collections.<String, Object>emptyMap();
    this.errorHandling = builder.errorHandling != null
        ? builder.errorHandling
        : HookErrorHandling.defaults();
  }

  /**
   * Returns whether this table is enabled for processing.
   *
   * <p>When false, the table is:
   * <ul>
   *   <li>Skipped during ETL (no source download or materialization)</li>
   *   <li>Excluded from the final schema metadata</li>
   * </ul>
   *
   * @return true if table is enabled (default), false to disable
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Returns the fully qualified class name of the ResponseTransformer.
   *
   * @return Class name, or null if not configured
   */
  public String getResponseTransformerClass() {
    return responseTransformerClass;
  }

  /**
   * Returns the list of row transformer configurations.
   *
   * @return Unmodifiable list of transformer configs
   */
  public List<TransformerConfig> getRowTransformers() {
    return rowTransformers;
  }

  /**
   * Returns the list of validator configurations.
   *
   * @return Unmodifiable list of validator configs
   */
  public List<ValidatorConfig> getValidators() {
    return validators;
  }

  /**
   * Returns the fully qualified class name of the DimensionResolver.
   *
   * @return Class name, or null if not configured
   */
  public String getDimensionResolverClass() {
    return dimensionResolverClass;
  }

  /**
   * Returns the fully qualified class name of the DataProvider.
   *
   * <p>When configured, the pipeline will use this provider to fetch data
   * instead of the built-in HttpSource. This is useful for:
   * <ul>
   *   <li>APIs that require batching (e.g., BLS with 50 series limit)</li>
   *   <li>Custom data sources (FTP, databases, etc.)</li>
   *   <li>Complex request/response handling</li>
   * </ul>
   *
   * @return Class name, or null if not configured
   */
  public String getDataProviderClass() {
    return dataProviderClass;
  }

  /**
   * Returns the fully qualified class name of the TableLifecycleListener.
   *
   * @return Class name, or null if not configured
   */
  public String getTableLifecycleListenerClass() {
    return tableLifecycleListenerClass;
  }

  /**
   * Returns the fully qualified class name of the VariableNormalizer.
   *
   * <p>When configured, the ETL pipeline will normalize API-specific variable
   * names to conceptual names for schema evolution support.
   *
   * @return Class name, or null if not configured
   * @see VariableNormalizer
   */
  public String getVariableNormalizerClass() {
    return variableNormalizerClass;
  }

  /**
   * Returns the configuration map for the VariableNormalizer.
   *
   * <p>This allows passing additional configuration to the normalizer,
   * such as the mapping file path for {@link MappingFileVariableNormalizer}.
   *
   * @return Configuration map, never null (may be empty)
   */
  public Map<String, Object> getVariableNormalizerConfig() {
    return variableNormalizerConfig;
  }

  /**
   * Returns the error handling configuration for hooks.
   *
   * @return Error handling configuration
   */
  public HookErrorHandling getErrorHandling() {
    return errorHandling;
  }

  /**
   * Returns whether any hooks are configured.
   *
   * @return true if at least one hook is configured
   */
  public boolean hasHooks() {
    return responseTransformerClass != null
        || !rowTransformers.isEmpty()
        || !validators.isEmpty()
        || dimensionResolverClass != null
        || tableLifecycleListenerClass != null
        || variableNormalizerClass != null;
  }

  /**
   * Creates a new builder for HooksConfig.
   *
   * @return A new Builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns an empty HooksConfig with no hooks configured.
   *
   * @return Empty HooksConfig
   */
  public static HooksConfig empty() {
    return new Builder().build();
  }

  /**
   * Creates a HooksConfig from a YAML/JSON map.
   *
   * @param map Configuration map
   * @return HooksConfig instance, or empty config if map is null
   */
  @SuppressWarnings("unchecked")
  public static HooksConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return empty();
    }

    Builder builder = builder();

    // Parse enabled flag (defaults to true)
    Object enabledObj = map.get("enabled");
    if (enabledObj instanceof Boolean) {
      builder.enabled((Boolean) enabledObj);
    } else if (enabledObj instanceof String) {
      builder.enabled(Boolean.parseBoolean((String) enabledObj));
    }

    Object responseTransformerObj = map.get("responseTransformer");
    if (responseTransformerObj instanceof String) {
      builder.responseTransformerClass((String) responseTransformerObj);
    }

    Object rowTransformersObj = map.get("rowTransformers");
    if (rowTransformersObj instanceof List) {
      List<TransformerConfig> transformers = new ArrayList<TransformerConfig>();
      for (Object item : (List<?>) rowTransformersObj) {
        if (item instanceof Map) {
          transformers.add(TransformerConfig.fromMap((Map<String, Object>) item));
        }
      }
      builder.rowTransformers(transformers);
    }

    Object validatorsObj = map.get("validators");
    if (validatorsObj instanceof List) {
      List<ValidatorConfig> validatorConfigs = new ArrayList<ValidatorConfig>();
      for (Object item : (List<?>) validatorsObj) {
        if (item instanceof Map) {
          validatorConfigs.add(ValidatorConfig.fromMap((Map<String, Object>) item));
        }
      }
      builder.validators(validatorConfigs);
    }

    Object dimensionResolverObj = map.get("dimensionResolver");
    if (dimensionResolverObj instanceof String) {
      builder.dimensionResolverClass((String) dimensionResolverObj);
    }

    Object dataProviderObj = map.get("dataProvider");
    if (dataProviderObj instanceof String) {
      builder.dataProviderClass((String) dataProviderObj);
    }

    Object tableListenerObj = map.get("tableLifecycleListener");
    if (tableListenerObj instanceof String) {
      builder.tableLifecycleListenerClass((String) tableListenerObj);
    }

    Object variableNormalizerObj = map.get("variableNormalizer");
    if (variableNormalizerObj instanceof String) {
      builder.variableNormalizerClass((String) variableNormalizerObj);
    }

    Object variableNormalizerConfigObj = map.get("variableNormalizerConfig");
    if (variableNormalizerConfigObj instanceof Map) {
      builder.variableNormalizerConfig((Map<String, Object>) variableNormalizerConfigObj);
    }

    Object errorHandlingObj = map.get("errorHandling");
    if (errorHandlingObj instanceof Map) {
      builder.errorHandling(HookErrorHandling.fromMap((Map<String, Object>) errorHandlingObj));
    }

    return builder.build();
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("HooksConfig{");
    boolean first = true;
    if (responseTransformerClass != null) {
      sb.append("responseTransformer='").append(responseTransformerClass).append("'");
      first = false;
    }
    if (!rowTransformers.isEmpty()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append("rowTransformers=").append(rowTransformers.size());
      first = false;
    }
    if (!validators.isEmpty()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append("validators=").append(validators.size());
      first = false;
    }
    if (dimensionResolverClass != null) {
      if (!first) {
        sb.append(", ");
      }
      sb.append("dimensionResolver='").append(dimensionResolverClass).append("'");
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Configuration for a row transformer.
   */
  public static class TransformerConfig {
    /** Type of transformer: "class" or "expression". */
    private final String type;
    /** Fully qualified class name (for type="class"). */
    private final String className;
    /** Column to transform (for type="expression"). */
    private final String column;
    /** SQL expression (for type="expression"). */
    private final String expression;

    private TransformerConfig(String type, String className, String column, String expression) {
      this.type = type;
      this.className = className;
      this.column = column;
      this.expression = expression;
    }

    public static TransformerConfig ofClass(String className) {
      return new TransformerConfig("class", className, null, null);
    }

    public static TransformerConfig ofExpression(String column, String expression) {
      return new TransformerConfig("expression", null, column, expression);
    }

    public String getType() {
      return type;
    }

    public String getClassName() {
      return className;
    }

    public String getColumn() {
      return column;
    }

    public String getExpression() {
      return expression;
    }

    public boolean isClassBased() {
      return "class".equals(type);
    }

    public boolean isExpressionBased() {
      return "expression".equals(type);
    }

    public static TransformerConfig fromMap(Map<String, Object> map) {
      String type = (String) map.get("type");
      String className = (String) map.get("class");
      String column = (String) map.get("column");
      String expression = (String) map.get("expression");
      return new TransformerConfig(type, className, column, expression);
    }

    @Override public String toString() {
      if (isClassBased()) {
        return "TransformerConfig{class='" + className + "'}";
      } else {
        return "TransformerConfig{expression='" + column + ":" + expression + "'}";
      }
    }
  }

  /**
   * Configuration for a validator.
   */
  public static class ValidatorConfig {
    /** Type of validator: "class" or "expression". */
    private final String type;
    /** Fully qualified class name (for type="class"). */
    private final String className;
    /** SQL condition expression (for type="expression"). */
    private final String condition;
    /** Action when condition is false: "drop", "warn", "fail" (for type="expression"). */
    private final String action;

    private ValidatorConfig(String type, String className, String condition, String action) {
      this.type = type;
      this.className = className;
      this.condition = condition;
      this.action = action;
    }

    public static ValidatorConfig ofClass(String className) {
      return new ValidatorConfig("class", className, null, null);
    }

    public static ValidatorConfig ofExpression(String condition, String action) {
      return new ValidatorConfig("expression", null, condition, action);
    }

    public String getType() {
      return type;
    }

    public String getClassName() {
      return className;
    }

    public String getCondition() {
      return condition;
    }

    public String getAction() {
      return action;
    }

    public boolean isClassBased() {
      return "class".equals(type);
    }

    public boolean isExpressionBased() {
      return "expression".equals(type);
    }

    public static ValidatorConfig fromMap(Map<String, Object> map) {
      String type = (String) map.get("type");
      String className = (String) map.get("class");
      String condition = (String) map.get("condition");
      String action = (String) map.get("action");
      return new ValidatorConfig(type, className, condition, action);
    }

    @Override public String toString() {
      if (isClassBased()) {
        return "ValidatorConfig{class='" + className + "'}";
      } else {
        return "ValidatorConfig{condition='" + condition + "', action='" + action + "'}";
      }
    }
  }

  /**
   * Error handling configuration for hooks.
   */
  public static class HookErrorHandling {
    /** Action for ResponseTransformer errors. */
    private final ErrorAction responseTransformerAction;
    /** Action for RowTransformer errors. */
    private final ErrorAction rowTransformerAction;
    /** Action for Validator errors. */
    private final ErrorAction validatorAction;

    public enum ErrorAction {
      /** Fail the entire operation. */
      FAIL,
      /** Skip the current row and continue. */
      SKIP_ROW,
      /** Log warning and continue including the row. */
      CONTINUE
    }

    private HookErrorHandling(ErrorAction responseTransformerAction,
        ErrorAction rowTransformerAction, ErrorAction validatorAction) {
      this.responseTransformerAction = responseTransformerAction;
      this.rowTransformerAction = rowTransformerAction;
      this.validatorAction = validatorAction;
    }

    public static HookErrorHandling defaults() {
      return new HookErrorHandling(ErrorAction.FAIL, ErrorAction.SKIP_ROW, ErrorAction.CONTINUE);
    }

    public ErrorAction getResponseTransformerAction() {
      return responseTransformerAction;
    }

    public ErrorAction getRowTransformerAction() {
      return rowTransformerAction;
    }

    public ErrorAction getValidatorAction() {
      return validatorAction;
    }

    public static HookErrorHandling fromMap(Map<String, Object> map) {
      if (map == null) {
        return defaults();
      }

      ErrorAction responseAction = ErrorAction.FAIL;
      ErrorAction rowAction = ErrorAction.SKIP_ROW;
      ErrorAction validatorAction = ErrorAction.CONTINUE;

      Object responseObj = map.get("responseTransformer");
      if (responseObj instanceof String) {
        responseAction = parseAction((String) responseObj);
      }

      Object rowObj = map.get("rowTransformer");
      if (rowObj instanceof String) {
        rowAction = parseAction((String) rowObj);
      }

      Object validatorObj = map.get("validator");
      if (validatorObj instanceof String) {
        validatorAction = parseAction((String) validatorObj);
      }

      return new HookErrorHandling(responseAction, rowAction, validatorAction);
    }

    private static ErrorAction parseAction(String value) {
      String normalized = value.toUpperCase().replace("-", "_").replace(" ", "_");
      try {
        return ErrorAction.valueOf(normalized);
      } catch (IllegalArgumentException e) {
        return ErrorAction.FAIL;
      }
    }
  }

  /**
   * Builder for HooksConfig.
   */
  public static class Builder {
    private boolean enabled = true; // Enabled by default
    private String responseTransformerClass;
    private List<TransformerConfig> rowTransformers;
    private List<ValidatorConfig> validators;
    private String dimensionResolverClass;
    private String dataProviderClass;
    private String tableLifecycleListenerClass;
    private String variableNormalizerClass;
    private Map<String, Object> variableNormalizerConfig;
    private HookErrorHandling errorHandling;

    /**
     * Sets whether this table is enabled for processing.
     *
     * @param enabled true to enable (default), false to disable
     * @return This builder
     */
    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * Sets the ResponseTransformer class name.
     *
     * @param className Fully qualified class name
     * @return This builder
     */
    public Builder responseTransformerClass(String className) {
      this.responseTransformerClass = className;
      return this;
    }

    /**
     * Sets the list of row transformer configurations.
     *
     * @param rowTransformers List of transformer configs
     * @return This builder
     */
    public Builder rowTransformers(List<TransformerConfig> rowTransformers) {
      this.rowTransformers = rowTransformers;
      return this;
    }

    /**
     * Sets the list of validator configurations.
     *
     * @param validators List of validator configs
     * @return This builder
     */
    public Builder validators(List<ValidatorConfig> validators) {
      this.validators = validators;
      return this;
    }

    /**
     * Sets the DimensionResolver class name.
     *
     * @param className Fully qualified class name
     * @return This builder
     */
    public Builder dimensionResolverClass(String className) {
      this.dimensionResolverClass = className;
      return this;
    }

    /**
     * Sets the DataProvider class name.
     *
     * <p>When configured, the pipeline uses this provider to fetch data
     * instead of the built-in HttpSource.
     *
     * @param className Fully qualified class name implementing DataProvider
     * @return This builder
     */
    public Builder dataProviderClass(String className) {
      this.dataProviderClass = className;
      return this;
    }

    /**
     * Sets the TableLifecycleListener class name.
     *
     * @param className Fully qualified class name
     * @return This builder
     */
    public Builder tableLifecycleListenerClass(String className) {
      this.tableLifecycleListenerClass = className;
      return this;
    }

    /**
     * Sets the VariableNormalizer class name.
     *
     * <p>When configured, the ETL pipeline will normalize API-specific variable
     * names to conceptual names for schema evolution support.
     *
     * @param className Fully qualified class name implementing VariableNormalizer
     * @return This builder
     * @see VariableNormalizer
     */
    public Builder variableNormalizerClass(String className) {
      this.variableNormalizerClass = className;
      return this;
    }

    /**
     * Sets the VariableNormalizer configuration map.
     *
     * <p>This allows passing additional configuration to the normalizer,
     * such as the mapping file path for MappingFileVariableNormalizer.
     *
     * @param config Configuration map
     * @return This builder
     */
    public Builder variableNormalizerConfig(Map<String, Object> config) {
      this.variableNormalizerConfig = config;
      return this;
    }

    /**
     * Sets the error handling configuration.
     *
     * @param errorHandling Error handling config
     * @return This builder
     */
    public Builder errorHandling(HookErrorHandling errorHandling) {
      this.errorHandling = errorHandling;
      return this;
    }

    /**
     * Builds the HooksConfig.
     *
     * @return A new HooksConfig instance
     */
    public HooksConfig build() {
      return new HooksConfig(this);
    }
  }
}
