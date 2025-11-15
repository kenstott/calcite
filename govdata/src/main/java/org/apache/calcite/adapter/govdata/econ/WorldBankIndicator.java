package org.apache.calcite.adapter.govdata.econ;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class WorldBankIndicator {
    @JsonProperty("code")
    private String code;

    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;

    @JsonProperty("unit")
    private String unit;

    // Getters and setters
    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getUnit() { return unit; }
    public void setUnit(String unit) { this.unit = unit; }
}

class WorldBankIndicatorCategory {
    @JsonProperty("category")
    private String category;

    @JsonProperty("items")
    private List<WorldBankIndicator> items;

    // Getters and setters
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public List<WorldBankIndicator> getItems() { return items; }
    public void setItems(List<WorldBankIndicator> items) { this.items = items; }
}

class WorldBankIndicatorConfig {
    @JsonProperty("indicators")
    private List<WorldBankIndicatorCategory> indicators;

    public List<WorldBankIndicatorCategory> getIndicators() { return indicators; }
    public void setIndicators(List<WorldBankIndicatorCategory> indicators) {
        this.indicators = indicators;
    }
}
