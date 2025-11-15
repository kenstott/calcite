package org.apache.calcite.adapter.govdata.econ;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorldBankIndicatorLoader {
    private static final String INDICATORS_FILE = "/worldbank/worldbank-indicators.json";
    private static WorldBankIndicatorConfig config;
    private static Map<String, WorldBankIndicator> indicatorMap;

    static {
        loadIndicators();
    }

    private static void loadIndicators() {
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream is = WorldBankIndicatorLoader.class.getResourceAsStream(INDICATORS_FILE)) {
            if (is == null) {
                throw new RuntimeException("Could not find " + INDICATORS_FILE);
            }
            config = mapper.readValue(is, WorldBankIndicatorConfig.class);

            // Build a map for quick lookup
            indicatorMap = new HashMap<>();
            for (WorldBankIndicatorCategory category : config.getIndicators()) {
                for (WorldBankIndicator indicator : category.getItems()) {
                    indicatorMap.put(indicator.getName(), indicator);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load World Bank indicators", e);
        }
    }

    public static List<String> getAllIndicatorCodes() {
        List<String> codes = new ArrayList<>();
        for (WorldBankIndicatorCategory category : config.getIndicators()) {
            for (WorldBankIndicator indicator : category.getItems()) {
                codes.add(indicator.getCode());
            }
        }
        return codes;
    }

    public static WorldBankIndicator getIndicatorByName(String name) {
        return indicatorMap.get(name);
    }

    public static String getIndicatorCode(String name) {
        WorldBankIndicator indicator = indicatorMap.get(name);
        return indicator != null ? indicator.getCode() : null;
    }
}
