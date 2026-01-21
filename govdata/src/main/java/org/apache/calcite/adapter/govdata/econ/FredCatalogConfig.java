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
package org.apache.calcite.adapter.govdata.econ;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FredCatalogConfig {
    private static final String CONFIG_FILE = "/econ/fred-catalog-config.json";
    private static Config config;

    static {
        loadConfig();
    }

    private static void loadConfig() {
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream is = FredCatalogConfig.class.getResourceAsStream(CONFIG_FILE)) {
            if (is == null) {
                throw new RuntimeException("Could not find " + CONFIG_FILE);
            }
            config = mapper.readValue(is, Config.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load FRED catalog configuration", e);
        }
    }

    // API configuration getters
    public static String getApiBaseUrl() {
        return config.getApi().getBaseUrl();
    }

    public static int getRateLimitRequestsPerMinute() {
        return config.getApi().getRateLimitRequestsPerMinute();
    }

    public static int getMaxResultsPerRequest() {
        return config.getApi().getMaxResultsPerRequest();
    }

    public static int getApiDelayMs() {
        return 60000 / config.getApi().getRateLimitRequestsPerMinute();
    }

    // Major categories getters
    public static List<Integer> getMajorCategoryIds() {
        return config.getMajorCategories().stream()
            .map(Category::getId)
            .collect(Collectors.toList());
    }

    public static List<Category> getMajorCategories() {
        return config.getMajorCategories();
    }

    // Search patterns getters
    public static List<String> getCommonSearchTerms() {
        return config.getSearchPatterns().getCommonTerms();
    }

    // Source detection getters
    public static Map<String, SourceDetector> getSourceDetectors() {
        return config.getSourceDetection();
    }

    // Status detection getters
    public static StatusDetection getStatusDetection() {
        return config.getStatusDetection();
    }

    // Nested configuration classes
    private static class Config {
        private ApiConfig api;
        private List<Category> majorCategories;
        private SearchPatterns searchPatterns;
        private Map<String, SourceDetector> sourceDetection;
        private StatusDetection statusDetection;

        // Getters and setters
        public ApiConfig getApi() { return api; }
        public void setApi(ApiConfig api) { this.api = api; }

        public List<Category> getMajorCategories() { return majorCategories; }
        public void setMajorCategories(List<Category> majorCategories) {
            this.majorCategories = majorCategories;
        }

        public SearchPatterns getSearchPatterns() { return searchPatterns; }
        public void setSearchPatterns(SearchPatterns searchPatterns) {
            this.searchPatterns = searchPatterns;
        }

        public Map<String, SourceDetector> getSourceDetection() { return sourceDetection; }
        public void setSourceDetection(Map<String, SourceDetector> sourceDetection) {
            this.sourceDetection = sourceDetection;
        }

        public StatusDetection getStatusDetection() { return statusDetection; }
        public void setStatusDetection(StatusDetection statusDetection) {
            this.statusDetection = statusDetection;
        }
    }

    public static class ApiConfig {
        private String baseUrl;
        private int rateLimitRequestsPerMinute;
        private int maxResultsPerRequest;
        private int requestTimeoutSeconds;

        // Getters and setters
        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }

        public int getRateLimitRequestsPerMinute() { return rateLimitRequestsPerMinute; }
        public void setRateLimitRequestsPerMinute(int limit) {
            this.rateLimitRequestsPerMinute = limit;
        }

        public int getMaxResultsPerRequest() { return maxResultsPerRequest; }
        public void setMaxResultsPerRequest(int max) { this.maxResultsPerRequest = max; }

        public int getRequestTimeoutSeconds() { return requestTimeoutSeconds; }
        public void setRequestTimeoutSeconds(int timeout) { this.requestTimeoutSeconds = timeout; }
    }

    public static class Category {
        private int id;
        private String name;
        private String description;

        // Getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
    }

    public static class SearchPatterns {
        private List<String> commonTerms;

        public List<String> getCommonTerms() { return commonTerms; }
        public void setCommonTerms(List<String> commonTerms) { this.commonTerms = commonTerms; }
    }

    public static class SourceDetector {
        private String name;
        private List<String> textPatterns;
        private List<String> seriesIdPatterns;

        // Getters and setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public List<String> getTextPatterns() { return textPatterns; }
        public void setTextPatterns(List<String> textPatterns) {
            this.textPatterns = textPatterns;
        }

        public List<String> getSeriesIdPatterns() { return seriesIdPatterns; }
        public void setSeriesIdPatterns(List<String> seriesIdPatterns) {
            this.seriesIdPatterns = seriesIdPatterns;
        }
    }

    public static class StatusDetection {
        private DiscontinuedIndicators discontinuedIndicators;
        private ActiveIndicators activeIndicators;

        // Getters and setters
        public DiscontinuedIndicators getDiscontinuedIndicators() { return discontinuedIndicators; }
        public void setDiscontinuedIndicators(DiscontinuedIndicators discontinued) {
            this.discontinuedIndicators = discontinued;
        }

        public ActiveIndicators getActiveIndicators() { return activeIndicators; }
        public void setActiveIndicators(ActiveIndicators active) {
            this.activeIndicators = active;
        }

        public static class DiscontinuedIndicators {
            private List<String> textPatterns;
            private List<String> titlePatterns;
            private String cutoffDate;
            private int staleDataYears;

            // Getters and setters
            public List<String> getTextPatterns() { return textPatterns; }
            public void setTextPatterns(List<String> textPatterns) {
                this.textPatterns = textPatterns;
            }

            public List<String> getTitlePatterns() { return titlePatterns; }
            public void setTitlePatterns(List<String> titlePatterns) {
                this.titlePatterns = titlePatterns;
            }

            public String getCutoffDate() { return cutoffDate; }
            public void setCutoffDate(String cutoffDate) { this.cutoffDate = cutoffDate; }

            public int getStaleDataYears() { return staleDataYears; }
            public void setStaleDataYears(int years) { this.staleDataYears = years; }
        }

        public static class ActiveIndicators {
            private String observationEndPattern;

            public String getObservationEndPattern() { return observationEndPattern; }
            public void setObservationEndPattern(String pattern) {
                this.observationEndPattern = pattern;
            }
        }
    }
}
