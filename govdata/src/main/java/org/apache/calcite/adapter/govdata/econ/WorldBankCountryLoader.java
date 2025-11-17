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
package org.apache.calcite.adapter.govdata.econ;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WorldBankCountryLoader {
    private static final String COUNTRIES_FILE = "/worldbank/worldbank-countries.json";
    private static Config config;

    static {
        loadCountries();
    }

    private static void loadCountries() {
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream is = WorldBankCountryLoader.class.getResourceAsStream(COUNTRIES_FILE)) {
            if (is == null) {
                throw new RuntimeException("Could not find " + COUNTRIES_FILE);
            }
            config = mapper.readValue(is, Config.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load World Bank countries", e);
        }
    }

    public static List<String> getG7() {
        return config.getCountryGroups().get("G7").getCountries();
    }

    public static List<String> getG20() {
        return config.getCountryGroups().get("G20").getCountries();
    }

    public static List<String> getMajorEconomies() {
        return config.getCountryGroups().get("MAJOR_ECONOMIES").getCountries();
    }

    public static List<String> getRegionalAggregates() {
        return config.getCountryGroups().get("REGIONAL_AGGREGATES").getRegions()
            .stream()
            .map(Config.RegionInfo::getCode)
            .collect(Collectors.toList());
    }

    public static List<String> getIncomeLevels() {
        return config.getCountryGroups().get("INCOME_LEVELS").getLevels()
            .stream()
            .map(Config.LevelInfo::getCode)
            .collect(Collectors.toList());
    }

    public static List<String> getAdditionalCountries() {
        Config.CountryGroup additionalGroup = config.getCountryGroups().get("ADDITIONAL_COUNTRIES");
        if (additionalGroup.getCountries() != null) {
            // If it's stored as a simple list of country codes
            return additionalGroup.getCountries();
        } else {
            // If we need to handle it differently, return empty list
            return new ArrayList<>();
        }
    }

    public static String getWorld() {
        return config.getSpecialCodes().get("WORLD").getCode();
    }

    public static List<String> getAllCountries() {
        List<String> all = new ArrayList<>();
        Config.AllCountriesComposition composition = config.getAllCountriesComposition();

        for (String groupName : composition.getIncludeGroups()) {
            switch (groupName) {
                case "G20":
                    all.addAll(getG20());
                    break;
                case "REGIONAL_AGGREGATES":
                    all.addAll(getRegionalAggregates());
                    break;
                case "INCOME_LEVELS":
                    all.addAll(getIncomeLevels());
                    break;
                case "ADDITIONAL_COUNTRIES":
                    all.addAll(getAdditionalCountries());
                    break;
            }
        }

        for (String specialCode : composition.getIncludeSpecial()) {
            if ("WORLD".equals(specialCode)) {
                all.add(getWorld());
            }
        }

        return all;
    }

    // Config as nested class
    private static class Config {
        @JsonProperty("countryGroups")
        private Map<String, CountryGroup> countryGroups;

        @JsonProperty("specialCodes")
        private Map<String, CountryInfo> specialCodes;

        @JsonProperty("allCountriesComposition")
        private AllCountriesComposition allCountriesComposition;

        // Getters and setters
        public Map<String, CountryGroup> getCountryGroups() { return countryGroups; }
        public void setCountryGroups(Map<String, CountryGroup> countryGroups) {
            this.countryGroups = countryGroups;
        }

        public Map<String, CountryInfo> getSpecialCodes() { return specialCodes; }
        public void setSpecialCodes(Map<String, CountryInfo> specialCodes) {
            this.specialCodes = specialCodes;
        }

        public AllCountriesComposition getAllCountriesComposition() { return allCountriesComposition; }
        public void setAllCountriesComposition(AllCountriesComposition composition) {
            this.allCountriesComposition = composition;
        }

        public static class CountryGroup {
            private String name;
            private String description;
            private List<String> countries;
            private List<RegionInfo> regions;
            private List<LevelInfo> levels;
            private List<CountryInfo> countryInfos;

            // Getters and setters
            public String getName() { return name; }
            public void setName(String name) { this.name = name; }

            public String getDescription() { return description; }
            public void setDescription(String description) { this.description = description; }

            public List<String> getCountries() { return countries; }
            public void setCountries(List<String> countries) { this.countries = countries; }

            public List<RegionInfo> getRegions() { return regions; }
            public void setRegions(List<RegionInfo> regions) { this.regions = regions; }

            public List<LevelInfo> getLevels() { return levels; }
            public void setLevels(List<LevelInfo> levels) { this.levels = levels; }

            public List<CountryInfo> getCountryInfos() { return countryInfos; }
            public void setCountryInfos(List<CountryInfo> countryInfos) { this.countryInfos = countryInfos; }
        }

        public static class CountryInfo {
            private String code;
            private String name;
            private String description;

            // Getters and setters
            public String getCode() { return code; }
            public void setCode(String code) { this.code = code; }

            public String getName() { return name; }
            public void setName(String name) { this.name = name; }

            public String getDescription() { return description; }
            public void setDescription(String description) { this.description = description; }
        }

        public static class RegionInfo {
            private String code;
            private String name;

            // Getters and setters
            public String getCode() { return code; }
            public void setCode(String code) { this.code = code; }

            public String getName() { return name; }
            public void setName(String name) { this.name = name; }
        }

        public static class LevelInfo {
            private String code;
            private String name;

            // Getters and setters
            public String getCode() { return code; }
            public void setCode(String code) { this.code = code; }

            public String getName() { return name; }
            public void setName(String name) { this.name = name; }
        }

        public static class AllCountriesComposition {
            private String description;
            private List<String> includeGroups;
            private List<String> includeSpecial;

            // Getters and setters
            public String getDescription() { return description; }
            public void setDescription(String description) { this.description = description; }

            public List<String> getIncludeGroups() { return includeGroups; }
            public void setIncludeGroups(List<String> includeGroups) { this.includeGroups = includeGroups; }

            public List<String> getIncludeSpecial() { return includeSpecial; }
            public void setIncludeSpecial(List<String> includeSpecial) { this.includeSpecial = includeSpecial; }
        }
    }
}
