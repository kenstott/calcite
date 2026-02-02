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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * CPS Voting Supplement table with voter registration and turnout microdata.
 *
 * <p>Provides individual-level voter registration and turnout data from the
 * Current Population Survey (CPS) Voting and Registration Supplement conducted
 * by the U.S. Census Bureau every 2 years (biennial) in November election years.
 *
 * <p>Important limitations:
 * <ul>
 *   <li>Census does NOT collect party affiliation data</li>
 *   <li>Data only available for even years (election years): 2010, 2012, 2014, etc.</li>
 *   <li>Microdata requires survey weights for accurate population estimates</li>
 * </ul>
 */
public class CpsVotingTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CpsVotingTable.class);

  private final CensusApiClient censusClient;
  private final List<Integer> votingYears;

  public CpsVotingTable(CensusApiClient censusClient, List<Integer> votingYears) {
    this.censusClient = censusClient;
    this.votingYears = votingYears != null ? votingYears : new ArrayList<>();
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("person_id", SqlTypeName.VARCHAR)
        .add("election_year", SqlTypeName.INTEGER)
        .add("state_fips", SqlTypeName.VARCHAR)
        .add("age", SqlTypeName.INTEGER)
        .add("age_group", SqlTypeName.VARCHAR)
        .add("race", SqlTypeName.VARCHAR)
        .add("hispanic_origin", SqlTypeName.VARCHAR)
        .add("education_level", SqlTypeName.VARCHAR)
        .add("registration_status", SqlTypeName.VARCHAR)
        .add("voted", SqlTypeName.VARCHAR)
        .add("weight", SqlTypeName.DECIMAL)
        .build();
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        try {
          List<Object[]> records = new ArrayList<>();

          // Fetch data for each election year
          for (int year : votingYears) {
            LOGGER.info("Fetching CPS Voting data for year {}", year);

            // CPS Voting Supplement key variables:
            // PES1 = Voted (1=no, 2=yes)
            // PES2 = Registration status (1=registered, 2=not registered)
            // GESTFIPS = State FIPS code
            // PRTAGE = Age
            // PTDTRACE = Race
            // PEHSPNON = Hispanic origin (1=Hispanic, 2=Not Hispanic)
            // PEEDUCA = Education level (codes 31-46)
            // PWSSWGT = Final person weight (for population estimates)
            // PWSSWGT is the survey weight used for voting supplements

            String variables = "PES1,PES2,GESTFIPS,PRTAGE,PTDTRACE,PEHSPNON,PEEDUCA,PWSSWGT";

            try {
              JsonNode data = censusClient.getCpsVotingData(year, variables);

              if (data.isArray() && data.size() > 1) {
                // First row is headers
                JsonNode headers = data.get(0);

                // Find column indices
                int pes1Idx = findColumnIndex(headers, "PES1");
                int pes2Idx = findColumnIndex(headers, "PES2");
                int stateIdx = findColumnIndex(headers, "GESTFIPS");
                int ageIdx = findColumnIndex(headers, "PRTAGE");
                int raceIdx = findColumnIndex(headers, "PTDTRACE");
                int hispanicIdx = findColumnIndex(headers, "PEHSPNON");
                int educationIdx = findColumnIndex(headers, "PEEDUCA");
                int weightIdx = findColumnIndex(headers, "PWSSWGT");

                // Process each row (skip header at index 0)
                for (int i = 1; i < data.size(); i++) {
                  JsonNode row = data.get(i);
                  if (!row.isArray()) {
                    continue;
                  }

                  try {
                    // Generate person ID (year + row index)
                    String personId = String.format("%d_%06d", year, i);

                    // Extract and decode values
                    int votedCode = getIntValue(row, pes1Idx);
                    int regCode = getIntValue(row, pes2Idx);
                    String stateFips = getStringValue(row, stateIdx);
                    int age = getIntValue(row, ageIdx);
                    int raceCode = getIntValue(row, raceIdx);
                    int hispanicCode = getIntValue(row, hispanicIdx);
                    int educationCode = getIntValue(row, educationIdx);
                    double weight = getDoubleValue(row, weightIdx);

                    // Decode CPS voting codes
                    String voted = decodeVoted(votedCode);
                    String registrationStatus = decodeRegistration(regCode);
                    String ageGroup = categorizeAge(age);
                    String race = decodeRace(raceCode);
                    String hispanicOrigin = decodeHispanic(hispanicCode);
                    String educationLevel = decodeEducation(educationCode);

                    // Create record
                    Object[] record = new Object[] {
                        personId,
                        year,
                        stateFips,
                        age,
                        ageGroup,
                        race,
                        hispanicOrigin,
                        educationLevel,
                        registrationStatus,
                        voted,
                        java.math.BigDecimal.valueOf(weight)
                    };

                    records.add(record);

                  } catch (Exception e) {
                    // Skip malformed records
                    LOGGER.debug("Skipping malformed record at row {}: {}", i, e.getMessage());
                  }
                }
              }

              LOGGER.info("Loaded {} CPS Voting records for year {}", records.size(), year);

            } catch (Exception e) {
              LOGGER.warn("Failed to fetch CPS Voting data for year {}: {}", year, e.getMessage());
              // Continue with other years
            }
          }

          LOGGER.info("Total CPS Voting records loaded: {}", records.size());
          return new CpsVotingEnumerator(records);

        } catch (Exception e) {
          LOGGER.error("Error fetching CPS Voting data", e);
          throw new RuntimeException("Failed to fetch CPS Voting data", e);
        }
      }
    };
  }

  private int findColumnIndex(JsonNode headers, String columnName) {
    for (int i = 0; i < headers.size(); i++) {
      if (headers.get(i).asText().equalsIgnoreCase(columnName)) {
        return i;
      }
    }
    return -1;
  }

  private String getStringValue(JsonNode row, int index) {
    if (index < 0 || index >= row.size()) {
      return null;
    }
    JsonNode node = row.get(index);
    return node.isNull() ? null : node.asText();
  }

  private int getIntValue(JsonNode row, int index) {
    String value = getStringValue(row, index);
    if (value == null || value.isEmpty()) {
      return -1;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private double getDoubleValue(JsonNode row, int index) {
    String value = getStringValue(row, index);
    if (value == null || value.isEmpty()) {
      return 0.0;
    }
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      return 0.0;
    }
  }

  private String decodeVoted(int code) {
    switch (code) {
      case 1: return "No";
      case 2: return "Yes";
      case 96: return "Refused";
      case 97: return "Don't know";
      case 98: return "No response";
      case 99: return "Not in universe";
      default: return "Unknown";
    }
  }

  private String decodeRegistration(int code) {
    switch (code) {
      case 1: return "Registered";
      case 2: return "Not registered";
      case 96: return "Refused";
      case 97: return "Don't know";
      case 98: return "No response";
      case 99: return "Not in universe";
      default: return "Unknown";
    }
  }

  private String categorizeAge(int age) {
    if (age < 18) return "Under 18";
    if (age < 25) return "18-24";
    if (age < 45) return "25-44";
    if (age < 65) return "45-64";
    return "65+";
  }

  private String decodeRace(int code) {
    switch (code) {
      case 1: return "White only";
      case 2: return "Black only";
      case 3: return "American Indian, Alaskan Native only";
      case 4: return "Asian only";
      case 5: return "Hawaiian/Pacific Islander only";
      case 6: return "White-Black";
      case 7: return "White-AI";
      case 8: return "White-Asian";
      case 9: return "White-HP";
      case 10: return "Black-AI";
      case 11: return "Black-Asian";
      case 12: return "Black-HP";
      case 13: return "AI-Asian";
      case 14: return "AI-HP";
      case 15: return "Asian-HP";
      case 16: return "W-B-AI";
      case 17: return "W-B-A";
      case 18: return "W-B-HP";
      case 19: return "W-AI-A";
      case 20: return "W-AI-HP";
      case 21: return "W-A-HP";
      case 22: return "B-AI-A";
      case 23: return "W-B-AI-A";
      case 24: return "W-AI-A-HP";
      case 25: return "Other 3 race combinations";
      case 26: return "Other 4 and 5 race combinations";
      default: return "Unknown";
    }
  }

  private String decodeHispanic(int code) {
    if (code == 1) return "Hispanic";
    if (code == 2) return "Not Hispanic";
    return "Unknown";
  }

  private String decodeEducation(int code) {
    if (code < 31) return "Less than high school";
    if (code < 39) return "High school graduate";
    if (code < 43) return "Some college";
    if (code == 43) return "Bachelor's degree";
    if (code >= 44) return "Advanced degree";
    return "Unknown";
  }

  private static class CpsVotingEnumerator implements Enumerator<Object[]> {
    private final Iterator<Object[]> iterator;
    private Object[] current;

    CpsVotingEnumerator(List<Object[]> records) {
      this.iterator = records.iterator();
    }

    @Override public Object[] current() {
      return current;
    }

    @Override public boolean moveNext() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      }
      return false;
    }

    @Override public void reset() {
      throw new UnsupportedOperationException("Reset not supported");
    }

    @Override public void close() {
      // Nothing to close
    }
  }
}
