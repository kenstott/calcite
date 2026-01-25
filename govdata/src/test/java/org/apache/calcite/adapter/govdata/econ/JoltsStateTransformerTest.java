/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for JoltsStateTransformer.
 */
@Tag("unit")
public class JoltsStateTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void testTransformOutputsCorrectFieldNames() throws Exception {
    // Simulate raw BLS TSV data (after JSON conversion)
    String rawData = "[{\"series_id\":\"JTS010000000000000JOL\",\"year\":\"2020\",\"period\":\"M01\",\"value\":\"1234.5\"}]";
    
    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("year", "2020");
    
    RequestContext context = RequestContext.builder()
        .url("https://download.bls.gov/pub/time.series/jt/jt.data.2.JobOpenings")
        .dimensionValues(dimensions)
        .build();
    
    JoltsStateTransformer transformer = new JoltsStateTransformer();
    String result = transformer.transform(rawData, context);
    
    JsonNode resultArray = MAPPER.readTree(result);
    assertTrue(resultArray.isArray());
    assertEquals(1, resultArray.size());
    
    JsonNode record = resultArray.get(0);
    
    // Verify field names match what the schema expects (lowercase)
    assertTrue(record.has("series"), "Should have 'series' field (not series_id)");
    assertTrue(record.has("state_fips"), "Should have 'state_fips' field");
    assertTrue(record.has("metric_type"), "Should have 'metric_type' field");
    assertTrue(record.has("date"), "Should have 'date' field");
    assertTrue(record.has("value"), "Should have 'value' field");
    assertTrue(record.has("year"), "Should have 'year' field");
    
    // Verify the values are properly extracted
    assertEquals("JTS010000000000000JOL", record.get("series").asText());
    assertEquals("01", record.get("state_fips").asText());
    assertEquals("2020-01-01", record.get("date").asText());
    assertEquals(1234.5, record.get("value").asDouble(), 0.001);
    assertEquals(2020, record.get("year").asInt());
    
    // metric_type should be derived from series ID
    assertNotNull(record.get("metric_type").asText());
  }
}
