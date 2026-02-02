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
package org.apache.calcite.adapter.govdata.etl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Tracks ETL run history for reproducibility and debugging.
 *
 * <p>Manifest JSON structure:
 * <pre>
 * {
 *   "runs": [{
 *     "runId": "20260130-100000-sec-10k-2026",
 *     "model": "sec-10k-2026-all.json",
 *     "started": "2026-01-30T10:00:00Z",
 *     "completed": "2026-01-30T12:30:00Z",
 *     "status": "SUCCESS",
 *     "results": {
 *       "schemasProcessed": 1,
 *       "schemasSucceeded": 1,
 *       "schemasFailed": 0,
 *       "details": {...}
 *     }
 *   }]
 * }
 * </pre>
 */
public class EtlRunManifest {

  private static final ObjectMapper MAPPER = createMapper();
  private static final SimpleDateFormat ISO_FORMAT = createIsoFormat();

  private final File manifestFile;
  private final ManifestData data;

  /**
   * Create or load manifest from file.
   */
  public EtlRunManifest(File manifestFile) throws IOException {
    this.manifestFile = manifestFile;
    if (manifestFile.exists()) {
      this.data = MAPPER.readValue(manifestFile, ManifestData.class);
    } else {
      this.data = new ManifestData();
    }
  }

  private static ObjectMapper createMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    return mapper;
  }

  private static SimpleDateFormat createIsoFormat() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    return sdf;
  }

  /**
   * Start a new run and record it in the manifest.
   *
   * @param modelFileName name of the model file
   * @return the created run entry
   */
  public RunEntry startRun(String modelFileName) {
    String runId = generateRunId(modelFileName);
    String started = formatTimestamp(new Date());

    RunEntry run = new RunEntry();
    run.runId = runId;
    run.model = modelFileName;
    run.started = started;
    run.status = RunStatus.RUNNING;

    data.runs.add(run);
    return run;
  }

  /**
   * Complete a run with results.
   *
   * @param run the run to complete
   * @param status final status
   * @param results result details
   */
  public void completeRun(RunEntry run, RunStatus status, Map<String, Object> results) {
    run.completed = formatTimestamp(new Date());
    run.status = status;
    run.results = results;
  }

  /**
   * Save manifest to file.
   */
  public void save() throws IOException {
    File parent = manifestFile.getParentFile();
    if (parent != null && !parent.exists()) {
      parent.mkdirs();
    }
    MAPPER.writeValue(manifestFile, data);
  }

  /**
   * Get all runs from the manifest.
   */
  public List<RunEntry> getRuns() {
    return new ArrayList<>(data.runs);
  }

  /**
   * Get the most recent run for a given model.
   */
  public RunEntry getLatestRun(String modelFileName) {
    RunEntry latest = null;
    for (RunEntry run : data.runs) {
      if (modelFileName.equals(run.model)) {
        if (latest == null || run.started.compareTo(latest.started) > 0) {
          latest = run;
        }
      }
    }
    return latest;
  }

  private String generateRunId(String modelFileName) {
    SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd-HHmmss");
    fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    String timestamp = fmt.format(new Date());

    // Extract base name without extension
    String baseName = modelFileName;
    int dotIdx = baseName.lastIndexOf('.');
    if (dotIdx > 0) {
      baseName = baseName.substring(0, dotIdx);
    }
    // Remove path components
    int slashIdx = baseName.lastIndexOf('/');
    if (slashIdx >= 0) {
      baseName = baseName.substring(slashIdx + 1);
    }

    return timestamp + "-" + baseName;
  }

  private String formatTimestamp(Date date) {
    synchronized (ISO_FORMAT) {
      return ISO_FORMAT.format(date);
    }
  }

  /**
   * Run status enumeration.
   */
  public enum RunStatus {
    RUNNING,
    SUCCESS,
    PARTIAL,
    FAILED
  }

  /**
   * Single run entry in the manifest.
   */
  public static class RunEntry {
    public String runId;
    public String model;
    public String started;
    public String completed;
    public RunStatus status;
    public Map<String, Object> results;

    // For Jackson deserialization
    public RunEntry() {
    }

    /**
     * Create results map with standard fields.
     */
    public static Map<String, Object> createResults(int schemasProcessed,
        int schemasSucceeded, int schemasFailed) {
      Map<String, Object> results = new HashMap<>();
      results.put("schemasProcessed", schemasProcessed);
      results.put("schemasSucceeded", schemasSucceeded);
      results.put("schemasFailed", schemasFailed);
      return results;
    }

    /**
     * Add detail to results.
     */
    public static void addDetail(Map<String, Object> results, String key, Object value) {
      @SuppressWarnings("unchecked")
      Map<String, Object> details = (Map<String, Object>) results.get("details");
      if (details == null) {
        details = new HashMap<>();
        results.put("details", details);
      }
      details.put(key, value);
    }
  }

  /**
   * Root manifest data structure.
   */
  public static class ManifestData {
    public List<RunEntry> runs = new ArrayList<>();
  }
}
