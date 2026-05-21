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
package org.apache.calcite.adapter.govdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Repairs FEC Iceberg tables on R2: converts year VARCHAR→INTEGER and
 * date columns VARCHAR→DATE without re-downloading from FEC APIs.
 *
 * <p>Run via: {@code ./gradlew :govdata:repairFec}
 * <br>Specific tables: {@code ./gradlew :govdata:repairFec -Ptables=candidates,committees}
 */
public class FecDataRepair {

  private static final String BUCKET      = "govdata-parquet-v1";
  private static final String FEC_PREFIX  = "fec/";
  private static final String TEMP_PREFIX = "fec-repair-temp/";

  // NULL-safe date format templates; COL is replaced with the actual column name.
  private static final String DUAL =
      "CASE WHEN COL IS NULL OR TRIM(COL) = '' THEN NULL "
      + "WHEN COL LIKE '%/%' THEN TRY_STRPTIME(COL, '%m/%d/%Y')::DATE "
      + "ELSE TRY_STRPTIME(LPAD(COL, 8, '0'), '%m%d%Y')::DATE END";
  private static final String SLASH =
      "CASE WHEN COL IS NULL OR TRIM(COL) = '' THEN NULL "
      + "ELSE TRY_STRPTIME(COL, '%m/%d/%Y')::DATE END";
  private static final String MON_DD_YY =
      "CASE WHEN COL IS NULL OR TRIM(COL) = '' THEN NULL "
      + "ELSE TRY_STRPTIME(COL, '%d-%b-%y')::DATE END";
  private static final String YYYYMMDD =
      "CASE WHEN COL IS NULL OR TRIM(COL) = '' THEN NULL "
      + "ELSE TRY_STRPTIME(COL, '%Y%m%d')::DATE END";
  private static final String MMDDYYYY =
      "CASE WHEN COL IS NULL OR TRIM(COL) = '' THEN NULL "
      + "ELSE TRY_STRPTIME(LPAD(COL, 8, '0'), '%m%d%Y')::DATE END";

  // Per-table: date columns → format template key.
  // All tables also get year VARCHAR→INTEGER regardless of this map.
  private static final Map<String, Map<String, String>> SPECS = new LinkedHashMap<>();
  static {
    SPECS.put("candidates",                   Collections.<String, String>emptyMap());
    SPECS.put("committees",                   Collections.<String, String>emptyMap());
    SPECS.put("candidate_committee_linkages", Collections.<String, String>emptyMap());
    SPECS.put("individual_contributions",     map("transaction_date",  DUAL));
    SPECS.put("committee_contributions",      map("transaction_date",  MMDDYYYY));
    SPECS.put("operating_expenditures",       map("transaction_date",  SLASH));
    SPECS.put("independent_expenditures",     map("transaction_date",  MON_DD_YY));
    SPECS.put("intercommittee_transactions",  map("transaction_date",  MMDDYYYY));
    SPECS.put("electioneering_communications",
        map("disbursement_date", MON_DD_YY, "communication_date", MON_DD_YY));
    SPECS.put("candidate_summaries",          map("coverage_end_date", SLASH));
    SPECS.put("committee_summaries",          map("coverage_end_date", YYYYMMDD));
  }

  public static void main(String[] args) throws Exception {
    Map<String, String> creds = R2CredentialProvider.resolve();
    String accessKey = creds.get("accessKeyId");
    String secretKey = creds.get("secretAccessKey");
    String endpoint  = creds.get("endpoint");

    AmazonS3      s3          = buildS3Client(accessKey, secretKey, endpoint);
    Configuration hadoopConf  = buildHadoopConf(accessKey, secretKey, endpoint);
    HadoopTables  hadoopTables = new HadoopTables(hadoopConf);

    Set<String> tables = (args.length > 0)
        ? new LinkedHashSet<String>(Arrays.asList(args))
        : SPECS.keySet();

    for (String table : tables) {
      if (!SPECS.containsKey(table)) {
        System.err.println("Unknown table: " + table + " — skipping");
        continue;
      }
      System.out.println("\n=== fec." + table + " ===");
      try {
        repair(table, SPECS.get(table), s3, hadoopConf, hadoopTables,
            accessKey, secretKey, endpoint);
        System.out.println("  DONE");
      } catch (Exception e) {
        System.err.println("  FAILED: " + e.getMessage());
        e.printStackTrace();
      }
    }
  }

  // ── Core repair logic ────────────────────────────────────────────────────────

  private static void repair(String table, Map<String, String> dateCols,
      AmazonS3 s3, Configuration hadoopConf, HadoopTables hadoopTables,
      String accessKey, String secretKey, String endpoint) throws Exception {

    String s3aLoc  = "s3a://"  + BUCKET + "/" + FEC_PREFIX + table;
    String s3Src   = "s3://"   + BUCKET + "/" + FEC_PREFIX + table;
    String tempDir = "s3://"   + BUCKET + "/" + TEMP_PREFIX + table + "/data";
    String tempKey = TEMP_PREFIX + table + "/data/";

    // 1. Read existing schema + partition spec
    System.out.println("  Loading schema...");
    Table existing = hadoopTables.load(s3aLoc);
    Schema oldSchema = existing.schema();
    boolean yearPartitioned = existing.spec().fields().stream()
        .anyMatch(pf -> oldSchema.findField(pf.sourceId()).name().equals("year"));

    // 2. Write corrected Parquet to temp S3 via DuckDB CLI
    System.out.println("  Transforming → temp...");
    String sql = buildSql(s3Src, tempDir, oldSchema, dateCols,
                           yearPartitioned, accessKey, secretKey, endpoint);
    execDuckDb(sql);

    // 3. List temp Parquet files
    List<S3ObjectSummary> tempFiles = listObjects(s3, BUCKET, tempKey)
        .stream().filter(o -> o.getKey().endsWith(".parquet"))
        .collect(Collectors.<S3ObjectSummary>toList());
    System.out.println("  " + tempFiles.size() + " Parquet files written");

    // 4. Build corrected schema + partition spec
    Schema newSchema = buildSchema(oldSchema, dateCols);
    PartitionSpec newSpec = yearPartitioned
        ? PartitionSpec.builderFor(newSchema).identity("year").build()
        : PartitionSpec.unpartitioned();

    // 5. Delete old Iceberg table from S3
    System.out.println("  Deleting old table...");
    deletePrefix(s3, BUCKET, FEC_PREFIX + table + "/");

    // 6. Create new empty Iceberg table with correct schema
    System.out.println("  Creating new Iceberg table...");
    Map<String, String> props = new HashMap<String, String>(existing.properties());
    Table newTable = hadoopTables.create(newSchema, newSpec, props, s3aLoc);

    // 7. Copy temp files → table data dir and register with Iceberg
    System.out.println("  Copying files and registering snapshot...");
    AppendFiles append = newTable.newAppend();
    for (S3ObjectSummary obj : tempFiles) {
      String srcKey  = obj.getKey();
      String relPath = srcKey.substring(tempKey.length()); // e.g. "year=2024/file.parquet"
      String destKey = FEC_PREFIX + table + "/data/" + relPath;

      // Read row count from temp file before copying (same content either way)
      long rows = readRowCount(hadoopConf, new Path("s3a://" + BUCKET + "/" + srcKey));
      long size = obj.getSize();

      // Server-side S3 copy (no data transfer through local machine)
      s3.copyObject(BUCKET, srcKey, BUCKET, destKey);

      String destS3a = "s3a://" + BUCKET + "/" + destKey;
      append.appendFile(buildDataFile(destS3a, size, rows, newSpec, yearPartitioned, relPath));
    }
    append.commit();
    System.out.println("  Snapshot committed.");

    // 8. Delete temp files
    System.out.println("  Cleaning up temp...");
    deletePrefix(s3, BUCKET, TEMP_PREFIX + table + "/");
  }

  // ── SQL builder ──────────────────────────────────────────────────────────────

  private static String buildSql(String src, String dest,
      Schema schema, Map<String, String> dateCols, boolean partByYear,
      String ak, String sk, String ep) {

    List<String> selects = new ArrayList<String>();
    List<String> excludes = new ArrayList<String>();

    // year → INTEGER
    if (schema.findField("year") != null) {
      selects.add("CAST(year AS INTEGER) AS year");
      excludes.add("year");
    }

    // date columns → DATE
    for (Map.Entry<String, String> e : dateCols.entrySet()) {
      String col  = e.getKey();
      String expr = e.getValue().replace("COL", col);
      selects.add(expr + " AS " + col);
      excludes.add(col);
    }

    // remaining columns pass through
    if (!excludes.isEmpty()) {
      selects.add("* EXCLUDE (" + join(excludes) + ")");
    } else {
      selects.add("*");
    }

    String host = ep.replaceFirst("https?://", "");
    StringBuilder sb = new StringBuilder();
    sb.append("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;");
    sb.append("SET s3_access_key_id='").append(ak).append("';");
    sb.append("SET s3_secret_access_key='").append(sk).append("';");
    sb.append("SET s3_endpoint='").append(host).append("';");
    sb.append("SET s3_url_style='path';");
    sb.append("COPY (SELECT ").append(join(selects));
    sb.append(" FROM iceberg_scan('").append(src).append("'))");
    sb.append(" TO '").append(dest).append("'");
    sb.append(" (FORMAT PARQUET");
    if (partByYear) {
      sb.append(", PARTITION_BY (year)");
    }
    sb.append(");");
    return sb.toString();
  }

  // ── Iceberg helpers ──────────────────────────────────────────────────────────

  private static Schema buildSchema(Schema old, Map<String, String> dateCols) {
    Set<String> dateColNames = dateCols.keySet();
    List<Types.NestedField> fields = new ArrayList<Types.NestedField>();
    for (Types.NestedField f : old.columns()) {
      if ("year".equals(f.name())) {
        fields.add(f.isRequired()
            ? Types.NestedField.required(f.fieldId(), "year", Types.IntegerType.get(), f.doc())
            : Types.NestedField.optional(f.fieldId(), "year", Types.IntegerType.get(), f.doc()));
      } else if (dateColNames.contains(f.name())) {
        fields.add(f.isRequired()
            ? Types.NestedField.required(f.fieldId(), f.name(), Types.DateType.get(), f.doc())
            : Types.NestedField.optional(f.fieldId(), f.name(), Types.DateType.get(), f.doc()));
      } else {
        fields.add(f);
      }
    }
    return new Schema(fields);
  }

  private static DataFile buildDataFile(String s3aPath, long size, long rows,
      PartitionSpec spec, boolean partitioned, String relPath) {
    DataFiles.Builder b = DataFiles.builder(spec)
        .withPath(s3aPath)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(size)
        .withRecordCount(rows);
    if (partitioned && relPath.contains("/")) {
      // relPath = "year=2024/data_0.parquet" → partitionPath = "year=2024"
      b.withPartitionPath(relPath.substring(0, relPath.lastIndexOf('/')));
    }
    return b.build();
  }

  @SuppressWarnings("deprecation")
  private static long readRowCount(Configuration conf, Path path) throws Exception {
    ParquetMetadata footer = ParquetFileReader.readFooter(
        conf, path, ParquetMetadataConverter.NO_FILTER);
    long total = 0;
    for (org.apache.parquet.hadoop.metadata.BlockMetaData block : footer.getBlocks()) {
      total += block.getRowCount();
    }
    return total;
  }

  // ── S3 helpers ───────────────────────────────────────────────────────────────

  private static List<S3ObjectSummary> listObjects(AmazonS3 s3, String bucket, String prefix) {
    List<S3ObjectSummary> result = new ArrayList<S3ObjectSummary>();
    ListObjectsV2Request req = new ListObjectsV2Request()
        .withBucketName(bucket).withPrefix(prefix);
    ListObjectsV2Result resp;
    do {
      resp = s3.listObjectsV2(req);
      result.addAll(resp.getObjectSummaries());
      req.setContinuationToken(resp.getNextContinuationToken());
    } while (resp.isTruncated());
    return result;
  }

  private static void deletePrefix(AmazonS3 s3, String bucket, String prefix) {
    List<S3ObjectSummary> objects = listObjects(s3, bucket, prefix);
    for (S3ObjectSummary obj : objects) {
      s3.deleteObject(new DeleteObjectRequest(bucket, obj.getKey()));
    }
    System.out.println("    Deleted " + objects.size() + " objects from " + prefix);
  }

  private static AmazonS3 buildS3Client(String ak, String sk, String endpoint) {
    return AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(endpoint, "auto"))
        .withCredentials(
            new AWSStaticCredentialsProvider(new BasicAWSCredentials(ak, sk)))
        .withPathStyleAccessEnabled(true)
        .build();
  }

  private static Configuration buildHadoopConf(String ak, String sk, String endpoint) {
    Configuration conf = new Configuration();
    conf.setClassLoader(FecDataRepair.class.getClassLoader());
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    conf.set("fs.s3a.access.key", ak);
    conf.set("fs.s3a.secret.key", sk);
    conf.set("fs.s3a.endpoint", endpoint.replaceFirst("https?://", ""));
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.connection.ssl.enabled", "true");
    return conf;
  }

  // ── DuckDB subprocess ────────────────────────────────────────────────────────

  private static void execDuckDb(String sql) throws Exception {
    ProcessBuilder pb = new ProcessBuilder("duckdb", "-c", sql);
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = new BufferedReader(new InputStreamReader(p.getInputStream()))
        .lines().collect(Collectors.joining("\n"));
    if (!p.waitFor(90, TimeUnit.MINUTES)) {
      p.destroyForcibly();
      throw new RuntimeException("DuckDB timed out after 90 min");
    }
    if (p.exitValue() != 0) {
      throw new RuntimeException("DuckDB failed:\n" + out);
    }
    if (!out.isEmpty()) {
      System.out.println("    DuckDB: " + out.trim());
    }
  }

  // ── Utilities ────────────────────────────────────────────────────────────────

  private static String join(List<String> parts) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < parts.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append(parts.get(i));
    }
    return sb.toString();
  }

  private static Map<String, String> map(String... pairs) {
    Map<String, String> m = new LinkedHashMap<String, String>();
    for (int i = 0; i + 1 < pairs.length; i += 2) {
      m.put(pairs[i], pairs[i + 1]);
    }
    return m;
  }
}
