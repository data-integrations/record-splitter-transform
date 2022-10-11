/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.common.stepsdesign;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cucumber.core.api.Scenario;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * GCP test hooks.
 */
public class TestSetupHooks {
  private static boolean firstFileSourceTestFlag = true;
  private static boolean firstFileSinkTestFlag = true;
  private static String fileSinkOutputFolder = StringUtils.EMPTY;
  public static boolean beforeAllFlag = true;

  @Before(order = 1, value = "@FILE_SOURCE_TEST")
  public static void setFileSourceAbsolutePath() {
    if (firstFileSourceTestFlag) {
      PluginPropertyUtils.addPluginProp("recordSplitterGCSCsvFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("recordSplitterGCSCsvFile")).getPath()).toString());
      firstFileSourceTestFlag = false;
    }
  }

  @Before(order = 1, value = "@FILE_SINK_TEST")
  public static void setFileSinkAbsolutePath(Scenario scenario) {
    if (firstFileSinkTestFlag) {
      PluginPropertyUtils.addPluginProp("recordSplitterAtTheRateOutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("recordSplitterAtTheRateOutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("recordSplitterCommaOutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("recordSplitterCommaOutputFile")).getPath()).toString());
      fileSinkOutputFolder = PluginPropertyUtils.pluginProp("filePluginOutputFolder");
      firstFileSinkTestFlag = false;
    }
    PluginPropertyUtils.addPluginProp("filePluginOutputFolder", Paths.get("target/" + fileSinkOutputFolder + "/"
       + (new SimpleDateFormat("yyyyMMdd-HH-mm-ssSSS").format(new Date()))).toAbsolutePath().toString());
  }

  @After(order = 1, value = "@FILE_SINK_TEST")
  public static void deleteFileSinkOutputFolder() throws IOException {
    FileUtils.deleteDirectory(new File(PluginPropertyUtils.pluginProp("filePluginOutputFolder")));
  }

  @Before(order = 1, value = "@BQ_SINK_TEST")
  public static void setTempTargetBQTableName() {
    String bqTargetTableName = "E2E_TARGET_" + UUID.randomUUID().toString().replaceAll("-", "_");
    PluginPropertyUtils.addPluginProp("bqTargetTable", bqTargetTableName);
    BeforeActions.scenario.write("BQ Target table name - " + bqTargetTableName);
  }

  @After(order = 1, value = "@BQ_SINK_TEST")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    String bqTargetTableName = PluginPropertyUtils.pluginProp("bqTargetTable");
    try {
      BigQueryClient.dropBqQuery(bqTargetTableName);
      BeforeActions.scenario.write("BQ Target table - " + bqTargetTableName + " deleted successfully");
      PluginPropertyUtils.removePluginProp("bqTargetTable");
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ Target Table " + bqTargetTableName + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  /**
   * Create BigQuery table with 3 columns (Id - Int, Value - Int, UID - string) containing random testdata.
   * Sample row:
   * Id | Value | UID
   * 22 | 968   | 245308db-6088-4db2-a933-f0eea650846a
   */
  @Before(order = 1, value = "@BQ_SOURCE_TEST")
  public static void createTempSourceBQTable() throws IOException, InterruptedException {
    String bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");
    StringBuilder records = new StringBuilder(StringUtils.EMPTY);
    for (int index = 2; index <= 25; index++) {
      records.append(" (").append(index).append(", ").append((int) (Math.random() * 1000 + 1)).append(", '")
        .append(UUID.randomUUID()).append("'), ");
    }
    BigQueryClient.getSoleQueryResult("create table `test_automation." + bqSourceTable + "` as " +
                                        "SELECT * FROM UNNEST([ " +
                                        " STRUCT(1 AS Id, " + ((int) (Math.random() * 1000 + 1)) + " as Value, " +
                                        "'" + UUID.randomUUID() + "' as UID), " +
                                        records +
                                        "  (26, " + ((int) (Math.random() * 1000 + 1)) + ", " +
                                        "'" + UUID.randomUUID() + "') " +
                                        "])");
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @After(order = 1, value = "@BQ_SOURCE_TEST or @BQ_SOURCE_DELIMITED_TEST")
  public static void deleteTempSourceBQTable() throws IOException, InterruptedException {
    String bqSourceTable = PluginPropertyUtils.pluginProp("bqSourceTable");
    BigQueryClient.dropBqQuery(bqSourceTable);
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " deleted successfully");
    PluginPropertyUtils.removePluginProp("bqSourceTable");
  }

  @Before(order = 1, value = "@BQ_SOURCE_DELIMITED_TEST")
  public static void createSourceBQTableForNormalizeTest() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("recordSplitterCreateBQTableQueryFile"),
                                   PluginPropertyUtils.pluginProp("recordSplitterInsertBQDataQueryFile"));
  }

  private static void createSourceBQTableWithQueries(String bqCreateTableQueryFile, String bqInsertDataQueryFile)
    throws IOException, InterruptedException {
    String bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");

    String createTableQuery = StringUtils.EMPTY;
    try {
      createTableQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
        ("/" + bqCreateTableQueryFile).toURI()))
        , StandardCharsets.UTF_8);
      createTableQuery = createTableQuery.replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
        .replace("TABLE_NAME", bqSourceTable);
    } catch (Exception e) {
      BeforeActions.scenario.write("Exception in reading " + bqCreateTableQueryFile + " - " + e.getMessage());
      Assert.fail("Exception in BigQuery testdata prerequisite setup " +
                    "- error in reading create table query file " + e.getMessage());
    }

    String insertDataQuery = StringUtils.EMPTY;
    try {
      insertDataQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
        ("/" + bqInsertDataQueryFile).toURI()))
        , StandardCharsets.UTF_8);
      insertDataQuery = insertDataQuery.replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
        .replace("TABLE_NAME", bqSourceTable);
    } catch (Exception e) {
      BeforeActions.scenario.write("Exception in reading " + bqInsertDataQueryFile + " - " + e.getMessage());
      Assert.fail("Exception in BigQuery testdata prerequisite setup " +
                    "- error in reading insert data query file " + e.getMessage());
    }
    BigQueryClient.getSoleQueryResult(createTableQuery);
    try {
      BigQueryClient.getSoleQueryResult(insertDataQuery);
    } catch (NoSuchElementException e) {
      // Insert query does not return any record.
      // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException
    }
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
    BeforeActions.scenario.write("BQ Source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 1)
  public static void overrideServiceAccountFilePathIfProvided() {
    if (beforeAllFlag) {
      String serviceAccountType = System.getenv("SERVICE_ACCOUNT_TYPE");
      if (serviceAccountType != null && !serviceAccountType.isEmpty()) {
        if (serviceAccountType.equalsIgnoreCase("FilePath")) {
          PluginPropertyUtils.addPluginProp("serviceAccountType", "filePath");
          String serviceAccountFilePath = System.getenv("SERVICE_ACCOUNT_FILE_PATH");
          if (!(serviceAccountFilePath == null) && !serviceAccountFilePath.equalsIgnoreCase("auto-detect")) {
            PluginPropertyUtils.addPluginProp("serviceAccount", serviceAccountFilePath);
          }
          return;
        }
        if (serviceAccountType.equalsIgnoreCase("JSON")) {
          PluginPropertyUtils.addPluginProp("serviceAccountType", "JSON");
          String serviceAccountJSON = System.getenv("SERVICE_ACCOUNT_JSON").replaceAll("[\r\n]+", " ");
          if (!(serviceAccountJSON == null) && !serviceAccountJSON.equalsIgnoreCase("auto-detect")) {
            PluginPropertyUtils.addPluginProp("serviceAccount", serviceAccountJSON);
          }
          return;
        }
        Assert.fail("ServiceAccount override failed - ServiceAccount type set in environment variable " +
                      "'SERVICE_ACCOUNT_TYPE' with invalid value: '" + serviceAccountType + "'. " +
                      "Value should be either 'FilePath' or 'JSON'");
      }
      beforeAllFlag = false;
    }
  }

  @Before(order = 1, value = "@GCS_CSV_DELIMITED_TEST")
  public static void createBucketWithDelimitedFile() throws IOException, URISyntaxException {
    createGCSBucketWithFile(PluginPropertyUtils.pluginProp("recordSplitterGCSCsvFile"));
  }

  @After(order = 1, value = "@GCS_CSV_DELIMITED_TEST")
  public static void deleteSourceBucketWithFile() {
    deleteGCSBucket(PluginPropertyUtils.pluginProp("gcsSourceBucketName"));
    PluginPropertyUtils.removePluginProp("gcsSourceBucketName");
    PluginPropertyUtils.removePluginProp("gcsSourcePath");
  }

  @Before(order = 1, value = "@GCS_SINK_TEST")
  public static void setTempTargetGCSBucketName() {
    String gcsTargetBucketName = "cdf-e2e-test-" + UUID.randomUUID();
    PluginPropertyUtils.addPluginProp("gcsTargetBucketName", gcsTargetBucketName);
    PluginPropertyUtils.addPluginProp("gcsTargetPath", "gs://" + gcsTargetBucketName);
    BeforeActions.scenario.write("GCS target bucket name - " + gcsTargetBucketName);
  }

  @After(order = 1, value = "@GCS_SINK_TEST")
  public static void deleteTargetBucketWithFile() {
    deleteGCSBucket(PluginPropertyUtils.pluginProp("gcsTargetBucketName"));
    PluginPropertyUtils.removePluginProp("gcsTargetBucketName");
    PluginPropertyUtils.removePluginProp("gcsTargetPath");
  }

  private static String createGCSBucketWithFile(String filePath) throws IOException, URISyntaxException {
    String bucketName = StorageClient.createBucket("cdf-e2e-test-" + UUID.randomUUID()).getName();
    StorageClient.uploadObject(bucketName, filePath, filePath);
    PluginPropertyUtils.addPluginProp("gcsSourceBucketName", bucketName);
    PluginPropertyUtils.addPluginProp("gcsSourcePath", "gs://" + bucketName + "/" + filePath);
    BeforeActions.scenario.write("Created GCS Bucket " + bucketName + " containing " + filePath + " file");
    return bucketName;
  }

  private static void deleteGCSBucket(String bucketName) {
    try {
      for (Blob blob : StorageClient.listObjects(bucketName).iterateAll()) {
        StorageClient.deleteObject(bucketName, blob.getName());
      }
      StorageClient.deleteBucket(bucketName);
      BeforeActions.scenario.write("Deleted GCS Bucket " + bucketName);
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        BeforeActions.scenario.write("GCS Bucket " + bucketName + " does not exist.");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }
}
