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

package io.cdap.plugin.recordsplitter.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.recordsplitter.actions.RecordSplitterActions;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * RecordSplitter Plugin related step design.
 */
public class RecordSplitterTransform implements CdfHelper {

  @Then("Validate OUT record count is equal to records transferred to target BigQuery table")
  public void validateOUTRecordCountIsEqualToRecordsTransferredToTargetBigQueryTable()
    throws IOException, InterruptedException, IOException {
    int targetBQRecordsCount = BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp("bqTargetTable"));
    BeforeActions.scenario.write("No of Records Transferred to BigQuery:" + targetBQRecordsCount);
    Assert.assertEquals("Out records should match with target BigQuery table records count",
                        CdfPipelineRunAction.getCountDisplayedOnSourcePluginAsRecordsOut(), targetBQRecordsCount);
  }

  @Then("Validate OUT record count of record splitter is equal to IN record count of sink")
  public void validateOUTRecordCountOfRecordSplitterIsEqualToINRecordCountOfSink() {
    Assert.assertEquals(recordOut(), RecordSplitterActions.getTargetRecordsCount());
  }

  @Then("Enter Record Splitter plugin outputSchema {string}")
  public void enterRecordSplitterPluginOutputSchema(String jsonOutputSchema) {
    RecordSplitterActions.enterOutputSchema(jsonOutputSchema);
  }

  @Then("Select Record Splitter plugin output schema action: {string}")
  public void selectRecordSplitterPluginOutputSchemaAction(String action) {
    RecordSplitterActions.selectOutputSchemaAction(action);
  }

  @Then("Validate output records in output folder path {string} is equal to expected output file {string}")
  public void validateOutputRecordsInOutputFolderIsEqualToExpectedOutputFile(String outputFolder
    , String expectedOutputFile) throws IOException {

    List<String> expectedOutput = new ArrayList<>();
    int expectedOutputRecordsCount = 0;
    try (BufferedReader bf1 = Files.newBufferedReader(Paths.get(PluginPropertyUtils.pluginProp(expectedOutputFile)))) {
      String line;
      while ((line = bf1.readLine()) != null) {
        expectedOutput.add(line);
        expectedOutputRecordsCount++;
      }

      List<Path> partFiles = Files.walk(Paths.get(PluginPropertyUtils.pluginProp(outputFolder)))
        .filter(Files::isRegularFile)
        .filter(file -> file.toFile().getName().startsWith("part-r")).collect(Collectors.toList());

      int outputRecordsCount = 0;
      for (Path partFile : partFiles) {
        try (BufferedReader bf = Files.newBufferedReader(partFile.toFile().toPath())) {
          String line1;
          while ((line1 = bf.readLine()) != null) {
            if (!(expectedOutput.contains(line1))) {
              Assert.fail("Output records not equal to expected output");
            }
            outputRecordsCount++;
          }
        }
      }
      Assert.assertEquals("Output records count should be equal to expected output records count"
        , expectedOutputRecordsCount, outputRecordsCount);
    }
  }
}
