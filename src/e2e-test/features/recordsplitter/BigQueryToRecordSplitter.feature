@RecordSplitter
Feature: RecordSplitter transform - Verify BigQuery source data transfer using RecordSplitter transformation

  @BQ_SOURCE_DELIMITED_TEST @BQ_SINK_TEST
  Scenario: To verify data is getting transferred from BigQuery to BigQuery sink plugin successfully with record splitter
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Record Splitter" from the plugins list as: "Transform"
    Then Connect plugins: "BigQuery" and "RecordSplitter" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "RecordSplitter" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "RecordSplitter"
    Then Enter input plugin property: "fieldToSplit" with value: "recordSplitterFieldToSplit"
    Then Enter input plugin property: "delimiter" with value: "recordSplitterSemiColonDelimiter"
    Then Enter input plugin property: "outputField" with value: "recordSplitterOutputField"
    Then Select Record Splitter plugin output schema action: "clear"
    Then Enter Record Splitter plugin outputSchema "recordSplitterValidOutputSchema"
    Then Validate "RecordSplitter" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "truncateTable"
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count is equal to records transferred to target BigQuery table

  @BQ_SOURCE_DELIMITED_TEST @FILE_SINK_TEST
  Scenario: To verify data is getting transferred from BigQuery to File sink plugin successfully with comma separated value
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Record Splitter" from the plugins list as: "Transform"
    Then Connect plugins: "BigQuery" and "RecordSplitter" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "RecordSplitter" and "File" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Capture the generated Output Schema
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "RecordSplitter"
    Then Enter input plugin property: "fieldToSplit" with value: "recordSplitterFieldToSplit"
    Then Enter input plugin property: "delimiter" with value: "recordSplitterCommaDelimiter"
    Then Enter input plugin property: "outputField" with value: "recordSplitterOutputField"
    Then Select Record Splitter plugin output schema action: "clear"
    Then Enter Record Splitter plugin outputSchema "recordSplitterValidOutputSchema"
    Then Validate "RecordSplitter" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "filePluginOutputFolder"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm-ss"
    Then Select dropdown plugin property: "format" with option value: "tsv"
    Then Click plugin property: "writeHeader"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate output records in output folder path "filePluginOutputFolder" is equal to expected output file "recordSplitterCommaOutputFile"

  @BQ_SOURCE_DELIMITED_TEST @FILE_SINK_TEST
  Scenario: To verify data is getting transferred from BigQuery to File sink plugin successfully with at the rate separated value
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Record Splitter" from the plugins list as: "Transform"
    Then Connect plugins: "BigQuery" and "RecordSplitter" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "RecordSplitter" and "File" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Capture the generated Output Schema
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "RecordSplitter"
    Then Enter input plugin property: "fieldToSplit" with value: "recordSplitterFieldToSplit"
    Then Enter input plugin property: "delimiter" with value: "recordSplitterAtTheRateDelimiter"
    Then Enter input plugin property: "outputField" with value: "recordSplitterOutputField"
    Then Select Record Splitter plugin output schema action: "clear"
    Then Enter Record Splitter plugin outputSchema "recordSplitterValidOutputSchema"
    Then Validate "RecordSplitter" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "filePluginOutputFolder"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm-ss"
    Then Select dropdown plugin property: "format" with option value: "tsv"
    Then Click plugin property: "writeHeader"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate output records in output folder path "filePluginOutputFolder" is equal to expected output file "recordSplitterAtTheRateOutputFile"
