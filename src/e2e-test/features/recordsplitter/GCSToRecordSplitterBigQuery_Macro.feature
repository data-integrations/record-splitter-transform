@RecordSplitter
Feature: RecordSplitter transform - Verify GCS to BigQuery sink plugin data transfer scenarios using RecordSplitter transform

  @GCS_CSV_DELIMITED_TEST @BQ_SINK_TEST @PLUGIN-1277
  Scenario: To verify data is getting transferred from GCS source to BigQuery sink as macro arguments with record splitter
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Record Splitter" from the plugins list as: "Transform"
    Then Connect plugins: "GCS" and "RecordSplitter" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "RecordSplitter" and "BigQuery" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Enter input plugin property: "referenceName" with value: "GCSReferenceName"
    Then Click on the Macro button of Property: "serviceAccountType" and set the value to: "serviceAccountType"
    Then Click on the Macro button of Property: "serviceFilePath" and set the value to: "serviceAccount"
    Then Click on the Macro button of Property: "serviceAccountJSON" and set the value to: "serviceAccount"
    Then Click on the Macro button of Property: "path" and set the value to: "GCSSourcePath"
    Then Click on the Macro button of Property: "formatMacroInput" and set the value to: "GCSSourceFormat"
    Then Click on the Macro button of Property: "skipHeaderMacroInput" and set the value to: "GCSSourceSkipHeader"
    Then Click on the Macro button of Property: "enableQuotedValuesMacroInput" and set the value to: "GCSSourceEnableQuotedValues"
    Then Select Macro action of output schema property: "outputSchemaMacroInput" and set the value to "GCSSourceOutputSchema"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "RecordSplitter"
    Then Click on the Macro button of Property: "fieldToSplit" and set the value to: "RecordSplitterFieldName"
    Then Click on the Macro button of Property: "delimiter" and set the value to: "RecordSplitterDelimiter"
    Then Click on the Macro button of Property: "outputField" and set the value to: "RecordSplitterOutputField"
    Then Select Record Splitter plugin output schema action: "clear"
    Then Enter Record Splitter plugin outputSchema "recordSplitterValidOutputSchema"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Macro button of Property: "projectId" and set the value to: "bqProjectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "bqDatasetProjectId"
    Then Click on the Macro button of Property: "serviceAccountType" and set the value to: "serviceAccountType"
    Then Click on the Macro button of Property: "serviceFilePath" and set the value to: "serviceAccount"
    Then Click on the Macro button of Property: "serviceAccountJSON" and set the value to: "serviceAccount"
    Then Click on the Macro button of Property: "dataset" and set the value to: "bqDataset"
    Then Click on the Macro button of Property: "table" and set the value to: "bqTable"
    Then Click on the Macro button of Property: "truncateTableMacroInput" and set the value to: "bqTruncateTable"
    Then Click on the Macro button of Property: "updateTableSchemaMacroInput" and set the value to: "bqUpdateTableSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "gcsSourcePath" for key "GCSSourcePath"
    Then Enter runtime argument value "csvFormat" for key "GCSSourceFormat"
    Then Enter runtime argument value "skipHeaderTrue" for key "GCSSourceSkipHeader"
    Then Enter runtime argument value "enableQuotedValuesTrue" for key "GCSSourceEnableQuotedValues"
    Then Enter runtime argument value "recordSplitterCsvFileOutputSchema" for key "GCSSourceOutputSchema"
    Then Enter runtime argument value "recordSplitterFieldToSplit" for key "RecordSplitterFieldName"
    Then Enter runtime argument value "recordSplitterCloseBracesDelimiter" for key "RecordSplitterDelimiter"
    Then Enter runtime argument value "recordSplitterOutputField" for key "RecordSplitterOutputField"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "bqTargetTable" for key "bqTable"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "bqTruncateTable" for key "bqTruncateTable"
    Then Enter runtime argument value "bqUpdateTableSchema" for key "bqUpdateTableSchema"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "gcsSourcePath" for key "GCSSourcePath"
    Then Enter runtime argument value "csvFormat" for key "GCSSourceFormat"
    Then Enter runtime argument value "skipHeaderTrue" for key "GCSSourceSkipHeader"
    Then Enter runtime argument value "enableQuotedValuesTrue" for key "GCSSourceEnableQuotedValues"
    Then Enter runtime argument value "recordSplitterCsvFileOutputSchema" for key "GCSSourceOutputSchema"
    Then Enter runtime argument value "recordSplitterFieldToSplit" for key "RecordSplitterFieldName"
    Then Enter runtime argument value "recordSplitterCloseBracesDelimiter" for key "RecordSplitterDelimiter"
    Then Enter runtime argument value "recordSplitterOutputField" for key "RecordSplitterOutputField"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "bqTargetTable" for key "bqTable"
    Then Enter runtime argument value "bqTruncateTable" for key "bqTruncateTable"
    Then Enter runtime argument value "bqUpdateTableSchema" for key "bqUpdateTableSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count is equal to records transferred to target BigQuery table
