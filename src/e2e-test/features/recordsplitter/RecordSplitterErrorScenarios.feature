@RecordSplitter
Feature: RecordSplitter Transform - Verify RecordSplitter plugin error Scenarios

  Scenario:Verify RecordSplitter plugin validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Record Splitter" from the plugins list as: "Transform"
    Then Navigate to the properties page of plugin: "RecordSplitter"
    Then Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | fieldToSplit   |
      | delimiter      |
      | outputField    |

  Scenario:Verify RecordSplitter plugin validation errors for mandatory output schema
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Record Splitter" from the plugins list as: "Transform"
    Then Navigate to the properties page of plugin: "RecordSplitter"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageOutputSchema" on the header

  Scenario:Verify RecordSplitter plugin validation errors for invalid output schema
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Record Splitter" from the plugins list as: "Transform"
    Then Navigate to the properties page of plugin: "RecordSplitter"
    Then Enter input plugin property: "fieldToSplit" with value: "recordSplitterFieldToSplit"
    Then Enter input plugin property: "delimiter" with value: "delimiter"
    Then Enter input plugin property: "outputField" with value: "recordSplitterOutputField"
    Then Enter Record Splitter plugin outputSchema "recordSplitterInvalidOutputSchema"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidOutputSchema" on the header

  @BQ_SOURCE_DELIMITED_TEST
  Scenario:Verify RecordSplitter plugin validation errors for incorrect data in Field to Split
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Record Splitter" from the plugins list as: "Transform"
    Then Connect plugins: "BigQuery" and "RecordSplitter" to establish connection
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
    Then Enter input plugin property: "fieldToSplit" with value: "recordSplitterInvalidFieldToSplit"
    Then Enter input plugin property: "delimiter" with value: "delimiter"
    Then Enter input plugin property: "outputField" with value: "recordSplitterOutputField"
    Then Enter Record Splitter plugin outputSchema "recordSplitterValidOutputSchema"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "fieldToSplit" is displaying an in-line error message: "errorMessageInvalidFieldToSplit"
