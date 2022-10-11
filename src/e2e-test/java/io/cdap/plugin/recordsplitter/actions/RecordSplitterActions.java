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
package io.cdap.plugin.recordsplitter.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfSchemaLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.JsonUtils;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.recordsplitter.locators.RecordSplitterLocators;

import java.util.Map;

/**
 * RecordSplitter plugin step actions.
 */
public class RecordSplitterActions {
  static {
    SeleniumHelper.getPropertiesLocators(RecordSplitterLocators.class);
  }

  public static int getTargetRecordsCount() {
    String incount = RecordSplitterLocators.targetRecordsCount.getText();
    return Integer.parseInt(incount.replaceAll(",", ""));
  }

  public static void enterOutputSchema(String jsonOutputSchema) {
    Map<String, String> outputSchema =
      JsonUtils.convertKeyValueJsonArrayToMap(PluginPropertyUtils.pluginProp(jsonOutputSchema));
    int index = 0;
    for (Map.Entry<String, String> entry : outputSchema.entrySet()) {
      ElementHelper.sendKeys(RecordSplitterLocators.outputSchemaFieldName(index), entry.getKey());
      ElementHelper.clickOnElement(RecordSplitterLocators.outputSchemaDataTypeDropdown(index));
      SeleniumDriver.getDriver()
        .findElement(RecordSplitterLocators.outputSchemaDataTypeOption(index, entry.getValue())).click();
      ElementHelper.clickOnElement(RecordSplitterLocators.outputSchemaAddRowButton(index));
      index++;
    }
  }

  public static void selectOutputSchemaAction(String action) {
    ElementHelper.selectDropdownOption(CdfSchemaLocators.schemaActions,
                                       CdfPluginPropertiesLocators.locateDropdownListItem(action));
    WaitHelper.waitForElementToBeHidden(CdfSchemaLocators.schemaActionType(action));
  }
}
