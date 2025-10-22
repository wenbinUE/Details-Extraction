function extractData() { // Production Usage
  try {
    var cloudFlareTunnelURI = "https://migrator.unienrol.com/";
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Uni Details");
    var uniId = sheet.getRange("C4").getValue();
    var spreadsheetId = SpreadsheetApp.getActiveSpreadsheet().getId();
    var apiUrl = 
    cloudFlareTunnelURI 
    + "/extract"
    + "?university_id=" + encodeURIComponent(uniId)
    + "&spreadsheet_id=" + encodeURIComponent(spreadsheetId);
    var response = UrlFetchApp.fetch(apiUrl);
    Logger.log(response.getResponseCode());
    Logger.log(response.getContentText());
    SpreadsheetApp.getUi().alert();
  } catch (e) {
    Logger.log("Error: " + e.toString());
  }
}

function extractData_2() { // Local Usage
  try {
    var cloudFlareTunnelURI = "https://albany-contracts-harrison-santa.trycloudflare.com";
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Uni Details");
    var uniId = sheet.getRange("C4").getValue();
    var spreadsheetId = SpreadsheetApp.getActiveSpreadsheet().getId();
    var apiUrl = 
    cloudFlareTunnelURI 
    + "/extract"
    + "?university_id=" + encodeURIComponent(uniId)
    + "&spreadsheet_id=" + encodeURIComponent(spreadsheetId);
    var response = UrlFetchApp.fetch(apiUrl);
    Logger.log(response.getResponseCode());
    Logger.log(response.getContentText());
    SpreadsheetApp.getUi().alert();
  } catch (e) {
    Logger.log("Error: " + e.toString());
  }
}

function triggerSendMessage() { // Testing Communication
  Logger.log("Deploy connection successful");
}