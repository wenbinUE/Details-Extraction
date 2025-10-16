function extractData() {
  try {
    var cloudFlareTunnelURI = "https://puzzle-tin-vegetables-typically.trycloudflare.com";
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

function triggerSendMessage() {
  Logger.log("Deploy connection successful");
}