function extractData() {
  try {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    var uniId = sheet.getRange("C4").getValue();
    var spreadsheetId = SpreadsheetApp.getActiveSpreadsheet().getId();
    var apiUrl =
      "https://immune-holder-immediate-waiver.trycloudflare.com/extract" +
      "?university_id=" +
      encodeURIComponent(uniId) +
      "&spreadsheet_id=" +
      encodeURIComponent(spreadsheetId);
    var response = UrlFetchApp.fetch(apiUrl);
    Logger.log(response.getResponseCode());
    Logger.log(response.getContentText());
  } catch (e) {
    Logger.log("Error: " + e.toString());
  }
}

function triggerSendMessage() {
  Logger.log("Deploy connection successful");
}
