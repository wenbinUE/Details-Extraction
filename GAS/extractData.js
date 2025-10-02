function extractData() {
  try {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    var uniId = sheet.getRange("C4").getValue();
    var apiUrl = "https://cross-dress-pontiac-ranges.trycloudflare.com/extract?university_id=" + uniId;
    var response = UrlFetchApp.fetch(apiUrl);
    Logger.log(response.getResponseCode());
    Logger.log(response.getContentText());
  } catch (e) {
    Logger.log("Error: " + e.toString());
  }
}
