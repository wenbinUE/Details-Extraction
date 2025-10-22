const { google } = require("googleapis");

async function sendToGoogleSheetZZ(
  rows,
  spreadsheetId,
  auth,
  extractionName,
  sheetName = "Course-Fee-Extraction-CWB"
) {
  const headers = [
    "Course ID",
    "University Name",
    "Course Name",
    "Fee Type",
    "Fee Name",
    "Period Start",
    "Period End",
    "Period Duration",
    "Previous Node",
    "Next Node",
    "Period Location",
    "Foreign Campus",
    "Local Fee Currency",
    "Local Fee Amount",
    "Local Fee Description",
    "International Fee Currency",
    "International Fee Amount",
    "International Fee Description",
  ];

  const sheets = google.sheets({ version: "v4", auth });
  const range = `'${sheetName}'!A1`;

  // Create "Course-Fee-Extraction-CWB" sheet if not exists
  try {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      resource: {
        requests: [{ addSheet: { properties: { title: sheetName } } }],
      },
    });
  } catch (e) {
    // "Status" sheet tab crated, no need to log errors
  }

  // Check if "Course-Fee-Extraction-CWB" sheet is empty (no headers)
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${sheetName}!A1:A1`,
  });

  const isEmpty =
    !res.data.values || res.data.values.length === 0 || !res.data.values[0][0];

  // Write headers only if "Course-Fee-Extraction-CWB" sheet is empty
  if (isEmpty) {
    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range,
      valueInputOption: "RAW",
      insertDataOption: "INSERT_ROWS",
      resource: { values: [headers] },
    });
  }

  // Append data rows
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    resource: { values: rows },
  });

  // Bold the header row
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    resource: {
      requests: [
        {
          repeatCell: {
            range: {
              sheetId: await getSheetId(sheets, spreadsheetId, sheetName),
              startRowIndex: 0,
              endRowIndex: 1,
            },
            cell: { userEnteredFormat: { textFormat: { bold: true } } },
            fields: "userEnteredFormat.textFormat.bold",
          },
        },
      ],
    },
  });

  async function getSheetId(sheets, spreadsheetId, sheetName) {
    const res = await sheets.spreadsheets.get({ spreadsheetId });
    const sheet = res.data.sheets.find((s) => s.properties.title === sheetName);
    return sheet.properties.sheetId;
  }

  console.log(`Data pushed to Course-Fee-Extraction-CWB: ${extractionName}`);
}

async function writeStatusToSheetZZ(spreadsheetId, moduleName, status, auth) {
  const headers = ["Extractions", "Status"];

  const sheets = google.sheets({ version: "v4", auth });
  const range = `'Status'!A2:B2`;

  // Create Status sheet if not exists
  try {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      resource: {
        requests: [{ addSheet: { properties: { title: "Status" } } }],
      },
    });
  } catch (e) {
    // "Status" sheet tab crated, no need to log errors
  }

  // Check if "Status" sheet is empty (no headers)
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `Status!A1:B1`,
  });

  const firstRow = res.data.values && res.data.values[0];
  const headersExist =
    firstRow && firstRow[0] === "Extractions" && firstRow[1] === "Status";

  // Write headers only if they don't exist
  if (!headersExist) {
    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: `Status!A1:B1`,
      valueInputOption: "RAW",
      insertDataOption: "INSERT_ROWS",
      resource: { values: [headers] },
    });
  }

  const values = [[moduleName, status]];
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    resource: { values },
  });

  // Bold the header row
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    resource: {
      requests: [
        {
          repeatCell: {
            range: {
              sheetId: await getSheetId(sheets, spreadsheetId, "Status"),
              startRowIndex: 0,
              endRowIndex: 1,
            },
            cell: { userEnteredFormat: { textFormat: { bold: true } } },
            fields: "userEnteredFormat.textFormat.bold",
          },
        },
      ],
    },
  });

  async function getSheetId(sheets, spreadsheetId, sheetName) {
    const res = await sheets.spreadsheets.get({ spreadsheetId });
    const sheet = res.data.sheets.find((s) => s.properties.title === sheetName);
    return sheet.properties.sheetId;
  }

  console.log(`Status for (${moduleName}) appended to Google Sheet: ${status}`);
}

module.exports = { sendToGoogleSheetZZ, writeStatusToSheetZZ };
