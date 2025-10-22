const express = require("express");
const app = express();
require("dotenv").config();

app.get("/", (req, res) => {
  res.send("Express server is running!");
});

app.get("/extract", async (req, res) => {
  const uniId = req.query.university_id;
  const spreadsheetId = req.query.spreadsheet_id;
  console.log("Successful extraction for university (id): " + uniId);
  console.log("Successful extraction for (spreadsheet ID): " + spreadsheetId);
  const extractDetails = require("./details-extraction"); // details-extraction.js
  const entryRequirement = require("./entry-requirement"); // entry-requirement.js
  const discSpec = require("./disc-spec"); // disc-spec.js
  const courseFeeNonDegree = require("./course-fee-non-degree"); // course-fee-non-degree.js
  const courseFeeDegree = require("./course-fee-degree"); // course-fee-degree.js
  const courseFeePartnerships = require("./course-fee-partnerships"); // course-fee-partner.js
  await extractDetails(uniId, spreadsheetId);
  await entryRequirement(uniId, spreadsheetId);
  await discSpec(uniId, spreadsheetId);
  await courseFeeNonDegree(uniId, spreadsheetId);
  await courseFeeDegree(uniId, spreadsheetId);
  await courseFeePartnerships(uniId, spreadsheetId);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("Server listening on port " + PORT);
});
