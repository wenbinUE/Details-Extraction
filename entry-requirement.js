const { MongoClient } = require("mongodb");
const { google } = require("googleapis");
const { ObjectId } = require("mongodb");
const fs = require("fs");
const path = require("path");
const TurndownService = require("turndown");
const turndownService = new TurndownService();

const url = process.env.MONGO_URL; // mongoDB connection URL
const dbName = process.env.DB_NAME; // database name


module.exports = async function extractDetails(uniId, spreadsheetId, sheetname = "ER-Extraction-CWB") {
  MongoClient.connect(
    url,
    { useNewUrlParser: true, useUnifiedTopology: true },
    async (err, client) => {
      if (err) {
        console.error("Connection error:", err);
        return;
      }

      console.log("Entry Requirement now using uni id: (" + uniId + ")");
      console.log("Connected successfully to MongoDB (Entry Requirement)");
      const db = client.db(dbName);
      const coursesCol = db.collection("courses");

      // Path to your Google Service Account credentials
      const credentialsPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
      const credentials = JSON.parse(fs.readFileSync(credentialsPath, "utf8"));

      const auth = new google.auth.JWT({
        email: credentials.client_email,
        key: credentials.private_key,
        scopes: ["https://www.googleapis.com/auth/spreadsheets"],
      });

      // Authorize once
      await auth.authorize();

      try {
        // code cleaning here
        const cursor = coursesCol.find();
        while (await cursor.hasNext()) {
          const doc = await cursor.next();

          let changed = false;

          // Changes data.entry_requirement.qualification from string to ObjectId
          if (doc.data && Array.isArray(doc.data.entry_requirement)) {
            doc.data.entry_requirement = doc.data.entry_requirement.map(function (
              req
            ) {
              if (
                req.qualification &&
                typeof req.qualification === "string" &&
                req.qualification.length === 24
              ) {
                req.qualification = ObjectId(req.qualification);
                changed = true;
              }
              return req;
            });
          }

          // Changes data.entry_requirement.score_method from string to ObjectId
          if (doc.data && Array.isArray(doc.data.entry_requirement)) {
            doc.data.entry_requirement = doc.data.entry_requirement.map(function (
              req
            ) {
              if (
                req.score_method &&
                typeof req.score_method === "string" &&
                req.score_method.length === 24
              ) {
                req.score_method = ObjectId(req.score_method);
                changed = true;
              }
              return req;
            });
          }

          if (changed) {
            await coursesCol.replaceOne({ _id: doc._id }, doc);
          }
        }

        // aggregation code here
        const result = await db
          .collection("courses")
          .aggregate([
            { $match: { university_id: ObjectId(uniId) } }, // matched university
            { $match: { "data.publish": "on" } }, // only published courses

            // open data dictionary, and then entry_requirement array
            { $unwind: { path: "$data", preserveNullAndEmptyArrays: true } },
            {
              $unwind: {
                path: "$data.entry_requirement",
                preserveNullAndEmptyArrays: true,
              },
            },
            // group with subject_qualification collection to get qualification names
            {
              $lookup: {
                from: "subject_qualifications",
                localField: "data.entry_requirement.qualification",
                foreignField: "_id",
                as: "qualification_details",
              },
            },
            // group with scoring_methods collection to get scoring method names
            {
              $lookup: {
                from: "scoring_methods",
                localField: "data.entry_requirement.score_method",
                foreignField: "_id",
                as: "score_method_details",
              },
            },
            {
              $group: {
                _id: "$_id",
                name: { $first: "$name" },
                qualifications: {
                  $push: {
                    scoring_method: "$score_method_details.name",
                    qualification_type: "$qualification_details.name",
                    requirement_summary:
                      "$data.entry_requirement.requirement_summary",
                    score_to_qualify: "$data.entry_requirement.score",
                    remarks: "$data.entry_requirement.remarks",
                    additional_requirement:
                      "$data.entry_requirement.additional_requirement",
                  },
                },
              },
            },
          ])
          .toArray();

        console.log(JSON.stringify(result, null, 2));

        const flattened = [];

        result.forEach((doc) => {
          for (let i = 0; i < doc.qualifications.length; i++) {

            // Changes qualification_type from array to comma-separated string
            let qualificationType = doc.qualifications[i]?.qualification_type;
            if (Array.isArray(qualificationType)) {
              qualificationType = qualificationType.join(", ");
            }

            // Changes scoring_method from array to comma-separated string
            let scoringMethod = doc.qualifications[i]?.scoring_method;
            if (Array.isArray(scoringMethod)) {
              scoringMethod = scoringMethod.join(", ");
            }

            // Changes additional_requirement from HTML to markdown
            let markedDown_additional_requirement = doc.qualifications[i]
              ?.additional_requirement
              ? turndownService.turndown(
                doc.qualifications[i].additional_requirement
              )
              : "";

            flattened.push([
              doc._id || "",
              doc.name || "",
              qualificationType || "",
              scoringMethod || "",
              doc.qualifications[i]?.requirement_summary || "",
              doc.qualifications[i]?.score_to_qualify || "",
              doc.qualifications[i]?.remarks || "",
              markedDown_additional_requirement || "",
            ]);
          }
        });

        //   console.log(flattened);
        await sendToGoogleSheet(flattened, sheetname, spreadsheetId, auth);
        await writeStatusToSheet(spreadsheetId, "Entry-Requirement", "/", auth); // "/" for success, "X" for fail
        await new Promise((resolve) => setTimeout(resolve, 2000));
      } catch (aggErr) {
        console.error("Aggregation error:", aggErr);
        await writeStatusToSheet(spreadsheetId, "Entry-Requirement", "X", auth);
      } finally {
        client.close();
      }
    }
  );
};

async function sendToGoogleSheet(rows, sheetName, spreadsheetId, auth) {
  const headers = [
    "Course ID",
    "Course Name",
    "Qualification Type",
    "Scoring Method",
    "Requirement Summary",
    "Score-to-qualify",
    "Remarks",
    "Additional Requirement (HTMl)",
  ];

  const allRows = [headers, ...rows];

  const sheets = google.sheets({ version: "v4", auth });
  // const spreadsheetId = "1CeeOcN8B2B2qj6oTVu2yYQPP5YFt7QXxeBdOoNFIhKw"; // from your Google Sheet URL
  const range = `'${sheetName}'!A1`; // starting cell

  // Optionally create the sheet if it doesn't exist
  try {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      resource: {
        requests: [
          {
            addSheet: {
              properties: { title: sheetName },
            },
          },
        ],
      },
    });
  } catch (e) {
    // Ignore error if sheet already exists, log others
    if (
      !(
        e.errors &&
        Array.isArray(e.errors) &&
        e.errors.some(err => err.reason === "duplicate")
      ) &&
      !(e.code === 400 && e.message && e.message.includes("already exists"))
    ) {
      console.error("Error adding Status sheet:", e.message || e);
    }
  }

  // Clear the sheet before writing
  await sheets.spreadsheets.values.clear({ spreadsheetId, range });

  // Write headers + data
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range,
    valueInputOption: "RAW",
    resource: { values: allRows },
  });

  // Bold the header row (row 1)
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
            cell: {
              userEnteredFormat: {
                textFormat: { bold: true },
              },
            },
            fields: "userEnteredFormat.textFormat.bold",
          },
        },
      ],
    },
  });

  // Helper to get sheetId by name
  async function getSheetId(sheets, spreadsheetId, sheetName) {
    const res = await sheets.spreadsheets.get({ spreadsheetId });
    const sheet = res.data.sheets.find((s) => s.properties.title === sheetName);
    return sheet.properties.sheetId;
  }

  console.log(`Data pushed to Google Sheet: ${sheetName}`);
}

async function writeStatusToSheet(spreadsheetId, moduleName, status, auth) {
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
    // Ignore error if sheet already exists, log others
    if (
      !(
        e.errors &&
        Array.isArray(e.errors) &&
        e.errors.some(err => err.reason === "duplicate")
      ) &&
      !(e.code === 400 && e.message && e.message.includes("already exists"))
    ) {
      console.error("Error adding Status sheet:", e.message || e);
    }
  }

  // Always append only the status row (no header)
  const values = [
    [moduleName, status],
  ];

  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    resource: { values },
  });

  console.log(`Status for (${moduleName}) appended to Google Sheet: ${status}`);
}
