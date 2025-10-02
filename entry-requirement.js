const { MongoClient } = require("mongodb");
const { google } = require("googleapis");
const { ObjectId } = require("mongodb");
const fs = require("fs");
const path = require("path");
const TurndownService = require("turndown");
const turndownService = new TurndownService();

const url = process.env.MONGO_URL; // mongoDB connection URL
const dbName = "production"; // database name


module.exports = async function extractDetails(uniId) {
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
            {
              $group: {
                _id: "$_id",
                name: { $first: "$name" },
                qualifications: {
                  $push: {
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
            let qualificationType = doc.qualifications[i]?.qualification_type;
            if (Array.isArray(qualificationType)) {
              qualificationType = qualificationType.join(", ");
            }
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
              doc.qualifications[i]?.requirement_summary || "",
              doc.qualifications[i]?.score_to_qualify || "",
              doc.qualifications[i]?.remarks || "",
              markedDown_additional_requirement,
            ]);
          }
        });

        //   console.log(flattened);

        await sendToGoogleSheet(flattened, "ER-Extraction");
      } catch (aggErr) {
        console.error("Aggregation error:", aggErr);
      } finally {
        client.close();
      }
    }
  );
};

async function sendToGoogleSheet(rows, sheetName) {
  // Path to your Google Service Account credentials JSON file
  const credentials = JSON.parse(
    fs.readFileSync(
      path.join(
        "C:",
        "Uni_Enrol_Intern",
        "ETL_Project",
        "Details_Extraction",
        "JSON_key",
        "mongouedetailsextration-2b7519da48c4.json"
      ),
      "utf8"
    )
  );

  const auth = new google.auth.JWT({
    email: credentials.client_email,
    key: credentials.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  // Authorize once
  await auth.authorize();

  const headers = [
    "Course ID",
    "Course Name",
    "Qualification Type",
    "Requirement Summary",
    "Score-to-qualify",
    "Remarks",
    "Additional Requirement (HTMl)",
  ];

  const allRows = [headers, ...rows];

  const sheets = google.sheets({ version: "v4", auth });
  const spreadsheetId = "1CeeOcN8B2B2qj6oTVu2yYQPP5YFt7QXxeBdOoNFIhKw"; // from your Google Sheet URL
  const range = `'ER-Extraction'!A1`; // starting cell

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
    // Ignore error if sheet already exists
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
