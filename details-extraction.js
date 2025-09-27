const { MongoClient } = require("mongodb");
const { google } = require("googleapis");
const { ObjectId } = require("mongodb");
const fs = require("fs");
const path = require("path");

const url = "mongodb://localhost:27017"; // mongoDB connection URL
const dbName = "production"; // database name

MongoClient.connect(
  url,
  { useNewUrlParser: true, useUnifiedTopology: true },
  async (err, client) => {
    if (err) {
      console.error("Connection error:", err);
      return;
    }

    console.log("Connected successfully to MongoDB");
    const db = client.db(dbName);
    const coursesCol = db.collection("courses");

    try {
      // code cleaning here
      const cursor = coursesCol.find();
      while (await cursor.hasNext()) {
        const doc = await cursor.next();

        let changed = false;

        // Convert level_of_studies array strings to ObjectId
        if (doc.data && Array.isArray(doc.data.level_of_studies)) {
          doc.data.level_of_studies = doc.data.level_of_studies.map(function (
            lvl
          ) {
            if (typeof lvl === "string" && lvl.length === 24)
              return ObjectId(lvl);
            return lvl;
          });
          changed = true;
        }

        // Convert campus_id string to ObjectId
        if (
          doc.campus_id &&
          typeof doc.campus_id === "string" &&
          doc.campus_id.length === 24
        ) {
          doc.campus_id = ObjectId(doc.campus_id);
          changed = true;
        }

        // Convert partner_duration to float
        if (doc.data) {
          let pd = parseFloat(doc.data.partner_duration);
          if (isNaN(pd)) pd = 0;
          if (doc.data.partner_duration !== pd) {
            doc.data.partner_duration = pd;
            changed = true;
          }

          // Convert local_year_fulltime to float
          let lf = parseFloat(doc.data.local_year_fulltime);
          if (isNaN(lf)) lf = 0;
          if (doc.data.local_year_fulltime !== lf) {
            doc.data.local_year_fulltime = lf;
            changed = true;
          }
        }

        // Change university_id from string to ObjectId
        if (
          doc.university_id &&
          typeof doc.university_id === "string" &&
          doc.university_id.length === 24
        ) {
          doc.university_id = ObjectId(doc.university_id);
          changed = true;
        }

        // If any changes were made, save the document back
        if (changed) {
          await coursesCol.replaceOne({ _id: doc._id }, doc);
        }
      }

      // Get the university IDs which their courses need to be published
      const publishedUniIds = await db
        .collection("courses")
        .distinct("university_id", { "data.publish": "on" });

      // Get the list of required universities
      const universities = await db
        .collection("universities")
        .find(
          { _id: { $in: publishedUniIds } },
          { projection: { _id: 1, name: 1 } }
        )
        .limit(5) // limit to 5 uni for testing
        .toArray();

      // aggregation code here
      for (const uni of universities) {
        const result = await db
          .collection("courses")
          .aggregate([
            { $match: { university_id: uni._id } }, // matched university
            { $match: { "data.publish": "on" } }, // only published courses

            { $unwind: { path: "$data", preserveNullAndEmptyArrays: true } }, // open the data dictionary
            {
              $unwind: {
                path: "$data.english_requirement", // open english_requirement array
                preserveNullAndEmptyArrays: true,
              },
            },

            {
              $lookup: {
                from: "levelofstudies",
                localField: "data.level_of_studies", // join levelofstudies collection
                foreignField: "_id",
                as: "level_details",
              },
            },
            {
              $lookup: {
                from: "campuses",
                localField: "campus_id", // join campuses collection
                foreignField: "_id",
                as: "campus_details",
              },
            },
            {
              $lookup: {
                from: "universities",
                localField: "university_id", // join universities collection
                foreignField: "_id",
                as: "university_details",
              },
            },
            {
              $group: {
                _id: "$_id",
                university_id: { $first: "$university_id" },
                university_name: {
                  $first: { $arrayElemAt: ["$university_details.name", 0] },
                },
                name: { $first: "$name" },
                reference_url: { $first: "$reference_url" },
                why_apply: { $first: "$data.why_apply" },
                course_level: {
                  $first: { $arrayElemAt: ["$level_details.name", 0] },
                },
                campus_name: {
                  $first: { $arrayElemAt: ["$campus_details.name", 0] },
                },
                duration: {
                  $first: {
                    $add: [
                      { $ifNull: ["$data.partner_duration", 0] },
                      { $ifNull: ["$data.local_year_fulltime", 0] },
                    ],
                  },
                },
                intakes: {
                  $first: {
                    $reduce: {
                      input: "$data.intakes",
                      initialValue: "",
                      in: {
                        $concat: [
                          "$$value",
                          { $cond: [{ $eq: ["$$value", ""] }, "", ", "] },
                          "$$this",
                        ],
                      },
                    },
                  },
                },
                ptptn_type: { $first: "$data.ptptn_type" },
                english_requirements: {
                  $push: {
                    type: "$data.english_requirement.type",
                    requirement: "$data.english_requirement.requirement",
                  },
                },
              },
            },
          ])
          .toArray();

        //   console.log("Aggregation result:", result);
        console.log(JSON.stringify(result, null, 2));
        // here you can send `result` to Google Sheets, etc.

        const flattened = result.map((doc, idx) => {
          const row = [
            idx === 0 ? doc.university_id : "", // University ID
            idx === 0 ? doc.university_name : "", // University Name
            doc._id, // Course ID
            doc.name, // Name
            doc.reference_url, // Reference URL
            doc.why_apply, // Why Apply (HTML)
            doc.course_level, // Course Level
            doc.campus_name, // Campus (Default)
            doc.duration, // Local Duration (Year) + Partner University Duration
            doc.duration, // Local Duration (Year) + Partner University.Duration (duplicate?)
            doc.intakes, // Intakes
            doc.ptptn_type, // PTPTN Type
          ];

          // English Exam Type & Requirement (1 to 9)

          // Fill up to 9 exam types
          for (let i = 0; i < 9; i++) {
            row.push(doc.english_requirements[i]?.type || "");
            row.push(doc.english_requirements[i]?.requirement || "");
          }

          return row;
        });

        // now send `flattened` to Google Sheets
        await sendToGoogleSheet(flattened, uni.name);
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    } catch (aggErr) {
      console.error("Aggregation error:", aggErr);
    } finally {
      client.close();
    }
  }
);

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
    "University ID",
    "University Name",
    "Course ID",
    "Name",
    "Reference URL",
    "Why Apply (HTML)",
    "Course Level",
    "Campus (Default)",
    "Local Duration (Year) + Partner University Duration",
    "Local Duration (Year) + Partner University Duration",
    "Intakes",
    "PTPTN Type",
    "English Exam Type 1",
    "English Exam Requirement 1",
    "English Exam Type 2",
    "English Exam Requirement 2",
    "English Exam Type 3",
    "English Exam Requirement 3",
    "English Exam Type 4",
    "English Exam Requirement 4",
    "English Exam Type 5",
    "English Exam Requirement 5",
    "English Exam Type 6",
    "English Exam Requirement 6",
    "English Exam Type 7",
    "English Exam Requirement 7",
    "English Exam Type 8",
    "English Exam Requirement 8",
    "English Exam Type 9",
    "English Exam Requirement 9",
  ];

  const allRows = [headers, ...rows];

  const sheets = google.sheets({ version: "v4", auth });
  const spreadsheetId = "1CeeOcN8B2B2qj6oTVu2yYQPP5YFt7QXxeBdOoNFIhKw"; // from your Google Sheet URL
  const range = `'${sheetName}'!A1:AD`; // starting cell

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
