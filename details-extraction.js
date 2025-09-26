const { MongoClient } = require("mongodb");
const { google } = require("googleapis");
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

        if (changed) {
          await coursesCol.replaceOne({ _id: doc._id }, doc);
        }
      }

      // aggregation code here
      const result = await db
        .collection("courses")
        .aggregate([
          { $match: { "data.publish": "on" } },

          { $unwind: { path: "$data", preserveNullAndEmptyArrays: true } },
          {
            $unwind: {
              path: "$data.english_requirement",
              preserveNullAndEmptyArrays: true,
            },
          },

          {
            $lookup: {
              from: "levelofstudies",
              localField: "data.level_of_studies",
              foreignField: "_id",
              as: "level_details",
            },
          },
          {
            $lookup: {
              from: "campuses",
              localField: "campus_id",
              foreignField: "_id",
              as: "campus_details",
            },
          },

          {
            $group: {
              _id: "$_id",
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

      const flattened = result.map((doc) => {
        const row = [
          doc._id,
          doc.name,
          doc.reference_url,
          doc.why_apply,
          doc.course_level,
          doc.campus_name,
          doc.duration,
          doc.duration,
          doc.intakes,
          doc.ptptn_type,
      ];

        // Fill up to 8 exam slots
        for (let i = 0; i < 8; i++) {
          row.push(doc.english_requirements[i]?.type || "");
          row.push(doc.english_requirements[i]?.requirement || "");
        }

        return row;
      });

      // now send `flattened` to Google Sheets
      await sendToGoogleSheet(flattened);
    } catch (aggErr) {
      console.error("Aggregation error:", aggErr);
    } finally {
      client.close();
    }
  }
);

async function sendToGoogleSheet(rows) {
  // Path to your credentials.json file
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
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  // Authorize once
  await auth.authorize();

  const sheets = google.sheets({ version: "v4", auth });

  const spreadsheetId = "1CeeOcN8B2B2qj6oTVu2yYQPP5YFt7QXxeBdOoNFIhKw"; // from your Google Sheet URL
  const range = `'Details-Extraction'!A2:Z`; // starting cell

  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range,
    valueInputOption: "RAW",
    resource: {
      values: rows,
    },
  });

  console.log('Data pushed to Google Sheet!');
}