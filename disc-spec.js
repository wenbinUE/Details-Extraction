const { MongoClient } = require("mongodb");
const { google } = require("googleapis");
const { ObjectId } = require("mongodb");
const fs = require("fs");
const path = require("path");

const url = process.env.MONGO_URL; // mongoDB connection URL
const dbName = process.env.DB_NAME; // database name

module.exports = async function extractDetails(uniId, spreadsheetId, sheetname = "Disc-Spec-Extraction-CWB") {
  MongoClient.connect(
    url,
    { useNewUrlParser: true, useUnifiedTopology: true },
    async (err, client) => {
      if (err) {
        console.error("Connection error:", err);
        return;
      }

      console.log("Disc-Spec now using uni id: (" + uniId + ")");
      console.log("Connected successfully to MongoDB (Disc-Spec)");
      const db = client.db(dbName);
      const coursesCol = db.collection("courses");

      // Path to your Google Service Account credentials
      const credentials = {
        client_email: process.env.GOOGLE_CLIENT_EMAIL,
        private_key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
      };

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

          // Changes discipline id from string to ObjectId
          if (
            doc.discipline &&
            typeof doc.discipline === "string" &&
            doc.discipline.length === 24
          ) {
            doc.discipline = ObjectId(doc.discipline);
            changed = true;
          }

          // Changes specialisations array id from string to ObjectId
          if (doc.specialisations && Array.isArray(doc.specialisations)) {
            doc.specialisations = doc.specialisations.map(function (special) {
              if (typeof special === "string" && special.length === 24)
                return ObjectId(special);
              changed = true;
              return special;
            });
          }

          if (changed) {
            await coursesCol.replaceOne({ _id: doc._id }, doc);
          }
        }

        console.log("done");

        const result = await db
          .collection("courses")
          .aggregate([
            { $match: { university_id: ObjectId(uniId) } }, // matched university
            { $match: { "data.publish": "on" } }, // only published courses

            // open specialisations array
            {
              $unwind: {
                path: "$specialisations",
                preserveNullAndEmptyArrays: true,
              },
            },

            // group with disciplines collection to get discipline names
            {
              $lookup: {
                from: "disciplines",
                localField: "discipline",
                foreignField: "_id",
                as: "discipline_details",
              },
            },
            {
              $lookup: {
                from: "specialisations",
                localField: "specialisations", // join specialisations collection
                foreignField: "_id",
                as: "specialisations_details",
              },
            },
          ])
          .toArray();

        console.log(JSON.stringify(result, null, 2));

        const flattened = [];

        const disciplineAdded = new Set();

        result.forEach((doc) => {
          // Only add discipline once per course
          const disciplineKey = doc._id + (doc.discipline_details[0]?.name || "");
          if (!disciplineAdded.has(disciplineKey)) {
            flattened.push([
              doc._id || "",
              doc.name || "",
              (doc.discipline_details && doc.discipline_details[0]?.name) ? doc.discipline_details[0].name : "",
              "Discipline"
            ]);
            disciplineAdded.add(disciplineKey);
          }

          // Specialisations rows
          if (doc.specialisations_details && doc.specialisations_details.length > 0) {
            doc.specialisations_details.forEach((spec) => {
              flattened.push([
                doc._id || "",
                doc.name || "",
                spec.name,
                "Specialisation"
              ]);
            });
          }
        });

        console.log(flattened);

        await sendToGoogleSheet(flattened, sheetname, spreadsheetId, auth);
        await writeStatusToSheet(spreadsheetId, "Disc-Spec", "/", auth); // "/" for success, "X" for fail
        await new Promise((resolve) => setTimeout(resolve, 2000));
      } catch (aggErr) {
        console.error("Aggregation error:", aggErr);
        await writeStatusToSheet(spreadsheetId, "Disc-Spec", "X", auth);
      } finally {
        client.close();
      }
    }
  );
};

async function sendToGoogleSheet(rows, sheetName, spreadsheetId, auth) {
  const headers = ["Course ID", "Course Name", "Major Name", "Major Type"];

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
  } catch (e) {}

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
