import type { ObjectId as ObjectIdType } from "mongodb";
import pkg from 'mongodb';
const { MongoClient, ObjectId } = pkg;
import { google } from "googleapis";
import * as fs from "fs";
import * as path from "path";
import TurndownService from "turndown";
import dotenv from 'dotenv';
import { sheets_v4 } from "googleapis";
const turndownService = new TurndownService();

dotenv.config();

const url = process.env.MONGO_URL; // mongoDB connection URL
const dbName = "production"; // database name

interface specialisation {
    _id: ObjectIdType;
}

const extractDetails = async (uniId: string): Promise<void> => {
    const client = await MongoClient.connect(url!);

    try {
        console.log("Disc-Spec now using uni id: (" + uniId + ")");
        console.log("Connected successfully to MongoDB (Disc-Spec)");
        const db = client.db(dbName);
        const coursesCol = db.collection("courses");


        // code cleaning here
        const cursor = coursesCol.find().limit(5);
        while (await cursor.hasNext()) {
            const doc = await cursor.next();
            if (!doc) continue;

            let changed = false;

            // Changes discipline id from string to ObjectId
            if (
                doc.discipline &&
                typeof doc.discipline === "string" &&
                doc.discipline.length === 24
            ) {
                doc.discipline = new ObjectId(doc.discipline);
                changed = true;
            }

            // Changes specialisations array id from string to ObjectId
            if (doc.specialisations && Array.isArray(doc.specialisations)) {
                doc.specialisations = doc.specialisations.map(function (special: string | specialisation) {
                    if (typeof special === "string" && special.length === 24)
                        return new ObjectId(special);
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
                { $match: { university_id: new ObjectId(uniId) } }, // matched university
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

        const flattened: (string | number)[][] = [];

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
            doc.specialisations_details.forEach((spec: { name: string }) => {
              flattened.push([
                doc._id || "",
                doc.name || "",
                spec.name,
                "Specialisation"
              ]);
            });
          }
        });

        // console.log(flattened);

        await sendToGoogleSheet(flattened, "Disc-Spec-Extraction");
        await new Promise((resolve) => setTimeout(resolve, 2000));
    } catch (aggErr) {
        console.error("Aggregation error:", aggErr);
    } finally {
        client.close();
    }
};

export default extractDetails;

async function sendToGoogleSheet(rows: (string | number)[][], sheetName: string) {
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

    const headers = ["Course ID", "Course Name", "Major Name", "Major Type"];

    const allRows = [headers, ...rows];

    const sheets = google.sheets({ version: "v4", auth });
    const spreadsheetId = "1CeeOcN8B2B2qj6oTVu2yYQPP5YFt7QXxeBdOoNFIhKw"; // from your Google Sheet URL
    const range = `'Disc-Spec-Extraction'!A1`; // starting cell

    // Optionally create the sheet if it doesn't exist
    try {
        await sheets.spreadsheets.batchUpdate({
            spreadsheetId,
            requestBody: {
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
        requestBody: { values: allRows },
    });

    // Bold the header row (row 1)
    await sheets.spreadsheets.batchUpdate({
        spreadsheetId,
        requestBody: {
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
    async function getSheetId(
        sheets: sheets_v4.Sheets,
        spreadsheetId: string,
        sheetName: string
    ) {
        const res = await sheets.spreadsheets.get({ spreadsheetId });
        const sheetList = res.data.sheets;
        if (!sheetList) throw new Error("No sheets found in spreadsheet.");
        const sheet = sheetList.find((s) => s.properties?.title === sheetName);
        if (!sheet || !sheet.properties?.sheetId) throw new Error(`Sheet "${sheetName}" not found.`);
        return sheet.properties.sheetId;
    }

    console.log(`Data pushed to Google Sheet: ${sheetName}`);
}
