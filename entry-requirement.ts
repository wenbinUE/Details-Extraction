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

interface EntryRequirement {
    qualification: string | ObjectIdType;
    score_method: string | ObjectIdType;
    score: number;
    is_additional: string;
    requirement_summary: string;
    additional_requirement: string;
    remarks: string;
}

interface Qualification {
    qualification_type: string[]; // Array of qualification names
    requirement_summary?: string[];
    score_to_qualify?: number[];
    remarks?: string[];
    additional_requirement?: string[];
}

interface EntryRequirementData {
    _id: string;
    name: string;
    qualifications: Qualification[];
}

const extractDetails = async (uniId: string): Promise<void> => {
    const client = await MongoClient.connect(url!);

    try {
        console.log("Entry Requirement now using uni id: (" + uniId + ")");
        console.log("Connected successfully to MongoDB (Entry Requirement)");
        const db = client.db(dbName);
        const coursesCol = db.collection("courses");

        // code cleaning here
        const cursor = coursesCol.find().limit(5);
        while (await cursor.hasNext()) {
            const doc = await cursor.next();
            if (!doc) continue;

            let changed = false;

            // Changes data.entry_requirement.qualification from string to ObjectId
            if (doc.data && Array.isArray(doc.data.entry_requirement)) {
                doc.data.entry_requirement = doc.data.entry_requirement.map(function (
                    req: EntryRequirement
                ) {
                    if (
                        req.qualification &&
                        typeof req.qualification === "string" &&
                        req.qualification.length === 24
                    ) {
                        req.qualification = new ObjectId(req.qualification);
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
                { $match: { university_id: new ObjectId(uniId) } }, // matched university
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

        const flattened: (string | number)[][] = [];

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
    } catch (err) {
        console.error("Aggregation error:", err);
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
