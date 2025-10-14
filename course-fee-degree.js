const { MongoClient } = require("mongodb");
const { google } = require("googleapis");
const { ObjectId } = require("mongodb");
const fs = require("fs");
const path = require("path");
const TurndownService = require("turndown");
const turndownService = new TurndownService();
require("dotenv").config();

const url = process.env.MONGO_URL; // mongoDB connection URL
const dbName = "production"; // database name

module.exports = async function extractDetails(
  uniId,
  spreadsheetId,
  sheetname = "Fee-Extraction-Degree-CWB"
) {
  MongoClient.connect(
    url,
    { useNewUrlParser: true, useUnifiedTopology: true },
    async (err, client) => {
      if (err) {
        console.error("Connection error:", err);
        return;
      }

      console.log(
        "Course Fee Extraction - Degree now using uni id: (" + uniId + ")"
      );
      console.log(
        "Connected successfully to MongoDB (Course Fee Extraction - Degree)"
      );
      const db = client.db(dbName);

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

      try {
        // aggregation code here
        const result = await db
          .collection("courses")
          .aggregate([
            { $match: { university_id: ObjectId(uniId) } }, // matched university
            { $match: { "data.publish": "on" } }, // only published courses
            // { $match: { _id: ObjectId("5dc29b812c2c7312fb2fb472") } },
            {
              $match: {
                "data.level_of_studies": ObjectId("5a1bbab02a2e3c29ecb9233b"),
              },
            }, // includes only degree courses

            // convert data.domesticstd_course_fees object to array
            {
              $addFields: {
                domesticstd_course_fees_arr: {
                  $objectToArray: "$data.domesticstd_course_fees",
                },
              },
            },
            // convert data.internationalstd_course_fees object to array
            {
              $addFields: {
                internationalstd_course_fees_arr: {
                  $objectToArray: "$data.internationalstd_course_fees",
                },
              },
            },
            // Unwind the domesticstd_course_fees_arr array
            {
              $unwind: {
                path: "$domesticstd_course_fees_arr",
                preserveNullAndEmptyArrays: true,
              },
            },
            {
              $match: {
                "domesticstd_course_fees_arr.k": { $ne: "13" }, // exclude the "13" fee category
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
                from: "universities",
                localField: "university_id", // join universities collection
                foreignField: "_id",
                as: "university_details",
              },
            },
            {
              $lookup: {
                from: "currencies",
                localField: "data.domesticstd_fee_currency", // join currencies collection for the domestic fee currency
                foreignField: "_id",
                as: "domestic_fee_currency_details",
              },
            },
            {
              $lookup: {
                from: "currencies",
                localField: "data.internationalstd_fee_currency", // join currencies collection for the international fee currency
                foreignField: "_id",
                as: "international_fee_currency_details",
              },
            },
            {
              $project: {
                _id: 1,
                university_name: {
                  $arrayElemAt: ["$university_details.name", 0],
                },
                name: 1,
                fee_name: { $arrayElemAt: ["$level_details.name", 0] },
                period_duration: { $ifNull: ["$data.local_year_fulltime", ""] },
                local_fee_currency: {
                  $arrayElemAt: ["$domestic_fee_currency_details.name", 0],
                },
                international_fee_currency: {
                  $arrayElemAt: ["$international_fee_currency_details.name", 0],
                },
                domestic_fee: "$domesticstd_course_fees_arr.v",
                international_fee: {
                  $arrayElemAt: [
                    {
                      $filter: {
                        input: "$internationalstd_course_fees_arr",
                        as: "intf",
                        cond: {
                          $eq: ["$$intf.k", "$domesticstd_course_fees_arr.k"],
                        },
                      },
                    },
                    0,
                  ],
                },
              },
            },
          ])
          .toArray();

        // console.log("Aggregation result:", result);
        // console.log(JSON.stringify(result, null, 2));

        const feeTypeMap = {
          1: "Tuition Fee",
          9: "Other Fee",
          10: "Registration Fee",
          5: "Other Cost",
          11: "Other Cost",
          12: "Other Cost",
        };

        const periodStart = ["P1", "P2", "P3", "P4", "P5", "N/A"];

        const periodEnd = ["P1", "P2", "P3", "P4", "P5", "N/A"];

        const previousNode = [
          "BEGIN",
          "P1",
          "P2",
          "P3",
          "P4",
          "P5",
          "TRANSFER",
          "N/A",
        ];

        const nextNode = ["FINAL", "CONTINUE", "TRANSFER", "N/A"];

        // Group all rows by course and fee type
        const grouped = {};

        result.forEach((doc) => {
          const feeTypeLabel =
            feeTypeMap[doc.domestic_fee.fees_category] ||
            doc.domestic_fee.fees_category ||
            "";
          const key = `${doc._id}_${feeTypeLabel}`;

          if (!grouped[key]) {
            grouped[key] = {
              ...doc,
              feeTypeLabel,
              local_fee_amount: 0,
              local_fee_details: [],
              intl_fee_amount: 0,
              intl_fee_details: [],
            };
          }

          // Sum for Other Cost
          if (feeTypeLabel === "Other Cost") {
            grouped[key].local_fee_amount +=
              Number(doc.domestic_fee.fees_amount) || 0;
            if (doc.domestic_fee.fees_detail)
              grouped[key].local_fee_details.push(
                turndownService.turndown(doc.domestic_fee.fees_detail)
              );

            // International fee sum (if present)
            if (doc.international_fee?.v?.fees_amount) {
              grouped[key].intl_fee_amount +=
                Number(doc.international_fee.v.fees_amount) || 0;
            }
            if (doc.international_fee?.v?.fees_detail) {
              grouped[key].intl_fee_details.push(
                turndownService.turndown(doc.international_fee.v.fees_detail)
              );
            }
          } else {
            // For other types, just take the first occurrence
            if (grouped[key].local_fee_amount === 0) {
              grouped[key].local_fee_amount =
                doc.domestic_fee.fees_amount || "";
              if (doc.domestic_fee.fees_detail)
                grouped[key].local_fee_details.push(
                  turndownService.turndown(doc.domestic_fee.fees_detail)
                );
              if (doc.international_fee?.v?.fees_amount) {
                grouped[key].intl_fee_amount =
                  turndownService.turndown(
                    String(doc.international_fee?.v?.fees_amount)
                  ) || "";
              }
              if (doc.international_fee?.v?.fees_detail)
                grouped[key].intl_fee_details.push(
                  turndownService.turndown(
                    String(doc.international_fee.v.fees_detail)
                  )
                );
            }
          }
        });

        const flattened = [];

        Object.values(grouped).forEach((doc) => {
          // Transform Period End Value
          const periodEndValue = "P" + (doc.period_duration || "");

          // Get fee type label
          const feeTypeLabel = doc.feeTypeLabel;

          // Get numeric fee amounts for local and international
          const localFeeAmountValue = Number(doc.local_fee_amount);
          const intlFeeAmountValue = Number(doc.intl_fee_amount);

          // Find whether the period duration is full or non-full
          const duration = Number(doc.period_duration);
          const isFullYear = Number.isInteger(duration) && duration > 0;

          if (isFullYear) {
            // If the period duration is full year (e.g. 3, 4)
            flattened.push([
              doc._id, // Course ID
              doc.university_name, // University Name
              doc.name, // Course Name
              feeTypeLabel, // Fee Type
              doc.fee_name || "", // Fee Name
              feeTypeLabel == "Tuition Fee" || feeTypeLabel == "Other Fee"
                ? "P1"
                : "N/A", // Period Start
              feeTypeLabel == "Tuition Fee" || feeTypeLabel == "Other Fee"
                ? periodEndValue
                : "N/A", // Period End
              feeTypeLabel == "Tuition Fee" || feeTypeLabel == "Other Fee"
                ? "1"
                : "0", // Period Duration
              feeTypeLabel == "Tuition Fee" || feeTypeLabel == "Other Fee"
                ? "BEGIN"
                : "N/A", // Previous Node
              feeTypeLabel == "Tuition Fee" || feeTypeLabel == "Other Fee"
                ? "FINAL"
                : "N/A", // Next Node
              "", // Period Location
              "", // Foreign Campus
              doc.local_fee_currency || "", // Local Fee Currency
              feeTypeLabel == "Tuition Fee" || feeTypeLabel == "Other Fee"
                ? localFeeAmountValue / duration
                : localFeeAmountValue, // Local Fee Amount (annual)
              doc.local_fee_details.filter(Boolean).join("\n"), // Local Fee Description
              doc.international_fee_currency || "", // International Fee Currency
              feeTypeLabel == "Tuition Fee" || feeTypeLabel == "Other Fee"
                ? intlFeeAmountValue / duration
                : intlFeeAmountValue, // International Fee Amount (annual)
              doc.intl_fee_details.filter(Boolean).join("\n"), // International Fee Description
            ]);
          } else if (!isFullYear) {
            // Else If the period duration is non-full year (e.g. 3.5, 4.3)

            // Process non-full year
            const remainder = (duration % 1) + 1;
            const roundedDownPeriodDuration = Math.floor(duration);

            if (
              feeTypeLabel === "Tuition Fee" ||
              feeTypeLabel === "Other Fee"
            ) {
              // separate tuition fee according to years
              for (let i = 1; i < 3; i++) {
                const annualFee = localFeeAmountValue / duration;
                const annualFeeForRemainder = annualFee * remainder;
                const annualIntlFee = intlFeeAmountValue / duration;
                const annualIntlFeeForRemainder = annualIntlFee * remainder;

                if (i < 3) {
                  flattened.push([
                    doc._id, // Course ID
                    doc.university_name, // University Name
                    doc.name, // Course Name
                    doc.feeTypeLabel, // Fee Type
                    doc.fee_name || "", // Fee Name
                    i !== 2 ? "P" + i : "P" + roundedDownPeriodDuration, // Period Start
                    i !== 2 ? "P" + (i + 1) : "P" + roundedDownPeriodDuration, // Period End
                    i !== 2 ? "1" : remainder, // Period Duration
                    i === 1 ? previousNode[0] : `P${i}`, // Previous Node
                    i !== 2 ? nextNode[1] : nextNode[0], // Next Node
                    "", // Period Location
                    "", // Foreign Campus
                    doc.local_fee_currency || "", // Local Fee Currency
                    i !== 2 ? annualFee : annualFeeForRemainder, // Local Fee Amount (annual)
                    doc.local_fee_details.filter(Boolean).join("\n"), // Local Fee Description
                    doc.international_fee_currency || "", // International Fee Currency
                    i !== 2 ? annualIntlFee : annualIntlFeeForRemainder, // International Fee Amount (annual)
                    doc.intl_fee_details.filter(Boolean).join("\n"), // International Fee Description
                  ]);
                }
              }
            } else if (
              feeTypeLabel !== "Tuition Fee" &&
              feeTypeLabel !== "Other Fee"
            ) {
              // for other fee types, just take the normal value

              flattened.push([
                doc._id, // Course ID
                doc.university_name, // University Name
                doc.name, // Course Name
                doc.feeTypeLabel, // Fee Type
                doc.fee_name || "", // Fee Name
                "N/A", // Period Start
                "N/A", // Period End
                "0", // Period Duration
                "N/A", // Previous Node
                "N/A", // Next Node
                "", // Period Location
                "", // Foreign Campus
                doc.local_fee_currency || "", // Local Fee Currency
                localFeeAmountValue, // Local Fee Amount (annual)
                doc.local_fee_details.filter(Boolean).join("\n"), // Local Fee Description
                doc.international_fee_currency || "", // International Fee Currency
                intlFeeAmountValue || 0, // International Fee Amount (annual)
                doc.intl_fee_details.filter(Boolean).join("\n"), // International Fee Description
              ]);
            }
          }
        });

        const cleanedFlattended = flattened.filter((row) => {
          const localFeeAmount = row[13];
          const localFeeDesc = row[14];
          const intlFeeAmount = row[16];
          const intlFeeDesc = row[17];

          const isEmpty = (v) =>
            v === undefined ||
            v === null ||
            (typeof v === "string" && v.trim() === "") ||
            v === 0 ||
            v === "0";

          // Remove row if ALL fee fields are empty/null/zero
          return !(
            isEmpty(localFeeAmount) &&
            isEmpty(localFeeDesc) &&
            isEmpty(intlFeeAmount) &&
            isEmpty(intlFeeDesc)
          );
        });

        // console.log(flattened);
        console.log(cleanedFlattended);

        // now send `flattened` to Google Sheets
        await sendToGoogleSheet(flattened, sheetname, spreadsheetId, auth);
        await writeStatusToSheet(spreadsheetId, "Course-Fee-Degree", "/", auth); // "/" for success, "X" for fail
        await new Promise((resolve) => setTimeout(resolve, 2000));
      } catch (aggErr) {
        console.error("Aggregation error:", aggErr);
        await writeStatusToSheet(spreadsheetId, "Course-Fee-Degree", "X", auth);
      } finally {
        client.close();
      }
    }
  );
};

async function sendToGoogleSheet(rows, sheetName, spreadsheetId, auth) {
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

  // Create Status sheet if not exists
  try {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      resource: {
        requests: [{ addSheet: { properties: { title: "Status" } } }],
      },
    });
  } catch (e) {}

  // Bold the headers
  const res = await sheets.spreadsheets.get({ spreadsheetId });
  const statusSheet = res.data.sheets.find(s => s.properties.title === "Status");
  if (statusSheet) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      resource: {
        requests: [
          {
            repeatCell: {
              range: {
                sheetId: statusSheet.properties.sheetId,
                startRowIndex: 0,
                endRowIndex: 1,
                startColumnIndex: 0,
                endColumnIndex: 2,
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
  }

  // Write header ONLY to A1:B1 (overwrite just the header row)
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: "Status!A1:B1",
    valueInputOption: "RAW",
    resource: { values: [["Module", "Status"]] },
  });

  // Always append only the status row (no header) starting from A2
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: "Status!A2:B2",
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    resource: { values: [[moduleName, status]] },
  });

  console.log(`Status for (${moduleName}) appended to Google Sheet: ${status}`);
}
