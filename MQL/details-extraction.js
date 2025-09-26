db.courses.find().forEach(function (doc) {
  // Change data.level_of_studies from array of strings to array of ObjectIds
  if (doc.data && Array.isArray(doc.data.level_of_studies)) {
    doc.data.level_of_studies = doc.data.level_of_studies.map(function (lvl) {
      if (typeof lvl === "string" && lvl.length === 24) return ObjectId(lvl);
      return lvl;
    });
  }

  // Change campus_id from string to ObjectId
  if (
    doc.campus_id &&
    typeof doc.campus_id === "string" &&
    doc.campus_id.length === 24
  ) {
    doc.campus_id = ObjectId(doc.campus_id);
  }

  // Change data.partner_duration from string to float. If not a number, set to 0
  pd = parseFloat(doc.data.partner_duration);
  if (isNaN(pd)) pd = 0;
  if (doc.data.partner_duration !== pd) {
    doc.data.partner_duration = pd;
  }

  // Change data.local_year_fulltime from string to float. If not a number, set to 0
  lf = parseFloat(doc.data.local_year_fulltime);
  if (isNaN(lf)) lf = 0;
  if (doc.data.local_year_fulltime !== lf) {
    doc.data.local_year_fulltime = lf;
  }

  // Save changes back to courses
  db.courses.save(doc);
});

db.courses.aggregate([
  // match only published courses
  { $match: { "data.publish": "on" } },

  // open data dictionary, and then english_requirement array
  { $unwind: { path: "$data", preserveNullAndEmptyArrays: true } },
  {
    $unwind: {
      path: "$data.english_requirement",
      preserveNullAndEmptyArrays: true,
    },
  },
  // group with levelofstudies collection to get course names
  {
    $lookup: {
      from: "levelofstudies",
      localField: "data.level_of_studies",
      foreignField: "_id",
      as: "level_details",
    },
  },
  // group with campuses collection to get campus names
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
]);
