const express = require('express');
const app = express();
require('dotenv').config();

app.get('/', (req, res) => {
  res.send('Express server is running!');
});

app.get('/extract', async (req, res) => {
  const uniId = req.query.university_id;
  console.log("Successful extraction for university (id): " + uniId);
  const extractDetails = require('./details-extraction'); // details-extraction.js
  const entryRequirement = require('./entry-requirement'); // entry-requirement.js
  const discSpec = require('./disc-spec'); // disc-spec.js
  const courseFeeNonDegree = require('./course-fee-non-degree') // course-fee-non-degree.js
  const courseFeeDegree = require('./course-fee-degree') // course-fee-degree.js
  // await extractDetails(uniId);
  // await entryRequirement(uniId);
  // await discSpec(uniId);
  // await courseFeeNonDegree(uniId);
  await courseFeeDegree(uniId);
  // await courseFeeNonDegree(uniId);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log('Server listening on port ' + PORT);
});