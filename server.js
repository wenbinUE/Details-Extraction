const express = require('express');
const app = express();
require('dotenv').config();

app.get('/', (req, res) => {
  res.send('Express server is running!');
});

app.get('/extract', async (req, res) => {
  console.log("Received request for /extract");
  const uniId = req.query.university_id;
  const extractDetails = require('./details-extraction');
  await extractDetails(uniId);
  res.send('Extraction complete for university: ' + uniId);
  console.log("Successful extraction for university: " + uniId);
    
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log('Server listening on port ' + PORT);
});