import express from 'express';
import type { Request, Response } from 'express';
import dotenv from 'dotenv';
import extractDetails from './details-extraction.js';
import entryRequirement from './entry-requirement.js';
import discSpec from './disc-spec.js';

dotenv.config();

const app = express();

app.get('/', (req: Request, res: Response) => {
  res.send('Express server is running!');
});

app.get('/extract', async (req: Request, res: Response) => {
  const uniId = req.query.university_id as string;
  console.log("Successful extraction for university (id): " + uniId);
  await extractDetails(uniId);
  await entryRequirement(uniId);
  await discSpec(uniId);
});

const PORT: number = Number(process.env.PORT) || 3000;
app.listen(PORT, () => {
  console.log('Server listening on port ' + PORT);
});