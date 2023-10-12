import { Pool } from 'pg';
import { ShowCondition } from "./xui/conditional-show.model";
import * as format from "pg-format";

const {
  Worker, isMainThread, parentPort,
} = require('node:worker_threads');

const pool = new Pool({
  user: 'datainv@ccd-data-store-performance2',
  host: 'localhost',
  database: 'ccd_data_store',
  password: process.env.DATABASE_PASSWORD,
  port: 5440,
  ssl: {
    rejectUnauthorized: false,
  }
});

const query = `
  SELECT 
     count(*) OVER() AS count
  FROM defstore.event_mandatory_field emf 
  JOIN case_event ce ON emf.event_id = ce.event_id AND emf.case_type_id = ce.case_type_id
  LEFT JOIN case_event bce ON bce.id = (
    SELECT max(id) FROM case_event t WHERE t.case_data_id = ce.case_data_id AND t.case_type_id = emf.case_type_id AND t.id < ce.id
  )
  JOIN case_data cd ON ce.case_data_id = cd.id
  WHERE emf.case_type_id = 'NFD'
  AND emf.event_id = 'caseworker-offline-document-verified'
  AND (ce.data->>emf.field_id IS NULL 
  OR (emf.complex_field_id IS NOT NULL AND ce.data #> (string_to_array(emf.field_id || '.' || emf.complex_field_id, '.')) IS NULL))
  LIMIT 1`;

const resultTable = 'event_with_missing_data';
const pageSize = 20000;
const maxWorkers = 10;

async function main() {
  if (isMainThread) {
    const { offsets, maxResults } = await prepare();
    let runningWorkers = maxWorkers;

    for (let i = 0; i < maxWorkers; i++) {
      const worker = new Worker(__filename, { execArgv: ["--require", "ts-node/register"] });

      worker.on('message', () => {
        if (offsets.length > 0) {
          const nextOffset = offsets.pop();
          console.log(`Processing: ${nextOffset} to ${nextOffset + pageSize} of ${maxResults}`);
          worker.postMessage(nextOffset);
        } else {
          worker.terminate();
          runningWorkers--;
          if (runningWorkers === 0) {
            pool.end();
          }
        }
      });
    }
  } else {
    parentPort.on("message", async (offset: number) => {
      await runQuery(offset);
      parentPort.postMessage("done");
    });
    parentPort.postMessage("ready");
  }

}

async function prepare() {
  const client = await pool.connect();

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${resultTable} (
      case_type_id VARCHAR(255),
      event_name VARCHAR(255),
      page_id VARCHAR(255),
      page_show_condition VARCHAR(1000),
      field_id VARCHAR(255),
      field_type_id VARCHAR(255),
      field_show_condition VARCHAR(1000),
      field_retain_hidden_value BOOLEAN,
      complex_field_show_condition VARCHAR(1000),
      complex_field_retain_hidden_value BOOLEAN,
      complex_field_id VARCHAR(255),
      case_data_id bigint,
      event_id bigint,
      state_id VARCHAR(255),
      data JSONB,
      old_data JSONB
    )  
`)
  await client.query(`TRUNCATE TABLE ${resultTable}`);
  const { rows } = await client.query(query);
  const maxResults = rows[0].count;

  client.release();

  const offsets = [];

  for (let i = 0; i < maxResults; i += pageSize) {
    offsets.push(i);
  }

  return { offsets, maxResults };
}

async function runQuery(offset: number) {
  const client = await pool.connect();

  const tableQuery = query.replace('count(*) OVER() AS count', `
    emf.case_type_id,
    emf.event_id,
    emf.page_id,
    emf.page_show_condition,
    emf.field_id,
    emf.field_type_id,
    emf.field_show_condition,
    emf.field_retain_hidden_value,
    emf.complex_field_show_condition,
    emf.complex_field_retain_hidden_value,
    emf.complex_field_id,
    ce.case_data_id,
    ce.id,
    ce.state_id,
    ce.data,
    bce.data as old_data
  `).replace('LIMIT 1', `LIMIT ${pageSize} OFFSET ${offset}`);

  const { rows } = await client.query(tableQuery);

  const rowsToInsert = rows
    .filter(isDataMissing)
    .map(formatRow);

  const insertQuery = `
    INSERT INTO ${resultTable} (
      case_type_id,
      event_name,
      page_id,
      page_show_condition,
      field_id,
      field_type_id,
      field_show_condition,
      field_retain_hidden_value,
      complex_field_show_condition,
      complex_field_retain_hidden_value,
      complex_field_id,
      case_data_id,
      event_id,
      state_id,
      data,
      old_data
    ) VALUES %L
  `;

  console.log("Inserting results: " + rowsToInsert.length)

  if (rowsToInsert.length > 0) {
    await client.query(format(insertQuery, rowsToInsert));
  }
  client.release();
}

function isDataMissing(row: any) {
  const pageShown = showCondition(row.page_show_condition, row.data);
  const fieldShown = showCondition(row.field_show_condition, row.data);
  const complexFieldShown = showCondition(row.complex_field_show_condition, row.data);
  const fullFieldName = row.complex_field_id ? row.field_id + '.' + row.complex_field_id : row.field_id;
  const dataPreviouslySet = row.old_data && !!getProperty(row.old_data, fullFieldName);
  const rhv = row.field_retain_hidden_value;
  const complexRhv = row.complex_field_retain_hidden_value;

  // if (row.complex_field_id) {
  //   console.log(fullFieldName, row.old_data, getProperty(row.old_data, fullFieldName));
  //   console.log(pageShown, fieldShown, complexFieldShown, dataPreviouslySet, rhv, complexRhv);
  //   console.log(row);
  //   console.log((pageShown && fieldShown && complexFieldShown) || (dataPreviouslySet && (rhv || complexRhv)));
  // }

  return (pageShown && fieldShown && complexFieldShown) || (dataPreviouslySet && (rhv || complexRhv));
}

function getProperty(row, key) {
  key.split(".").forEach(k => row ? row=row[k] : undefined)
  return row;
}

function showCondition(condition: string, data: any) {
  if (!condition) {
    return true;
  }

  const result = new ShowCondition(condition).match(data);
  // console.log(condition, data, result);
  return result;
}

function formatRow(row) {
  return [
    row.case_type_id,
    row.event_id,
    row.page_id,
    row.page_show_condition,
    row.field_id,
    row.field_type_id,
    row.field_show_condition,
    row.field_retain_hidden_value,
    row.complex_field_show_condition,
    row.complex_field_retain_hidden_value,
    row.complex_field_id,
    row.case_data_id,
    row.id,
    row.state_id,
    row.data,
    row.old_data
  ];
}

main().catch(console.error);
