"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var client_1 = require("@clickhouse/client");
const winston = require('winston');

if (process.env.telemetry_local_storage_enabled === 'true' && process.env.telemetry_local_storage_type === 'clickhouse') {
  // connecting clickhouse db
  var client = client_1.createClient({
    // host: process.env.CLICKHOUSE_HOST ?? 'http://localhost:8123',
    // username: process.env.CLICKHOUSE_USER ?? 'default',
    // password: process.env.CLICKHOUSE_PASSWORD ?? '1234',
    host: process.env.CLICKHOUSE_HOST,
    username: process.env.CLICKHOUSE_USER,
    password: process.env.CLICKHOUSE_PASSWORD,
  })

  // creating schema & table
  client.exec({
    query: `
    CREATE TABLE IF NOT EXISTS telemetry
    (
      id String,
      ver String,
      params Tuple(msgid String),
      ets UInt64,
      events Tuple(
        eid String,
        ets UInt64,
        ver String,
        mid String,
        actor Tuple(id String, type String),
        context Tuple(channel String, pdata Tuple(id String, ver String, pid String), env String, sid String, did String, cdata Array(Tuple(id String, type String)), rollup Tuple(l1 String), uid String),
        object Tuple(id String, ver String, type String),
        edata Tuple (
	        id String,
	        type String,
	        mode String,
          pageid String,
          duration Float64,
          uri String,
          subtype String,
          target String,
          values Tuple(
            Tuple(original_text String),
            Tuple(response_text String),
            Tuple(response_correct_words_array Array(String)),
            Tuple(response_incorrect_words_array Array(String)),
            Tuple(response_word_array_result Array(String)),
            Tuple(response_word_result String),
            Tuple(accuracy_percentage Float64),
            Tuple(duration UInt32)
            )
        )
      ),
      channel String,
      pid String,
      mid String,
      syncts UInt64
    )
    ENGINE MergeTree()
    ORDER BY (id)
  `,
  }).then(() => {
    console.log("table created successfully!")
  }).catch((error) => {
    console.log("error while creating db", error);
  });

}

class ClickhouseDispatcher extends winston.Transport {

  constructor(options) {
    super();
  }
  log(level, msg, meta, callback) {

    console.log("msg", msg)
    let msgData = JSON.parse(msg)
    console.log("msgData", msgData)

    //inserting into clickhouse
    // client.insert({
    //   table: 'telemetry',
    //   // structure should match the desired format, JSONEachRow in this example
    //   values: [
    //     { id: msgData.id, ver: msgData.ver, params: msgData.params, ets: msgData.ets, events: msgData.events[0], channel: msgData.channel, pid: msgData.pid, mid: msgData.mid, syncts: msgData.syncts }
    //   ],
    //   format: 'JSONEachRow',
    // }).then(() => {
    //   console.log("data inserted successfully!", meta)
    //   callback()
    // }).catch((error) => {
    //   console.log("error while inserting data", error);
    // });

    //inserting multiple data
    let promises = [];
    for (const iterator of msgData.events) {
      console.log("iterator", iterator.eid)
      promises.push(client.insert({
        table: 'telemetry',
        // structure should match the desired format, JSONEachRow in this example
        values: [
          { id: msgData.id, ver: msgData.ver, params: msgData.params, ets: msgData.ets, events: iterator, channel: msgData.channel, pid: msgData.pid, mid: msgData.mid, syncts: msgData.syncts }
        ],
        format: 'JSONEachRow',
      }))
    }

    Promise.all(promises)
    .then(() => {
      console.log("data inserted successfully!")
      callback()
    }).catch((err) => {
      console.log("Unable to insert data", err)
    })

  }
}

async function clickhouse(payload, callback) {

  // const resultSet = await client.query({
  //   query: 'SELECT * FROM telemetry',
  //   format: 'JSONEachRow',
  // })
  // const dataset = await resultSet.json()

  // //console.log("dataset", dataset)
  // if (dataset) {
  //   callback(null, dataset)
  // } else {
  //   callback(null, null)
  // }
  // let queryOptions = "api.sunbird.telemetry"
  console.log("payload", payload)

  let queryOptions = "SELECT * from telemetry";

  client.query({
    query: queryOptions,
    format: 'JSONEachRow',
  }).then(async (res) => {
    console.log("data fetched successfully")
    const dataset = await res.json()
    callback(null, dataset)
  }).catch((err) => {
    console.log("error while fetching data", err)
    callback(err, null)
  })

}

async function clickhouseQuery(payload, callback) {

  console.log("clickhouseQuery payload", payload.params)

  if(payload.query.event == 'loginAttempt' && payload.query.interface == 'registration.portal') {
    var queryOptions = "SELECT COUNT(*) from telemetry WHERE events.edata.pageid = 'sign-in' AND events.edata.id = 'login-auth-principal'  AND events.context.pdata.id = 'registration.portal'";
  }
  if (payload.query.event == 'loginSuccess' && payload.query.interface == 'registration.portal') {
    var queryOptions = "SELECT COUNT(*) from telemetry WHERE events.edata.id = 'login-success' AND events.edata.pageid = 'digilocker-callback' AND events.context.pdata.id = 'registration.portal';"
  }
  if (payload.query.event == 'successUdiseLink' && payload.query.interface == 'registration.portal') {
    var queryOptions = "SELECT COUNT(*) from telemetry WHERE events.edata.id = 'link-udise' AND events.context.pdata.id = 'registration.portal';"
  }
  if (payload.query.event == 'registerSuccess' && payload.query.interface == 'registration.portal') {
    var queryOptions = "SELECT COUNT(*) from telemetry WHERE events.edata.id = 'registration-success' AND events.context.pdata.id = 'registration.portal';"
  }
  if (payload.query.event == 'claimApproveSuccess' && payload.query.interface == 'registration.portal') {
    var queryOptions = "SELECT COUNT(*) from telemetry WHERE events.edata.id = 'claim-approval' AND events.context.pdata.id = 'registration.portal';"
  }
  if(payload.query.event == 'loginAttempt' && payload.query.interface == 'avasar.wallet') {
    var queryOptions = "SELECT COUNT(*) from telemetry WHERE events.edata.id = 'login-sso'  AND events.context.pdata.id = 'avasar.wallet'";
  }
  if(payload.query.event == 'loginSuccess' && payload.query.interface == 'avasar.wallet') {
    var queryOptions = "SELECT COUNT(*) from telemetry WHERE events.edata.id = 'login-success'  AND events.context.pdata.id = 'avasar.wallet'";
  }
  if(payload.query.event == 'registerSuccess' && payload.query.interface == 'avasar.wallet') {
    var queryOptions = "SELECT COUNT(*) from telemetry WHERE events.edata.id = 'registration-success'  AND events.context.pdata.id = 'avasar.wallet'";
  }
  if(payload.query.event == 'downloadPdf' && payload.query.interface == 'avasar.wallet') {
    var queryOptions = "SELECT COUNT(*) from telemetry WHERE events.edata.id = 'download-pdf'  AND events.context.pdata.id = 'avasar.wallet'";
  }
  if(payload.query.event == 'shareFile' && payload.query.interface == 'avasar.wallet') {
    var queryOptions = "SELECT COUNT(*) from telemetry WHERE events.edata.id = 'share-file'  AND events.context.pdata.id = 'avasar.wallet'";
  }
  if(payload.query.event == 'downLoadJson' && payload.query.interface == 'avasar.wallet') {
    var queryOptions = "SELECT COUNT(*) from telemetry WHERE events.edata.id = 'download-json'  AND events.context.pdata.id = 'avasar.wallet'";
  }

  client.query({
    query: queryOptions,
    format: 'JSONEachRow',
  }).then(async (res) => {
    console.log("data fetched successfully")
    const dataset = await res.json()
    callback(null, dataset)
  }).catch((err) => {
    console.log("error while fetching data", err)
    callback(err, null)
  })

}

winston.transports.clickhouse = ClickhouseDispatcher;

module.exports = { ClickhouseDispatcher, clickhouse, clickhouseQuery };
