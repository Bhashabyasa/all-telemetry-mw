require('dotenv').config();
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var mysql = require('mysql');
const winston = require('winston');
const fetch = require('node-fetch');

if (process.env.telemetry_mysql_log_enabled === 'true' && process.env.telemetry_local_storage_type === 'mysql') {
    // create mysql connection
    var connection = mysql.createConnection({
        host: process.env.MYSQL_HOST,
        port: process.env.MYSQL_PORT,
        user: process.env.MYSQL_USER,
        password: process.env.MYSQL_PASSWORD,
        database: process.env.MYSQL_DATABASE
    });

    // check mysql connection
    connection.connect(function (err) {

    // check if any issue with connection
        if (err) {
            console.log("Mysql Connection Error",err)
            process.exit(1);
        } else {
            if (connection) {
                console.log("MySQL DB Connected!");
            }

            var table_name = process.env.table_prefix + "_" + "telemetry";
            // At the first time run, This command will run to create table if table not exist
            var sql = "CREATE TABLE IF NOT EXISTS ?? (id int(11) NOT NULL AUTO_INCREMENT,api_id varchar(255) NOT NULL,ver varchar(15) NOT NULL,params longtext NOT NULL CHECK (json_valid(params)),ets bigint(20) NOT NULL,events longtext NOT NULL CHECK(json_valid(events)),channel varchar(50) NOT NULL,pid varchar(50) NOT NULL,mid varchar(50) NOT NULL,syncts bigint(20) NOT NULL,PRIMARY KEY (id))";

            // for run table creation query
            connection.query(sql,[table_name], function (err, result) {
                if (err) throw console.log("error while creating db");
                console.log(result.message);
            });
        }
    });
}

class MysqlDispatcher extends winston.Transport {

    constructor(options) {
        super();
    }

    log(level, msg, meta, callback) {
        console.log("msg", msg);
        let msgData = JSON.parse(msg);
        console.log("msgData", msgData);

        let promises = [];

        if (process.env.telemetry_mysql_log_enabled === 'true' && process.env.telemetry_local_storage_type === 'mysql') {
            var table_name = process.env.table_prefix + "_" + "telemetry";
            try {
                for (const iterator of msgData.events) {
                    //insert query to mysql table one by one
                    promises.push(
                        connection.query("INSERT INTO ?? (api_id, ver, params, ets, events, channel, pid, mid, syncts) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", [table_name, msgData.id, msgData.ver, JSON.stringify(msgData.params), msgData.ets, JSON.stringify(iterator), iterator.context.channel, iterator.context.pdata.pid, msgData.mid, msgData.syncts])
                    );
                }
            } catch (err) {
                return callback(err);
            }
        }

        Promise.all(promises)
            .then(() => {

                if (process.env.telemetry_mysql_log_enabled === 'true' && process.env.telemetry_local_storage_type === 'mysql') {
                    console.log("data inserted successfully!");
                }

                // If sendAnonymousDataToALL is set yes data will send to ALL server.
                if (process.env.sendAnonymousDataToALL === 'yes' && process.env.UrlForAnonymousDataToALL != '') {

                    // This will remove student data from ALL student data
                    let anonymousEventsArr = msgData.events.filter(function(event) {
                        if (event.eid === "LOG" && event.edata.type === "api_login_call") {
                            return false; // skip
                        }
                        return true;
                    }).map((event) => {
                        event.context.uid = 'anonymous';
                        event.context.cdata = event.context.cdata.map((cdataEle) => {
                            if (cdataEle.type === "Buddy User") {
                                cdataEle.id = 'anonymous';
                                return cdataEle;
                            } else {
                                return cdataEle;
                            }
                        })
                        return event;
                    });

                    // anonymous data will be replaces msgData events which will added to ALL telemetry
                    msgData.events = anonymousEventsArr;
                    // used fetch to parallel logging to telemetry server for a anonymous data
                    fetch(process.env.UrlForAnonymousDataToALL, {
                        method: 'POST',
                        body: JSON.stringify(msgData),
                        headers: { 'Content-Type': 'application/json' }
                    }).then(res => {
                        if (res.status === 200 || res.status === 201) {
                            console.log("Data Logged into ALL telemetry")
                        } else {
                            console.log("Data is not logged into ALL telemetry")
                        }
                    })
                    .catch(err => console.log("Unable to insert data into ALL telemetry",err))
                }
                callback()

            }).catch((err) => {
                console.log("Unable to insert data", err);
                return callback(err);
            })
        }
}

winston.transports.mysql = MysqlDispatcher;

module.exports = { MysqlDispatcher }
