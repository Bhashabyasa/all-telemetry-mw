const winston = require("winston");
const { MongoClient } = require("mongodb");

class MongoDBDispatcher extends winston.Transport {
  constructor(options) {
    super();

    this.options = options;

    this.url = `mongodb://${this.options.mongodbUser}:${this.options.mongodbPassword}@${this.options.mongodbHost}:${this.options.mongodbPort}/`;
    this.dbName = this.options.mongodbDatabase;
    this.collectionName = `${this.options.mongodbCollectionName}_telemetry`;
    this.client = null;
    this.db = null;
    this.collection = null;

    (async () => {
      try {
        this.client = await MongoClient.connect(this.url, {});

        console.log("MongoDB Connected!");
        this.db = this.client.db(this.dbName);

        const collections = await this.db
          .listCollections({ name: this.collectionName })
          .toArray();
        if (collections.length === 0) {
          await this.db.createCollection(this.collectionName);
          console.log(
            `Collection '${this.collectionName}' created successfully`
          );
        } else {
          console.log(`Collection '${this.collectionName}' already exists`);
        }

        this.collection = this.db.collection(this.collectionName);

        // Create indexes if they do not exist
        await this.collection.createIndexes([
          { key: { api_id: 1 } },
          { key: { ver: 1 } },
          { key: { ets: 1 } },
          { key: { syncts: 1 } },
        ]);
        console.log("Indexes created successfully");
      } catch (err) {
        console.error("MongoDB Connection Error", err);
        throw err;
      }
    })();
  }
  async log(level, msg, meta, callback) {
    let message = msg;
    if (typeof message === "string") {
      try {
        message = JSON.parse(msg);
      } catch (err) {
        console.error("Failed to parse message", err);
        return callback(err);
      }
    }

    const promises = [];

    try {
      for (const event of message.events) {
        const pid = event.context.pdata ? event.context.pdata.pid : undefined;
        promises.push(
          this.collection.insertOne({
            api_id: message.id,
            ver: message.ver,
            params: message.params,
            ets: message.ets,
            events: event,
            channel: event.context.channel,
            pid: pid,
            mid: message.mid,
            syncts: message.syncts,
          })
        );
      }
    } catch (err) {
      return callback(err);
    }

    if (
      process.env.sendAnonymousDataToALL === "yes" &&
      process.env.UrlForAnonymousDataToALL
    ) {
      let anonymousEventsArr = message.events
        .filter((event) => {
          if (event.eid === "LOG" && event.edata.type === "api_login_call") {
            return false; // skip
          }
          return true;
        })
        .map((event) => {
          event.context.uid = "anonymous";
          event.context.cdata = event.context.cdata
            .filter((cdataEle) => {
              if (
                cdataEle.type === "school_name" ||
                cdataEle.type === "class_studying_id" ||
                cdataEle.type === "udise_code"
              ) {
                return false; // skip
              }
              return true;
            })
            .map((cdataEle) => {
              if (cdataEle.type === "Buddy User") {
                cdataEle.id = "anonymous";
              }
              return cdataEle;
            });
          return event;
        });

      message.events = anonymousEventsArr;

      try {
        const response = await fetch(process.env.UrlForAnonymousDataToALL, {
          method: "POST",
          body: JSON.stringify(message),
          headers: { "Content-Type": "application/json" },
        });

        console.log(JSON.stringify(response));

        if (response.status === 200 || response.status === 201) {
          console.log("Data logged into ALL telemetry");
        } else {
          console.log("Data is not logged into ALL telemetry");
        }
      } catch (err) {
        console.error("Unable to insert data into MongoDB", err);
      }
    }

    callback();
  }
}
winston.transports.mongodb = MongoDBDispatcher;

module.exports = MongoDBDispatcher;
