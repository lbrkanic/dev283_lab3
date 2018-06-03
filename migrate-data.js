'use strict';

const async = require('async');
const mongodb = require('mongodb');

const url = 'mongodb://localhost:27017';
const dbName = 'edx-course-db';

const customerData = require('./m3-customer-data.json');
const customerAddressData = require('./m3-customer-address-data.json');

let customersMerged = [];
let tasks = [];

mongodb.MongoClient.connect(url, (error, client) => {
  if (error) {
    return process.exit(1);
  }

  let db = client.db(dbName);

  let order = process.argv[2] || 100; // default is 100

  let t1, t2;
  t1 = new Date().getTime();

  db.collection('customers').remove({}, (error) => {
    if (error) {
      console.error(error);
      process.exit(1);
    }

    customerData.forEach((customer, index) => {
      customersMerged[index] = Object.assign(customerData[index],
        customerAddressData[index]);

      if (index % order === 0) {
        let start = index;
        let end = (index + order < customerData.length) ? (index + order) : 
          customerData.length;
        
        tasks.push((callback) => {
          db.collection('customers').insert(customersMerged.slice(start, end),
            (error, customers) => {
            callback(error, customers);
          });
        });
      }
    });

    async.parallel(tasks, (error, customers) => {
      t2 = new Date().getTime();
      console.log('Exectution time: ' + (t2 - t1) + 'ms');
      client.close();
    });

  });

});
