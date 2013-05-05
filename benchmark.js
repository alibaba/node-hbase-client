/*!
 * node-hbase-client - benchmark.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var HBase = require('./');
var config = require('./test/config');
var utility = require('utility');

var client = HBase.create(config);

var concurrency = parseInt(process.argv[2], 10) || 10;
console.log('concurrency %d', concurrency);

var now = Date.now();

var j = 0;
var putResult = {
  success: 0,
  fail: 0,
  total: 0,
  use: 0,
};

function callPut() {
  var row = utility.md5(now + 'test row' + j++);
  var startTime = Date.now();
  client.putRow('tcif_acookie_user', row, {
    'cf1:history': 'history ' + row + ' ' + j,
    'cf1:qualifier2': 'qualifier2 ' + row + ' ' + j,
  }, function (err) {
    putResult.total++;
    putResult.use += Date.now() - startTime;
    if (err) {
      putResult.fail++;
    } else {
      putResult.success++;
    }
    if (putResult.total % 1000 === 0) {
      console.log('Concurrency: %d', concurrency);
      console.log('Put QPS: %d\nRT %d ms', 
        (putResult.total / putResult.use * 1000).toFixed(0), 
        (putResult.use / putResult.total).toFixed(2));
      console.log('Total %d, Success: %d, Fail: %d', putResult.total, putResult.success, putResult.fail);
    }
  });
}

// var i = 0;
// function callGet() {
//   console.time('get');
//   var row = utility.md5(now + 'test row' + i++);
//   // Get `f1:name, f2:age` from `user` table.
//   var param = new HBase.Get(row);
//   param.addColumn('cf1', 'history');
//   param.addColumn('cf1', 'qualifier2');
//   client.get('tcif_acookie_user', param, function (err, result) {
//     err && console.log(err);
//     var kvs = result.raw();
//     // for (var i = 0; i < kvs.length - 1; i++) {
//     //   var kv = kvs[i];
//     //   // console.log('[%s] key: `%s`, value: `%s`', new Date(), kv.toString(), kv.getValue().toString());
//     // }
//     console.timeEnd('get');
//     if (i % 100 === 0) {
//       console.log(i, row);
//     }
//     // console.log('size: %d', kvs.length);
//   });
// }

callPut();
setTimeout(function () {
  for (var i = 0; i < concurrency; i++) {
    setInterval(callPut, 10);
  }
}, 1000);


// setTimeout(function () {
//   setInterval(function () {
//     callGet();
//     callPut();
//   }, 20);
// }, 1000);
