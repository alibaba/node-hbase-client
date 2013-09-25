/*!
 * node-hbase-client - examples/helloworld.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var HBase = require('../');
var config = require('../test/config_test');
var utility = require('utility');

var client = HBase.create(config);

var now = '1';

var j = 0;
function callPut() {
  console.time('put');
  var row = utility.md5(now + 'test row' + j++);
  client.putRow('tcif_acookie_user', row, {
    'cf1:history': 'history ' + row + ' ' + j,
    'cf1:qualifier2': 'qualifier2 ' + row + ' ' + j,
  }, function (err) {
    if (err) {
      throw err;
    }
    console.timeEnd('put');
  });
}

var i = 0;
var done = 0;
var totalUse = 0;
function callGet() {
  // console.time('get');
  var row = utility.md5(now + 'test row' + i++);
  // Get `f1:name, f2:age` from `user` table.
  var param = new HBase.Get(row);
  param.addColumn('cf1', 'history');
  param.addColumn('cf1', 'qualifier2');
  var start = Date.now();
  client.get('tcif_acookie_user', param, function (err, result) {
    done++;
    var use = Date.now() - start;
    totalUse += use;
    if (err) {
      throw err;
    }
    var kvs = result.raw();
    // for (var i = 0; i < kvs.length - 1; i++) {
    //   var kv = kvs[i];
    //   console.log('[%s] key: `%s`, value: `%s`', new Date(), kv.toString(), kv.getValue().toString());
    // }
    // console.timeEnd('get');
    if (done % 100 === 0) {
      console.log('%d %s %sms', done, row, (totalUse / done).toFixed(2));
    }
    // console.log('size: %d', kvs.length);
  });
}

callPut();

setTimeout(function () {
  setInterval(function () {
    callGet();
    // callPut();
  }, 20);
}, 1000);
