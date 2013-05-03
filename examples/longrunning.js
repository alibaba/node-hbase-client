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
var config = require('../test/config');
var utility = require('utility');

var client = HBase.create(config);

var j = 0;
function callPut() {
  console.time('put');
  var row = utility.md5('test row' + j++);
  client.putRow('tcif_acookie_actions', row, {
    'f:history': 'history ' + row,
    'f:qualifier2': 'qualifier2 ' + row,
  }, function (err) {
    err && console.log(err);
    console.timeEnd('put');
  });
}

var i = 0;
function callGet() {
  console.time('get');
  var row = utility.md5('test row' + i++);
  // Get `f1:name, f2:age` from `user` table.
  var param = new HBase.Get(row);
  param.addColumn('f', 'history');
  param.addColumn('f', 'qualifier2');
  client.get('tcif_acookie_actions', param, function (err, result) {
    err && console.log(err);
    var kvs = result.raw();
    for (var i = 0; i < kvs.length; i++) {
      var kv = kvs[i];
      console.log('[%s] key: `%s`, value: `%s`', new Date(), kv.toString(), kv.getValue().toString());
    }
    console.timeEnd('get');
    // console.log('size: %d', kvs.length);
  });
}

callPut();

setTimeout(function () {
  setInterval(function () {
    callGet();
    callPut();
  }, 100);
}, 1000);
