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

var client = HBase.create(config);

var i = 0;
function call() {
  console.time('get');
  // Get `f1:name, f2:age` from `user` table.
  var param = new HBase.Get('e0abMDAwMDAwMDAwMDAwMDAxNQ==' + i++);
  param.addColumn('f', 'history');
  param.addColumn('f', 'qualifier2');
  client.get('tcif_acookie_actions', param, function (err, result) {
    // console.log(err);
    var kvs = result.raw();
    for (var i = 0; i < kvs.length; i++) {
      var kv = kvs[i];
      // console.log('[%s] key: `%s`, value: `%s`', new Date(), kv.toString(), kv.getValue().toString());
    }
    console.timeEnd('get');
    // console.log('size: %d', kvs.length);
  });
}

call();

setTimeout(function () {
  setInterval(call, 100);
}, 1000);
