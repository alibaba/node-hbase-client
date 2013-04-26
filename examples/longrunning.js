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

// Get `f1:name, f2:age` from `user` table.
var param = new HBase.Get('e0abMDAwMDAwMDAwMDAwMDAxNQ==');
param.addColumn('f', 'history');
param.addColumn('f', 'qualifier2');

function call() {
  client.get('tcif_acookie_actions', param, function (err, result) {
    console.log(err);
    var kvs = result.raw();
    for (var i = 0; i < kvs.length; i++) {
      var kv = kvs[i];
      console.log('[%s] key: `%s`, value: `%s`', new Date(), kv.toString(), kv.getValue().toString());
    }
  });
}

setInterval(call, 5000);
call();
