/**!
 * node-hbase-client - test/splittest.js
 *
 * Copyright(c) Alibaba Group Holding Limited.
 * MIT Licensed
 *
 * Authors:
 *   苏千 <suqian.yf@alibaba-inc.com> (http://fengmk2.github.com)
 */

'use strict';

/**
 * Module dependencies.
 */

var hbase = require('../');
var config = require('./config_test');

var client = hbase.create(config);

client.on('error', function (err) {
  console.log(err);
});

var index = 0;
function createMultiPut() {
  var current = index++;
  var rows = [];
  for (var i = 0; i < 2000; i++) {
    rows.push({
      row: 'mput-bench-test#' + Math.round(Math.random() * 1000000),
      'cf1:col1': 'col_value#' + Math.round(Math.random() * 1000000),
    });
  }
  console.log('mput#%d, %d rows', current, rows.length);
  client.mput(config.tableUser, rows, function (err, results) {
    if (err) {
      console.log('mput#%d error: %s', current, err);
    } else {
      console.log('mput#%d success %d', current, results.length);
    }
  });
}

setInterval(createMultiPut, 1000);
