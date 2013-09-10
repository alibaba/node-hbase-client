/*!
 * node-hbase-client - examples/mget.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com> (http://fengmk2.github.com)
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var HBase = require('../');
var config = require('../test/config_test');
var pedding = require('pedding');

var client = HBase.create(config);

var rows = [
  '02d7MDAwMDAwMDAwMDAwMDAwMw==a',
  // '24e3MDAwMDAwMDAwMDAwMDAwNA==b',
  // '58c8MDAwMDAwMDAwMDAwMDAwMQ==c',
  // 'a98eMDAwMDAwMDAwMDAwMDAwMg==d',
  // '02d7MDAwMDAwMDAwMDAwMDAwMw==a',
  // '24e3MDAwMDAwMDAwMDAwMDAwNA==b',
  // '58c8MDAwMDAwMDAwMDAwMDAwMQ==c',
  // 'a98eMDAwMDAwMDAwMDAwMDAwMg==d',
  // '02d7MDAwMDAwMDAwMDAwMDAwMw==a',
  // '24e3MDAwMDAwMDAwMDAwMDAwNA==b',
  // '58c8MDAwMDAwMDAwMDAwMDAwMQ==c',
  // 'a98eMDAwMDAwMDAwMDAwMDAwMg==d',
  // '02d7MDAwMDAwMDAwMDAwMDAwMw==a',
  // '24e3MDAwMDAwMDAwMDAwMDAwNA==b',
  // '58c8MDAwMDAwMDAwMDAwMDAwMQ==c',
  // 'a98eMDAwMDAwMDAwMDAwMDAwMg==d',
  // '02d7MDAwMDAwMDAwMDAwMDAwMw==a',
  // '24e3MDAwMDAwMDAwMDAwMDAwNA==b',
  // '58c8MDAwMDAwMDAwMDAwMDAwMQ==c',
  // 'a98eMDAwMDAwMDAwMDAwMDAwMg==d'
];

var mgetCount = 0;
var doneCount = 0;
function runMGet(i) {
  client.mget('tcif_acookie_actions', rows, ['f:history', 'f:bigcontent'], function (err, datas) {
    if (err) {
      throw err;
    }
    doneCount++;
    console.log('[%s] mget #%d want %d, got %d, %d sent, %d done', 
      Date(), mgetCount, i, rows.length, datas.length, mgetCount, doneCount);
  });
}

function runGet(i) {
  client.getRow('tcif_acookie_actions', rows[0], ['f:history', 'f:bigcontent'], function (err, row) {
    if (err) {
      throw err;
    }
    console.log('[%s] get #%d want %d, got %s', Date(), i, rows.length, row);
  });
}

function test(fn) {
  var count = 10;
  if (mgetCount > 350) {
    return;
  }
  for (var i = 0; i < count; i++) {
    mgetCount++;
    fn(mgetCount);
  }
}

test(runMGet);
setInterval(function () {
  test(runMGet);
}, 300);

// test(runGet);
// setInterval(function () {
//   test(runGet);
// }, 1000);
