/*!
 * node-hbase-client - benchmark.js
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

var concurrency = parseInt(process.argv[2], 10) || 10;
var method = process.argv[3] || 'get';
console.log('method: %s, concurrency %d', method, concurrency);

var j = 0;
var i = 0;
var putResult = {
  success: 0,
  fail: 0,
  total: 0,
  use: 0,
  start: 0,
};
var getResult = {
  success: 0,
  fail: 0,
  total: 0,
  use: 0,
  start: 0,
};
var mgetResult = {
  success: 0,
  fail: 0,
  total: 0,
  use: 0,
  start: 0,
};

var putStop = false;
var getStop = false;
var MAX_NUM = 1000000;

function showResult(name, result) {
  var use = Date.now() - result.start;
  console.log('---------------- ' + name + ' --------------------');
  console.log('Concurrency: %d', concurrency);
  console.log('Total: %s ms\nQPS: %d\nRT %d ms',
    use,
    (result.total / use * 1000).toFixed(0),
    (result.use / result.total).toFixed(2));
  console.log('Total %d, Success: %d, Fail: %d', result.total, result.success, result.fail);
  console.log('-------------------------------------------');
}

function callPut(callback) {
  if (putStop) {
    return;
  }
  var row = utility.md5('test row' + j++);
  var startTime = Date.now();
  client.putRow('tcif_acookie_user', row, {
    'cf1:history': 'history ' + row + ' ' + j,
    'cf1:qualifier2': 'qualifier2 ' + row + ' ' + j,
  }, function (err) {
    putResult.total++;
    // if (putResult.total >= MAX_NUM) {
    //   console.log('putStop');
    //   putStop = true;
    // }
    putResult.use += Date.now() - startTime;
    if (err) {
      if (putResult.fail % 20 === 0) {
        console.error('[%s] %s: %s', new Date(), err.name, err.message);
      }
      putResult.fail++;
    } else {
      putResult.success++;
    }
    if (putStop || putResult.total % 10000 === 0) {
      showResult('Put()', putResult);
    }
    callback && callback();
  });
}

function callGet(callback) {
  if (getStop) {
    return;
  }
  var row = utility.md5('test row' + i++);
  var startTime = Date.now();
  var param = new HBase.Get(row);
  param.addColumn('cf1', 'history');
  param.addColumn('cf1', 'qualifier2');
  client.get('tcif_acookie_user', param, function (err, result) {
    // var kvs = result.raw();
    // for (var i = 0; i < kvs.length; i++) {
    //   var kv = kvs[i];
    //   console.log('key: `%s`, value: `%s`', kv.toString(), kv.getValue().toString());
    // }
    getResult.total++;
    // if (getResult.total >= MAX_NUM) {
    //   console.log('getStop');
    //   getStop = true;
    // }
    getResult.use += Date.now() - startTime;
    if (err) {
      console.log('%s: %s', err.name, err.message);
      getResult.fail++;
    } else {
      getResult.success++;
    }
    if (getStop || getResult.total % 10000 === 0) {
      showResult('Get()', getResult);
    }
    callback && callback();
  });
}

function callGetRow(callback) {
  if (getStop) {
    return;
  }
  var row = utility.md5('test row' + i++);
  var startTime = Date.now();
  client.getRow('tcif_acookie_user', row, ['cf1:history', 'cf1:qualifier2'], function (err, rows) {
    getResult.total++;
    // if (getResult.total >= MAX_NUM) {
    //   console.log('getStop');
    //   getStop = true;
    // }
    getResult.use += Date.now() - startTime;
    if (err) {
      console.log('%s: %s', err.name, err.message);
      getResult.fail++;
    } else {
      getResult.success++;
    }
    if (getStop || getResult.total % 10000 === 0) {
      showResult('GetRow()', getResult);
    }
    callback && callback();
  });
}

var MGetCount = 500;

function callMGet(callback) {
  if (getStop) {
    return;
  }
  var rows = [];
  for (var i = 0; i < MGetCount; i++) {
    var row = utility.md5('test row' + i++);
    rows.push(row);
  }
  var startTime = Date.now();
  client.mget('tcif_acookie_user', rows, ['cf1:history', 'cf1:qualifier2'], function (err, items) {
    // mgetResult.total += count;
    mgetResult.total++;
    // if (getResult.total >= MAX_NUM) {
    //   console.log('getStop');
    //   getStop = true;
    // }
    mgetResult.use += Date.now() - startTime;
    if (err) {
      console.log('%s: %s', err.name, err.message);
      mgetResult.fail++;
    } else {
      mgetResult.success++;
    }
    if (getStop || mgetResult.total % 1000 === 0) {
      showResult('MGet(), count: ' + MGetCount, mgetResult);
    }
    callback && callback();
  });
}

var NO = 0;
function runner(fun) {
  console.log('runner#%d start...', NO++);
  var next = function () {
    fun(next);
  };
  next();
}

callGet();

setTimeout(function () {
  for (var i = 0; i < concurrency; i++) {
    if (method === 'put') {
      putResult.start = Date.now();
      runner(callPut);
    } else if (method === 'getRow') {
      getResult.start = Date.now();
      runner(callGetRow);
    } else if (method === 'mget') {
      mgetResult.start = Date.now();
      runner(callMGet);
    } else {
      getResult.start = Date.now();
      runner(callGet);
    }
  }
}, 2000);

setInterval(function () {
  if (method === 'put') {
    showResult('Put()', getResult);
  } else if (method === 'getRow') {
    showResult('GetRow()', getResult);
  } else if (method === 'mget') {
    showResult('MGet(), count: ' + MGetCount, mgetResult);
  } else {
    showResult('Get()', getResult);
  }
}, 60000);
