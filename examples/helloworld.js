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

var client = HBase.create({
  zookeeperHosts: [
    '127.0.0.1:2181', '127.0.0.1:2182',
  ],
  zookeeperRoot: '/hbase-0.94',
});

// Put
var put = new HBase.Put('foo');
put.add('f1', 'name', 'foo');
put.add('f1', 'age', '18');
client.put('user', put, function (err) {
  console.log(err);
});

// Get `f1:name, f2:age` from `user` table.
var param = new HBase.Get('foo');
param.addColumn('f1', 'name');
param.addColumn('f1', 'age');

client.get('user', param, function (err, result) {
  console.log(err);
  var kvs = result.raw();
  for (var i = 0; i < kvs.length; i++) {
    var kv = kvs[i];
    console.log('key: `%s`, value: `%s`', kv.toString(), kv.getValue().toString());
  }
});
