/**!
 * node-hbase-client - test/config_test.js
 *
 * Copyright(c) 2014 Alibaba Group Holding Limited.
 *
 * Authors:
 *   苏千 <suqian.yf@taobao.com> (http://fengmk2.github.com)
 */

"use strict";

/**
 * Module dependencies.
 */

var fs = require('fs');
var path = require('path');

var config = {
  // 0.94.x
  zookeeperHosts: [
    '127.0.0.1',
  ],
  zookeeperRoot: '/hbase',

  // logger: console,
  logger: {
    warn: function () {},
    info: function () {},
    error: function () {},
  },
  rpcTimeout: 40000,
  hostnamePart: '127.0.0.1',
  invalidHost: '127.0.0.1',
  invalidPort: 65535,
  tableUser: 'test_table',
  tableActions: 'test_table_actions',
  regionServer: '127.0.0.1:36020',
};

config.clusters = [
  {
    zookeeperHosts: config.zookeeperHosts,
    zookeeperRoot: config.zookeeperRoot,
  },
  // {
  //   zookeeperHosts: [
  //     '127.0.0.1:40060',
  //   ],
  //   zookeeperRoot: '/hbase-0.94.16',
  // }
];

var customConfig = path.join(__dirname, 'config.js');
if (fs.existsSync(customConfig)) {
  config = require(customConfig);
}

module.exports = config;
