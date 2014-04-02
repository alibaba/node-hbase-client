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

var config = {
  // 0.94.0
  zookeeperHosts: [
    '10.232.98.74',
    '10.232.98.75', '10.232.98.76',
    '10.232.98.77', '10.232.98.78',
  ],
  zookeeperRoot: '/hbase-rd-test-0.94',

  // logger: console,
  logger: {
    warn: function () {},
    info: function () {},
    error: function () {},
  },
  rpcTimeout: 40000,
  hostnamePart: '.kgb.sqa.cm4',
  invalidHost: '10.232.98.58',
  invalidPort: 65535,
  tableUser: 'tcif_acookie_user',
  tableActions: 'tcif_acookie_actions',
  regionServer: 'dw48.kgb.sqa.cm4:36020',
};

config.clusters = [
  {
    zookeeperHosts: config.zookeeperHosts,
    zookeeperRoot: config.zookeeperRoot,
  },
  {
    zookeeperHosts: [
      '10.232.98.53:40060',
      '10.232.98.54:40060',
      '10.232.98.55:40060',
    ],
    zookeeperRoot: '/hbase-0.94.16',
  }
];

module.exports = config;
