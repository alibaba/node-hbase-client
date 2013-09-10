/*!
 * node-hbase-client - test/config_test.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var config = {
  zookeeperHosts: [
    '10.232.98.74',
    '10.232.98.75', '10.232.98.76',
    '10.232.98.77', '10.232.98.78'
  ],
  zookeeperRoot: '/hbase-rd-test-0.94',
  // logger: console,
  logger: {
    warn: function () {},
    info: function () {},
    error: function () {},
  },
  rpcTimeout: 40000,
};

module.exports = config;
