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
    '10.232.98.74:2181', '10.232.98.75:2181', '10.232.98.76:2181', 
    '10.232.98.77:2181', '10.232.98.78:2181'
  ],
  zookeeperRoot: '/hbase-rd-test-0.94',
  // logger: console,
  logger: {
    warn: function () {},
    info: function () {},
    error: function () {},
  },
};

module.exports = config;
