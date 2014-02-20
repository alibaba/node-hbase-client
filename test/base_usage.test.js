/**!
 * node-hbase-client - test/base_usage.test.js
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

var should = require('should');
var mm = require('mm');
var Client = require('../').Client;

describe('base_usage.test.js', function () {
  var client = null;
  before(function () {
    client = Client.create(config);
  });

  after(function (done) {
    setTimeout(done, 1000);
  });

  afterEach(mm.restore);

  describe('getRow()', function () {

  });
});
