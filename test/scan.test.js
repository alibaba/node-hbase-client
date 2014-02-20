/**!
 * node-hbase-client - test/scan.test.js
 *
 * Copyright(c) 2013 - 2014 Alibaba Group Holding Limited.
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
var pedding = require('pedding');
var utils = require('./support/utils');
var should = require('should');
var HRegionInfo = require('../lib/hregion_info');
var Scan = require('../lib/scan');
var HConstants = require('../lib/hconstants');
var DataOutputBuffer = require('../lib/data_output_buffer');

describe('test/scan.test.js', function () {
  describe('write()', function () {
    var testJavaBytes = utils.createTestBytes('scan');
    var cases = [
      "tcif_acookie_actions",
      ".meta.",
    ];

    it('should convert Scan to bytes', function () {
      for (var i = 0; i < cases.length; i++) {
        var tableName = cases[i];
        var startRow = HRegionInfo.createRegionName(tableName,
          HConstants.EMPTY_START_ROW, HConstants.ZEROES, false);
        var scan = new Scan(startRow).addFamily(HConstants.CATALOG_FAMILY);

        var out = new DataOutputBuffer();
        scan.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write', tableName, bytes);
      }
    });
  });
});

