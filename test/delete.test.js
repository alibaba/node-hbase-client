/*!
 * node-hbase-client - test/delete.test.js
 * Copyright(c) 2013 tangyao <tangyao@alibaba-inc.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var pedding = require('pedding');
var utils = require('./support/utils');
var should = require('should');
var Get = require('../lib/get');
var DataOutputBuffer = require('../lib/data_output_buffer');

describe('test/delete.test.js', function () {
  
  var testJavaBytes = utils.createTestBytes('get');
  var cases = [
    // family, qualifier, row, maxVersions
    ['f', 'history', '0f48MDAwMDAwMDAwMDAwMDAwMA==', 1],
    ['f', 'history', '2dbbMDAwMDAwMDAwMDAwMTAwMA==', 50],
    ['f', 'history', '中文rowkey', 100],
  ];
 
  
  
});
