/*!
 * node-hbase-client - test/result.test.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var pedding = require('pedding');
var utils = require('./support/utils');
var should = require('should');
var Result = require('../').Result;

describe('test/result.test.js', function () {
  
  var cases = [
    // row, size
    ['e0abMDAwMDAwMDAwMDAwMDAxNQ==', 11],
    ['4edaMDAwMDAwMDAwMDAwMDAxNg==', 11],
    ['7c32MDAwMDAwMDAwMDAwMDAxNw==', 11],
    ['0ed7MDAwMDAwMDAwMDAwMDAxOA==', 11],
    ['f390MDAwMDAwMDAwMDAwMDAxOQ==', 11],
  ];

  describe('readFields(io)', function () {
    
    it('should convert Bytes to Result', function () {
      for (var i = 0; i < cases.length; i++) {
        var item = cases[i];
        var row = item[0];
        var size = item[1];
        var filename = 'result/write_f_history_' + row + '_size_' + size;
        var io = utils.createDataInputBuffer(filename);
        var result = new Result();
        result.readFields(io);
        result.size().should.equal(size);
        var kvs = result.raw();
        kvs.should.length(size);
        for (var j = 0; j < kvs.length; j++) {
          var kv = kvs[j];
          var r = kv.getRow();
          var value = kv.getValue();
          r.toString().should.equal(row);
          value.toString().should.include(row);
          // console.log(r.toString(), ':', value.toString());
        }
      }
    });

  });


});
