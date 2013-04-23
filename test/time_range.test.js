/*!
 * node-hbase-client - test/time_range.test.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var pedding = require('pedding');
var Long = require('long');
var utils = require('./support/utils');
var should = require('should');
var TimeRange = require('../lib/time_range');
var DataOutputBuffer = require('../').DataOutputBuffer;

describe('test/time_range.test.js', function () {
  
  var testJavaBytes = utils.createTestBytes('time_range');
  var cases = [
    // min, max
    [0, 10000000, false],
    [10000000, 999999999999, false],
    [0, Math.pow(2, 53), false],
    [0, 9007199254740991, false],
    [0, Long.MAX_VALUE, false],
    [Long.MIN_VALUE, Long.MAX_VALUE, false],
  ];

  describe('write()', function () {
    
    it('should convert TimeRange to bytes', function () {
      
      for (var i = 0; i < cases.length; i++) {
        var item = cases[i];
        var min = item[0];
        var max = item[1];
        var allTime = item[2];

        var tr = new TimeRange(min, max, allTime);
        var out = new DataOutputBuffer();
        tr.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write_' + min, max, bytes);
      }
    });

  });

  describe('readFields()', function () {
    
    it('should convert bytes to TimeRange', function () {
      cases.forEach(function (item) {
        var min = item[0];
        var max = item[1];
        var allTime = item[2];

        var tr = new TimeRange(min, max, allTime);

        var filename = 'time_range/write_' + min + '_' + max;
        var io = utils.createDataInputBuffer(filename);
        var tr = new TimeRange();
        tr.readFields(io);
        tr.minStamp.toString().should.equal(min.toString());
        tr.maxStamp.toString().should.equal(max.toString());
        tr.allTime.should.equal(allTime);
      });
      
    });

  });

});
