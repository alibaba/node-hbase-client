/*!
 * node-hbase-client - test/writable_utils.test.js
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
var WritableUtils = require('../lib/writable_utils');
var DataOutputBuffer = require('../lib/data_output_buffer');

describe('test/writable_utils.test.js', function () {
  
  var testJavaBytes = utils.createTestBytes('writable_utils');

  describe('writeVLong() and readVLong()', function () {
    
    it('should convert Long to bytes', function (done) {
      var values = [
        0,
        -1, -11,
        -99, 
        -100, 
        -111, -112, -113, -114,
        -256,
        -1024,
        -65535,
        -999999999,
        -2147483646, -2147483645, -2147483644,
        -2147483647, 

        // -2147483648,
        // -4294967295, -4294967294, 
        // -4294967296, -4294967297, -4294967298,
        // -42949672961, -92949672961,
        // -929496729611,
        // -100100100100, 
        // -100100100100100, 
        // -10010010010010010, 
        // -1099511627776, // - Math.pow(2, 40)

        1, 11, 50, 99, 100, 
        111, 112, 113, 125, 126, 127, 128,
        256, 255,
        1000, 1024, 10000, 100000, 1000000,
        2147483646, 2147483645, 2147483644,
        2147483647,
        Math.pow(2, 30),

        // 4294967294, 
        // 4294967293,
        // 4294967295, // 0xffffffff
        // 4294967296, 42949672961, 42949672962, 429496729611,
        // 9294967296, 99999999999,
        // 100100100100, 
        // 100100100100100, 
        // 10010010010010010, 
        // 90071992547409,
        // 9007199254740992, // Math.pow(2, 53), max number
      ];

      done = pedding(values.length, done);

      values.forEach(function (v) {
        var out = new DataOutputBuffer();
        WritableUtils.writeVLong(out, v);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('writeVLong', v, bytes);

        var filename = 'writeVLong_' + v;
        var io = utils.createTestStream('writable_utils', filename);
        WritableUtils.readVLong(io, function (err, readV) {
          should.not.exists(err);
          readV.should.equal(v);
          done();
        });
      });

    });

  });
});