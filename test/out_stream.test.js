/*!
 * node-hbase-client - test/out_stream.test.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var should = require('should');
var fs = require('fs');
var path = require('path');
var OutStream = require('../lib/out_stream');

var fixtures = path.join(__dirname, 'fixtures');

describe('test/out_stream.test.js', function () {
  
  var mockSocket = {
    bytes: null,
    write: function (bytes, offset, length) {
      offset = offset || 0;
      length = length || bytes.length;
      this.bytes = bytes.slice(offset, length);
    }
  };

  var testJavaBytes = function (method, v, bytes) {
    var javaBytes = fs.readFileSync(path.join(fixtures, method + '_' + v + '.java.bytes'));
    if (javaBytes.length !== bytes.length) {
      console.log('%s(%s): js:', method, v, bytes, 'java:', javaBytes);
    }
    bytes.length.should.equal(javaBytes.length, method + ' ' + v);
    // console.log(v, bytes, javaBytes)
    // bytes.should.eql(javaBytes);
    for (var i = 0; i < bytes.length; i++) {
      if (bytes[i] !== javaBytes[i]) {
        console.log('%s(%s): js:', method, v, bytes, 'java:', javaBytes);
      }
      bytes[i].should.equal(javaBytes[i]);;
    }
  };

  var out = new OutStream(mockSocket);

  describe('writeLong()', function () {
    
    it('should convert Long to 8 bytes', function () {
      var values = [
        -1, -11,
        -99, 
        -100, 
        -999999999,
        -2147483646, -2147483645, -2147483644,
        -2147483647, -2147483648,
        -4294967295, -4294967294, 
        -4294967296, -4294967297, -4294967298,
        -42949672961, -92949672961,
        -929496729611,
        -100100100100, 
        -100100100100100, 
        -10010010010010010, 
        // -1099511627776, // - Math.pow(2, 40)

        1, 11, 50, 99, 100, 1000, 10000, 100000, 1000000,
        2147483646, 2147483645, 2147483644,
        2147483647,
        4294967294, 4294967293,
        4294967295, // 0xffffffff
        4294967296, 42949672961, 42949672962, 429496729611,
        9294967296, 99999999999,
        100100100100, 
        100100100100100, 
        10010010010010010, 
        90071992547409,
        9007199254740992, // Math.pow(2, 53), max number
      ];
      for (var i = 0; i < values.length; i++) {
        var v = values[i];
        out.writeLong(v);
        mockSocket.bytes.should.length(8);
        testJavaBytes('writeLong', v, mockSocket.bytes);
      }
    });

  });

  describe('writeInt()', function () {
    
    it('should convert Int to 4 bytes', function () {
      var values = [
        -1, -11,
        -99, 
        -100,
        -999999999,
        -2147483647, 
        -2147483648, 

        1, 11, 50, 99, 100, 1000, 10000, 100000, 1000000,
        999999999,
        2147483646, 2147483645, 2147483644,
        2147483647,
      ];
      for (var i = 0; i < values.length; i++) {
        var v = values[i];
        out.writeInt(v);
        mockSocket.bytes.should.length(4);
        testJavaBytes('writeInt', v, mockSocket.bytes);
      }
    });

  });

  describe('writeBoolean()', function () {
    
    it('should convert Boolean to 1 byte', function () {
      var values = [
        true, false,
      ];
      for (var i = 0; i < values.length; i++) {
        var v = values[i];
        out.writeBoolean(v);
        mockSocket.bytes.should.length(1);
        testJavaBytes('writeBoolean', v, mockSocket.bytes);
      }
    });

  });

  describe('writeByte()', function () {
    
    it('should convert int to 1 byte', function () {
      var values = [
        1, 2, 3, 100,
        -127, -128, 127, 128,
        -1, 255, 254, 
      ];
      for (var i = 0; i < values.length; i++) {
        var v = values[i];
        out.writeByte(v);
        mockSocket.bytes.should.length(1);
        testJavaBytes('writeByte', v, mockSocket.bytes);
      }
    });

  });

  describe('writeChar() and writeShort()', function () {
    
    it('should convert Char/Short to 2 bytes', function () {
      var values = [
        -1, -11,
        -99, 
        -100,
        -32767,
        -32768,

        1, 11, 50, 99, 100, 1000, 10000, 
        32766,
        32767,
        32768, 32769, 32770,
        65535, 65536,
      ];
      for (var i = 0; i < values.length; i++) {
        var v = values[i];
        out.writeChar(v);
        mockSocket.bytes.should.length(2);
        testJavaBytes('writeChar', v, mockSocket.bytes);

        out.writeShort(v);
        mockSocket.bytes.should.length(2);
        testJavaBytes('writeChar', v, mockSocket.bytes);
      }
    });

  });

});
