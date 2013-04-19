/*!
 * node-hbase-client - test/get.test.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var utils = require('./support/utils');
var should = require('should');
var Get = require('../lib/get');
var OutStream = require('../lib/out_stream');

describe('test/get.test.js', function () {
  
  var testJavaBytes = utils.createTestBytes('get');

  describe('write()', function () {
    
    it('should convert Get to bytes', function () {
      var cases = [
        // family, qualifier, row, v
        ['f', 'history', '0f48MDAwMDAwMDAwMDAwMDAwMA==', 0],
      ];
      for (var i = 0; i < cases.length; i++) {
        var item = cases[i];
        var row = item[2];
        var v = item[3];
        var family = item[0];
        var qualifier = item[1];
        var get = new Get(row);
        get.addColumn(family, qualifier);
        var mockSocket = utils.mockSocket();
        var out = new OutStream(mockSocket);
        get.write(out);
        mockSocket.bytes.length.should.above(0);
        testJavaBytes('write_' + family + '_' + qualifier, v, mockSocket.bytes);
      }
    });

  });


});
