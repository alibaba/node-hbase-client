/*!
 * node-hbase-client - test/put.test.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
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
var Put = require('../lib/put');
var HConstants = require('../lib/hconstants');
var DataOutputBuffer = require('../lib/data_output_buffer');

describe('test/put.test.js', function () {

  describe('createPutKeyValue()', function () {
    it('should return a keyvalue', function () {

      var items = [
        // row, f, q, v
        ['1', 'f', 'q', '1'],
        ['r', 'f', 'q', '1'],
        ['58c8MDAwMDAwMDAwMDAwMDAwMQ==', 'f', 'q', '1'],
        ['58c8MDAwMDAwMDAwMDAwMDAwMQ==', 'f', 'history', '58c8MDAwMDAwMDAwMDAwMDAwMQ==这是值啊!'],
      ];
      items.forEach(function (item) {
        var r = item[0];
        var f = item[1];
        var q = item[2];
        var v = item[3];
        var put = new Put(r);
        var kv = put.createPutKeyValue(f, q, HConstants.LATEST_TIMESTAMP, v);
        utils.checkBytes(kv.bytes, 
          fs.readFileSync(path.join(utils.fixtures, 
            'put', 'createPutKeyValue_' + r + '_' + f + '_' + q + '_' + v + '.java.bytes')));
      });
      
    });
  });

  describe('write()', function () {
    
    var testJavaBytes = utils.createTestBytes('put');
    var cases = [
      "58c8MDAwMDAwMDAwMDAwMDAwMQ==",
      "2dbbMDAwMDAwMDAwMDAwMTAwMA==",
      "c06eMDAwMDAwMDAxOTg0MDEyMw==",
      "4917MDAwMDAwMDAwMDAwMTk4NA==",
      "0f48MDAwMDAwMDAwMDAwMDAwMA==",
    ];

    it('should convert Put to bytes', function () {
      for (var i = 0; i < cases.length; i++) {
        var row = cases[i];
        var put = new Put(row);
        var family = 'f';
        put.add(family, 'history', "value" + row + "这是值啊!");
        put.add(family, 'qualifier2', "qualifier2 value" + row + "这是值啊! qualifier2");
        var out = new DataOutputBuffer();
        put.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write', row, bytes);
      }
    });

  });



});

