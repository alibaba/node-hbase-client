/*!
 * node-hbase-client - test/get.test.js
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
var Get = require('../lib/get');
var DataOutputBuffer = require('../').DataOutputBuffer;

describe('test/get.test.js', function () {
  
  var testJavaBytes = utils.createTestBytes('get');
  var cases = [
    // family, qualifier, row, maxVersions
    ['f', 'history', '0f48MDAwMDAwMDAwMDAwMDAwMA==', 1],
    ['f', 'history', '2dbbMDAwMDAwMDAwMDAwMTAwMA==', 50],
    ['f', 'history', '中文rowkey', 100],
  ];

  describe('write()', function () {
    
    it('should convert Get to bytes', function () {
      
      for (var i = 0; i < cases.length; i++) {
        var item = cases[i];
        var row = item[2];
        var family = item[0];
        var qualifier = item[1];
        var maxVersions = item[3];
        var get = new Get(row);
        get.addColumn(family, qualifier);
        get.setMaxVersions(maxVersions)
        var out = new DataOutputBuffer();
        get.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write_' + family + '_' + qualifier + '_' + maxVersions, 
          row, bytes);
      }
    });

  });

  describe('readFields()', function () {
    
    it('should convert bytes to Get', function () {
      cases.forEach(function (item) {
        var row = item[2];
        var family = item[0];
        var qualifier = item[1];
        var maxVersions = item[3];
        var filename = 'get/write_' + family + '_' + qualifier + '_' + maxVersions + '_' + row;
        var io = utils.createDataInputBuffer(filename);
        // var get = Get.call(null);
        var get = new Get();
        get.readFields(io);
        get.row.should.eql(new Buffer(row));
        get.familyMap.should.have.keys(family);
        get.familyMap.f.should.eql([new Buffer(qualifier)]);
        get.version.should.equal(2);
        get.maxVersions.should.equal(maxVersions);
        get.attributes.should.eql({});
        get.cacheBlocks.should.equal(true);
        should.not.exists(get.filter);
        get.hasFilter.should.equal(false);
      });
      
    });

  });

});
