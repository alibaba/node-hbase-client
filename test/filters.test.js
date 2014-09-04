/**!
 * node-hbase-client - test/filters.test.js
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
var filters = require('../').filters;
var DataOutputBuffer = require('../lib/data_output_buffer');
var utils = require('./support/utils');
var testJavaBytes = utils.createTestBytes('filters');

describe('test/filters.test.js', function () {
  describe('FilterList()', function () {
    describe('write()', function () {
      it('should convert FilterList to bytes', function () {
        var filterList = new filters.FilterList();
        filterList.addFilter(new filters.FirstKeyOnlyFilter());
        filterList.addFilter(new filters.KeyOnlyFilter());
        var out = new DataOutputBuffer();
        filterList.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write', 'FilterList', bytes);

        filterList.toString().should.equal('FilterList AND (2/2): FirstKeyOnlyFilter,KeyOnlyFilter(lenAsVal: false)');
      });
    });
  });
  describe('ColumnPrefixFilter()', function () {
    describe('write()', function () {
      it('should convert ColumnPrefixFilter to bytes', function () {
        var filter = new filters.ColumnPrefixFilter('abc');
        var out = new DataOutputBuffer();
        filter.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write', 'ColumnPrefixFilter', bytes);

        filter.toString().should.equal('ColumnPrefixFilter(prefix: abc)');
      });
    });
  });
  describe('ColumnRangeFilter()', function () {
    describe('write()', function () {
      it('should convert ColumnRangeFilter to bytes', function () {
        var filter = new filters.ColumnRangeFilter('a', true, 'b', true);
        var out = new DataOutputBuffer();
        filter.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write', 'ColumnRangeFilter', bytes);

        filter.toString().should.equal('ColumnRangeFilter(minColumn: a, minColumnInclusive: true, minColumn: b, minColumnInclusive: true)');
      });
    });
  });
});
