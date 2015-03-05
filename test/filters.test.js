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
  describe('BinaryComparator()', function () {
    describe('write()', function () {
      it ('should convert BinaryComparator to bytes', function () {
        var comparator = new filters.BinaryComparator('abc');
        var out = new DataOutputBuffer();
        comparator.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write', 'BinaryComparator', bytes);

        comparator.toString().should.equal('BinaryComparator(value: abc)');
      });
    });
  });
  describe('BinaryPrefixComparator()', function () {
    describe('write()', function () {
      it ('should convert BinaryPrefixComparator to bytes', function () {
        var comparator = new filters.BinaryPrefixComparator('abc');
        var out = new DataOutputBuffer();
        comparator.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write', 'BinaryPrefixComparator', bytes);

        comparator.toString().should.equal('BinaryPrefixComparator(value: abc)');
      });
    });
  });
  describe('NullComparator()', function () {
    describe('write()', function () {
      it ('should convert NullComparator to bytes', function () {
        var comparator = new filters.NullComparator();
        var out = new DataOutputBuffer();
        comparator.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write', 'NullComparator', bytes);

        comparator.toString().should.equal('NullComparator');
      });
    });
  });
  describe('BitComparator()', function () {
    describe('write()', function () {
      it ('should convert BitComparator to bytes', function () {
        var comparator = new filters.BitComparator('0', filters.BitComparator.BitwiseOp.AND);
        var out = new DataOutputBuffer();
        comparator.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write', 'BitComparator', bytes);

        comparator.toString().should.equal('BitComparator(value: 0, bitOperator: AND)');
      });
    });
  });
  describe('RegexStringComparator()', function () {
    describe('write()', function () {
      it ('should convert RegexStringComparator to bytes', function () {
        var comparator = new filters.RegexStringComparator('ab*');
        var out = new DataOutputBuffer();
        comparator.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write', 'RegexStringComparator', bytes);

        comparator.toString().should.equal('RegexStringComparator(pattern: ab*, charset: UTF-8)');
      });
    });
  });
  describe('SubstringComparator()', function () {
    describe('write()', function () {
      it ('should convert SubstringComparator to bytes', function () {
        var comparator = new filters.SubstringComparator('ABC');
        var out = new DataOutputBuffer();
        comparator.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write', 'SubstringComparator', bytes);

        comparator.toString().should.equal('SubstringComparator(substr: abc)');
      });
    });
  });
  describe('SingleColumnValueFilter()', function () {
    describe('write()', function () {
      it('should convert SingleColumnValueFilter to bytes', function () {
        var filter = new filters.SingleColumnValueFilter('family', 'qualifier', 'EQUAL', 'value');
        var out = new DataOutputBuffer();
        filter.write(out);
        var bytes = out.getData();
        bytes.length.should.above(0);
        testJavaBytes('write', 'SingleColumnValueFilter', bytes);

        filter.toString().should.equal('SingleColumnValueFilter(family: family, qualifier: qualifier, compareOp: EQUAL, comparator: BinaryComparator(value: value))');
      });
    });
  });
});
