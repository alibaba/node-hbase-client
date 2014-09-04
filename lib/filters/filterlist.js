/**!
 * node-hbase-client - lib/filters/filterlist.js
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

var util = require('util');
var HbaseObjectWritable = require('../io/hbase_object_writable');

/**
 * Implementation of {@link Filter} that represents an ordered List of Filters
 * which will be evaluated with a specified boolean operator {@link Operator#MUST_PASS_ALL}
 * (<code>AND</code>) or {@link Operator#MUST_PASS_ONE} (<code>OR</code>).
 * Since you can use Filter Lists as children of Filter Lists, you can create a
 * hierarchy of filters to be evaluated.
 *
 * <br/>
 * {@link Operator#MUST_PASS_ALL} evaluates lazily: evaluation stops as soon as one filter does
 * not include the KeyValue.
 *
 * <br/>
 * {@link Operator#MUST_PASS_ONE} evaluates non-lazily: all filters are always evaluated.
 *
 * <br/>
 * Defaults to {@link Operator#MUST_PASS_ALL}.
 * <p>TODO: Fix creation of Configuration on serialization and deserialization.
 */

function FilterList(options) {
  options = options || {};
  this.operator = options.operator || FilterList.Operator.MUST_PASS_ALL;
  this.filters = [];
}

FilterList.MAX_LOG_FILTERS = 5;

FilterList.Operator = {
  MUST_PASS_ALL: 0,
  MUST_PASS_ONE: 1
};

FilterList.classname = 'org.apache.hadoop.hbase.filter.FilterList';

FilterList.prototype.getClass = function () {
  return {getName: function() {return FilterList.classname;}};
};

var proto = FilterList.prototype;

proto.write = function (out) {
  out.writeByte(this.operator);
  out.writeInt(this.filters.length);
  for (var i = 0; i < this.filters.length; i++) {
    // org.apache.hadoop.hbase.io.Writable
    HbaseObjectWritable.writeObject(out, this.filters[i], 'Writable.class');
  }
};

proto.addFilter = function (filter) {
  this.filters.push(filter);
};

proto.toString = function () {
  var endIndex = this.filters.length;
  if (endIndex > FilterList.MAX_LOG_FILTERS) {
    endIndex = FilterList.MAX_LOG_FILTERS;
  }
  return util.format('%s %s (%d/%d): %s',
    this.constructor.name,
    this.operator === FilterList.Operator.MUST_PASS_ALL ? 'AND' : 'OR',
    endIndex,
    this.filters.length,
    this.filters.slice(0, endIndex).toString()
  );
};

module.exports = FilterList;
