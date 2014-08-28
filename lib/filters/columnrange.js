/**!
 * node-hbase-client - lib/filters/columnprefix.js
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

/**

 */
function ColumnRangeFilter(minColumn, maxColumn, minColumnInclusive, maxColumnInclusive) {
  this.minColumn = minColumn;
  this.maxColumn = maxColumn;
  this.minColumnInclusive = minColumnInclusive == null ? true : minColumnInclusive;
  this.maxColumnInclusive = maxColumnInclusive == null ? false : maxColumnInclusive;
}

ColumnRangeFilter.classname = 'org.apache.hadoop.hbase.filter.ColumnRangeFilter';

ColumnRangeFilter.prototype.write = function (out) {
  out.writeBoolean(this.minColumn == null);
  out.writeByte(this.minColumn.length);
  out.writeBytes(this.minColumn);
  out.writeBoolean(this.minColumnInclusive);

  out.writeBoolean(this.maxColumn == null);
  out.writeByte(this.maxColumn.length);
  out.writeBytes(this.maxColumn);
  out.writeBoolean(this.maxColumnInclusive);
};

ColumnRangeFilter.prototype.getClass = function () {
  return {getName: function() {return ColumnRangeFilter.classname;}};
};

ColumnRangeFilter.prototype.toString = function () {
  return 'ColumnRangeFilter(prefix: ' + this.prefix + ')';
};

module.exports = ColumnRangeFilter;
