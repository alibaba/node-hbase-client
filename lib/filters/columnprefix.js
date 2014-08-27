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
function ColumnPrefixFilter(prefix) {
  this.prefix = prefix
}

ColumnPrefixFilter.classname = 'org.apache.hadoop.hbase.filter.ColumnPrefixFilter';

ColumnPrefixFilter.prototype.write = function (out) {
  out.writeBytes(this.prefix);
};

ColumnPrefixFilter.prototype.toString = function () {
  return 'ColumnPrefixFilter(prefix: ' + this.prefix + ')';
};

module.exports = ColumnPrefixFilter;
