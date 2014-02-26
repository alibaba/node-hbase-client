/**!
 * node-hbase-client - lib/filters/first_keyonly.js
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
 * A filter that will only return the first KV from each row.
 * <p>
 * This filter can be used to more efficiently perform row count operations.
 */
function FirstKeyOnlyFilter() {

}

FirstKeyOnlyFilter.classname = 'org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter';

FirstKeyOnlyFilter.prototype.write = function (out) {};

FirstKeyOnlyFilter.prototype.toString = function () {
  return 'FirstKeyOnlyFilter';
};

module.exports = FirstKeyOnlyFilter;
