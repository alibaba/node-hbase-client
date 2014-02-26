/**!
 * node-hbase-client - lib/filters/keyonly.js
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
 * A filter that will only return the key component of each KV (the value will
 * be rewritten as empty).
 * <p>
 * This filter can be used to grab all of the keys without having to also grab
 * the values.
 */
function KeyOnlyFilter(lenAsVal) {
  this.lenAsVal = !!lenAsVal;
}

KeyOnlyFilter.classname = 'org.apache.hadoop.hbase.filter.KeyOnlyFilter';

KeyOnlyFilter.prototype.write = function (out) {
  out.writeBoolean(this.lenAsVal);
};

KeyOnlyFilter.prototype.toString = function () {
  return 'KeyOnlyFilter(lenAsVal: ' + this.lenAsVal + ')';
};

module.exports = KeyOnlyFilter;
