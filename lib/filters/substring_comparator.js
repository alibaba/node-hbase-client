/**!
 * node-hbase-client - lib/filters/substring_comparator.js
 *
 *
 *
 * Authors:
 *   Takayuki Hasegawa <takayuki.hasegawa@gree.net>
 */

"use strict";

/**
 * Module dependencies.
 */
var Bytes = require('../util/bytes');

/**
 * This comparator is for use with SingleColumnValueFilter, for filtering based on
 * the value of a given column. Use it to test if a given substring appears
 * in a cell value in the column. The comparison is case insensitive.
 * <p>
 * Only EQUAL or NOT_EQUAL tests are valid with this comparator.
 */
function SubstringComparator(substr) {
  if (!Buffer.isBuffer(substr)) {
    substr = new Buffer(substr.toLowerCase());
  }
  this.substr = substr;
}

SubstringComparator.classname = 'org.apache.hadoop.hbase.filter.SubstringComparator';

SubstringComparator.prototype.toString = function () {
  return 'SubstringComparator(substr: ' + this.substr + ')';
};

SubstringComparator.prototype.write = function (out) {
  out.writeUTF(this.substr);
};

module.exports = SubstringComparator;
