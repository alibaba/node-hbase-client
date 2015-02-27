/**!
 * node-hbase-client - lib/filters/null_comparator.js
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
 * A binary comparator which lexicographically compares against the specified
 * byte array using {@link org.apache.hadoop.hbase.util.Bytes#compareTo(byte[], byte[])}.
 */
function NullComparator() {
  this.value = new Buffer(0);
}

NullComparator.classname = 'org.apache.hadoop.hbase.filter.NullComparator';

NullComparator.prototype.toString = function () {
  return 'NullComparator';
};

NullComparator.prototype.write = function (out) {
  Bytes.writeByteArray(out, 0);
};

module.exports = NullComparator;
