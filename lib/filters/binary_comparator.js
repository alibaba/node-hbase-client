/**!
 * node-hbase-client - lib/filters/binary_comparator.js
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
function BinaryComparator(value) {
  if (!Buffer.isBuffer(value)) {
    value = new Buffer(value);
  }
  this.value = value;
}

BinaryComparator.classname = 'org.apache.hadoop.hbase.filter.BinaryComparator';

BinaryComparator.prototype.toString = function () {
  return 'BinaryComparator(value: ' + this.value + ')';
};

BinaryComparator.prototype.write = function (out) {
  Bytes.writeByteArray(out, this.value);
};

module.exports = BinaryComparator;
