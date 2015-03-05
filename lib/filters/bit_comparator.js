/**!
 * node-hbase-client - lib/filters/bit_comparator.js
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
 * A bit comparator which performs the specified bitwise operation on each of the bytes
 * with the specified byte array. Then returns whether the result is non-zero.
 */
function BitComparator(value, bitOperator) {
  if (!Buffer.isBuffer(value)) {
    value = new Buffer(value);
  }
  this.value = value;
  this.bitOperator = bitOperator;
}

BitComparator.BitwiseOp = {
  AND: 'AND',
  OR: 'OR',
  XOR: 'XOR'
};

BitComparator.classname = 'org.apache.hadoop.hbase.filter.BitComparator';

BitComparator.prototype.toString = function () {
  return 'BitComparator(value: ' + this.value + ', bitOperator: ' + this.bitOperator + ')';
};

BitComparator.prototype.write = function (out) {
  Bytes.writeByteArray(out, this.value);
  out.writeUTF(this.bitOperator);
};

module.exports = BitComparator;
