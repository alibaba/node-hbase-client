/**!
 * node-hbase-client - lib/filters/binary_prefix_comparator.js
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
 * A comparator which compares against a specified byte array, but only compares
 * up to the length of this byte array. For the rest it is similar to
 * {@link BinaryComparator}.
 */
function BinaryPrefixComparator(value) {
  if (!Buffer.isBuffer(value)) {
    value = new Buffer(value);
  }
  this.value = value;
}

BinaryPrefixComparator.classname = 'org.apache.hadoop.hbase.filter.BinaryPrefixComparator';

BinaryPrefixComparator.prototype.toString = function () {
  return 'BinaryPrefixComparator(value: ' + this.value + ')';
};

BinaryPrefixComparator.prototype.write = function (out) {
  Bytes.writeByteArray(out, this.value);
};

module.exports = BinaryPrefixComparator;
