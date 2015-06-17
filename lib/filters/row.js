/**!
 * node-hbase-client - lib/filters/row.js
 *
 *
 *
 * Authors:
 *   David Boyer <dave.github@yougeezer.co.uk>
 */

"use strict";

/**
 * Module dependencies.
 */
var HbaseObjectWritable = require('../io/hbase_object_writable');
var BinaryComparator = require('./binary_comparator');

/**
 * This filter is used to filter rows based on another filter. It takes a {@link CompareFilter.CompareOp}
 * operator (equal, greater, not equal, etc), and either a byte [] value or
 * a WritableByteArrayComparable.
 * <p>
 * If we have a byte [] value then we just do a lexicographic compare. For
 * example, if passed value is 'b' and cell has 'a' and the compare operator
 * is LESS, then we will filter out this cell (return true).  If this is not
 * sufficient (eg you want to deserialize a long and then compare it to a fixed
 * long value), then you can pass in your own comparator instead.
 */
function RowFilter(compareOp, value) {
  if (typeof value === "string") {
    value = new Buffer(value);
  }
  if (Buffer.isBuffer(value)) {
    value = new BinaryComparator(value);
  }
  this.compareOp = compareOp;
  this.comparator = value;
}

RowFilter.classname = 'org.apache.hadoop.hbase.filter.RowFilter';

RowFilter.prototype.write = function (out) {
  out.writeUTF(this.compareOp);
  HbaseObjectWritable.writeObject(out, this.comparator, 'Writable.class');
};

RowFilter.prototype.getClass = function () {
  return {getName: function() {return RowFilter.classname;}};
};

RowFilter.prototype.toString = function () {
  return 'RowFilter(compareOp: ' + this.compareOp + ', comparator: ' + this.comparator + ')';
};

module.exports = RowFilter;
