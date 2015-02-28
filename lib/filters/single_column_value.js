/**!
 * node-hbase-client - lib/filters/single_column_value.js
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
var WritableUtils = require('../writable_utils');
var HbaseObjectWritable = require('../io/hbase_object_writable');
var BinaryComparator = require('./binary_comparator');

/**
 * This filter is used to filter cells based on value. It takes a {@link CompareFilter.CompareOp}
 * operator (equal, greater, not equal, etc), and either a byte [] value or
 * a WritableByteArrayComparable.
 * <p>
 * If we have a byte [] value then we just do a lexicographic compare. For
 * example, if passed value is 'b' and cell has 'a' and the compare operator
 * is LESS, then we will filter out this cell (return true).  If this is not
 * sufficient (eg you want to deserialize a long and then compare it to a fixed
 * long value), then you can pass in your own comparator instead.
 * <p>
 * You must also specify a family and qualifier.  Only the value of this column
 * will be tested. When using this filter on a {@link Scan} with specified
 * inputs, the column to be tested should also be added as input (otherwise
 * the filter will regard the column as missing).
 * <p>
 * To prevent the entire row from being emitted if the column is not found
 * on a row, use {@link #setFilterIfMissing}.
 * Otherwise, if the column is found, the entire row will be emitted only if
 * the value passes.  If the value fails, the row will be filtered out.
 * <p>
 * In order to test values of previous versions (timestamps), set
 * {@link #setLatestVersionOnly} to false. The default is true, meaning that
 * only the latest version's value is tested and all previous versions are ignored.
 * <p>
 * To filter based on the value of all scanned columns, use {@link ValueFilter}.
 */
function SingleColumnValueFilter(family, qualifier, compareOp, value) {
  if (!Buffer.isBuffer(family)) {
    family = new Buffer(family);
  }
  if (!Buffer.isBuffer(qualifier)) {
    qualifier = new Buffer(qualifier);
  }
  if (typeof value === "string") {
    value = new Buffer(value);
  }
  if (Buffer.isBuffer(value)) {
    value = new BinaryComparator(value);
  }
  this.family = family;
  this.qualifier = qualifier;
  this.compareOp = compareOp;
  this.comparator = value;

  this.foundColumn = false;
  this.matchedColumn = false;
  this.filterIfMissing = false;
  this.latestVersionOnly = true;
}

SingleColumnValueFilter.classname = 'org.apache.hadoop.hbase.filter.SingleColumnValueFilter';

SingleColumnValueFilter.prototype.write = function (out) {
  Bytes.writeByteArray(out, this.family);
  Bytes.writeByteArray(out, this.qualifier);
  out.writeUTF(this.compareOp);
  HbaseObjectWritable.writeObject(out, this.comparator, 'Writable.class');
  out.writeBoolean(this.foundColumn);
  out.writeBoolean(this.matchedColumn);
  out.writeBoolean(this.filterIfMissing);
  out.writeBoolean(this.latestVersionOnly);
};

SingleColumnValueFilter.prototype.getClass = function () {
  return {getName: function() {return SingleColumnValueFilter.classname;}};
};

SingleColumnValueFilter.prototype.toString = function () {
  return 'SingleColumnValueFilter(family: ' + this.family + ', qualifier: ' + this.qualifier + ', compareOp: ' + this.compareOp + ', comparator: ' + this.comparator + ')';
};

SingleColumnValueFilter.prototype.setFilterIfMissing = function (filterIfMissing) {
  this.filterIfMissing = filterIfMissing;
};

SingleColumnValueFilter.prototype.setLatestVersionOnly = function (latestVersionOnly) {
  this.latestVersionOnly = latestVersionOnly;
};

module.exports = SingleColumnValueFilter;
