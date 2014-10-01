/**!
 * node-hbase-client - lib/filters/columnprange.js
 *
 *
 *
 * Authors:
 *   Martin Cizek <martin.cizek@socialbakers.com> (http://github.com/wision)
 */

"use strict";

/**
 * Module dependencies.
 */

/**

 */
function ColumnRangeFilter(minColumn, minColumnInclusive, maxColumn, maxColumnInclusive) {
  minColumn = minColumn === undefined ? '' : minColumn;
  maxColumn = maxColumn === undefined ? '' : maxColumn;

  if (!Buffer.isBuffer(minColumn)) {
    minColumn = new Buffer(minColumn);
  }
  if (!Buffer.isBuffer(maxColumn)) {
    maxColumn = new Buffer(maxColumn);
  }

  this.minColumn = minColumn;
  this.maxColumn = maxColumn;
  this.minColumnInclusive = minColumnInclusive === null ? true : minColumnInclusive;
  this.maxColumnInclusive = maxColumnInclusive === null ? false : maxColumnInclusive;
}

ColumnRangeFilter.classname = 'org.apache.hadoop.hbase.filter.ColumnRangeFilter';

ColumnRangeFilter.prototype.write = function (out) {
  out.writeBoolean(this.minColumn === null);
  if (this.minColumn) {
    out.writeByte(this.minColumn.length);
    out.writeBytes(this.minColumn);
  } else {
    out.writeByte(0);
  }
  out.writeBoolean(this.minColumnInclusive);

  out.writeBoolean(this.maxColumn === null);
  if (this.maxColumn) {
    out.writeByte(this.maxColumn.length);
    out.writeBytes(this.maxColumn);
  } else {
    out.writeByte(0);
  }
  out.writeBoolean(this.maxColumnInclusive);
};

ColumnRangeFilter.prototype.getClass = function () {
  return {getName: function() {return ColumnRangeFilter.classname;}};
};

ColumnRangeFilter.prototype.toString = function () {
  return 'ColumnRangeFilter(minColumn: ' + this.minColumn+ ', minColumnInclusive: ' + this.minColumnInclusive + ', minColumn: ' + this.maxColumn + ', minColumnInclusive: ' + this.maxColumnInclusive + ')';
};

module.exports = ColumnRangeFilter;
