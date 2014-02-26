/**!
 * node-hbase-client - lib/scan.js
 *
 * Copyright(c) 2013 - 2014 Alibaba Group Holding Limited.
 *
 * Authors:
 *   苏千 <suqian.yf@taobao.com> (http://fengmk2.github.com)
 */

"use strict";

/**
 * Module dependencies.
 */

var util = require('util');
var OperationWithAttributes = require('./operation_with_attributes');
var Bytes = require('./util/bytes');
var HConstants = require('./hconstants');
var TimeRange = require('./time_range');

var RAW_ATTR = "_raw_";
var ISOLATION_LEVEL = "_isolationlevel_";
// 0.94.16 SCAN_VERSION = (byte)2, but inside alibaba SCAN_VERSION = (byte) 6;
var SCAN_VERSION = 2;
// If application wants to collect scan metrics, it needs to
// call scan.setAttribute(SCAN_ATTRIBUTES_ENABLE, Bytes.toBytes(Boolean.TRUE))
var SCAN_ATTRIBUTES_METRICS_ENABLE = "scan.attributes.metrics.enable";
var SCAN_ATTRIBUTES_METRICS_DATA = "scan.attributes.metrics.data";

/**
 * Create a Scan operation for the range of rows specified.
 *
 * @param [startRow] row to start scanner at or after (inclusive), defautl is `EMPTY_START_ROW`
 * @param [stopRow] row to stop scanner before (exclusive), defautl is `EMPTY_START_ROW`
 */
function Scan(startRow, stopRow) {
  OperationWithAttributes.call(this);

  this.startRow = startRow || HConstants.EMPTY_START_ROW;
  this.stopRow = stopRow || HConstants.EMPTY_END_ROW;
  if (!Buffer.isBuffer(this.startRow)) {
    this.startRow = Bytes.toBytes(this.startRow);
  }
  if (!Buffer.isBuffer(this.stopRow)) {
    this.stopRow = Bytes.toBytes(this.stopRow);
  }

  this.maxVersions = 1;
  this.batch = -1;

  /*
   * -1 means no caching
   */
  this.caching = -1;
  this.maxResultSize = -1;
  this.cacheBlocks = true;
  this.filter = null;
  this.tr = new TimeRange();
  this.familyMap = {};
}

util.inherits(Scan, OperationWithAttributes);

Scan.prototype.getRow = function () {
  return this.startRow;
};

/**
 * Get all columns from the specified family.
 * <p>
 * Overrides previous calls to addColumn for this family.
 * @param family family name
 * @return this
 */
Scan.prototype.addFamily = function (family) {
  this.familyMap[family] = null;
  return this;
};

/**
 * Get the column from the specified family with the specified qualifier.
 * <p>
 * Overrides previous calls to addFamily for this family.
 * @param family family name
 * @param qualifier column qualifier
 * @return this
 */
Scan.prototype.addColumn = function (family, qualifier) {
  var set = this.familyMap[family];
  if (!set) {
    this.familyMap[family] = set = [];
  }
  set.push(qualifier);
  return this;
};

/**
 * Get versions of columns only within the specified timestamp range,
 * [minStamp, maxStamp).  Note, default maximum versions to return is 1.  If
 * your time range spans more than one version and you want all versions
 * returned, up the number of versions beyond the defaut.
 * @param minStamp minimum timestamp value, inclusive
 * @param maxStamp maximum timestamp value, exclusive
 * @throws IOException if invalid time range
 * @see #setMaxVersions()
 * @see #setMaxVersions(int)
 * @return this
 */
Scan.prototype.setTimeRange = function (minStamp, maxStamp) {
  this.tr = new TimeRange(minStamp, maxStamp);
  return this;
};

/**
 * Get versions of columns with the specified timestamp. Note, default maximum
 * versions to return is 1.  If your time range spans more than one version
 * and you want all versions returned, up the number of versions beyond the
 * defaut.
 * @param timestamp version timestamp
 * @see #setMaxVersions()
 * @see #setMaxVersions(int)
 * @return this
 */
Scan.prototype.setTimeStamp = function (timestamp) {
  this.tr = new TimeRange(timestamp, timestamp + 1);
  return this;
};

/**
 * Apply the specified server-side filter when performing the Scan.
 * @param filter filter to run on the server
 * @return this
 */
Scan.prototype.setFilter = function (filter) {
  this.filter = filter;
  return this;
};

Scan.prototype.write = function (out) {
  out.writeByte(SCAN_VERSION);
  Bytes.writeByteArray(out, this.startRow);
  Bytes.writeByteArray(out, this.stopRow);
  out.writeInt(this.maxVersions);
  out.writeInt(this.batch);
  out.writeInt(this.caching);
  out.writeBoolean(this.cacheBlocks);
  if (!this.filter) {
    out.writeBoolean(false);
  } else {
    out.writeBoolean(true);
    Bytes.writeByteArray(out, Bytes.toBytes(this.filter.constructor.classname));
    this.filter.write(out);
  }
  this.tr.write(out);
  out.writeInt(Object.keys(this.familyMap).length);
  for (var family in this.familyMap) {
    Bytes.writeByteArray(out, Bytes.toBytes(family));
    var columnSet = this.familyMap[family];
    if (columnSet && columnSet.length > 0) {
      out.writeInt(columnSet.length);
      for (var i = 0; i < columnSet.length; i++) {
        var qualifier = columnSet[i];
        Bytes.writeByteArray(out, Bytes.toBytes(qualifier));
      }
    } else {
      out.writeInt(0);
    }
  }
  this.writeAttributes(out);
  out.writeLong(this.maxResultSize);
};


module.exports = Scan;
